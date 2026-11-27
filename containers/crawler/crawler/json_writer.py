"""Ordered process writes with bounded batches and per-record acknowledgements."""
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
import math
import multiprocessing
import os
import pickle

from twisted.internet.defer import Deferred
from twisted.internet.threads import deferToThread
from twisted.python.failure import Failure

from crawler.worker_lifecycle import initialize_worker
from libs.ipc.jsonio import append_jsonl


def append_records(payloads):
    """Keep successes and errors separate when part of a batch fails."""
    outcomes = []
    for payload in payloads:
        try:
            entry = pickle.loads(payload)
            path, record = entry[:2]
            if len(entry) == 3 and entry[2]:
                append_jsonl(path, record, sync=True)
            else:
                append_jsonl(path, record)
        except Exception as error:
            outcomes.append(error)
        else:
            outcomes.append(None)
    return outcomes


class JsonWriterPool:
    MAX_INFLIGHT_BATCHES = 2

    def __init__(self, *, batch_size=8, batch_bytes=4 * 1024**2,
                 flush_interval=0.05, on_broken=None, stats=None):
        if batch_size <= 0 or batch_bytes <= 0 or not math.isfinite(flush_interval) or flush_interval < 0:
            raise ValueError("Writer batch limits must be positive and flush interval finite/non-negative")
        from twisted.internet import reactor
        self._reactor = reactor
        self.batch_size = batch_size
        self.batch_bytes = batch_bytes
        self.flush_interval = flush_interval
        self.stats = stats
        self._on_broken = on_broken
        self._broken = None
        # Waiting entries retain original records under Scrapy backpressure;
        # only the batch builder and one lookahead record are encoded here.
        self._waiting = deque()
        self._batch = []
        self._batch_bytes = 0
        self._inflight = 0
        self._inflight_bytes = 0
        self._oversized = False
        self._timer = None
        self._flush_due = False
        self._pumping = False
        self._closing = False
        self._closed = None
        self._shutdown_started = False
        self._executor = ProcessPoolExecutor(
            max_workers=1, mp_context=multiprocessing.get_context("spawn"),
            initializer=initialize_worker, initargs=(os.getpid(),),
        )

    def write(self, path, record, *, sync=False):
        if self._closing:
            raise RuntimeError("JSON writer is closing")
        if self._broken is not None:
            raise self._broken
        result = Deferred()
        self._waiting.append([str(path), record, result, None, sync])
        self._pump()
        return result

    def _cancel_timer(self):
        if self._timer is not None:
            if self._timer.active():
                self._timer.cancel()
            self._timer = None

    def _timer_fired(self):
        self._timer = None
        self._flush_due = True
        self._pump()

    def _mark_broken(self, error):
        if self._broken is None:
            self._broken = error
            if self._on_broken is not None:
                self._on_broken()

    @staticmethod
    def _respond(entry, error):
        result = entry[2]
        if not result.called:
            if error is None:
                result.callback(None)
            else:
                result.errback(Failure(error))

    def _pump(self):
        if self._pumping:
            return
        self._pumping = True
        try:
            while True:
                if self._broken is not None:
                    self._cancel_timer()
                    entries = self._batch + list(self._waiting)
                    self._batch = []
                    self._batch_bytes = 0
                    self._waiting.clear()
                    for entry in entries:
                        self._respond(entry, self._broken)
                    break
                if self._oversized or self._inflight >= self.MAX_INFLIGHT_BATCHES:
                    break
                if self._batch and ((self._closing and not self._waiting) or self._flush_due
                                    or len(self._batch) >= self.batch_size
                                    or self._batch_bytes >= self.batch_bytes):
                    self._submit_batch()
                    continue
                if not self._waiting:
                    break
                entry = self._waiting[0]
                if entry[3] is None:
                    try:
                        # Encode once: exact transport-byte accounting without
                        # walking the same record again in the feeder thread.
                        entry[3] = pickle.dumps((entry[0], entry[1], True) if entry[4] else (entry[0], entry[1]),
                                                protocol=pickle.HIGHEST_PROTOCOL)
                    except Exception as error:
                        self._waiting.popleft()
                        self._respond(entry, error)
                        continue
                size = len(entry[3])
                if self._batch and self._batch_bytes + size > self.batch_bytes:
                    self._submit_batch()
                    continue
                if size > self.batch_bytes:
                    # Preserve existing large records, without combining them
                    # or running another batch alongside the oversized write.
                    if self._inflight:
                        break
                    self._waiting.popleft()
                    self._batch = [entry]
                    self._batch_bytes = size
                    self._submit_batch(oversized=True)
                    continue
                self._waiting.popleft()
                self._batch.append(entry)
                self._batch_bytes += size
                if self._timer is None:
                    self._flush_due = False
                    self._timer = self._reactor.callLater(self.flush_interval, self._timer_fired)
        finally:
            self._pumping = False
            self._update_stats()
            self._check_closed()

    def _submit_batch(self, *, oversized=False):
        self._cancel_timer()
        self._flush_due = False
        batch, size = self._batch, self._batch_bytes
        self._batch, self._batch_bytes = [], 0
        self._inflight += 1
        self._inflight_bytes += size
        self._oversized = oversized
        if self.stats is not None:
            self.stats.inc_value("json_writer/batches")
            if oversized:
                self.stats.inc_value("json_writer/oversized_records")
        try:
            future = self._executor.submit(append_records, [entry[3] for entry in batch])
        except BaseException as error:
            self._complete(batch, size, oversized, error=error)
            return

        def completed(done):
            try:
                outcomes = done.result()
                if (not isinstance(outcomes, list) or len(outcomes) != len(batch)
                        or any(x is not None and not isinstance(x, BaseException) for x in outcomes)):
                    raise BrokenProcessPool("Invalid JSON writer acknowledgement")
            except BaseException as error:
                self._reactor.callFromThread(self._complete, batch, size, oversized, error=error)
            else:
                self._reactor.callFromThread(self._complete, batch, size, oversized, outcomes=outcomes)

        # Do not capture the Future inside its own callback: that would retain
        # large records in a reference cycle after their writes have finished.
        future.add_done_callback(completed)

    def _complete(self, batch, size, oversized, *, outcomes=None, error=None):
        pumping = self._pumping
        self._pumping = True
        try:
            self._inflight -= 1
            self._inflight_bytes -= size
            if oversized:
                self._oversized = False
            if isinstance(error, BrokenProcessPool):
                self._mark_broken(error)
            if error is not None:
                outcomes = [error] * len(batch)
            for entry, outcome in zip(batch, outcomes):
                self._respond(entry, outcome)
        finally:
            self._pumping = pumping
        if not pumping:
            self._pump()

    def _update_stats(self):
        if self.stats is not None:
            lookahead = (len(self._waiting[0][3]) if self._waiting and self._waiting[0][3] is not None else 0)
            self.stats.set_value("json_writer/inflight_batches", self._inflight)
            self.stats.set_value("json_writer/buffered_bytes", self._inflight_bytes + self._batch_bytes + lookahead)

    def close(self):
        if self._closed is None:
            self._closed = Deferred()
            self._closing = True
            self._cancel_timer()
            self._pump()
        return self._closed

    def _check_closed(self):
        if (self._closing and not self._waiting and not self._batch and not self._inflight
                and not self._shutdown_started):
            self._shutdown_started = True
            deferToThread(self._executor.shutdown, wait=True).chainDeferred(self._closed)
