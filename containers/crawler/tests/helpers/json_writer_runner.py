"""Exercise real spawned output workers under Scrapy's asyncio reactor."""
import asyncio
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone
import json
import multiprocessing
import os
from pathlib import Path
import signal
import sys
import time
from types import SimpleNamespace


def slow_batch(payloads, parent_pid):
    from crawler.json_writer import append_records
    assert os.getpid() != parent_pid
    time.sleep(0.01)
    return append_records(payloads)


async def run(root):
    from scrapy.utils.defer import maybe_deferred_to_future
    from crawler import pipelines, json_writer

    class FrozenDatetime:
        @staticmethod
        def now(tz):
            return datetime(2026, 11, 21, 6, 20, tzinfo=timezone.utc)

    pipelines.datetime = FrozenDatetime
    pipelines.current_interval = lambda interval: ("20261121", "0620")
    spider = SimpleNamespace(crawler_id=0)
    items = [{"url": f"https://example.test/{i}", "domain": "example.test",
              "content": "<title>測試 café 😀</title>" * 100,
              "outlinks": [{"url": "https://other.test", "domain": "other.test", "anchor": "你好"}],
              "title": "測試", "hreflang_count": 2, "has_json_ld": True}
             for i in range(12)]
    items.append({"url": "https://example.test/fail", "fail_reason": "HTTP 404"})
    inline = pipelines.JsonPipeline(str(root / "inline"), 10)
    inline.open_spider(spider)
    for item in items:
        assert inline.process_item(item, spider) is item
    inline.close_spider(spider)
    assert spider.items_written == len(items) and spider.items_ok == len(items) - 1
    spider.items_written = spider.items_ok = 0

    # The child task itself is top-level/picklable, with a slow append allowing
    # the reactor responsiveness assertion to be independent of machine speed.
    original_submit = json_writer.ProcessPoolExecutor

    def create_executor(**kwargs):
        executor = original_submit(**kwargs)
        submit = executor.submit

        def wrapped(fn, payloads):
            assert fn is json_writer.append_records
            return submit(slow_batch, payloads, os.getpid())

        executor.submit = wrapped
        return executor

    json_writer.ProcessPoolExecutor = create_executor
    writer = pipelines.JsonPipeline(str(root / "process"), 10, writer_processes=1)
    writer.open_spider(spider)
    running = True
    ticks = 0

    async def tick():
        nonlocal ticks
        while running:
            ticks += 1
            await asyncio.sleep(0.002)

    ticker = asyncio.create_task(tick())
    try:
        writes = [writer.process_item(item, spider) for item in items]
        assert writer._pool._inflight <= 2
        closed = writer.close_spider(spider)
        results = await asyncio.gather(*(maybe_deferred_to_future(d) for d in writes))
        assert results == items
        await maybe_deferred_to_future(closed)
    finally:
        running = False
        await ticker
    expected = next((root / "inline").rglob("*.jsonl")).read_bytes()
    actual = next((root / "process").rglob("*.jsonl")).read_bytes()
    assert actual == expected, "JSONL bytes, timestamps, schema, or record order changed"
    assert len(actual.splitlines()) == len(items)
    assert writer._pending == 0 and ticks >= 4
    assert spider.items_written == len(items) and spider.items_ok == len(items) - 1
    assert not multiprocessing.active_children()

    broken = []
    faulty = pipelines.JsonPipeline(str(root / "failed"), 10, writer_processes=1,
                                    on_broken=lambda: broken.append(True))
    faulty.open_spider(spider)
    await maybe_deferred_to_future(faulty.process_item(items[0], spider))
    pid = next(iter(faulty._pool._executor._processes))
    os.kill(pid, signal.SIGKILL)
    try:
        await maybe_deferred_to_future(faulty.process_item(items[1], spider))
    except BrokenProcessPool:
        pass
    else:
        raise AssertionError("Writer child death was ignored")
    assert broken == [True]
    await maybe_deferred_to_future(faulty.close_spider(spider))
    assert not multiprocessing.active_children()
    print(json.dumps({"records": len(items), "reactor_ticks": ticks, "child_death_detected": True}))


if __name__ == "__main__":
    from scrapy.utils.reactor import install_reactor
    install_reactor("twisted.internet.asyncioreactor.AsyncioSelectorReactor")
    from twisted.internet.task import react
    from scrapy.utils.defer import deferred_from_coro
    react(lambda reactor: deferred_from_coro(run(Path(sys.argv[1]))))
