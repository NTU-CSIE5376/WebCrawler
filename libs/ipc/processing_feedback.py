"""Bounded measured processing feedback; no site-policy or business-score fields."""
from collections import deque
import math
import statistics
import time


class ProcessingFeedback:
    def __init__(self, window_sec=120, max_domains=2000, max_samples=512):
        self.window_sec = window_sec
        self.max_domains = max_domains
        self.max_samples = max_samples
        self.domains = {}

    def _entry(self, domain_id, now):
        if domain_id not in self.domains:
            if len(self.domains) >= self.max_domains:
                oldest = min(self.domains, key=lambda key: self.domains[key]['touched'])
                del self.domains[oldest]
            self.domains[domain_id] = dict(samples=deque(maxlen=self.max_samples),
                                          active=deque(maxlen=self.max_samples), touched=now)
        entry = self.domains[domain_id]
        entry['touched'] = now
        return entry

    def admitted(self, domain_id, active, *, now=None):
        now = time.monotonic() if now is None else now
        self._entry(domain_id, now)['active'].append((now, active))

    def completed(self, domain_id, latency_sec, *, now=None):
        if not math.isfinite(latency_sec) or latency_sec <= 0:
            return
        now = time.monotonic() if now is None else now
        self._entry(domain_id, now)['samples'].append((now, latency_sec))

    def snapshot(self, *, now=None, wall_time=None):
        now = time.monotonic() if now is None else now
        result = {}
        for domain_id, entry in list(self.domains.items()):
            if now - entry['touched'] > self.window_sec:
                del self.domains[domain_id]
                continue
            for key in ('samples', 'active'):
                queue = entry[key]
                while queue and queue[0][0] < now - self.window_sec:
                    queue.popleft()
            samples = [value for _, value in entry['samples']]
            active = max((value for _, value in entry['active']), default=0)
            if samples and active:
                latency = statistics.mean(samples)
                result[str(domain_id)] = dict(samples=len(samples), service_latency_sec=latency,
                                              service_latency_p90_sec=sorted(samples)[min(len(samples)-1,int(.9*len(samples)))],
                                              observed_parallelism=active,
                                              estimated_completion_rate=active/latency)
        return dict(version=1, generated_at=time.time() if wall_time is None else wall_time,
                    window_sec=self.window_sec, domains=result)
