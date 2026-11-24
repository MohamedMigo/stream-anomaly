"""Real-time stream processing in pure Python: event-time tumbling windows with a watermark
(late-event handling), per-key aggregation, and online anomaly detection (EWMA z-score).
Same concepts as Kafka Streams / Flink, small enough to read in one sitting.
"""
from __future__ import annotations

import math
import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Iterator


@dataclass(frozen=True)
class Event:
    ts: float      # event time, seconds
    key: str
    value: float


@dataclass
class WindowResult:
    key: str
    start: float
    end: float
    count: int
    mean: float
    maximum: float
    anomaly: bool = False
    z: float = 0.0


class EWMADetector:
    """Online mean/variance with exponential decay. Flags values far from the recent norm."""

    def __init__(self, alpha: float = 0.1, threshold: float = 3.5, warmup: int = 10):
        self.alpha, self.threshold, self.warmup = alpha, threshold, warmup
        self.mean = 0.0; self.var = 0.0; self.n = 0

    def update(self, x: float) -> tuple[bool, float]:
        z = 0.0
        if self.n >= self.warmup:
            z = (x - self.mean) / (math.sqrt(self.var) + 1e-9)
        flagged = abs(z) > self.threshold
        if not flagged:  # do not let outliers poison the baseline
            if self.n == 0:
                self.mean = x
            else:
                d = x - self.mean
                self.mean += self.alpha * d
                self.var = (1 - self.alpha) * (self.var + self.alpha * d * d)
        self.n += 1
        return flagged, z


class WindowedProcessor:
    def __init__(self, window: float = 10.0, allowed_lateness: float = 5.0, **det):
        self.window, self.lateness, self.det_args = window, allowed_lateness, det
        self.open: dict[tuple[str, float], list[float]] = defaultdict(list)
        self.detectors: dict[str, EWMADetector] = {}
        self.watermark = -math.inf
        self.dropped_late = 0

    def _start(self, ts: float) -> float:
        return math.floor(ts / self.window) * self.window

    def process(self, ev: Event) -> Iterator[WindowResult]:
        start = self._start(ev.ts)
        if start + self.window <= self.watermark:  # window already closed
            self.dropped_late += 1
            return
        self.open[(ev.key, start)].append(ev.value)
        self.watermark = max(self.watermark, ev.ts - self.lateness)
        yield from self._close_ready()

    def _close_ready(self) -> Iterator[WindowResult]:
        for (key, start) in sorted([k for k in self.open if k[1] + self.window <= self.watermark], key=lambda k: k[1]):
            vals = self.open.pop((key, start))
            mean = sum(vals) / len(vals)
            det = self.detectors.setdefault(key, EWMADetector(**self.det_args))
            flagged, z = det.update(mean)
            yield WindowResult(key, start, start + self.window, len(vals), round(mean, 3), max(vals), flagged, round(z, 2))

    def flush(self) -> Iterator[WindowResult]:
        self.watermark = math.inf
        yield from self._close_ready()

    def run(self, events: Iterable[Event]) -> list[WindowResult]:
        out = []
        for e in events:
            out.extend(self.process(e))
        out.extend(self.flush())
        return out


def simulate(n: int = 3000, seed: int = 1, spike_at: float = 700.0) -> list[Event]:
    """Latency metrics for 2 services; 'api' spikes around spike_at. Some events arrive out of order."""
    r = random.Random(seed)
    evs = []
    for i in range(n):
        t = i * 0.5
        for key, base in (("api", 120), ("db", 40)):
            v = r.gauss(base, base * 0.08)
            if key == "api" and spike_at <= t < spike_at + 30:
                v *= 3
            evs.append(Event(t + r.uniform(0, .4), key, v))
    # shuffle slightly to mimic network reordering
    for i in range(0, len(evs) - 6, 3):
        j = i + r.randint(1, 5)
        evs[i], evs[j] = evs[j], evs[i]
    return evs


if __name__ == "__main__":
    p = WindowedProcessor(window=10, allowed_lateness=5)
    res = p.run(simulate())
    print(f"{len(res)} windows, late dropped: {p.dropped_late}")
    for w in res:
        if w.anomaly:
            print("ANOMALY", w)
