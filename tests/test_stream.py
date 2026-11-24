from stream.engine import EWMADetector, Event, WindowedProcessor, simulate


def test_windows_close_on_watermark_and_aggregate():
    p = WindowedProcessor(window=10, allowed_lateness=5)
    out = []
    for e in [Event(1, "a", 10), Event(5, "a", 20), Event(9.5, "a", 30), Event(13, "a", 1)]:
        out += list(p.process(e))
    assert out == []                         # watermark 8 < window end 10
    out += list(p.process(Event(15, "a", 1)))  # watermark 10 >= 10 -> first window closes
    assert out[0].count == 3 and out[0].mean == 20 and out[0].start == 0


def test_late_events_dropped():
    p = WindowedProcessor(window=10, allowed_lateness=0)
    list(p.process(Event(25, "a", 1)))
    list(p.process(Event(3, "a", 1)))
    assert p.dropped_late == 1


def test_detector_flags_spike_only():
    d = EWMADetector(warmup=5)
    for x in [10, 10.2, 9.8, 10.1, 9.9, 10, 10.1, 9.9]:
        assert not d.update(x)[0]
    assert d.update(40)[0]


def test_simulation_finds_the_api_spike():
    res = WindowedProcessor(window=10, allowed_lateness=5).run(simulate())
    flagged = [w for w in res if w.anomaly]
    assert flagged and all(w.key == "api" for w in flagged)
    assert all(690 <= w.start <= 740 for w in flagged)
