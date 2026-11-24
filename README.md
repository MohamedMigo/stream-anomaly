# Stream Anomaly

Real-time stream processing in pure Python, built around the ideas behind Kafka Streams and Flink:

- **Event-time tumbling windows** per key (service)
- **Watermark + allowed lateness**: out-of-order events are handled, and events for already-closed windows are counted as dropped
- **Online anomaly detection** with an EWMA mean/variance z-score. Outliers do not update the baseline, so a spike cannot hide itself
- A **simulator** producing latency metrics for two services with network reordering and a 30 second spike on `api`

## Run
```bash
pip install -r requirements.txt
python -m stream.engine
pytest -q
```
Output on the simulated stream (300 windows, exactly the injected spike is flagged and nothing else):
```
ANOMALY WindowResult(key='api', start=700, end=710, mean=363.8, z=106.9)
ANOMALY WindowResult(key='api', start=710, end=720, mean=359.4, z=105.0)
ANOMALY WindowResult(key='api', start=720, end=730, mean=354.3, z=102.8)
```
## Use with a real source
`WindowedProcessor.process(Event(ts, key, value))` is a generator, so you can feed it from a Kafka consumer, a socket or a log tail and react to each `WindowResult` as it closes.
