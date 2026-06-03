import json
import os
import time

import redis


METRICS_KEY = os.getenv("KAFKA_METRICS_KEY", "kafka:metrics")
LATENCIES_KEY = f"{METRICS_KEY}:latencies"
BACKLOG_KEY = f"{METRICS_KEY}:backlog"

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)


def reset_metrics(expected_messages: int = 0):
    redis_client.delete(METRICS_KEY, LATENCIES_KEY, BACKLOG_KEY)
    redis_client.hset(
        METRICS_KEY,
        mapping={
            "expected_messages": expected_messages,
            "produced": 0,
            "success": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "retries": 0,
            "recovered": 0,
            "dlq": 0,
            "temporary_failures": 0,
            "started_at": time.time(),
            "finished_at": 0,
        },
    )


def mark_produced(count: int = 1):
    redis_client.hincrby(METRICS_KEY, "produced", count)


def record_success(message: dict, hit: bool, latency_ms: float):
    pipe = redis_client.pipeline()
    pipe.hincrby(METRICS_KEY, "success", 1)
    pipe.hincrby(METRICS_KEY, "cache_hits" if hit else "cache_misses", 1)

    if int(message.get("attempts", 0)) > 0:
        pipe.hincrby(METRICS_KEY, "recovered", 1)

    pipe.rpush(LATENCIES_KEY, latency_ms)
    pipe.execute()


def record_retry():
    pipe = redis_client.pipeline()
    pipe.hincrby(METRICS_KEY, "retries", 1)
    pipe.hincrby(METRICS_KEY, "temporary_failures", 1)
    pipe.execute()


def record_dlq():
    pipe = redis_client.pipeline()
    pipe.hincrby(METRICS_KEY, "dlq", 1)
    pipe.hincrby(METRICS_KEY, "temporary_failures", 1)
    pipe.execute()


def record_backlog(sample: dict):
    redis_client.rpush(BACKLOG_KEY, json.dumps(sample))


def snapshot() -> dict:
    raw = redis_client.hgetall(METRICS_KEY)

    ints = {
        "expected_messages",
        "produced",
        "success",
        "cache_hits",
        "cache_misses",
        "retries",
        "recovered",
        "dlq",
        "temporary_failures",
    }

    data = {}

    for key, value in raw.items():
        if key in ints:
            data[key] = int(value)
        else:
            data[key] = float(value)

    latencies = [float(v) for v in redis_client.lrange(LATENCIES_KEY, 0, -1)]
    backlog = [json.loads(v) for v in redis_client.lrange(BACKLOG_KEY, 0, -1)]
    elapsed = max(time.time() - data.get("started_at", time.time()), 0.001)

    latencies_sorted = sorted(latencies)

    return {
        **data,
        "throughput_qps": round(data.get("success", 0) / elapsed, 4),
        "latency_p50_ms": round(percentile(latencies_sorted, 50), 4),
        "latency_p95_ms": round(percentile(latencies_sorted, 95), 4),
        "retry_rate": rate(data.get("retries", 0), data.get("produced", 0)),
        "recovery_rate": rate(data.get("recovered", 0), data.get("retries", 0)),
        "dlq_rate": rate(data.get("dlq", 0), data.get("produced", 0)),
        "backlog_samples": backlog,
        "max_backlog": max((s.get("total_backlog", 0) for s in backlog), default=0),
    }


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0

    idx = int(len(values) * p / 100)
    idx = min(idx, len(values) - 1)
    return values[idx]


def rate(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0

    return round(numerator / denominator, 4)
