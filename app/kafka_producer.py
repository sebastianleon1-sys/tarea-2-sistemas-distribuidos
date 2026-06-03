import argparse
import time

from kafka_common import MAIN_TOPIC, ensure_topics, make_message, make_producer
from kafka_metrics import mark_produced, reset_metrics
from traffic_generator import generate_batch


def publish_queries(
    distribution: str,
    n_queries: int,
    seed: int | None,
    rate_qps: float,
    spike_after: int | None,
    spike_multiplier: float,
    reset: bool,
):
    ensure_topics()

    if reset:
        reset_metrics(expected_messages=n_queries)

    producer = make_producer()
    queries = generate_batch(n_queries, distribution, seed=seed)
    base_interval = 1.0 / rate_qps if rate_qps > 0 else 0
    produced = 0
    started_at = time.time()

    for query in queries:
        elapsed = time.time() - started_at
        interval = base_interval

        if spike_after is not None and elapsed >= spike_after:
            interval = base_interval / max(spike_multiplier, 1.0)

        message = make_message(query)
        producer.send(MAIN_TOPIC, key=message["id"], value=message)
        produced += 1
        mark_produced()

        if interval > 0:
            time.sleep(interval)

    producer.flush()
    producer.close()
    print(f"[Producer] Publicadas {produced} consultas en {MAIN_TOPIC}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer para Tarea 2")
    parser.add_argument("--distribution", choices=["zipf", "uniform"], default="zipf")
    parser.add_argument("--queries", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rate-qps", type=float, default=200.0)
    parser.add_argument("--spike-after", type=int, default=None)
    parser.add_argument("--spike-multiplier", type=float, default=4.0)
    parser.add_argument("--no-reset", action="store_true")
    args = parser.parse_args()

    publish_queries(
        distribution=args.distribution,
        n_queries=args.queries,
        seed=args.seed,
        rate_qps=args.rate_qps,
        spike_after=args.spike_after,
        spike_multiplier=args.spike_multiplier,
        reset=not args.no_reset,
    )
