import argparse
import os
import random
import time

from kafka import KafkaConsumer

from cache import get_from_cache, set_in_cache
from kafka_common import (
    MAIN_TOPIC,
    RETRY_TOPIC,
    ensure_topics,
    json_deserializer,
    make_producer,
)
from kafka_metrics import record_dlq, record_retry, record_success
from main import TTL_BY_QUERY, build_compute_fn
from traffic_generator import Query


GROUP_ID = os.getenv("KAFKA_GROUP_ID", "geo-workers")


class TemporaryResponseFailure(Exception):
    pass


def should_fail(failure_rate: float, fail_until: float | None) -> bool:
    if fail_until is not None and time.time() < fail_until:
        return True

    return random.random() < failure_rate


def process_message(
    message: dict,
    producer,
    max_attempts: int,
    retry_delay_ms: int,
    failure_rate: float,
    fail_until: float | None,
    response_delay_ms: int,
):
    query = Query.from_dict(message["query"])
    key = query.cache_key()
    start = time.perf_counter()

    cached, hit = get_from_cache(key)

    if hit:
        latency_ms = (time.time() - message["created_at"]) * 1000
        record_success(message, hit=True, latency_ms=latency_ms)
        return cached

    try:
        if should_fail(failure_rate, fail_until):
            raise TemporaryResponseFailure("Generador de Respuestas no disponible")

        if response_delay_ms > 0:
            time.sleep(response_delay_ms / 1000)

        result = build_compute_fn(query)()
        set_in_cache(key, result, ttl=TTL_BY_QUERY.get(query.query_type, 60))

        latency_ms = (time.time() - message["created_at"]) * 1000
        record_success(message, hit=False, latency_ms=latency_ms)
        return result
    except TemporaryResponseFailure as exc:
        message["attempts"] = int(message.get("attempts", 0)) + 1
        message["last_error"] = str(exc)

        if message["attempts"] > max_attempts:
            producer.send(
                os.getenv("KAFKA_DLQ_TOPIC", "geo-queries-dlq"),
                key=message["id"],
                value=message,
            )
            record_dlq()
            return None

        if retry_delay_ms > 0:
            time.sleep(retry_delay_ms / 1000)

        producer.send(RETRY_TOPIC, key=message["id"], value=message)
        record_retry()
        return None
    finally:
        _ = (time.perf_counter() - start) * 1000


def run_consumer(
    max_attempts: int,
    retry_delay_ms: int,
    failure_rate: float,
    fail_for_s: int,
    response_delay_ms: int,
):
    ensure_topics()
    producer = make_producer()
    fail_until = time.time() + fail_for_s if fail_for_s > 0 else None

    consumer = KafkaConsumer(
        MAIN_TOPIC,
        RETRY_TOPIC,
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        group_id=GROUP_ID,
        value_deserializer=json_deserializer,
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=25,
    )

    print(
        "[Consumer] Escuchando "
        f"{MAIN_TOPIC}/{RETRY_TOPIC} | grupo={GROUP_ID} | "
        f"max_attempts={max_attempts}"
    )

    try:
        for record in consumer:
            process_message(
                record.value,
                producer,
                max_attempts=max_attempts,
                retry_delay_ms=retry_delay_ms,
                failure_rate=failure_rate,
                fail_until=fail_until,
                response_delay_ms=response_delay_ms,
            )
            consumer.commit()
    finally:
        producer.flush()
        producer.close()
        consumer.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka consumer para Tarea 2")
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--retry-delay-ms", type=int, default=250)
    parser.add_argument("--failure-rate", type=float, default=0.0)
    parser.add_argument("--fail-for-s", type=int, default=0)
    parser.add_argument("--response-delay-ms", type=int, default=0)
    args = parser.parse_args()

    run_consumer(
        max_attempts=args.max_attempts,
        retry_delay_ms=args.retry_delay_ms,
        failure_rate=args.failure_rate,
        fail_for_s=args.fail_for_s,
        response_delay_ms=args.response_delay_ms,
    )
