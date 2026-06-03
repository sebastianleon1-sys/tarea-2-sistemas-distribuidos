#!/usr/bin/env python3

import json
import os
import subprocess
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


OUTPUT_DIR = "experiment_results_kafka"
N_QUERIES = 1000
SEED = 42

os.makedirs(OUTPUT_DIR, exist_ok=True)


SCENARIOS = [
    {
        "name": "kafka_1_consumer",
        "consumers": 1,
        "rate_qps": 200,
        "failure_rate": 0.0,
        "fail_for_s": 0,
        "response_delay_ms": 10,
    },
    {
        "name": "kafka_2_consumers",
        "consumers": 2,
        "rate_qps": 200,
        "failure_rate": 0.0,
        "fail_for_s": 0,
        "response_delay_ms": 10,
    },
    {
        "name": "kafka_4_consumers",
        "consumers": 4,
        "rate_qps": 200,
        "failure_rate": 0.0,
        "fail_for_s": 0,
        "response_delay_ms": 10,
    },
    {
        "name": "falla_temporal",
        "consumers": 2,
        "rate_qps": 200,
        "failure_rate": 0.0,
        "fail_for_s": 8,
        "response_delay_ms": 10,
    },
    {
        "name": "reintentos",
        "consumers": 2,
        "rate_qps": 200,
        "failure_rate": 0.18,
        "fail_for_s": 0,
        "response_delay_ms": 10,
    },
    {
        "name": "spike_trafico",
        "consumers": 2,
        "rate_qps": 80,
        "spike_after": 3,
        "spike_multiplier": 8,
        "failure_rate": 0.0,
        "fail_for_s": 0,
        "response_delay_ms": 10,
    },
]


def compose(*args, capture=False, check=True):
    cmd = ["docker", "compose", "--profile", "kafka", *args]
    return subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
        check=check,
    )


def stop_all():
    compose("down", "-v", "--remove-orphans", check=False)
    time.sleep(2)


def start_base_services():
    compose("up", "-d", "redis", "kafka")


def build_images():
    compose("build", "app", "kafka-producer", "kafka-consumer", "kafka-monitor")


def flush_redis():
    compose("exec", "-T", "redis", "redis-cli", "flushall")


def start_monitor():
    remove_container("kafka_backlog_monitor")
    compose(
        "run",
        "-d",
        "--name",
        "kafka_backlog_monitor",
        "kafka-monitor",
    )


def start_consumers(scenario):
    names = []

    for i in range(scenario["consumers"]):
        name = f"{scenario['name']}_consumer_{i + 1}"
        remove_container(name)
        compose(
            "run",
            "-d",
            "--name",
            name,
            "kafka-consumer",
            "python",
            "kafka_consumer.py",
            "--max-attempts",
            "3",
            "--retry-delay-ms",
            "250",
            "--failure-rate",
            str(scenario.get("failure_rate", 0.0)),
            "--fail-for-s",
            str(scenario.get("fail_for_s", 0)),
            "--response-delay-ms",
            str(scenario.get("response_delay_ms", 0)),
        )
        names.append(name)

    return names


def run_producer(scenario):
    cmd = [
        "run",
        "--rm",
        "kafka-producer",
        "python",
        "kafka_producer.py",
        "--distribution",
        "zipf",
        "--queries",
        str(N_QUERIES),
        "--rate-qps",
        str(scenario["rate_qps"]),
        "--seed",
        str(SEED),
    ]

    if scenario.get("spike_after") is not None:
        cmd.extend(["--spike-after", str(scenario["spike_after"])])
        cmd.extend(["--spike-multiplier", str(scenario["spike_multiplier"])])

    compose(*cmd)


def redis_int(field):
    result = compose(
        "exec",
        "-T",
        "redis",
        "redis-cli",
        "hget",
        "kafka:metrics",
        field,
        capture=True,
        check=False,
    )

    try:
        return int(result.stdout.strip() or 0)
    except ValueError:
        return 0


def wait_until_done(timeout_s=180):
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        success = redis_int("success")
        dlq = redis_int("dlq")
        expected = redis_int("expected_messages") or N_QUERIES

        if success + dlq >= expected:
            return True

        time.sleep(1)

    return False


def export_report(name):
    output_path = f"/app/output/{name}.json"
    result = compose(
        "run",
        "--rm",
        "kafka-producer",
        "python",
        "kafka_report.py",
        "--output",
        output_path,
        capture=True,
    )
    json_start = result.stdout.find("{")

    if json_start == -1:
        raise RuntimeError(f"No se pudo leer JSON de métricas:\n{result.stdout}")

    data = json.loads(result.stdout[json_start:])

    host_path = os.path.join("output", f"{name}.json")
    final_path = os.path.join(OUTPUT_DIR, f"{name}.json")

    if os.path.exists(host_path):
        os.replace(host_path, final_path)
    else:
        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    return data


def cleanup_run(containers):
    for name in containers + ["kafka_backlog_monitor"]:
        remove_container(name)


def remove_container(name):
    subprocess.run(
        ["docker", "rm", "-f", name],
        capture_output=True,
        text=True,
        check=False,
    )


def run_scenario(scenario):
    print(f"\n=== {scenario['name']} ===")
    stop_all()
    start_base_services()
    flush_redis()
    start_monitor()
    containers = start_consumers(scenario)
    time.sleep(3)
    run_producer(scenario)

    completed = wait_until_done()

    if not completed:
        print("[WARN] Timeout esperando término; se exportan métricas parciales.")

    time.sleep(2)
    report = export_report(scenario["name"])
    report.update(
        {
            "scenario": scenario["name"],
            "consumers": scenario["consumers"],
            "n_queries": N_QUERIES,
            "seed": SEED,
        }
    )

    cleanup_run(containers)
    return report


def save_plot(fig, name):
    fig.savefig(os.path.join(OUTPUT_DIR, name), dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  OK {name}")


def generate_plots(results):
    fig, ax = plt.subplots(figsize=(8, 5))
    scaling = [r for r in results if r["scenario"].startswith("kafka_")]
    ax.plot(
        [r["consumers"] for r in scaling],
        [r.get("throughput_qps", 0) for r in scaling],
        marker="o",
    )
    ax.set_title("Throughput vs Consumers Kafka")
    ax.set_xlabel("Consumers")
    ax.set_ylabel("Consultas exitosas/s")
    save_plot(fig, "01_throughput_consumers.png")

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(
        [r["scenario"] for r in results],
        [r.get("latency_p95_ms", 0) for r in results],
    )
    ax.set_title("Latencia p95 por escenario")
    ax.set_ylabel("ms")
    ax.tick_params(axis="x", rotation=25)
    save_plot(fig, "02_latencia_p95_escenarios.png")

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(
        [r["scenario"] for r in results],
        [r.get("max_backlog", 0) for r in results],
    )
    ax.set_title("Backlog máximo por escenario")
    ax.set_ylabel("Mensajes pendientes")
    ax.tick_params(axis="x", rotation=25)
    save_plot(fig, "03_backlog_maximo.png")

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(
        [r["scenario"] for r in results],
        [r.get("retry_rate", 0) * 100 for r in results],
        label="Retry rate",
    )
    ax.bar(
        [r["scenario"] for r in results],
        [r.get("dlq_rate", 0) * 100 for r in results],
        label="DLQ rate",
        alpha=0.75,
    )
    ax.set_title("Reintentos y DLQ por escenario")
    ax.set_ylabel("% de consultas publicadas")
    ax.tick_params(axis="x", rotation=25)
    ax.legend()
    save_plot(fig, "04_retry_dlq_rates.png")


def run_all():
    results = []
    build_images()

    for scenario in SCENARIOS:
        results.append(run_scenario(scenario))

    all_path = os.path.join(OUTPUT_DIR, "all_kafka_results.json")

    with open(all_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    generate_plots(results)
    stop_all()
    print(f"\nResultados guardados en ./{OUTPUT_DIR}/")


if __name__ == "__main__":
    run_all()
