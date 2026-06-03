# Guía para video de demostración Tarea 2

Duración sugerida: 8 a 10 minutos. Subir a YouTube o Drive como enlace visible, no como archivo descargable.

## 1. Presentación breve

- Nombre del proyecto.
- Explicar que se reutiliza la Tarea 1 y se agrega Kafka.
- Mostrar componentes: Redis, Kafka, Producer, Consumers, Retry topic, DLQ y métricas.

## 2. Mostrar estructura del código

Archivos clave:

- `app/kafka_producer.py`
- `app/kafka_consumer.py`
- `app/kafka_metrics.py`
- `app/kafka_monitor.py`
- `run_kafka_experiments.py`
- `docker-compose.yml`

Explicar que todos los consumers usan el grupo `geo-workers`.

## 3. Demo del flujo normal

```bash
docker compose --profile kafka up -d redis kafka
docker compose --profile kafka run -d --name demo_consumer kafka-consumer
docker compose --profile kafka run --rm kafka-producer python kafka_producer.py --queries 50 --rate-qps 20
docker compose --profile kafka run --rm kafka-producer python kafka_report.py
```

Mostrar `success`, `cache_hits`, `cache_misses`, `throughput_qps`, `latency_p50_ms` y `latency_p95_ms`.

## 4. Demo de reintentos

```bash
docker compose --profile kafka run -d --name demo_retry_consumer kafka-consumer python kafka_consumer.py --max-attempts 3 --retry-delay-ms 250 --failure-rate 0.4
docker compose --profile kafka run --rm kafka-producer python kafka_producer.py --queries 50 --rate-qps 20
docker compose --profile kafka run --rm kafka-producer python kafka_report.py
```

Mostrar que suben `retries` y `recovered`.

## 5. Demo de DLQ

Forzar fallas permanentes:

```bash
docker compose --profile kafka run -d --name demo_dlq_consumer kafka-consumer python kafka_consumer.py --max-attempts 1 --retry-delay-ms 100 --failure-rate 1.0
docker compose --profile kafka run --rm kafka-producer python kafka_producer.py --queries 20 --rate-qps 10
docker compose --profile kafka run --rm kafka-producer python kafka_report.py
```

Mostrar que sube `dlq` y `dlq_rate`.

## 6. Demo de experimentos y gráficos

Mostrar carpeta:

```text
experiment_results_kafka/
```

Abrir o mostrar:

- `01_throughput_consumers.png`
- `02_latencia_p95_escenarios.png`
- `03_backlog_maximo.png`
- `04_retry_dlq_rates.png`
- `all_kafka_results.json`

## 7. Cierre

Responder en voz:

- Kafka aumenta resiliencia, pero agrega latencia.
- Los reintentos reducen pérdida de consultas.
- Más consumidores ayudan hasta que CPU/memoria/dataset/Redis se vuelven cuello de botella.
- El backlog crece durante sobrecarga o fallas y luego se vacía con consumidores activos.

## Limpieza

```bash
docker rm -f demo_consumer demo_retry_consumer demo_dlq_consumer
docker compose --profile kafka down -v --remove-orphans
```
