import argparse
import json
import os

from kafka_metrics import snapshot


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta métricas Kafka")
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    data = snapshot()
    text = json.dumps(data, indent=2)
    print(text)

    if args.output:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)

        with open(args.output, "w", encoding="utf-8") as f:
            f.write(text)
