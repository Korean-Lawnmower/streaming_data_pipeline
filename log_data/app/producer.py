import os, time, csv, sys, json
from kafka import KafkaProducer
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "logs_raw")
CSV_PATH = os.getenv("CSV_PATH", "/app/ingest/logs.csv")
RATE = float(os.getenv("SEND_RATE_PER_SEC", "5"))

def main():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        acks="all",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    )

    sent = 0
    delay = 1.0 / RATE if RATE > 0 else 0.0

    if not os.path.isfile(CSV_PATH):
        print(f"[ERROR] CSV not found: {CSV_PATH}", file=sys.stderr)
        sys.exit(1)

    while True:  # keep looping forever
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                sent += 1
                if sent % 60 == 0:
                    row["accessed_date"] = "BAD_DATE"
                else:
                    row["accessed_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                # send with item_category as Kafka key
                producer.send(TOPIC, key=row["item_category"], value=row)

                if delay > 0:
                    time.sleep(delay)
                if sent % 100 == 0:
                    print(f"[INFO] sent={sent}", flush=True)

        # loop again with fresh timestamps each pass

    producer.flush()
    print(f"✅ Done. total sent={sent}")

if __name__ == "__main__":
    main()
