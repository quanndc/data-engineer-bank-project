"""Replay the source transaction file into Kafka in date order.

Emulates the bank's card/payment switch: one JSON event per transaction,
keyed by customer so that one customer's events stay in one partition.

    python3 producer.py                 # as fast as possible (batch load)
    python3 producer.py --rate 500      # 500 events/s (streaming demo)
    python3 producer.py --rate 500 --skip 100000 --limit 20000   # a fresh slice for a new demo
"""
import argparse
import json
import os
import time

import pandas as pd
from confluent_kafka import Producer

SOURCE = "/data/source/dwh/fact_transactions.csv"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file", default=SOURCE)
    ap.add_argument("--topic", default="transactions")
    ap.add_argument("--rate", type=float, default=0, help="events per second; 0 = unthrottled")
    ap.add_argument("--limit", type=int, default=0, help="send only N events; 0 = all")
    ap.add_argument("--skip", type=int, default=0, help="skip the first N events (date order)")
    args = ap.parse_args()

    # Amounts stay strings so no precision is lost before Spark casts them to Decimal(18,2)
    df = pd.read_csv(args.file, dtype={"transaction_amount": str, "account_balance": str})
    df = df.sort_values(["date_key", "transaction_key"], kind="stable")
    df = df.iloc[args.skip:]
    if args.limit:
        df = df.head(args.limit)

    producer = Producer({
        "bootstrap.servers": os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092"),
        "linger.ms": 20,
        "batch.num.messages": 10000,
        "compression.type": "lz4",
        "queue.buffering.max.messages": 500000,
        "enable.idempotence": True,     # no duplicates caused by producer retries
    })

    start = time.time()
    for i, row in enumerate(df.itertuples(index=False), start=1):
        event = row._asdict()
        event["produced_at_ms"] = int(time.time() * 1000)   # for end-to-end latency (B3)
        payload = json.dumps(event, default=int)
        while True:
            try:
                producer.produce(args.topic, key=str(event["customer_key"]), value=payload)
                break
            except BufferError:          # local queue full: let it drain
                producer.poll(0.1)
        producer.poll(0)
        if args.rate:
            # sleep until this event's scheduled send time (absolute schedule, no drift)
            delay = start + i / args.rate - time.time()
            if delay > 0:
                time.sleep(delay)
        if i % 200000 == 0:
            print(f"  {i:,} events sent ({i / (time.time() - start):,.0f}/s)", flush=True)

    undelivered = producer.flush(60)
    elapsed = time.time() - start
    print(f"done: {len(df):,} events to '{args.topic}' in {elapsed:,.1f}s "
          f"({len(df) / elapsed:,.0f}/s), undelivered={undelivered}")


if __name__ == "__main__":
    main()
