from kafka import KafkaConsumer
from collections import defaultdict
from datetime import datetime
import json
import statistics
import csv
import time

KAFKA_BROKER = "broker:9092"
TOPIC = "prices"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    auto_offset_reset="earliest",
    consumer_timeout_ms=8000,
    max_poll_records=5000,
    fetch_max_bytes=52_428_800,
)

print("Reading entire 'prices' topic...")
buckets = defaultdict(list)
n = 0
t0 = time.time()

for msg in consumer:
    tx = msg.value
    ts = datetime.fromisoformat(tx["timestamp"]).replace(second=0, microsecond=0)
    buckets[(tx["symbol"], ts)].append(tx["price"])
    n += 1
    if n % 20000 == 0:
        print(f"  ... {n} messages read ({n/(time.time()-t0):.0f} msg/s)")

print(f"\nConsumed {n} trades into {len(buckets)} 1-minute windows in {time.time()-t0:.1f}s.\n")

rows = []
for (symbol, ts), prices in sorted(buckets.items()):
    if len(prices) < 2:
        continue
    rows.append({
        "symbol":      symbol,
        "window":      ts.isoformat(),
        "trades":      len(prices),
        "avg_price":   round(statistics.mean(prices), 4),
        "volatility":  round(statistics.stdev(prices), 4),
        "min_price":   round(min(prices), 4),
        "max_price":   round(max(prices), 4),
        "price_range": round(max(prices) - min(prices), 4),
    })

with open("training_data.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)

print(f"Wrote {len(rows)} rows to training_data.csv")

by_symbol = defaultdict(int)
for r in rows:
    by_symbol[r["symbol"]] += 1
print("\nWindows per symbol:")
for sym, cnt in by_symbol.items():
    print(f"  {sym}: {cnt}")
