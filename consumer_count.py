from kafka import KafkaConsumer
from collections import Counter, defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='count-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# 🔑 ĐÂY LÀ "STATE" — biến sống ngoài vòng lặp, nhớ giữa các tin nhắn
store_counts = Counter()              # {'Warsaw': 5, 'Krakow': 3, ...}
total_amount = defaultdict(float)     # {'Warsaw': 1234.56, ...}
msg_count = 0

print("Counting consumer — summary every 10 messages...\n")

for message in consumer:
    tx = message.value
    store = tx['store']

    # Cập nhật state
    store_counts[store] += 1
    total_amount[store] += tx['amount']
    msg_count += 1

    # In bảng tóm tắt mỗi 10 tin nhắn
    if msg_count % 10 == 0:
        print(f"\n{'='*55}")
        print(f"{'Store':<12} {'Count':>8} {'Total PLN':>12} {'Avg PLN':>10}")
        print(f"{'-'*55}")
        for store in sorted(store_counts):
            n = store_counts[store]
            s = total_amount[store]
            avg = s / n
            print(f"{store:<12} {n:>8} {s:>12.2f} {avg:>10.2f}")
        print(f"{'='*55}")
        print(f"Total messages processed: {msg_count}\n")