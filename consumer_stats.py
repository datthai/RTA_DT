from kafka import KafkaConsumer
from collections import defaultdict
import json

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',
    group_id='stats-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# State: với mỗi category mới, tự tạo dict với giá trị khởi tạo
stats = defaultdict(lambda: {
    'count': 0,
    'total': 0.0,
    'min': float('inf'),       # +∞ → mọi số đều nhỏ hơn
    'max': float('-inf'),      # -∞ → mọi số đều lớn hơn
})
msg_count = 0

print("Stats consumer — per-category statistics...\n")

for message in consumer:
    tx = message.value
    cat = tx['category']
    amt = tx['amount']

    stats[cat]['count'] += 1
    stats[cat]['total'] += amt
    if amt < stats[cat]['min']:
        stats[cat]['min'] = amt
    if amt > stats[cat]['max']:
        stats[cat]['max'] = amt

    msg_count += 1
    if msg_count % 10 == 0:
        print(f"\n{'='*68}")
        print(f"{'Category':<14} {'Count':>8} {'Total PLN':>12} {'Min PLN':>10} {'Max PLN':>10}")
        print(f"{'-'*68}")
        for cat in sorted(stats):
            s = stats[cat]
            print(f"{cat:<14} {s['count']:>8} {s['total']:>12.2f} {s['min']:>10.2f} {s['max']:>10.2f}")
        print(f"{'='*68}")