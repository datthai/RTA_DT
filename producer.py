from kafka import KafkaProducer
import json, random, time
from datetime import datetime

# 1. Kết nối tới Kafka
producer = KafkaProducer(
    bootstrap_servers='broker:9092',
    # value_serializer: tự động chuyển dict Python → JSON → bytes
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Danh sách để random
stores = ['Warsaw', 'Krakow', 'Gdansk', 'Wroclaw']
categories = ['electronics', 'clothing', 'food', 'books']

# 3. Hàm sinh ra 1 giao dịch giả
def generate_transaction():
    tx_num = random.randint(1, 9999)
    return {
        'tx_id': f'TX{tx_num:04d}',
        'user_id': f'u{random.randint(1, 20):02d}',
        'amount': round(random.uniform(5.0, 5000.0), 2),
        'store': random.choice(stores),
        'category': random.choice(categories),
        'timestamp': datetime.now().isoformat(),
    }

# 4. Gửi 50 giao dịch, mỗi giây 1 cái
for i in range(50):
    tx = generate_transaction()
    producer.send('transactions', value=tx)
    print(f"[{i+1:02d}] {tx['tx_id']} | {tx['amount']:8.2f} PLN | {tx['store']}")
    time.sleep(1)

# 5. flush() = đảm bảo mọi tin nhắn đã gửi đi
producer.flush()
producer.close()
print("Done.")