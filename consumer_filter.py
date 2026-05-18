from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'transactions',                                # đọc từ topic này
    bootstrap_servers='broker:9092',
    auto_offset_reset='earliest',                  # đọc từ đầu nếu lần đầu
    group_id='filter-group',                       # tên nhóm consumer
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Listening for large transactions (amount > 1000)...\n")

# Vòng lặp vô hạn — chờ tin nhắn mới
for message in consumer:
    tx = message.value                             # đã là dict
    if tx['amount'] > 1000:
        print(f"ALERT: {tx['tx_id']} | {tx['amount']:.2f} PLN | {tx['store']} | {tx['category']}")