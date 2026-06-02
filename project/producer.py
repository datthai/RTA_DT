"""
Crypto Price Producer — reads BTC/ETH prices from Binance WebSocket,
publishes to Kafka topic 'prices'.
"""
import json
from datetime import datetime
from kafka import KafkaProducer
import websocket

KAFKA_BROKER = "localhost:29092"
TOPIC = "prices"

SYMBOLS = ["btcusdt", "ethusdt"]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def on_message(ws, message):
    data = json.loads(message)
    event = {
        "symbol":     data["s"],
        "price":      float(data["p"]),
        "quantity":   float(data["q"]),
        "trade_time": data["T"],
        "timestamp":  datetime.now().isoformat(),
    }
    producer.send(TOPIC, value=event)
    print(f"{event['symbol']:>8s}  {event['price']:>12.2f} USDT  qty={event['quantity']:.4f}")


def on_error(ws, error):
    print(f"[ERROR] {error}")


def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed")
    producer.flush()


def on_open(ws):
    print(f"Connected to Binance. Streaming: {SYMBOLS}")


if __name__ == "__main__":
    streams = "/".join(f"{s}@trade" for s in SYMBOLS)
    url = f"wss://stream.binance.com:9443/stream?streams={streams}"

    def on_message_wrapped(ws, message):
        msg = json.loads(message)
        if "data" in msg:
            on_message(ws, json.dumps(msg["data"]))

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message_wrapped,
        on_error=on_error,
        on_close=on_close,
    )
    ws.run_forever()
