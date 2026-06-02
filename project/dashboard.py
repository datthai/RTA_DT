from kafka import KafkaConsumer, TopicPartition
from datetime import datetime
import json
import streamlit as st
import pandas as pd
import plotly.graph_objects as go

st.set_page_config(page_title="Crypto Monitor", layout="wide")
st.title("Real-Time Crypto Price Monitor")

REFRESH_SECONDS = st.sidebar.slider("Refresh interval (s)", 2, 30, 5)
MAX_POINTS = st.sidebar.slider("Points per chart", 50, 500, 200)


@st.cache_resource
def get_consumers():
    prices = KafkaConsumer(
        bootstrap_servers="broker:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    alerts = KafkaConsumer(
        bootstrap_servers="broker:9092",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    return prices, alerts


def read_recent_prices(consumer, n=200):
    parts = [TopicPartition("prices", p) for p in consumer.partitions_for_topic("prices")]
    consumer.assign(parts)
    consumer.seek_to_end()
    end = {p: consumer.position(p) for p in parts}
    for p in parts:
        consumer.seek(p, max(0, end[p] - n))
    return [msg.value for msg in consumer]


def read_all_alerts(consumer):
    topics = consumer.partitions_for_topic("alerts")
    if not topics:
        return []
    parts = [TopicPartition("alerts", p) for p in topics]
    consumer.assign(parts)
    consumer.seek_to_beginning()
    return [msg.value for msg in consumer]


prices_consumer, alerts_consumer = get_consumers()
price_points = read_recent_prices(prices_consumer, MAX_POINTS * 2)
alerts = read_all_alerts(alerts_consumer)

if not price_points:
    st.warning("No price data in Kafka. Is producer.py running?")
    st.stop()

df = pd.DataFrame(price_points)
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.sort_values("timestamp")

col1, col2 = st.columns(2)

for col, symbol in zip([col1, col2], ["BTCUSDT", "ETHUSDT"]):
    sub = df[df["symbol"] == symbol].tail(MAX_POINTS)
    if sub.empty:
        col.warning(f"No data for {symbol}")
        continue

    latest = sub["price"].iloc[-1]
    first = sub["price"].iloc[0]
    delta = latest - first
    delta_pct = (delta / first) * 100

    col.metric(label=symbol, value=f"${latest:,.2f}",
               delta=f"{delta:+.2f} ({delta_pct:+.2f}%)")

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=sub["timestamp"], y=sub["price"],
        mode="lines", line=dict(width=1.5),
    ))
    fig.update_layout(
        height=350, margin=dict(l=10, r=10, t=10, b=10),
        showlegend=False, yaxis_title="USDT",
    )
    col.plotly_chart(fig, use_container_width=True)

st.subheader(f"Anomaly alerts ({len(alerts)})")

if alerts:
    alerts_df = pd.DataFrame(alerts)
    alerts_df = alerts_df.sort_values("window_start", ascending=False).head(20)
    cols_to_show = ["window_start", "symbol", "trades", "avg_price",
                    "volatility", "min_price", "max_price", "alert_type"]
    available = [c for c in cols_to_show if c in alerts_df.columns]
    st.dataframe(alerts_df[available], use_container_width=True, hide_index=True)
else:
    st.info("No alerts yet. Run spark_processor.ipynb Cell 8 to publish alerts.")

st.caption(f"Last update: {datetime.now().strftime('%H:%M:%S')} - refreshing every {REFRESH_SECONDS}s")

import time
time.sleep(REFRESH_SECONDS)
st.rerun()
