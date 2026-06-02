# Real-Time Crypto Price Monitoring & Anomaly Detection

Course project for **Real-Time Analytics (222891-D)** — SGH Warsaw School of Economics, Spring 2025/2026.

## Business Problem

Cryptocurrency markets operate 24/7 with high volatility. Sudden price movements
can indicate market manipulation, flash crashes, or trading opportunities.
This project detects anomalous price movements in real time and surfaces them
to a live dashboard.

## Architecture
┌──────────────┐     ┌─────────────┐     ┌──────────────────┐     ┌──────────┐
│   Binance    │     │   Kafka     │     │ Spark Structured │     │   Kafka  │
│  WebSocket   │────►│  (prices)   │────►│    Streaming     │────►│ (alerts) │
│  (BTC,ETH)   │     │             │     │  1-min windows   │     │          │
└──────────────┘     └─────────────┘     │  volatility calc │     └────┬─────┘
└──────────────────┘          │
│                    │
▼                    ▼
┌──────────────────┐     ┌──────────┐
│   Flask API      │     │Streamlit │
│  /score (ML)     │     │Dashboard │
│ IsolationForest  │     │  charts  │
└──────────────────┘     │ + alerts │
└──────────┘
## Components

| File | Purpose | Stack |
|---|---|---|
| `producer.py` | Streams live trades from Binance WebSocket to Kafka topic `prices` | kafka-python, websocket-client |
| `data_collector.py` | Drains historical prices from Kafka and aggregates into 1-minute windows for ML training | kafka-python |
| `train_model.ipynb` | Trains one IsolationForest per symbol on collected windows; saves to `model.pkl` | scikit-learn, pandas |
| `spark_processor.ipynb` | Reads `prices` stream, computes per-window volatility, publishes anomalies to `alerts` topic | PySpark Structured Streaming |
| `app.py` | Flask REST API exposing `/score`, `/health`, `/stats` endpoints; loads `model.pkl` once at startup | Flask, joblib |
| `dashboard.py` | Live Streamlit dashboard — price charts + anomaly alerts table; auto-refresh every 5s | Streamlit, Plotly |

## How to Run

### Prerequisites
- Docker Desktop with `compose.yaml` providing Kafka broker + JupyterLab
- Python 3.10+ on the host (for the producer)
- Ports 8999 (JupyterLab), 5000 (Flask), 8501 (Streamlit), 29092 (Kafka host) free

### Step 1 — Start infrastructure
```bash
cd jupyterlab-project
docker compose up -d
```

### Step 2 — Run the producer (on host)
```cmd
cd RTA_DT/project
pip install kafka-python websocket-client
python producer.py
```
Leave running. The producer streams ~3-5 trades/second into Kafka.

### Step 3 — Train the model (one-time, in JupyterLab)
After producer has run for ~30+ minutes:
```bash
cd /home/jovyan/notebooks/crypto-project
python data_collector.py          # writes training_data.csv
# then open and run all cells of train_model.ipynb → produces model.pkl
```

### Step 4 — Start Flask API
```bash
python app.py                     # listens on 0.0.0.0:5000
```

### Step 5 — Start Spark Streaming
Open `spark_processor.ipynb` in JupyterLab and run all cells in order.
Cell 8 publishes anomaly alerts to Kafka topic `alerts`.

### Step 6 — Start Dashboard
```bash
python -m streamlit run dashboard.py --server.port 8501 --server.address 0.0.0.0
```
Open `http://localhost:8501` in a browser.

## API Reference

### `GET /health`
Liveness check. Returns loaded model symbols and feature names.

### `POST /score`
Score one transaction window for anomaly.

**Request:**
```json
{
  "symbol": "BTCUSDT",
  "volatility": 25.0,
  "price_range": 100.0,
  "trades": 2800
}
```

**Response:**
```json
{
  "symbol": "BTCUSDT",
  "label": "NORMAL",
  "anomaly_score": 0.0483,
  "features_used": {"volatility": 25.0, "price_range": 100.0, "trades": 2800}
}
```

`label` is `NORMAL`, `ANOMALY`, or `UNKNOWN_SYMBOL`.
`anomaly_score`: positive = normal, negative = anomaly (IsolationForest decision function).

### `GET /stats`
Counters for total requests, anomaly count, normal count.

## ML Model Details

- **Algorithm:** Isolation Forest (sklearn)
- **One model per symbol** — BTC and ETH live on different price scales (BTC ~67k, ETH ~1.9k); a shared model would be dominated by BTC.
- **Features:** `volatility` (stddev of price), `price_range` (max − min), `trades` (count)
- **Hyperparameters:** `contamination=0.1`, `n_estimators=100`, `random_state=42`
- **Training data:** ~25 windows per symbol collected from the Kafka `prices` topic

## Data Source

Binance public WebSocket — `wss://stream.binance.com:9443/stream` — streaming
real BTC/USDT and ETH/USDT trades. No API key required.

## Tech Stack

| Layer | Technology |
|---|---|
| Message broker | Apache Kafka 7.8 (KRaft mode) |
| Stream processing | Apache Spark Structured Streaming 3.5+ |
| ML | scikit-learn (IsolationForest) |
| API | Flask 3.0 |
| Dashboard | Streamlit + Plotly |
| Orchestration | Docker Compose |
| Language | Python 3.11 |

## Screenshots

See `screenshots/` directory.

## Author

Đạt Thái — Real-Time Analytics, SGH WSE Spring 2025/2026.
