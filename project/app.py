"""
Flask Scoring API — exposes the trained IsolationForest models via REST.

Endpoints:
  GET  /health   — liveness check
  POST /score    — predict anomaly for one window
  GET  /stats    — request counters
"""
from flask import Flask, request, jsonify
from threading import Lock
import joblib
import numpy as np

# Load models once at startup (not per request)
MODELS = joblib.load("model.pkl")
FEATURES = ["volatility", "price_range", "trades"]

app = Flask(__name__)

# Thread-safe counters for /stats
counters = {"total": 0, "anomaly": 0, "normal": 0, "unknown_symbol": 0}
counters_lock = Lock()


def predict(symbol, volatility, price_range, trades):
    """Score one window. Returns (label, score) or (UNKNOWN, 0) if symbol not trained."""
    if symbol not in MODELS:
        return "UNKNOWN_SYMBOL", 0.0
    X = np.array([[volatility, price_range, trades]])
    model = MODELS[symbol]
    pred = model.predict(X)[0]                  # -1 anomaly, +1 normal
    score = float(model.decision_function(X)[0])
    label = "ANOMALY" if pred == -1 else "NORMAL"
    return label, score


@app.route("/health")
def health():
    return jsonify({
        "status": "ok",
        "models_loaded": list(MODELS.keys()),
        "features": FEATURES,
    })


@app.route("/score", methods=["POST"])
def score():
    tx = request.get_json()

    # Validate required fields
    if not tx:
        return jsonify({"error": "request body must be JSON"}), 400
    for field in ["symbol"] + FEATURES:
        if field not in tx:
            return jsonify({"error": f"missing field: {field}"}), 400
    if tx["volatility"] < 0 or tx["trades"] < 0 or tx["price_range"] < 0:
        return jsonify({"error": "features must be non-negative"}), 400

    label, score_value = predict(
        tx["symbol"],
        float(tx["volatility"]),
        float(tx["price_range"]),
        int(tx["trades"]),
    )

    # Update counters thread-safely
    with counters_lock:
        counters["total"] += 1
        if label == "ANOMALY":
            counters["anomaly"] += 1
        elif label == "NORMAL":
            counters["normal"] += 1
        else:
            counters["unknown_symbol"] += 1

    return jsonify({
        "symbol":        tx["symbol"],
        "label":         label,
        "anomaly_score": round(score_value, 4),
        "features_used": {f: tx[f] for f in FEATURES},
    })


@app.route("/stats")
def stats():
    with counters_lock:
        return jsonify(dict(counters))


if __name__ == "__main__":
    print(f"Loaded models for: {list(MODELS.keys())}")
    app.run(host="0.0.0.0", port=5000, debug=False)
