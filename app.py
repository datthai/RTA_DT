from flask import Flask, request, jsonify
from threading import Lock

app = Flask(__name__)

# ---- HW 1: shared counters with thread-safe lock ----
counters = {"total": 0, "high": 0, "critical": 0}
counters_lock = Lock()

def score_transaction(tx):
    score = 0
    rules = []
    if tx.get("amount", 0) > 3000:
        score += 3; rules.append("R1: amount > 3000")
    if tx.get("category") == "electronics" and tx.get("amount", 0) > 1500:
        score += 2; rules.append("R2: electronics > 1500")
    if tx.get("hour", 12) < 6:
        score += 2; rules.append("R3: night hour")
    risk_level = (
        "CRITICAL" if score >= 5
        else "HIGH" if score >= 3
        else "MEDIUM" if score >= 1
        else "LOW"
    )
    return {"score": score, "risk_level": risk_level, "triggered_rules": rules}

@app.route("/score", methods=["POST"])
def score():
    tx = request.get_json()

    # HW 2 — validation: missing amount
    if not tx or "amount" not in tx:
        return jsonify({"error": "Missing required field 'amount'"}), 400

    # HW 2 — validation: negative amount
    if tx["amount"] < 0:
        return jsonify({"error": "Field 'amount' cannot be negative"}), 400

    result = score_transaction(tx)
    result["tx_id"] = tx.get("tx_id", "unknown")

    # HW 1 — update counters (thread-safe)
    with counters_lock:
        counters["total"] += 1
        if result["risk_level"] == "HIGH":
            counters["high"] += 1
        elif result["risk_level"] == "CRITICAL":
            counters["critical"] += 1

    return jsonify(result)

# HW 1 — /stats endpoint
@app.route("/stats")
def stats():
    with counters_lock:
        return jsonify({
            "total_requests": counters["total"],
            "high_alerts":    counters["high"],
            "critical_alerts": counters["critical"],
        })

@app.route("/health")
def health():
    return jsonify({"status": "ok", "version": "1.1-homework"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
