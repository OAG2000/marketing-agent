"""
APP.PY - Flask API that serves anomaly data to the dashboard.

Endpoints:
  GET /api/overview?date=2026-03-29        → summary cards data
  GET /api/anomalies?date=2026-03-29       → all anomalies with context
  GET /api/analyze?date=2026-03-29         → run Claude analysis (on demand)
  GET /api/dates                           → available dates in DB
  GET /api/campaigns                       → all campaign metadata
"""

from dotenv import load_dotenv
load_dotenv()

from flask import Flask, jsonify, request
from flask_cors import CORS
from src.schema import get_connection
from src.detector import detect_anomalies
from src.context_builder import build_context, format_prompt
from src.analyzer import analyze_anomalies

app = Flask(__name__, static_folder="dashboard", static_url_path="")
CORS(app)


@app.route("/")
def index():
    return app.send_static_file("index.html")


@app.route("/api/dates")
def get_dates():
    """Return all available dates in the database."""
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT day FROM daily_metrics ORDER BY day"
    ).fetchall()
    conn.close()
    return jsonify({"dates": [r["day"] for r in rows]})


@app.route("/api/campaigns")
def get_campaigns():
    """Return all campaign metadata with adset details."""
    conn = get_connection()
    campaigns = conn.execute("SELECT * FROM campaigns").fetchall()

    result = []
    for c in campaigns:
        adsets = conn.execute(
            "SELECT * FROM adsets WHERE campaign_id = ?", (c["campaign_id"],)
        ).fetchall()

        result.append({
            "campaign_id": c["campaign_id"],
            "campaign_name": c["campaign_name"],
            "objective": c["objective"],
            "start_time": c["start_time"],
            "adsets": [dict(a) for a in adsets],
        })

    conn.close()
    return jsonify({"campaigns": result})


@app.route("/api/overview")
def get_overview():
    """Return summary stats for a given date."""
    date = request.args.get("date")
    if not date:
        conn = get_connection()
        row = conn.execute("SELECT MAX(day) as d FROM daily_metrics").fetchone()
        date = row["d"]
        conn.close()

    anomalies = detect_anomalies(date)

    # Group by campaign
    campaigns_affected = set(a["campaign_id"] for a in anomalies)
    negative = [a for a in anomalies if a["severity"] == "negative"]
    positive = [a for a in anomalies if a["severity"] == "positive"]

    # Get total active campaigns for this date
    conn = get_connection()
    total = conn.execute(
        "SELECT COUNT(DISTINCT campaign_id) as c FROM daily_metrics WHERE day = ?",
        (date,),
    ).fetchone()
    conn.close()

    return jsonify({
        "date": date,
        "total_campaigns": total["c"],
        "total_anomalies": len(anomalies),
        "negative_anomalies": len(negative),
        "positive_anomalies": len(positive),
        "campaigns_affected": len(campaigns_affected),
    })


@app.route("/api/anomalies")
def get_anomalies():
    """Return detailed anomalies with context for a given date."""
    date = request.args.get("date")
    if not date:
        conn = get_connection()
        row = conn.execute("SELECT MAX(day) as d FROM daily_metrics").fetchone()
        date = row["d"]
        conn.close()

    anomalies = detect_anomalies(date)
    enriched_list = build_context(anomalies)

    result = []
    for ec in enriched_list:
        camp = ec["campaign"]

        # Format trends for JSON
        trends = {}
        for metric, data in ec["trends"].items():
            trends[metric] = [
                {"day": d["day"], "value": round(d["value"], 2) if d["value"] else None}
                for d in data
            ]

        result.append({
            "campaign": camp,
            "test_date": ec["test_date"],
            "anomalies": ec["anomalies"],
            "trends": trends,
            "cross_campaign": ec["cross_campaign"],
            "correlated_negative": ec["correlated_negative"],
            "correlated_positive": ec["correlated_positive"],
        })

    return jsonify({"date": date, "campaigns": result})


@app.route("/api/analyze")
def run_analysis():
    """Run Claude analysis for a specific campaign on a date (on-demand)."""
    date = request.args.get("date")
    campaign_id = request.args.get("campaign_id")

    if not date or not campaign_id:
        return jsonify({"error": "date and campaign_id required"}), 400

    anomalies = detect_anomalies(date, campaign_ids=[campaign_id])
    if not anomalies:
        return jsonify({"analysis": "No anomalies detected for this campaign on this date."})

    enriched_list = build_context(anomalies)
    if not enriched_list:
        return jsonify({"analysis": "Could not build context."})

    prompt = format_prompt(enriched_list[0])
    analysis = analyze_anomalies(enriched_list[0], prompt)

    return jsonify({
        "campaign_id": campaign_id,
        "date": date,
        "analysis": analysis,
    })


if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    print(f"\n🚀 Marketing Anomaly Dashboard running on port {port}\n")
    app.run(host="0.0.0.0", port=port, debug=False)