"""
DETECTOR.PY - Z-score anomaly detection engine.

For each campaign, for each "test day":
1. Grab the previous BASELINE_WINDOW days of data
2. Compute mean and std dev for each relevant metric
3. Calculate z-score for the test day
4. Flag if |z-score| > threshold

Key design choices:
- Rolling baseline: adapts to gradual trends, catches sudden changes
- Metric filtering by objective: avoids false alarms on irrelevant metrics
- Handles sparse data: skips metrics with too few data points
"""

import sqlite3
from typing import Optional, List, Dict
from datetime import datetime, timedelta
from src.schema import get_connection
from src.config import (
    BASELINE_WINDOW,
    Z_SCORE_THRESHOLD,
    MIN_BASELINE_DAYS,
    METRIC_MAPPING,
    HIGHER_IS_BETTER,
    LOWER_IS_BETTER,
)


def get_campaign_objective(campaign_id: str, conn: sqlite3.Connection) -> Optional[str]:
    """Look up a campaign's objective from metadata."""
    row = conn.execute(
        "SELECT objective FROM campaigns WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()
    return row["objective"] if row else None


def get_relevant_metrics(objective: str) -> list:
    """
    Return which metrics to monitor for this campaign type.
    Falls back to OUTCOME_APP_PROMOTION if objective not mapped
    (safe default since most of your campaigns are app promotion).
    """
    return METRIC_MAPPING.get(objective, METRIC_MAPPING["OUTCOME_APP_PROMOTION"])


def get_baseline_data(
    campaign_id: str, test_date: str, window: int, conn: sqlite3.Connection
) -> list:
    """
    Fetch the WINDOW days of data BEFORE test_date for a campaign.
    
    Example: test_date = 2026-03-29, window = 14
    → fetches data from 2026-03-15 to 2026-03-28
    
    This is the "normal" behavior we compare against.
    """
    test_dt = datetime.strptime(test_date, "%Y-%m-%d")
    start_dt = test_dt - timedelta(days=window)

    rows = conn.execute(
        """
        SELECT * FROM daily_metrics
        WHERE campaign_id = ?
          AND day >= ?
          AND day < ?
        ORDER BY day
        """,
        (campaign_id, start_dt.strftime("%Y-%m-%d"), test_date),
    ).fetchall()

    return [dict(r) for r in rows]


def get_test_day_data(
    campaign_id: str, test_date: str, conn: sqlite3.Connection
) -> Optional[Dict]:
    """Fetch a single day's metrics for a campaign."""
    row = conn.execute(
        """
        SELECT * FROM daily_metrics
        WHERE campaign_id = ? AND day = ?
        """,
        (campaign_id, test_date),
    ).fetchone()

    return dict(row) if row else None


def compute_z_score(value: float, mean: float, std: float) -> Optional[float]:
    """
    Z-score = (value - mean) / std
    
    Returns None if std is 0 (metric never varies = can't detect anomalies)
    or if value is None.
    """
    if value is None or std == 0:
        return None
    return (value - mean) / std


def classify_anomaly(metric: str, z_score: float) -> dict:
    """
    Determine if the anomaly is GOOD or BAD based on metric direction.
    
    Example:
    - CTR z-score = +3.0 → CTR spiked → GOOD (higher is better)
    - CPI z-score = +3.0 → CPI spiked → BAD (lower is better)
    - CTR z-score = -3.0 → CTR dropped → BAD
    """
    direction = "spike" if z_score > 0 else "drop"

    if metric in HIGHER_IS_BETTER:
        severity = "positive" if z_score > 0 else "negative"
    elif metric in LOWER_IS_BETTER:
        severity = "negative" if z_score > 0 else "positive"
    else:
        severity = "neutral"

    return {"direction": direction, "severity": severity}


def detect_anomalies(test_date: str, campaign_ids: Optional[List[str]] = None) -> list:
    """
    Main detection function.
    
    Args:
        test_date: The day to check for anomalies (e.g. "2026-03-29")
        campaign_ids: Optional list of specific campaigns. None = all campaigns.
    
    Returns:
        List of anomaly dicts, each containing:
        - campaign info (id, name, objective)
        - metric name
        - test day value, baseline mean, baseline std
        - z-score
        - direction (spike/drop) and severity (positive/negative/neutral)
    """
    conn = get_connection()
    anomalies = []

    if campaign_ids is None:
        rows = conn.execute("SELECT campaign_id FROM campaigns").fetchall()
        campaign_ids = [r["campaign_id"] for r in rows]

    for cid in campaign_ids:
        objective = get_campaign_objective(cid, conn)
        if not objective:
            continue

        metrics = get_relevant_metrics(objective)
        baseline = get_baseline_data(cid, test_date, BASELINE_WINDOW, conn)
        test_day = get_test_day_data(cid, test_date, conn)

        if not test_day:
            continue

        if len(baseline) < MIN_BASELINE_DAYS:
            continue

        for metric in metrics:
            baseline_values = [
                row[metric] for row in baseline
                if row.get(metric) is not None
            ]

            if len(baseline_values) < MIN_BASELINE_DAYS:
                continue

            test_value = test_day.get(metric)
            if test_value is None:
                continue

            mean = sum(baseline_values) / len(baseline_values)
            variance = sum((v - mean) ** 2 for v in baseline_values) / len(
                baseline_values
            )
            std = variance ** 0.5

            z = compute_z_score(test_value, mean, std)
            if z is None:
                continue

            if abs(z) >= Z_SCORE_THRESHOLD:
                classification = classify_anomaly(metric, z)

                camp_row = conn.execute(
                    "SELECT campaign_name FROM campaigns WHERE campaign_id = ?",
                    (cid,),
                ).fetchone()

                anomalies.append(
                    {
                        "campaign_id": cid,
                        "campaign_name": camp_row["campaign_name"] if camp_row else cid,
                        "objective": objective,
                        "test_date": test_date,
                        "metric": metric,
                        "test_value": round(test_value, 2),
                        "baseline_mean": round(mean, 2),
                        "baseline_std": round(std, 2),
                        "z_score": round(z, 2),
                        "direction": classification["direction"],
                        "severity": classification["severity"],
                        "baseline_days": len(baseline_values),
                    }
                )

    conn.close()

    anomalies.sort(key=lambda x: abs(x["z_score"]), reverse=True)
    return anomalies


# ── Quick test runner ──────────────────────────────────────────────
# ── Detailed test runner ───────────────────────────────────────────
if __name__ == "__main__":
    from src.schema import get_connection as _get_conn

    test = "2026-03-28"
    print(f"\n{'='*80}")
    print(f"  ANOMALY DETECTION REPORT — {test}")
    print(f"{'='*80}\n")

    results = detect_anomalies(test)

    if not results:
        print("No anomalies detected.")
    else:
        # Group anomalies by campaign for cleaner reading
        from collections import defaultdict
        by_campaign = defaultdict(list)
        for a in results:
            by_campaign[a["campaign_id"]].append(a)

        conn = _get_conn()

        print(f"Total anomalies: {len(results)} across {len(by_campaign)} campaigns\n")

        for cid, camp_anomalies in by_campaign.items():
            # Fetch campaign metadata
            camp = conn.execute(
                "SELECT * FROM campaigns WHERE campaign_id = ?", (cid,)
            ).fetchone()

            # Fetch adset details for this campaign
            adsets = conn.execute(
                "SELECT adset_name, geo, platform, gender, final_budget, budget_source "
                "FROM adsets WHERE campaign_id = ?", (cid,)
            ).fetchall()

            # Fetch test day spend
            test_row = conn.execute(
                "SELECT amount_spent_inr, reach, impressions, registrations_completed, "
                "app_installs, purchases, purchase_roas "
                "FROM daily_metrics WHERE campaign_id = ? AND day = ?",
                (cid, test),
            ).fetchone()

            total_budget = sum(a["final_budget"] for a in adsets if a["final_budget"])
            geos = ", ".join(sorted(set(a["geo"] for a in adsets)))
            platforms = ", ".join(sorted(set(a["platform"] for a in adsets)))
            genders = ", ".join(sorted(set(a["gender"] for a in adsets)))

            neg_count = sum(1 for a in camp_anomalies if a["severity"] == "negative")
            pos_count = sum(1 for a in camp_anomalies if a["severity"] == "positive")

            print(f"{'─'*80}")
            print(f"  CAMPAIGN: {camp['campaign_name']}")
            print(f"{'─'*80}")
            print(f"  ID:            {cid}")
            print(f"  Objective:     {camp['objective']}")
            print(f"  Running since: {camp['start_time']}")
            print(f"  Geos:          {geos}")
            print(f"  Platforms:     {platforms}")
            print(f"  Genders:       {genders}")
            print(f"  Total budget:  ₹{total_budget:,.0f}/day across {len(adsets)} adsets")

            if test_row:
                print(f"\n  Test day snapshot ({test}):")
                print(f"    Spend: ₹{test_row['amount_spent_inr']:,.2f}" if test_row['amount_spent_inr'] else "    Spend: N/A")
                print(f"    Reach: {test_row['reach']:,.0f}" if test_row['reach'] else "    Reach: N/A")
                print(f"    Registrations: {test_row['registrations_completed']}" if test_row['registrations_completed'] else "    Registrations: N/A")
                print(f"    Installs: {test_row['app_installs']}" if test_row['app_installs'] else "    Installs: N/A")
                print(f"    Purchases: {test_row['purchases']}" if test_row['purchases'] else "    Purchases: N/A")
                print(f"    ROAS: {test_row['purchase_roas']:.2f}" if test_row['purchase_roas'] else "    ROAS: N/A")

            print(f"\n  Anomalies: {len(camp_anomalies)} ({neg_count} negative, {pos_count} positive)")
            print()

            for a in sorted(camp_anomalies, key=lambda x: abs(x["z_score"]), reverse=True):
                icon = "🔴" if a["severity"] == "negative" else "🟢" if a["severity"] == "positive" else "🟡"
                pct_change = ((a["test_value"] - a["baseline_mean"]) / a["baseline_mean"] * 100) if a["baseline_mean"] != 0 else 0

                print(f"    {icon} {a['metric']}")
                print(f"       Today: {a['test_value']}  |  14d avg: {a['baseline_mean']} ± {a['baseline_std']}")
                print(f"       Z-score: {a['z_score']}  |  Change: {pct_change:+.1f}%  |  {a['direction'].upper()}")
                print()

            # Show adset breakdown
            print(f"  Adset breakdown:")
            for ad in adsets:
                print(f"    • {ad['adset_name']:25s} | {ad['geo']:12s} | {ad['platform']:8s} | {ad['gender']:6s} | ₹{ad['final_budget']:,.0f}/day ({ad['budget_source']})")
            print()

        conn.close()

        # Summary
        print(f"\n{'='*80}")
        print(f"  SUMMARY")
        print(f"{'='*80}")
        all_neg = [a for a in results if a["severity"] == "negative"]
        all_pos = [a for a in results if a["severity"] == "positive"]
        print(f"  🔴 Negative anomalies (needs attention): {len(all_neg)}")
        print(f"  🟢 Positive anomalies (good surprises):  {len(all_pos)}")
        if all_neg:
            worst = all_neg[0]
            print(f"\n  ⚠️  Worst anomaly:")
            print(f"     {worst['campaign_name'][:50]}")
            print(f"     {worst['metric']} = {worst['test_value']} (z-score: {worst['z_score']})")