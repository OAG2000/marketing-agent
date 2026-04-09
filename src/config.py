"""
CONFIG.PY - All tunable settings in one place.

Think of this as your agent's "preferences". It defines:
- Which metrics matter for which campaign objective
- How sensitive the anomaly detection is (z-score threshold)
- How many days of history to use as baseline
"""

# How many past days to use for calculating "normal" behavior
# Too short (7) = noisy baseline, too long (30) = slow to adapt
BASELINE_WINDOW = 14

# Z-score threshold: how many standard deviations = anomaly?
# ±2.0 catches ~5% of days as anomalies (good starting point)
# ±1.5 = more sensitive (more alerts), ±2.5 = less sensitive (fewer alerts)
Z_SCORE_THRESHOLD = 2.0

# Minimum days of data needed before we can detect anomalies
# (need enough history to compute meaningful mean/std)
MIN_BASELINE_DAYS = 7

# Which metrics to track per campaign objective
# Key insight: not all metrics matter for all campaigns
# e.g., tracking ROAS on an awareness campaign is meaningless
METRIC_MAPPING = {
    "OUTCOME_APP_PROMOTION": [
        "amount_spent_inr",
        "cost_per_result",
        "registrations_completed",
        "cost_per_registration_completed",
        "app_installs",
        "cost_per_app_install",
        "ctr_link",
        "cpc_link",
        "cpm",
    ],
    "OUTCOME_AWARENESS": [
        "amount_spent_inr",
        "reach",
        "frequency",
        "cpm",
        "ctr_link",
        "impressions",
    ],
    "OUTCOME_SALES": [
        "amount_spent_inr",
        "purchases",
        "purchase_roas",
        "cost_per_purchase",
        "purchases_conversion_value",
        "ctr_link",
        "cpm",
    ],
    "CONVERSIONS": [
        "amount_spent_inr",
        "cost_per_result",
        "results",
        "registrations_completed",
        "cost_per_registration_completed",
        "ctr_link",
        "cpc_link",
        "cpm",
    ],
}

# Metrics where LOWER is worse (drop = bad)
# vs metrics where HIGHER is worse (spike = bad)
# This helps the agent say "CTR dropped" not just "CTR anomaly"
HIGHER_IS_BETTER = [
    "results",
    "registrations_completed",
    "app_installs",
    "purchases",
    "purchase_roas",
    "purchases_conversion_value",
    "reach",
    "ctr_link",
    "ctr_all",
]

LOWER_IS_BETTER = [
    "cost_per_result",
    "cost_per_registration_completed",
    "cost_per_app_install",
    "cost_per_purchase",
    "cpc_link",
    "cpm",
    "frequency",
]