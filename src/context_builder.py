"""
CONTEXT_BUILDER.PY - Assembles rich context for LLM root cause analysis.

The detector says WHAT is anomalous. This module answers WHY it might be,
by gathering all the surrounding context a human media buyer would check:

1. Campaign metadata (objective, age, geos, platforms)
2. Adset structure (budget distribution, targeting details)
3. Correlated anomalies (multiple metrics moving together = stronger signal)
4. Recent trend (last 5 days) so LLM can see if it's sudden or gradual
5. Cross-campaign comparison — NOW SEGMENTED BY GEO REGION + PLATFORM
"""

from typing import List, Dict, Optional
from collections import defaultdict
from datetime import datetime, timedelta
from src.schema import get_connection


def get_campaign_context(campaign_id: str, conn) -> dict:
    """
    Pull everything we know about a campaign: metadata + adsets.
    """
    camp = conn.execute(
        "SELECT * FROM campaigns WHERE campaign_id = ?", (campaign_id,)
    ).fetchone()

    adsets = conn.execute(
        "SELECT * FROM adsets WHERE campaign_id = ?", (campaign_id,)
    ).fetchall()

    if not camp:
        return {}

    total_budget = sum(a["final_budget"] for a in adsets if a["final_budget"])
    geos = list(set(a["geo"] for a in adsets))
    platforms = list(set(a["platform"] for a in adsets))
    genders = list(set(a["gender"] for a in adsets))

    start = camp["start_time"]
    if start:
        try:
            age_days = (datetime.now() - datetime.strptime(start, "%Y-%m-%d")).days
        except ValueError:
            age_days = None
    else:
        age_days = None

    return {
        "campaign_id": campaign_id,
        "campaign_name": camp["campaign_name"],
        "objective": camp["objective"],
        "start_time": start,
        "campaign_age_days": age_days,
        "total_daily_budget": total_budget,
        "num_adsets": len(adsets),
        "geos": geos,
        "platforms": platforms,
        "genders": genders,
        "adsets": [
            {
                "name": a["adset_name"],
                "geo": a["geo"],
                "platform": a["platform"],
                "gender": a["gender"],
                "budget": a["final_budget"],
                "budget_source": a["budget_source"],
            }
            for a in adsets
        ],
    }


def get_recent_trend(campaign_id: str, test_date: str, metric: str, days: int = 5, conn=None) -> list:
    """
    Fetch the last N days of a specific metric for a campaign.
    
    This lets the LLM see the SHAPE of the problem:
    - [30, 31, 29, 30, 85] = sudden spike on last day
    - [30, 35, 42, 55, 85] = gradual increase
    - [30, 28, 31, 5, 85]  = volatile
    """
    test_dt = datetime.strptime(test_date, "%Y-%m-%d")
    start_dt = test_dt - timedelta(days=days - 1)

    rows = conn.execute(
        """
        SELECT day, {} FROM daily_metrics
        WHERE campaign_id = ? AND day >= ? AND day <= ?
        ORDER BY day
        """.format(metric),
        (campaign_id, start_dt.strftime("%Y-%m-%d"), test_date),
    ).fetchall()

    return [{"day": r["day"], "value": r[metric]} for r in rows]


def get_cross_campaign_check(test_date: str, metric: str, campaign_id: str, conn) -> dict:
    """
    Compare this campaign's metric move vs SIMILAR campaigns.
    
    OLD APPROACH: Compared against ALL 15 campaigns (meaningless —
    mixing India Android with GCC iOS gives useless averages)
    
    NEW APPROACH: Segments by geo region + platform.
    - KSA iOS campaign compared against other GCC iOS campaigns
    - India Android campaign compared against other India Android campaigns
    
    Matching priority:
    1. Same geo region + same platform (best comparison)
    2. Same geo region + any platform (fallback)
    3. All campaigns (last resort if <2 similar campaigns exist)
    """
    test_dt = datetime.strptime(test_date, "%Y-%m-%d")
    baseline_start = (test_dt - timedelta(days=7)).strftime("%Y-%m-%d")

    # ── Step 1: Figure out this campaign's geo region and platform ──
    adsets = conn.execute(
        "SELECT geo, platform FROM adsets WHERE campaign_id = ?", (campaign_id,)
    ).fetchall()

    if not adsets:
        return _compute_market_comparison(
            test_date, baseline_start, metric, campaign_id, conn,
            filter_sql="", filter_params=[], match_level="all_campaigns"
        )

    campaign_geos = set(a["geo"] for a in adsets)
    campaign_platforms = set(a["platform"] for a in adsets)

    # Classify into geo regions
    # GCC countries have similar auction dynamics, audience sizes, and costs
    # Indian states share the India auction pool
    gcc_countries = {"UAE", "Saudi", "Qatar", "Kuwait", "Oman", "Bahrain"}
    india_states = {"India", "Kerala", "Karnataka", "Tamil Nadu", "Telangana",
                    "West Bengal", "J&K"}

    is_gcc = bool(campaign_geos & gcc_countries)
    is_india = bool(campaign_geos & india_states)

    if is_gcc:
        region_geos = gcc_countries
        region_name = "GCC"
    elif is_india:
        region_geos = india_states
        region_name = "India"
    else:
        region_geos = campaign_geos
        region_name = "Same geo"

    # ── Step 2: Find similar campaigns ──
    geo_placeholders = ",".join(["?"] * len(region_geos))

    # All campaigns in same geo region
    similar_campaigns = conn.execute(
        f"""
        SELECT DISTINCT a.campaign_id FROM adsets a
        WHERE a.geo IN ({geo_placeholders})
          AND a.campaign_id != ?
        """,
        list(region_geos) + [campaign_id],
    ).fetchall()
    similar_ids = [r["campaign_id"] for r in similar_campaigns]

    # Further filter by platform (iOS vs Android matters a lot)
    platform_filtered = []
    if "All" not in campaign_platforms:
        platform_list = list(campaign_platforms)
        plat_placeholders = ",".join(["?"] * len(platform_list))
        platform_campaigns = conn.execute(
            f"""
            SELECT DISTINCT a.campaign_id FROM adsets a
            WHERE a.geo IN ({geo_placeholders})
              AND a.platform IN ({plat_placeholders})
              AND a.campaign_id != ?
            """,
            list(region_geos) + platform_list + [campaign_id],
        ).fetchall()
        platform_filtered = [r["campaign_id"] for r in platform_campaigns]

    # ── Step 3: Pick the best comparison group ──
    # Need at least 2 campaigns to make a meaningful average
    if len(platform_filtered) >= 2:
        compare_ids = platform_filtered
        match_level = f"same_region_{region_name}_same_platform"
    elif len(similar_ids) >= 2:
        compare_ids = similar_ids
        match_level = f"same_region_{region_name}"
    else:
        compare_ids = None
        match_level = "all_campaigns"

    # ── Step 4: Compute the comparison ──
    if compare_ids:
        id_placeholders = ",".join(["?"] * len(compare_ids))
        filter_sql = f"AND campaign_id IN ({id_placeholders})"
        filter_params = compare_ids
    else:
        filter_sql = ""
        filter_params = []

    return _compute_market_comparison(
        test_date, baseline_start, metric, campaign_id, conn,
        filter_sql=filter_sql, filter_params=filter_params,
        match_level=match_level
    )


def _compute_market_comparison(
    test_date, baseline_start, metric, campaign_id, conn,
    filter_sql="", filter_params=[], match_level="all_campaigns"
) -> dict:
    """
    Core math for cross-campaign comparison.
    
    Computes:
    1. Market segment's 7-day baseline avg
    2. Market segment's test day avg
    3. This campaign's 7-day baseline avg
    4. This campaign's test day value
    5. % change for both
    6. Diagnosis based on relative magnitude
    """
    # Market segment baseline (7-day avg, excluding this campaign)
    market_baseline = conn.execute(
        f"""
        SELECT AVG({metric}) as avg_val, COUNT(DISTINCT campaign_id) as cnt
        FROM daily_metrics
        WHERE day >= ? AND day < ? AND {metric} IS NOT NULL
          AND campaign_id != ?
          {filter_sql}
        """,
        [baseline_start, test_date, campaign_id] + filter_params,
    ).fetchone()

    # Market segment test day
    market_today = conn.execute(
        f"""
        SELECT AVG({metric}) as avg_val, COUNT(DISTINCT campaign_id) as cnt
        FROM daily_metrics
        WHERE day = ? AND {metric} IS NOT NULL
          AND campaign_id != ?
          {filter_sql}
        """,
        [test_date, campaign_id] + filter_params,
    ).fetchone()

    # This campaign's baseline
    camp_baseline = conn.execute(
        f"""
        SELECT AVG({metric}) as avg_val FROM daily_metrics
        WHERE campaign_id = ? AND day >= ? AND day < ? AND {metric} IS NOT NULL
        """,
        (campaign_id, baseline_start, test_date),
    ).fetchone()

    # This campaign's test day
    camp_today = conn.execute(
        f"""
        SELECT {metric} as val FROM daily_metrics
        WHERE campaign_id = ? AND day = ? AND {metric} IS NOT NULL
        """,
        (campaign_id, test_date),
    ).fetchone()

    # Calculate % changes
    mkt_base = market_baseline["avg_val"] if market_baseline else None
    mkt_today_val = market_today["avg_val"] if market_today else None
    camp_base = camp_baseline["avg_val"] if camp_baseline else None
    camp_today_val = camp_today["val"] if camp_today else None

    market_pct_change = None
    campaign_pct_change = None

    if mkt_base and mkt_today_val and mkt_base != 0:
        market_pct_change = round(((mkt_today_val - mkt_base) / mkt_base) * 100, 2)

    if camp_base and camp_today_val and camp_base != 0:
        campaign_pct_change = round(((camp_today_val - camp_base) / camp_base) * 100, 2)

    # Diagnosis logic:
    # - Market moved < 3% → campaign-specific (market is stable)
    # - Campaign moved > 2x market → campaign-specific with market pressure
    # - Otherwise → market-wide (everyone affected similarly)
    # if market_pct_change is not None and campaign_pct_change is not None:
    #     if abs(market_pct_change) < 3:
    #         diagnosis = "campaign_specific"
    #     elif abs(campaign_pct_change) > abs(market_pct_change) * 2:
    #         diagnosis = "campaign_specific_with_market_pressure"
    #     else:
    #         diagnosis = "market_wide"
    if market_pct_change is not None and campaign_pct_change is not None:
        # Rule 1: If they moved in OPPOSITE directions, it's campaign-specific.
        # You got worse while everyone else improved = your problem.
        opposite_directions = (
            (campaign_pct_change > 0 and market_pct_change < 0) or
            (campaign_pct_change < 0 and market_pct_change > 0)
        )

        if opposite_directions:
            diagnosis = "campaign_specific"

        # Rule 2: Market barely moved → campaign-specific
        elif abs(market_pct_change) < 3:
            diagnosis = "campaign_specific"

        # Rule 3: Campaign moved way more than market → campaign-specific with pressure
        elif abs(campaign_pct_change) > abs(market_pct_change) * 2:
            diagnosis = "campaign_specific_with_market_pressure"

        # Rule 4: Both moved similarly in same direction → market-wide
        else:
            diagnosis = "market_wide"
    else:
        diagnosis = "unknown"

    return {
        "metric": metric,
        "market_7d_baseline": round(mkt_base, 2) if mkt_base else None,
        "market_today": round(mkt_today_val, 2) if mkt_today_val else None,
        "market_pct_change": market_pct_change,
        "campaign_7d_baseline": round(camp_base, 2) if camp_base else None,
        "campaign_today": round(camp_today_val, 2) if camp_today_val else None,
        "campaign_pct_change": campaign_pct_change,
        "diagnosis": diagnosis,
        "match_level": match_level,
        "num_comparison_campaigns": market_today["cnt"] if market_today else 0,
    }


def build_context(anomalies: list) -> list:
    """
    Main function: takes anomaly list from detector, enriches each
    campaign's anomalies with full context, returns structured dicts
    ready to be formatted into LLM prompts.
    """
    if not anomalies:
        return []

    conn = get_connection()

    by_campaign = defaultdict(list)
    for a in anomalies:
        by_campaign[a["campaign_id"]].append(a)

    enriched = []

    for cid, camp_anomalies in by_campaign.items():
        test_date = camp_anomalies[0]["test_date"]

        # 1. Campaign + adset metadata
        context = get_campaign_context(cid, conn)
        if not context:
            continue

        # 2. Recent trend for each anomalous metric (last 5 days)
        trends = {}
        for a in camp_anomalies:
            trends[a["metric"]] = get_recent_trend(
                cid, test_date, a["metric"], days=5, conn=conn
            )

        # 3. Cross-campaign check — NOW SEGMENTED BY GEO + PLATFORM
        cross_checks = {}
        for a in camp_anomalies:
            if a["metric"] not in cross_checks:
                cross_checks[a["metric"]] = get_cross_campaign_check(
                    test_date, a["metric"], cid, conn
                )

        # 4. Correlation analysis
        neg_metrics = [a["metric"] for a in camp_anomalies if a["severity"] == "negative"]
        pos_metrics = [a["metric"] for a in camp_anomalies if a["severity"] == "positive"]

        # 5. Package everything
        enriched.append(
            {
                "campaign": context,
                "test_date": test_date,
                "anomalies": camp_anomalies,
                "trends": trends,
                "cross_campaign": cross_checks,
                "correlated_negative": neg_metrics,
                "correlated_positive": pos_metrics,
            }
        )

    conn.close()
    return enriched


def format_prompt(enriched_context: dict) -> str:
    """
    Convert one campaign's enriched context into a readable LLM prompt.
    """
    c = enriched_context
    camp = c["campaign"]

    lines = []
    lines.append("=" * 60)
    lines.append(f"CAMPAIGN UNDER REVIEW: {camp['campaign_name']}")
    lines.append("=" * 60)

    # Section 1: Campaign identity
    lines.append("\n## CAMPAIGN PROFILE")
    lines.append(f"- Campaign ID: {camp['campaign_id']}")
    lines.append(f"- Objective: {camp['objective']}")
    lines.append(f"- Running since: {camp['start_time']} ({camp['campaign_age_days']} days old)" if camp['campaign_age_days'] else f"- Running since: {camp['start_time']}")
    lines.append(f"- Total daily budget: ₹{camp['total_daily_budget']:,.0f}")
    lines.append(f"- Number of adsets: {camp['num_adsets']}")
    lines.append(f"- Target geos: {', '.join(camp['geos'])}")
    lines.append(f"- Platforms: {', '.join(camp['platforms'])}")
    lines.append(f"- Gender targeting: {', '.join(camp['genders'])}")

    # Section 2: Adset breakdown
    lines.append("\n## ADSET STRUCTURE")
    for ad in camp["adsets"]:
        lines.append(
            f"  • {ad['name']:25s} | Geo: {ad['geo']:12s} | Platform: {ad['platform']:8s} | "
            f"Gender: {ad['gender']:6s} | Budget: ₹{ad['budget']:,.0f}/day ({ad['budget_source']})"
        )

    # Section 3: Anomalies detected
    lines.append(f"\n## ANOMALIES DETECTED ON {c['test_date']}")
    lines.append(f"Total: {len(c['anomalies'])} anomalies")

    if c["correlated_negative"]:
        lines.append(f"⚠ Correlated NEGATIVE anomalies: {', '.join(c['correlated_negative'])}")
    if c["correlated_positive"]:
        lines.append(f"✓ Correlated POSITIVE anomalies: {', '.join(c['correlated_positive'])}")

    for a in sorted(c["anomalies"], key=lambda x: abs(x["z_score"]), reverse=True):
        pct = ((a["test_value"] - a["baseline_mean"]) / a["baseline_mean"] * 100) if a["baseline_mean"] != 0 else 0
        severity_icon = "🔴" if a["severity"] == "negative" else "🟢" if a["severity"] == "positive" else "🟡"
        lines.append(f"\n  {severity_icon} {a['metric'].upper()}")
        lines.append(f"    Today's value:  {a['test_value']}")
        lines.append(f"    14-day average: {a['baseline_mean']} ± {a['baseline_std']}")
        lines.append(f"    Z-score:        {a['z_score']} ({a['direction']}, {a['severity']})")
        lines.append(f"    Change:         {pct:+.1f}% from baseline")

    # Section 4: Recent trends
    lines.append("\n## RECENT TRENDS (last 5 days)")
    for metric, trend in c["trends"].items():
        values = [f"{t['day']}: {t['value']}" for t in trend]
        lines.append(f"  {metric}: {' → '.join(values)}")

    # Section 5: Cross-campaign comparison — NOW GEO + PLATFORM SEGMENTED
    lines.append("\n## CROSS-CAMPAIGN CHECK (geo & platform segmented)")
    for metric, check in c["cross_campaign"].items():
        if check["market_pct_change"] is not None and check["campaign_pct_change"] is not None:
            lines.append(
                f"  {metric}:"
                f"\n    This campaign: {check['campaign_7d_baseline']} → {check['campaign_today']} ({check['campaign_pct_change']:+.1f}%)"
                f"\n    Segment avg:   {check['market_7d_baseline']} → {check['market_today']} ({check['market_pct_change']:+.1f}%)"
                f"\n    Compared against: {check['match_level']} ({check['num_comparison_campaigns']} campaigns)"
                f"\n    Diagnosis:     {check['diagnosis'].upper().replace('_', ' ')}"
            )

    return "\n".join(lines)


# ── Test runner ────────────────────────────────────────────────────
if __name__ == "__main__":
    from src.detector import detect_anomalies

    test_date = "2026-03-29"
    print(f"\nDetecting anomalies for {test_date}...")
    anomalies = detect_anomalies(test_date)
    print(f"Found {len(anomalies)} anomalies. Building context...\n")

    enriched_list = build_context(anomalies)

    for ec in enriched_list:
        prompt = format_prompt(ec)
        print(prompt)
        print("\n" + "=" * 80 + "\n")