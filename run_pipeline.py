"""
RUN_PIPELINE.PY - Ties everything together.

Usage:
    python run_pipeline.py                    # analyze latest day in DB
    python run_pipeline.py 2026-03-29         # analyze specific date
    python run_pipeline.py 2026-03-25 2026-03-29  # analyze date range

This is the one command that runs the entire anomaly detection pipeline:
    Detect anomalies → Build context → Send to Claude → Print report
"""
from dotenv import load_dotenv
load_dotenv()
import sys
from datetime import datetime, timedelta
from src.detector import detect_anomalies
from src.context_builder import build_context, format_prompt
from src.analyzer import analyze_all
from src.schema import get_connection



def get_latest_date():
    """Get the most recent date in the database."""
    conn = get_connection()
    row = conn.execute("SELECT MAX(day) as max_day FROM daily_metrics").fetchone()
    conn.close()
    return row["max_day"]


def run_pipeline(test_date: str):
    """Run full pipeline for a single date."""
    print(f"\n{'='*80}")
    print(f"  MARKETING ANOMALY AGENT — {test_date}")
    print(f"{'='*80}")

    # Phase 1: Detection
    print(f"\n🔍 Phase 1: Detecting anomalies...")
    anomalies = detect_anomalies(test_date)

    if not anomalies:
        print(f"  ✅ No anomalies detected for {test_date}. All campaigns normal.")
        return

    neg = sum(1 for a in anomalies if a["severity"] == "negative")
    pos = sum(1 for a in anomalies if a["severity"] == "positive")
    print(f"  Found {len(anomalies)} anomalies ({neg} negative, {pos} positive)")

    # Phase 2: Context building
    print(f"\n📋 Phase 2: Building diagnostic context...")
    enriched_list = build_context(anomalies)
    prompt_texts = [format_prompt(ec) for ec in enriched_list]
    print(f"  Built context for {len(enriched_list)} campaigns")

    # Phase 3: LLM Analysis
    print(f"\n🤖 Phase 3: Claude analyzing root causes...")
    results = analyze_all(enriched_list, prompt_texts)

    # Phase 4: Report
    print(f"\n\n{'='*80}")
    print(f"  📊 FINAL REPORT — {test_date}")
    print(f"{'='*80}")

    for r in results:
        print(f"\n{'─'*80}")
        print(f"  Campaign: {r['campaign_name']}")
        print(f"  Anomalies detected: {r['num_anomalies']}")
        print(f"{'─'*80}\n")
        print(r["analysis"])
        print()

    # Summary footer
    print(f"\n{'='*80}")
    print(f"  Pipeline complete. {len(anomalies)} anomalies across {len(results)} campaigns analyzed.")
    print(f"{'='*80}\n")


def main():
    if len(sys.argv) == 1:
        # No args: use latest date in DB
        date = get_latest_date()
        print(f"No date specified. Using latest date in DB: {date}")
        run_pipeline(date)

    elif len(sys.argv) == 2:
        # Single date
        run_pipeline(sys.argv[1])

    elif len(sys.argv) == 3:
        # Date range
        start = datetime.strptime(sys.argv[1], "%Y-%m-%d")
        end = datetime.strptime(sys.argv[2], "%Y-%m-%d")
        current = start
        while current <= end:
            run_pipeline(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)

    else:
        print("Usage:")
        print("  python run_pipeline.py                         # latest date")
        print("  python run_pipeline.py 2026-03-29              # specific date")
        print("  python run_pipeline.py 2026-03-25 2026-03-29   # date range")


if __name__ == "__main__":
    main()