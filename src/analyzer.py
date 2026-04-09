"""
ANALYZER.PY - Sends enriched anomaly context to Claude for root cause analysis.

The context_builder assembled all the evidence. This module:
1. Wraps it in a system prompt that makes Claude a performance marketing analyst
2. Sends it to the Claude API
3. Parses and returns the structured diagnosis

The system prompt is the most important part — it tells Claude:
- Your role (senior media buyer reviewing a campaign)
- What to look for (common root causes in Meta ads)
- How to structure the response (root cause, evidence, action)
- What NOT to do (don't be generic, don't hallucinate fixes)
"""
from dotenv import load_dotenv
load_dotenv()
import os
import json
from typing import List, Dict

# We use the anthropic SDK — make sure ANTHROPIC_API_KEY is set in environment
try:
    import anthropic
except ImportError:
    print("Run: pip install anthropic")
    raise


# ── System prompt: this is what makes the LLM a marketing expert ──
SYSTEM_PROMPT = """You are a senior performance marketing analyst specializing in Meta (Facebook/Instagram) ads for mobile app campaigns. You're reviewing anomaly alerts from an automated detection system.

YOUR CONTEXT:
- You're analyzing campaigns for a mobile app running across India and GCC countries (UAE, KSA, Qatar, Kuwait, Oman, Bahrain)
- Campaigns target both Android and iOS, with objectives ranging from app installs to purchases to awareness
- The detection system uses z-score based anomaly detection on a 14-day rolling baseline

YOUR TASK:
For each anomaly report, provide:

1. **ROOT CAUSE ANALYSIS** — What is most likely causing this anomaly? Consider:
   - Creative fatigue (high frequency + declining CTR)
   - Audience saturation (old campaign + small geo + rising costs)
   - Budget/delivery issues (underspend, overspend, pacing problems)
   - Platform-level changes — BUT ONLY if the cross-campaign diagnosis says "MARKET WIDE". If the campaign moved >2x the market average, the root cause is campaign-specific even if the market also moved. Do NOT default to "market-wide" unless the data clearly supports it.
   - Seasonality or market events
   - Targeting overlap between adsets cannibalizing each other
   - iOS attribution issues (SKAdNetwork limitations)
   - Correlation patterns (multiple metrics moving together tells a story)

2. **EVIDENCE** — What specific data points from the report support your diagnosis?

3. **SEVERITY** — Rate as LOW / MEDIUM / HIGH / CRITICAL with reasoning

4. **RECOMMENDED ACTIONS** — Specific, actionable next steps (not generic advice). Reference actual adsets, geos, budgets where relevant.

5. **WHAT TO MONITOR** — What should the team watch over the next 2-3 days?

RULES:
- Be specific. Reference actual numbers, campaign names, geos, adsets from the data.
- If the cross-campaign check shows the metric is moving market-wide, say so clearly — don't blame the campaign for a platform-level issue.
- If anomalies are correlated (e.g., CTR drop + CPI spike), explain the causal chain.
- For positive anomalies (good surprises), still explain WHY and whether it's sustainable.
- Don't suggest actions that contradict the campaign's objective.
- Be concise. This is an operational alert, not an essay.
- CRITICAL: Do not attribute anomalies to "market-wide" causes unless the cross-campaign diagnosis explicitly says "MARKET WIDE". If it says "CAMPAIGN SPECIFIC" or "CAMPAIGN SPECIFIC WITH MARKET PRESSURE", the root cause is primarily within the campaign itself — focus on creative fatigue, audience saturation, budget issues, or targeting problems. Market pressure may be a secondary factor but never the primary root cause in these cases.
"""


def analyze_anomalies(enriched_context: dict, prompt_text: str) -> str:
    """
    Send one campaign's anomaly context to Claude and get root cause analysis.
    
    Args:
        enriched_context: The full context dict from context_builder
        prompt_text: The formatted prompt string from format_prompt()
    
    Returns:
        Claude's analysis as a string
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return "[ERROR] ANTHROPIC_API_KEY not set. Run: export ANTHROPIC_API_KEY='your-key-here'"

    client = anthropic.Anthropic(api_key=api_key)

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            system=SYSTEM_PROMPT,
            messages=[
                {
                    "role": "user",
                    "content": f"""Analyze the following anomaly report and provide your diagnosis:

{prompt_text}

Provide your analysis in the following format:
## Root Cause
## Evidence  
## Severity
## Recommended Actions
## What to Monitor""",
                }
            ],
        )
        return response.content[0].text

    except anthropic.APIError as e:
        return f"[API ERROR] {str(e)}"
    except Exception as e:
        return f"[ERROR] {str(e)}"


def analyze_all(enriched_list: list, prompt_texts: list) -> list:
    """
    Analyze all campaigns' anomalies. Returns list of
    (campaign_name, prompt, analysis) tuples.
    
    We process sequentially (not parallel) to respect rate limits
    and keep output readable.
    """
    results = []
    
    for ec, prompt in zip(enriched_list, prompt_texts):
        camp_name = ec["campaign"]["campaign_name"]
        print(f"\n🤖 Analyzing: {camp_name[:60]}...")
        
        analysis = analyze_anomalies(ec, prompt)
        results.append({
            "campaign_name": camp_name,
            "campaign_id": ec["campaign"]["campaign_id"],
            "num_anomalies": len(ec["anomalies"]),
            "prompt": prompt,
            "analysis": analysis,
        })
        print(f"   ✓ Done")
    
    return results


# ── Test runner ────────────────────────────────────────────────────
if __name__ == "__main__":
    from src.detector import detect_anomalies
    from src.context_builder import build_context, format_prompt

    test_date = "2026-03-29"

    print(f"Step 1: Detecting anomalies for {test_date}...")
    anomalies = detect_anomalies(test_date)
    print(f"  Found {len(anomalies)} anomalies")

    print(f"\nStep 2: Building context...")
    enriched_list = build_context(anomalies)
    prompt_texts = [format_prompt(ec) for ec in enriched_list]
    print(f"  Built context for {len(enriched_list)} campaigns")

    print(f"\nStep 3: Sending to Claude for analysis...")
    results = analyze_all(enriched_list, prompt_texts)

    # Print full report
    print(f"\n\n{'='*80}")
    print(f"  ANOMALY ANALYSIS REPORT — {test_date}")
    print(f"{'='*80}")

    for r in results:
        print(f"\n{'─'*80}")
        print(f"  📊 {r['campaign_name']}")
        print(f"  Anomalies: {r['num_anomalies']}")
        print(f"{'─'*80}")
        print(r["analysis"])
        print()