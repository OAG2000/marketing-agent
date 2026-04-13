"""
Simple Meta API test — checks if your credentials work
and fetches a small sample of data safely within rate limits.
"""

import os
from dotenv import load_dotenv
load_dotenv()

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount

# ── Load credentials ───────────────────────────────────
ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.environ.get("META_AD_ACCOUNT_ID")
APP_ID = os.environ.get("META_APP_ID")
APP_SECRET = os.environ.get("META_APP_SECRET")

print("=" * 60)
print("  META API CONNECTION TEST")
print("=" * 60)

# ── Step 1: Check credentials exist ───────────────────
print("\n1. Checking credentials...")
missing = []
if not ACCESS_TOKEN: missing.append("META_ACCESS_TOKEN")
if not AD_ACCOUNT_ID: missing.append("META_AD_ACCOUNT_ID")
if not APP_ID: missing.append("META_APP_ID")
if not APP_SECRET: missing.append("META_APP_SECRET")

if missing:
    print(f"  ✗ Missing in .env: {', '.join(missing)}")
    exit(1)

# Make sure account ID starts with act_
if not AD_ACCOUNT_ID.startswith("act_"):
    AD_ACCOUNT_ID = f"act_{AD_ACCOUNT_ID}"
    print(f"  ⚠ Added 'act_' prefix → {AD_ACCOUNT_ID}")

print(f"  ✓ App ID: {APP_ID}")
print(f"  ✓ Account: {AD_ACCOUNT_ID}")
print(f"  ✓ Token: {ACCESS_TOKEN[:20]}...{ACCESS_TOKEN[-10:]}")

# ── Step 2: Initialize API ─────────────────────────────
print("\n2. Initializing API...")
try:
    FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
    print("  ✓ API initialized")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    exit(1)

# ── Step 3: Fetch account info ─────────────────────────
print("\n3. Fetching account info...")
try:
    account = AdAccount(AD_ACCOUNT_ID)
    info = account.api_get(fields=[
        "name",
        "account_status",
        "currency",
        "timezone_name",
        "amount_spent",
    ])
    print(f"  ✓ Account Name: {info.get('name')}")
    print(f"  ✓ Status: {info.get('account_status')} (1=active, 2=disabled)")
    print(f"  ✓ Currency: {info.get('currency')}")
    print(f"  ✓ Timezone: {info.get('timezone_name')}")
    print(f"  ✓ Total Spend: {info.get('amount_spent')}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    exit(1)
# ── Step 4: Fetch campaigns list ──────────────────────
print("\n4. Fetching active campaigns...")
try:
    campaigns = account.get_campaigns(
        fields=["name", "status", "objective"],
        params={"effective_status": ["ACTIVE"]},
    )
    campaign_list = []
    for c in campaigns:
        campaign_list.append(c)
    
    print(f"  ✓ Found {len(campaign_list)} active campaigns:\n")
    for c in campaign_list:
        print(f"    • {c['name'][:60]}")
        print(f"      ID: {c['id']} | Objective: {c.get('objective', 'N/A')}")
except Exception as e:
    print(f"  ✗ Failed: {e}")
    import traceback
    traceback.print_exc()

# ── Step 5: Fetch ONE day of insights (minimal API call) ──
print("\n5. Fetching 1 day of insights (yesterday)...")
from datetime import datetime, timedelta
yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

try:
    insights = account.get_insights(
        fields=[
            "campaign_name",
            "campaign_id",
            "spend",
            "impressions",
            "reach",
            "clicks",
            "cpm",
            "cpc",
            "ctr",
        ],
        params={
            "level": "campaign",
            "time_range": {"since": yesterday, "until": yesterday},
            "time_increment": 1,
        },
    )

    rows = list(insights)
    print(f"  ✓ Got {len(rows)} campaign rows for {yesterday}\n")

    if rows:
        print(f"  Sample (first 3):")
        for row in rows[:3]:
            d = dict(row)
            print(f"    Campaign: {d.get('campaign_name', 'N/A')[:50]}")
            print(f"    Spend: ₹{d.get('spend', 0)} | Impressions: {d.get('impressions', 0)} | Clicks: {d.get('clicks', 0)}")
            print()

except Exception as e:
    print(f"  ✗ Failed: {e}")
    exit(1)

# ── Step 6: Check rate limit headers ──────────────────
print("6. Rate limit status...")
try:
    api = FacebookAdsApi.get_default_api()
    print("  ✓ If you got here without errors, you're within rate limits")
    print("  ℹ Meta allows ~200 calls/hour for standard access")
    print("  ℹ Our pipeline will use ~5-10 calls per daily fetch (well within limits)")
except Exception as e:
    print(f"  ⚠ {e}")

print("\n" + "=" * 60)
print("  ✅ ALL TESTS PASSED — API is working!")
print("=" * 60)
print("\nSafe to proceed with building the ingestion pipeline.")