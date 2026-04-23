"""
Check your Meta API rate limit usage before making calls.
"""

import os
import requests
from dotenv import load_dotenv
load_dotenv()

ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.environ.get("META_AD_ACCOUNT_ID")

if not AD_ACCOUNT_ID.startswith("act_"):
    AD_ACCOUNT_ID = f"act_{AD_ACCOUNT_ID}"

# This is a lightweight call that returns rate limit headers
url = f"https://graph.facebook.com/v21.0/{AD_ACCOUNT_ID}"
params = {
    "access_token": ACCESS_TOKEN,
    "fields": "name",
}

response = requests.get(url, params=params)
headers = response.headers

print("=" * 60)
print("  META API RATE LIMIT STATUS")
print("=" * 60)

# Meta returns usage info in these headers
usage_header = headers.get("x-business-use-case-usage", "Not available")
app_usage = headers.get("x-app-usage", "Not available")
ad_account_usage = headers.get("x-ad-account-usage", "Not available")

print(f"\n  Business usage:    {usage_header[:200]}")
print(f"  App usage:         {app_usage}")
print(f"  Ad account usage:  {ad_account_usage}")

# Parse if available
import json
if usage_header != "Not available":
    try:
        usage = json.loads(usage_header)
        for account_id, limits in usage.items():
            for limit in limits:
                print(f"\n  Account: {account_id}")
                print(f"    Call count:    {limit.get('call_count', '?')}% used")
                print(f"    CPU time:      {limit.get('total_cputime', '?')}% used")
                print(f"    Total time:    {limit.get('total_time', '?')}% used")
                print(f"    Type:          {limit.get('type', '?')}")
                print(f"    Est. recovery: {limit.get('estimated_time_to_regain_access', '?')} minutes")

                # Traffic light
                call_pct = limit.get('call_count', 0)
                if call_pct < 50:
                    print(f"\n  🟢 SAFE — {100-call_pct}% remaining")
                elif call_pct < 80:
                    print(f"\n  🟡 MODERATE — {100-call_pct}% remaining, slow down")
                else:
                    print(f"\n  🔴 HIGH — only {100-call_pct}% remaining, wait before more calls")
    except json.JSONDecodeError:
        print("  Could not parse usage header")

print()