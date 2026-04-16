# """
# META_API.PY - Fetches campaign metrics from Meta Marketing API.

# Replaces manual CSV export. Pulls the same metrics your CSV had.
# Seed CSV data stays in DB — this adds new days on top.

# Rate limit safe: uses 2-3 API calls per fetch (well under 200/hr limit).
# """

# import os
# from datetime import datetime, timedelta
# from dotenv import load_dotenv

# load_dotenv()

# from facebook_business.api import FacebookAdsApi
# from facebook_business.adobjects.adaccount import AdAccount

# from src.schema import get_connection, create_tables


# # ── Credentials ────────────────────────────────────────────────────
# ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN")
# AD_ACCOUNT_ID = os.environ.get("META_AD_ACCOUNT_ID")
# APP_ID = os.environ.get("META_APP_ID")
# APP_SECRET = os.environ.get("META_APP_SECRET")


# def init_api():
#     """Initialize Meta Marketing API."""
#     if not all([ACCESS_TOKEN, AD_ACCOUNT_ID, APP_ID, APP_SECRET]):
#         raise ValueError("Missing Meta API credentials in .env")

#     account_id = AD_ACCOUNT_ID
#     if not account_id.startswith("act_"):
#         account_id = f"act_{account_id}"

#     FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
#     return account_id


# def extract_action(actions, action_type):
#     """
#     Meta returns actions as a list:
#     [{"action_type": "app_install", "value": "42"}, ...]
#     Extract a specific action type's value.
#     """
#     if not actions:
#         return None
#     for action in actions:
#         if action.get("action_type") == action_type:
#             try:
#                 return float(action["value"])
#             except (ValueError, KeyError):
#                 return None
#     return None


# def extract_result_field(field_data):
#     """
#     Meta returns results and cost_per_result as nested structures:
#     [{'indicator': 'actions:xxx', 'values': [{'value': '28'}]}]

#     This extracts the numeric value.
#     """
#     if not field_data or not isinstance(field_data, list):
#         return None
#     try:
#         return float(field_data[0]["values"][0]["value"])
#     except (IndexError, KeyError, TypeError, ValueError):
#         return None


# def fetch_insights(date_start: str, date_end: str) -> list:
#     """
#     Fetch daily campaign-level insights from Meta API.

#     Args:
#         date_start: "2026-04-08" format
#         date_end: "2026-04-08" format (inclusive)

#     Returns:
#         List of dicts matching your DB schema
#     """
#     account_id = init_api()
#     account = AdAccount(account_id)

#     print(f"  Fetching {date_start} to {date_end}...")

#     # ── Single API call with all fields ────────────────────────
#     try:
#         insights = account.get_insights(
#             fields=[
#                 "campaign_name",
#                 "campaign_id",
#                 "spend",
#                 "impressions",
#                 "reach",
#                 "frequency",
#                 "cpm",
#                 "clicks",
#                 "cpc",
#                 "ctr",
#                 "actions",
#                 "action_values",
#                 "cost_per_action_type",
#                 "cost_per_result",
#                 "results",
#             ],
#             params={
#                 "level": "campaign",
#                 "time_range": {"since": date_start, "until": date_end},
#                 "time_increment": 1,  # daily breakdown
#             },
#         )

#         # Paginate through all results
#         rows_raw = []
#         for insight in insights:
#             rows_raw.append(dict(insight))

#     except Exception as e:
#         print(f"  ✗ API error: {e}")
#         return []

#     print(f"  ✓ Got {len(rows_raw)} raw rows from API")

#     # ── Transform to match DB schema ───────────────────────────
#     rows = []
#     for data in rows_raw:
#         actions = data.get("actions", [])
#         action_values = data.get("action_values", [])
#         cost_per_actions = data.get("cost_per_action_type", [])

#         spend = float(data.get("spend", 0) or 0)
#         impressions = float(data.get("impressions", 0) or 0)

#         # ── Results and cost_per_result — Meta's native values ─
#         results = extract_result_field(data.get("results"))
#         cost_per_result = extract_result_field(data.get("cost_per_result"))

#         # Extract result type from the indicator field
#         result_type = None
#         if data.get("results") and isinstance(data["results"], list):
#             try:
#                 result_type = data["results"][0].get("indicator", "")
#             except (IndexError, KeyError):
#                 pass

#         # ── Extract actions ────────────────────────────────────
#         # App installs — try multiple action type names
#         app_installs = (
#             extract_action(actions, "app_install")
#             or extract_action(actions, "mobile_app_install")
#             or extract_action(actions, "omni_app_install")
#         )

#         # Registrations
#         registrations = (
#             extract_action(actions, "app_custom_event.fb_mobile_complete_registration")
#             or extract_action(actions, "complete_registration")
#             or extract_action(actions, "omni_complete_registration")
#         )

#         in_app_registrations = extract_action(
#             actions, "app_custom_event.fb_mobile_complete_registration"
#         )
#         website_registrations = extract_action(
#             actions, "offsite_conversion.fb_pixel_complete_registration"
#         )

#         # Purchases
#         purchases = (
#             extract_action(actions, "purchase")
#             or extract_action(actions, "omni_purchase")
#         )
#         in_app_purchases = extract_action(
#             actions, "app_custom_event.fb_mobile_purchase"
#         )

#         # ── Extract action values (revenue) ────────────────────
#         purchase_value = (
#             extract_action(action_values, "purchase")
#             or extract_action(action_values, "omni_purchase")
#         )
#         in_app_purchase_value = extract_action(
#             action_values, "app_custom_event.fb_mobile_purchase"
#         )

#         # ── Extract cost per action ────────────────────────────
#         cost_per_install = (
#             extract_action(cost_per_actions, "app_install")
#             or extract_action(cost_per_actions, "mobile_app_install")
#             or extract_action(cost_per_actions, "omni_app_install")
#         )

#         cost_per_registration = (
#             extract_action(cost_per_actions, "app_custom_event.fb_mobile_complete_registration")
#             or extract_action(cost_per_actions, "complete_registration")
#             or extract_action(cost_per_actions, "omni_complete_registration")
#         )

#         cost_per_purchase = (
#             extract_action(cost_per_actions, "purchase")
#             or extract_action(cost_per_actions, "omni_purchase")
#         )

#         # ── Derived metrics ────────────────────────────────────
#         purchase_roas = None
#         if purchase_value and spend > 0:
#             purchase_roas = purchase_value / spend

#         # ── Build row matching DB schema ───────────────────────
#         row = {
#             "campaign_name": data.get("campaign_name"),
#             "campaign_id": data.get("campaign_id"),
#             "day": data.get("date_start"),
#             "delivery_status": "active",
#             "result_type": result_type,
#             "results": results,
#             "reach": float(data.get("reach", 0) or 0) or None,
#             "frequency": float(data.get("frequency", 0) or 0) or None,
#             "cost_per_result": cost_per_result,
#             "amount_spent_inr": spend if spend > 0 else None,
#             "impressions": impressions if impressions > 0 else None,
#             "cpm": float(data.get("cpm", 0) or 0) or None,
#             "link_clicks": float(data.get("clicks", 0) or 0) or None,
#             "cpc_link": float(data.get("cpc", 0) or 0) or None,
#             "ctr_link": float(data.get("ctr", 0) or 0) or None,
#             "clicks_all": float(data.get("clicks", 0) or 0) or None,
#             "ctr_all": None,
#             "cpc_all": None,
#             "in_app_purchases": in_app_purchases,
#             "registrations_completed": registrations,
#             "in_app_registrations": in_app_registrations,
#             "website_registrations": website_registrations,
#             "cost_per_registration_completed": cost_per_registration,
#             "purchases": purchases,
#             "purchases_conversion_value": purchase_value,
#             "in_app_purchases_conversion_value": in_app_purchase_value,
#             "cost_per_purchase": cost_per_purchase,
#             "purchase_roas": purchase_roas,
#             "app_installs": app_installs,
#             "cost_per_app_install": cost_per_install,
#             "app_activations": None,
#             "in_app_sessions": None,
#             "website_landing_page_views": None,
#             "instagram_follows": None,
#         }
#         rows.append(row)

#     return rows


# def save_to_db(rows: list):
#     """Insert fetched rows into the database."""
#     if not rows:
#         print("  No rows to save.")
#         return 0

#     create_tables()
#     conn = get_connection()
#     cursor = conn.cursor()

#     db_columns = [
#         "campaign_name", "campaign_id", "day", "delivery_status", "result_type",
#         "results", "reach", "frequency", "cost_per_result", "amount_spent_inr",
#         "impressions", "cpm", "link_clicks", "cpc_link", "ctr_link",
#         "clicks_all", "ctr_all", "cpc_all", "in_app_purchases",
#         "registrations_completed", "in_app_registrations", "website_registrations",
#         "cost_per_registration_completed", "purchases", "purchases_conversion_value",
#         "in_app_purchases_conversion_value", "cost_per_purchase", "purchase_roas",
#         "app_installs", "cost_per_app_install", "app_activations",
#         "in_app_sessions", "website_landing_page_views", "instagram_follows",
#     ]

#     placeholders = ", ".join(["?"] * len(db_columns))
#     col_names = ", ".join(db_columns)

#     inserted = 0
#     for row in rows:
#         values = tuple(row.get(col) for col in db_columns)
#         try:
#             cursor.execute(
#                 f"INSERT OR REPLACE INTO daily_metrics ({col_names}) VALUES ({placeholders})",
#                 values,
#             )
#             inserted += 1
#         except Exception as e:
#             print(f"  ⚠ Skipped: {e}")

#     conn.commit()
#     conn.close()
#     return inserted


# # ── Public functions ───────────────────────────────────────────────

# def fetch_today():
#     """Fetch yesterday's data (today is incomplete until midnight)."""
#     yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
#     print(f"\n📡 Fetching data for {yesterday}...")
#     rows = fetch_insights(yesterday, yesterday)
#     inserted = save_to_db(rows)
#     print(f"  → {inserted} rows saved to DB")
#     return inserted


# def fetch_date_range(start: str, end: str):
#     """Fetch a specific date range."""
#     print(f"\n📡 Fetching data for {start} to {end}...")
#     rows = fetch_insights(start, end)
#     inserted = save_to_db(rows)
#     print(f"  → {inserted} rows saved to DB")
#     return inserted


# def fetch_latest_gap():
#     """
#     Smart fetch: checks what's the latest date in DB,
#     fetches from there to yesterday. Fills the gap automatically.
#     """
#     conn = get_connection()
#     row = conn.execute("SELECT MAX(day) as latest FROM daily_metrics").fetchone()
#     conn.close()

#     latest = row["latest"] if row and row["latest"] else None
#     yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

#     if not latest:
#         print("  DB is empty. Use CSV seed data first or run backfill.")
#         return 0

#     if latest >= yesterday:
#         print(f"  DB already up to date (latest: {latest})")
#         return 0

#     # Fetch from day after latest to yesterday
#     start = (datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
#     print(f"\n📡 Filling gap: {start} to {yesterday} (DB had data until {latest})")
#     rows = fetch_insights(start, yesterday)
#     inserted = save_to_db(rows)
#     print(f"  → {inserted} rows saved to DB")
#     return inserted


# # ── CLI runner ─────────────────────────────────────────────────────
# if __name__ == "__main__":
#     import sys

#     if len(sys.argv) == 1:
#         # Default: smart fill gap
#         fetch_latest_gap()

#     elif sys.argv[1] == "today":
#         fetch_today()

#     elif sys.argv[1] == "backfill":
#         days = int(sys.argv[2]) if len(sys.argv) > 2 else 30
#         end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
#         start = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
#         fetch_date_range(start, end)

#     elif len(sys.argv) == 3:
#         fetch_date_range(sys.argv[1], sys.argv[2])

#     else:
#         print("Usage:")
#         print("  python -m src.meta_api                         # smart fill gap")
#         print("  python -m src.meta_api today                   # fetch yesterday")
#         print("  python -m src.meta_api backfill 30             # last 30 days")
#         print("  python -m src.meta_api 2026-04-01 2026-04-08   # specific range")

import os
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount


# ── Credentials ────────────────────────────────────────────────────
ACCESS_TOKEN = os.environ.get("META_ACCESS_TOKEN")
AD_ACCOUNT_ID = os.environ.get("META_AD_ACCOUNT_ID")
APP_ID = os.environ.get("META_APP_ID")
APP_SECRET = os.environ.get("META_APP_SECRET")


def init_api():
    if not all([ACCESS_TOKEN, AD_ACCOUNT_ID, APP_ID, APP_SECRET]):
        raise ValueError("Missing Meta API credentials in .env")

    account_id = AD_ACCOUNT_ID
    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    FacebookAdsApi.init(APP_ID, APP_SECRET, ACCESS_TOKEN)
    return account_id


def extract_action(actions, action_type):
    if not actions:
        return None
    for action in actions:
        if action.get("action_type") == action_type:
            try:
                return float(action["value"])
            except:
                return None
    return None


def extract_result_field(field_data):
    if not field_data or not isinstance(field_data, list):
        return None
    try:
        return float(field_data[0]["values"][0]["value"])
    except:
        return None


def fetch_insights(date_start: str, date_end: str) -> list:
    account_id = init_api()
    account = AdAccount(account_id)

    print(f"📡 Fetching {date_start} to {date_end}...")

    try:
        insights = account.get_insights(
            fields=[
                "campaign_name",
                "campaign_id",
                "spend",
                "impressions",
                "reach",
                "frequency",
                "cpm",
                "clicks",
                "cpc",
                "ctr",
                "actions",
                "action_values",
                "cost_per_action_type",
                "cost_per_result",
                "results",
            ],
            params={
                "level": "campaign",
                "time_range": {"since": date_start, "until": date_end},
                "time_increment": 1,
            },
        )

        rows_raw = [dict(insight) for insight in insights]

    except Exception as e:
        print(f"❌ API error: {e}")
        return []

    print(f"✅ Got {len(rows_raw)} rows")

    rows = []
    for data in rows_raw:
        actions = data.get("actions", [])
        action_values = data.get("action_values", [])
        cost_per_actions = data.get("cost_per_action_type", [])

        spend = float(data.get("spend", 0) or 0)

        results = extract_result_field(data.get("results"))
        cost_per_result = extract_result_field(data.get("cost_per_result"))

        result_type = None
        if data.get("results"):
            try:
                result_type = data["results"][0].get("indicator", "")
            except:
                pass

        app_installs = (
            extract_action(actions, "app_install")
            or extract_action(actions, "mobile_app_install")
        )

        registrations = (
            extract_action(actions, "complete_registration")
            or extract_action(actions, "omni_complete_registration")
        )

        purchases = extract_action(actions, "purchase")

        purchase_value = extract_action(action_values, "purchase")

        cost_per_install = extract_action(cost_per_actions, "app_install")
        cost_per_registration = extract_action(cost_per_actions, "complete_registration")
        cost_per_purchase = extract_action(cost_per_actions, "purchase")

        purchase_roas = purchase_value / spend if purchase_value and spend > 0 else None

        row = {
            "campaign_name": data.get("campaign_name"),
            "campaign_id": data.get("campaign_id"),
            "day": data.get("date_start"),
            "delivery_status": "active",
            "result_type": result_type,
            "results": results,
            "reach": float(data.get("reach", 0) or 0) or None,
            "frequency": float(data.get("frequency", 0) or 0) or None,
            "cost_per_result": cost_per_result,
            "amount_spent_inr": spend if spend > 0 else None,
            "impressions": float(data.get("impressions", 0) or 0) or None,
            "cpm": float(data.get("cpm", 0) or 0) or None,
            "link_clicks": float(data.get("clicks", 0) or 0) or None,
            "cpc_link": float(data.get("cpc", 0) or 0) or None,
            "ctr_link": float(data.get("ctr", 0) or 0) or None,
            "clicks_all": float(data.get("clicks", 0) or 0) or None,
            "in_app_purchases": purchases,
            "registrations_completed": registrations,
            "purchases": purchases,
            "purchases_conversion_value": purchase_value,
            "cost_per_purchase": cost_per_purchase,
            "purchase_roas": purchase_roas,
            "app_installs": app_installs,
            "cost_per_app_install": cost_per_install,
            "cost_per_registration_completed": cost_per_registration,
        }

        rows.append(row)

    return rows


def save_to_csv(rows: list, file_path="data/all_metrics.csv"):
    if not rows:
        print("No rows to save")
        return 0

    new_df = pd.DataFrame(rows)

    if os.path.exists(file_path):
        old_df = pd.read_csv(file_path)
        combined = pd.concat([old_df, new_df], ignore_index=True)

        combined = combined.drop_duplicates(
            subset=["campaign_id", "day"], keep="last"
        )
    else:
        combined = new_df

    combined.to_csv(file_path, index=False)

    print(f"✅ CSV updated. Total rows: {len(combined)}")
    return len(new_df)


# ── Public functions ───────────────────────────────────────────────

def fetch_today():
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    rows = fetch_insights(yesterday, yesterday)
    return save_to_csv(rows)


def fetch_date_range(start: str, end: str):
    rows = fetch_insights(start, end)
    return save_to_csv(rows)


# ── CLI runner ─────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    if len(sys.argv) == 3:
        fetch_date_range(sys.argv[1], sys.argv[2])

    elif len(sys.argv) == 2 and sys.argv[1] == "today":
        fetch_today()

    else:
        print("Usage:")
        print("python -m src.meta_api 2026-04-15 2026-04-15")
        print("python -m src.meta_api today")