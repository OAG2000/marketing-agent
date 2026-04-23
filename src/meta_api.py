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


# ── Initialize Meta API ────────────────────────────────────────────
def init_api():

    if not all([
        ACCESS_TOKEN,
        AD_ACCOUNT_ID,
        APP_ID,
        APP_SECRET,
    ]):
        raise ValueError("Missing Meta API credentials in .env")

    account_id = AD_ACCOUNT_ID

    if not account_id.startswith("act_"):
        account_id = f"act_{account_id}"

    FacebookAdsApi.init(
        APP_ID,
        APP_SECRET,
        ACCESS_TOKEN,
    )

    return account_id


# ── Helpers ────────────────────────────────────────────────────────
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


# ── Fetch Meta Insights ────────────────────────────────────────────
def fetch_insights(date_start: str, date_end: str) -> list:

    account_id = init_api()

    account = AdAccount(account_id)

    print(f"\n📡 Fetching {date_start} → {date_end}")

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

                "time_range": {
                    "since": date_start,
                    "until": date_end,
                },

                "time_increment": 1,
            },
        )

        rows_raw = [dict(insight) for insight in insights]

    except Exception as e:

        print(f"❌ API Error: {e}")

        return []

    print(f"✅ Got {len(rows_raw)} rows")

    rows = []

    for data in rows_raw:

        actions = data.get("actions", [])

        action_values = data.get("action_values", [])

        cost_per_actions = data.get(
            "cost_per_action_type",
            [],
        )

        spend = float(data.get("spend", 0) or 0)

        # ── Results ───────────────────────────────────────────────
        results = extract_result_field(
            data.get("results")
        )

        cost_per_result = extract_result_field(
            data.get("cost_per_result")
        )

        result_type = None

        if data.get("results"):

            try:
                result_type = (
                    data["results"][0]
                    .get("indicator", "")
                )

            except:
                pass

        # ── Installs ──────────────────────────────────────────────
        app_installs = (
            extract_action(actions, "app_install")
            or extract_action(
                actions,
                "mobile_app_install",
            )
            or extract_action(
                actions,
                "omni_app_install",
            )
        )

        # ── Registrations ─────────────────────────────────────────
        registrations = (
            extract_action(
                actions,
                "app_custom_event.fb_mobile_complete_registration",
            )
            or extract_action(
                actions,
                "complete_registration",
            )
            or extract_action(
                actions,
                "omni_complete_registration",
            )
        )

        # ── Purchases ─────────────────────────────────────────────
        purchases = (
            extract_action(actions, "purchase")
            or extract_action(
                actions,
                "omni_purchase",
            )
        )

        # ── Revenue ───────────────────────────────────────────────
        purchase_value = (
            extract_action(
                action_values,
                "purchase",
            )
            or extract_action(
                action_values,
                "omni_purchase",
            )
        )

        # ── Cost Per Metrics ──────────────────────────────────────
        cost_per_install = (
            extract_action(
                cost_per_actions,
                "app_install",
            )
            or extract_action(
                cost_per_actions,
                "mobile_app_install",
            )
            or extract_action(
                cost_per_actions,
                "omni_app_install",
            )
        )

        cost_per_registration = (
            extract_action(
                cost_per_actions,
                "app_custom_event.fb_mobile_complete_registration",
            )
            or extract_action(
                cost_per_actions,
                "complete_registration",
            )
            or extract_action(
                cost_per_actions,
                "omni_complete_registration",
            )
        )

        cost_per_purchase = (
            extract_action(
                cost_per_actions,
                "purchase",
            )
            or extract_action(
                cost_per_actions,
                "omni_purchase",
            )
        )

        # ── ROAS ──────────────────────────────────────────────────
        purchase_roas = None

        if purchase_value and spend > 0:
            purchase_roas = purchase_value / spend

        # ── Final Row ─────────────────────────────────────────────
        row = {

            "campaign_name":
                data.get("campaign_name"),

            "campaign_id":
                data.get("campaign_id"),

            "day":
                data.get("date_start"),

            "delivery_status":
                "active",

            "result_type":
                result_type,

            "results":
                results,

            "reach":
                float(
                    data.get("reach", 0) or 0
                ) or None,

            "frequency":
                float(
                    data.get("frequency", 0) or 0
                ) or None,

            "cost_per_result":
                cost_per_result,

            "amount_spent_inr":
                spend if spend > 0 else None,

            "impressions":
                float(
                    data.get("impressions", 0) or 0
                ) or None,

            "cpm":
                float(
                    data.get("cpm", 0) or 0
                ) or None,

            "link_clicks":
                float(
                    data.get("clicks", 0) or 0
                ) or None,

            "cpc_link":
                float(
                    data.get("cpc", 0) or 0
                ) or None,

            "ctr_link":
                float(
                    data.get("ctr", 0) or 0
                ) or None,

            "clicks_all":
                float(
                    data.get("clicks", 0) or 0
                ) or None,

            "registrations_completed":
                registrations,

            "purchases":
                purchases,

            "purchases_conversion_value":
                purchase_value,

            "cost_per_purchase":
                cost_per_purchase,

            "purchase_roas":
                purchase_roas,

            "app_installs":
                app_installs,

            "cost_per_app_install":
                cost_per_install,

            "cost_per_registration_completed":
                cost_per_registration,
        }

        rows.append(row)

    return rows


# ── Save CSV ───────────────────────────────────────────────────────
def save_to_csv(
    rows: list,
    file_path="data/all_metrics.csv",
):

    if not rows:
        print("⚠ No rows to save")
        return 0

    new_df = pd.DataFrame(rows)

    # ── Existing CSV ─────────────────────────────────────────────
    if os.path.exists(file_path):

        old_df = pd.read_csv(file_path)

        combined = pd.concat(
            [old_df, new_df],
            ignore_index=True,
        )

        combined = combined.drop_duplicates(
            subset=["campaign_id", "day"],
            keep="last",
        )

    else:

        combined = new_df

    # ── Save ─────────────────────────────────────────────────────
    combined.to_csv(
        file_path,
        index=False,
    )

    print(f"✅ CSV updated")
    print(f"✅ Total rows: {len(combined)}")

    return len(new_df)


# ── TODAY DATA (LIVE DATA) ─────────────────────────────────────────
def fetch_today():

    today = datetime.now().strftime("%Y-%m-%d")

    print(f"\n🚀 Fetching TODAY'S data ({today})")

    rows = fetch_insights(today, today)

    return save_to_csv(rows)


# ── YESTERDAY DATA ────────────────────────────────────────────────
def fetch_yesterday():

    yesterday = (
        datetime.now() - timedelta(days=1)
    ).strftime("%Y-%m-%d")

    print(f"\n🚀 Fetching YESTERDAY'S data ({yesterday})")

    rows = fetch_insights(
        yesterday,
        yesterday,
    )

    return save_to_csv(rows)


# ── Date Range ────────────────────────────────────────────────────
def fetch_date_range(
    start: str,
    end: str,
):

    rows = fetch_insights(
        start,
        end,
    )

    return save_to_csv(rows)


# ── Backfill ──────────────────────────────────────────────────────
def backfill_from_15th():

    start_date = "2026-04-15"

    end_date = datetime.now().strftime(
        "%Y-%m-%d"
    )

    print(
        f"\n🚀 Backfilling {start_date} → {end_date}"
    )

    rows = fetch_insights(
        start_date,
        end_date,
    )

    return save_to_csv(rows)


# ── CLI ───────────────────────────────────────────────────────────
if __name__ == "__main__":

    import sys

    # ── TODAY ────────────────────────────────────────────────────
    if (
        len(sys.argv) == 2
        and sys.argv[1] == "today"
    ):

        fetch_today()

    # ── YESTERDAY ───────────────────────────────────────────────
    elif (
        len(sys.argv) == 2
        and sys.argv[1] == "yesterday"
    ):

        fetch_yesterday()

    # ── BACKFILL ────────────────────────────────────────────────
    elif (
        len(sys.argv) == 2
        and sys.argv[1] == "backfill"
    ):

        backfill_from_15th()

    # ── CUSTOM RANGE ────────────────────────────────────────────
    elif len(sys.argv) == 3:

        fetch_date_range(
            sys.argv[1],
            sys.argv[2],
        )

    # ── HELP ────────────────────────────────────────────────────
    else:

        print("\nUsage:\n")

        print(
            "python -m src.meta_api today"
        )

        print(
            "python -m src.meta_api yesterday"
        )

        print(
            "python -m src.meta_api backfill"
        )

        print(
            "python -m src.meta_api 2026-04-15 2026-04-23"
        )