# """
# INGEST.PY - Loads CSV data + metadata into SQLite.

# This is the module you'll swap out later when moving to Meta API.
# Right now: reads CSV file → cleans column names → inserts into DB.
# Later: calls Meta Marketing API → same cleaning → same DB insert.
# Everything downstream (detector, analyzer) stays untouched.
# """

# import pandas as pd
# import sqlite3
# from src.schema import get_connection, create_tables


# # ── Campaign metadata (from what you shared) ──────────────────────
# CAMPAIGN_METADATA = [
#     ("120237325237390318", "New App promotion oman and bahrain Campaign 22 jan 2026", "OUTCOME_APP_PROMOTION", "2026-01-22"),
#     ("120233503097150318", "New Awareness campaign 24/10/25 – duplicate (07/11/25)", "OUTCOME_AWARENESS", "2025-11-08"),
#     ("120231060494030318", "New qatar App promotion Campaign 23 sept 25", "OUTCOME_APP_PROMOTION", "2025-09-23"),
#     ("120228467191530318", "India IOS app promotion purchase campaign 09/08/25 copy of 9/10/24", "OUTCOME_APP_PROMOTION", "2025-08-06"),
#     ("120225757421370318", "UAE App promotion Campaign 26 June 25", "OUTCOME_APP_PROMOTION", "2025-06-26"),
#     ("120217652310390318", "Advantage+ app Purchase campaign 21/12/2024 Campaign – april duplicate", "OUTCOME_APP_PROMOTION", "2025-04-05"),
#     ("120215731086760318", "Advantage+ shopping campaign 02/03/2025 Campaign", "OUTCOME_SALES", "2025-03-02"),
#     ("120212785502620318", "New App IOS KSA promotion campaign", "OUTCOME_APP_PROMOTION", "2024-12-21"),
#     ("120212196648420318", "Advantage+ app campaign maximize value 27/11/2024 Campaign (Duplicate)", "OUTCOME_APP_PROMOTION", "2024-11-27"),
#     ("120209414239730318", "Advantage+ KSA app campaign 01/08/2024 Campaign", "OUTCOME_APP_PROMOTION", "2024-08-02"),
#     ("120209316450690318", "All-Lookalike-manual-registration 27-07-24", "OUTCOME_APP_PROMOTION", "2024-07-27"),
#     ("23848934971910317", "Bottom Funnel – Copy", "CONVERSIONS", "2022-01-07"),
#     ("23848933924920317", "NF App Conversions", "CONVERSIONS", "2022-01-07"),
#     ("23848721470940317", "Middle Funnel Ad 3 (fb-insta)", "CONVERSIONS", "2021-12-03"),
#     ("23848721350610317", "Middle Funnel 2 (web visit)", "CONVERSIONS", "2021-12-03"),
# ]

# # ── Adset metadata (from the table you shared) ────────────────────
# ADSET_METADATA = [
#     ("120240776372720318", "Kerala iOS", "120228467191530318", 1500, "Adset", "CONV", "Kerala", "iOS", "All"),
#     ("120237326041150318", "Bahrain", "120237325237390318", 1500, "Adset", "CONV", "Bahrain", "Android", "All"),
#     ("120237325237380318", "Oman", "120237325237390318", 1500, "Adset", "CONV", "Oman", "Android", "All"),
#     ("120235403586100318", "India Awareness", "120233503097150318", 1000, "Adset", "REACH", "India", "All", "All"),
#     ("120235206373410318", "Telangana", "120209316450690318", 1600, "Adset", "CONV", "Telangana", "Android", "All"),
#     ("120232977073190318", "West Bengal", "120209316450690318", 1400, "Adset", "CONV", "West Bengal", "Android", "All"),
#     ("120232331476970318", "Kuwait", "120231060494030318", 1200, "Adset", "CONV", "Kuwait", "Android", "All"),
#     ("120232126416800318", "Kerala Purchase", "120209316450690318", 3000, "Adset", "CONV", "Kerala", "Android", "All"),
#     ("120231798491900318", "iOS Female", "120228467191530318", 900, "Adset", "CONV", "India", "iOS", "Female"),
#     ("120231061562190318", "Qatar iOS", "120212785502620318", 1000, "Adset", "CONV", "Qatar", "iOS", "All"),
#     ("120231060494040318", "Qatar Android", "120231060494030318", 1000, "Adset", "CONV", "Qatar", "Android", "All"),
#     ("120231041586310318", "KSA iOS", "120212785502620318", 1200, "Adset", "CONV", "Saudi", "iOS", "All"),
#     ("120231008004810318", "UAE iOS", "120212785502620318", 1000, "Adset", "CONV", "UAE", "iOS", "All"),
#     ("120228989644790318", "India Female", "120209316450690318", 3600, "Adset", "CONV", "India", "Android", "Female"),
#     ("120228467191510318", "iOS Purchase", "120228467191530318", 10500, "Adset", "CONV", "India", "iOS", "All"),
#     ("120227542434050318", "Telangana Female", "120209316450690318", 1000, "Adset", "CONV", "Telangana", "Android", "Female"),
#     ("120226622057440318", "J&K Female", "120209316450690318", 1000, "Adset", "CONV", "J&K", "Android", "Female"),
#     ("120225757421360318", "UAE Android", "120225757421370318", 1650, "Adset", "CONV", "UAE", "Android", "All"),
#     ("120224689884980318", "Karnataka", "120209316450690318", 2000, "Adset", "CONV", "Karnataka", "Android", "All"),
#     ("120219111780410318", "Advantage+ Lookalike", "120215731086760318", 2000, "Campaign (CBO)", "CONV", "India", "All", "All"),
#     ("120217652310380318", "Advantage+ India", "120217652310390318", 37000, "Adset", "CONV", "India", "Android", "All"),
#     ("120213601599270318", "Tamil Nadu", "120209316450690318", 1400, "Adset", "CONV", "Tamil Nadu", "Android", "All"),
#     ("120212196648430318", "Value Campaign", "120212196648420318", 6000, "Adset", "VALUE", "India", "Android", "All"),
#     ("120209414239750318", "KSA Android", "120209414239730318", 2500, "Adset", "CONV", "Saudi", "Android", "All"),
#     ("23848934971900317", "Bottom Funnel", "23848934971910317", 400, "Adset", "CONV", "India", "All", "Custom"),
#     ("23848933924950317", "App Conversions", "23848933924920317", 1000, "Adset", "CONV", "India", "Android", "Custom"),
#     ("23848721471100317", "Middle Funnel 3", "23848721470940317", 350, "Adset", "CONV", "India", "All", "Custom"),
#     ("23848721350710317", "Middle Funnel 2", "23848721350610317", 150, "Adset", "CONV", "India", "All", "Custom"),
# ]

# # ── Mapping CSV campaign names → campaign IDs ─────────────────────
# # CSV names don't have IDs, so we map them manually
# NAME_TO_ID = {
#     "New App promotion oman and bahrain Campaign 22 jan 2026": "120237325237390318",
#     "New Awareness campaign 24/10/25 –duplicate (07/11/25)": "120233503097150318",
#     "New qatar App promotion Campaign 23 sept 25": "120231060494030318",
#     "India IOS app promotion purchase campaign  09/08/25 copy of 9/10/24": "120228467191530318",
#     "UAE App promotion Campaign 26 June 25": "120225757421370318",
#     "Advantage+ app Purchase campaign 21/12/2024 Campaign – april duplicate": "120217652310390318",
#     "Advantage+ shopping campaign 02/03/2025 Campaign": "120215731086760318",
#     "New App IOS KSA promotion campaign": "120212785502620318",
#     "Advantage+ app campaign maximize value  27/11/2024 Campaign (Duplicate). Campaign": "120212196648420318",
#     "Advantage+ KSA app campaign 01/08/2024 Campaign": "120209414239730318",
#     "All-Lookalike-manual-registration 27-07-24": "120209316450690318",
#     "Bottom Funnel – Copy": "23848934971910317",
#     "NF App Conversions": "23848933924920317",
#     "Middle Funnel Ad 3(fb-insta)": "23848721470940317",
#     "Middle Funnel 2(web visit)": "23848721350610317",
# }


# def clean_column_name(col):
#     """Convert messy CSV column names to clean DB-friendly names."""
#     return (
#         col.lower()
#         .replace("(", "").replace(")", "").replace(",", "")
#         .replace("/", "_").replace(" ", "_")
#         .replace("'", "").replace("1000_", "")
#         .strip("_")
#     )


# # Column mapping: cleaned CSV name → DB column name
# COLUMN_MAP = {
#     "campaign_name": "campaign_name",
#     "day": "day",
#     "delivery_status": "delivery_status",
#     "result_type": "result_type",
#     "results": "results",
#     "reach": "reach",
#     "frequency": "frequency",
#     "cost_per_result": "cost_per_result",
#     "amount_spent_inr": "amount_spent_inr",
#     "impressions": "impressions",
#     "cpm_cost_per_impressions": "cpm",
#     "link_clicks": "link_clicks",
#     "cpc_cost_per_link_click": "cpc_link",
#     "ctr_link_click-through_rate": "ctr_link",
#     "clicks_all": "clicks_all",
#     "ctr_all": "ctr_all",
#     "cpc_all": "cpc_all",
#     "in-app_purchases": "in_app_purchases",
#     "registrations_completed": "registrations_completed",
#     "in-app_registrations_completed": "in_app_registrations",
#     "website_registrations_completed": "website_registrations",
#     "cost_per_registration_completed": "cost_per_registration_completed",
#     "purchases": "purchases",
#     "purchases_conversion_value": "purchases_conversion_value",
#     "in-app_purchases_conversion_value": "in_app_purchases_conversion_value",
#     "cost_per_purchase": "cost_per_purchase",
#     "purchase_roas_return_on_ad_spend": "purchase_roas",
#     "app_installs": "app_installs",
#     "cost_per_app_install": "cost_per_app_install",
#     "app_activiations": "app_activations",
#     "in-app_sessions": "in_app_sessions",
#     "website_landing_page_views": "website_landing_page_views",
#     "instagram_follows": "instagram_follows",
# }


# def ingest_csv(csv_path: str):
#     """Main ingestion: CSV file → SQLite database."""
    
#     create_tables()
#     conn = get_connection()
#     cursor = conn.cursor()

#     # ── 1. Load campaign metadata ──────────────────────────────
#     print("Loading campaign metadata...")
#     for row in CAMPAIGN_METADATA:
#         cursor.execute(
#             "INSERT OR REPLACE INTO campaigns VALUES (?, ?, ?, ?)", row
#         )
#     print(f"  → {len(CAMPAIGN_METADATA)} campaigns loaded.")

#     # ── 2. Load adset metadata ─────────────────────────────────
#     print("Loading adset metadata...")
#     for row in ADSET_METADATA:
#         cursor.execute(
#             "INSERT OR REPLACE INTO adsets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row
#         )
#     print(f"  → {len(ADSET_METADATA)} adsets loaded.")

#     # ── 3. Load daily metrics from CSV ─────────────────────────
#     print(f"Reading CSV: {csv_path}")
#     df = pd.read_csv(csv_path)

#     # Clean column names
#     df.columns = [clean_column_name(c) for c in df.columns]

#     # Rename to match DB schema
#     df = df.rename(columns={k: v for k, v in COLUMN_MAP.items() if k in df.columns})

#     # Map campaign names to IDs
#     df["campaign_id"] = df["campaign_name"].map(NAME_TO_ID)

#     # Parse dates consistently
#     df["day"] = pd.to_datetime(df["day"]).dt.strftime("%Y-%m-%d")

#     # Drop columns not in our schema
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
    
#     # Keep only columns that exist in both dataframe and schema
#     available = [c for c in db_columns if c in df.columns]
#     df = df[available]

#     # Replace NaN with None for SQLite
#     df = df.where(pd.notnull(df), None)

#     # Insert rows
#     placeholders = ", ".join(["?"] * len(available))
#     col_names = ", ".join(available)
    
#     inserted = 0
#     for _, row in df.iterrows():
#         try:
#             cursor.execute(
#                 f"INSERT OR REPLACE INTO daily_metrics ({col_names}) VALUES ({placeholders})",
#                 tuple(row[available])
#             )
#             inserted += 1
#         except Exception as e:
#             print(f"  ⚠ Skipped row: {e}")

#     conn.commit()
#     conn.close()
#     print(f"  → {inserted} daily metric rows loaded.")
#     print("Ingestion complete!")


# if __name__ == "__main__":
#     ingest_csv("data/metrics.csv")

"""
INGEST.PY - Loads CSV data + metadata into SQLite.

Handles column name differences between Meta exports.
Merges old + new CSVs into a single database.
"""

import pandas as pd
import os
from src.schema import get_connection, create_tables


# ── Campaign metadata ──────────────────────────────────────────────
CAMPAIGN_METADATA = [
    ("120237325237390318", "New App promotion oman and bahrain Campaign 22 jan 2026", "OUTCOME_APP_PROMOTION", "2026-01-22"),
    ("120233503097150318", "New Awareness campaign 24/10/25 – duplicate (07/11/25)", "OUTCOME_AWARENESS", "2025-11-08"),
    ("120231060494030318", "New qatar App promotion Campaign 23 sept 25", "OUTCOME_APP_PROMOTION", "2025-09-23"),
    ("120228467191530318", "India IOS app promotion purchase campaign 09/08/25 copy of 9/10/24", "OUTCOME_APP_PROMOTION", "2025-08-06"),
    ("120225757421370318", "UAE App promotion Campaign 26 June 25", "OUTCOME_APP_PROMOTION", "2025-06-26"),
    ("120217652310390318", "Advantage+ app Purchase campaign 21/12/2024 Campaign – april duplicate", "OUTCOME_APP_PROMOTION", "2025-04-05"),
    ("120215731086760318", "Advantage+ shopping campaign 02/03/2025 Campaign", "OUTCOME_SALES", "2025-03-02"),
    ("120212785502620318", "New App IOS KSA promotion campaign", "OUTCOME_APP_PROMOTION", "2024-12-21"),
    ("120212196648420318", "Advantage+ app campaign maximize value 27/11/2024 Campaign (Duplicate)", "OUTCOME_APP_PROMOTION", "2024-11-27"),
    ("120209414239730318", "Advantage+ KSA app campaign 01/08/2024 Campaign", "OUTCOME_APP_PROMOTION", "2024-08-02"),
    ("120209316450690318", "All-Lookalike-manual-registration 27-07-24", "OUTCOME_APP_PROMOTION", "2024-07-27"),
    ("23848934971910317", "Bottom Funnel – Copy", "CONVERSIONS", "2022-01-07"),
    ("23848933924920317", "NF App Conversions", "CONVERSIONS", "2022-01-07"),
    ("23848721470940317", "Middle Funnel Ad 3 (fb-insta)", "CONVERSIONS", "2021-12-03"),
    ("23848721350610317", "Middle Funnel 2 (web visit)", "CONVERSIONS", "2021-12-03"),
]

# ── Adset metadata ─────────────────────────────────────────────────
ADSET_METADATA = [
    ("120240776372720318", "Kerala iOS", "120228467191530318", 1500, "Adset", "CONV", "Kerala", "iOS", "All"),
    ("120237326041150318", "Bahrain", "120237325237390318", 1500, "Adset", "CONV", "Bahrain", "Android", "All"),
    ("120237325237380318", "Oman", "120237325237390318", 1500, "Adset", "CONV", "Oman", "Android", "All"),
    ("120235403586100318", "India Awareness", "120233503097150318", 1000, "Adset", "REACH", "India", "All", "All"),
    ("120235206373410318", "Telangana", "120209316450690318", 1600, "Adset", "CONV", "Telangana", "Android", "All"),
    ("120232977073190318", "West Bengal", "120209316450690318", 1400, "Adset", "CONV", "West Bengal", "Android", "All"),
    ("120232331476970318", "Kuwait", "120231060494030318", 1200, "Adset", "CONV", "Kuwait", "Android", "All"),
    ("120232126416800318", "Kerala Purchase", "120209316450690318", 3000, "Adset", "CONV", "Kerala", "Android", "All"),
    ("120231798491900318", "iOS Female", "120228467191530318", 900, "Adset", "CONV", "India", "iOS", "Female"),
    ("120231061562190318", "Qatar iOS", "120212785502620318", 1000, "Adset", "CONV", "Qatar", "iOS", "All"),
    ("120231060494040318", "Qatar Android", "120231060494030318", 1000, "Adset", "CONV", "Qatar", "Android", "All"),
    ("120231041586310318", "KSA iOS", "120212785502620318", 1200, "Adset", "CONV", "Saudi", "iOS", "All"),
    ("120231008004810318", "UAE iOS", "120212785502620318", 1000, "Adset", "CONV", "UAE", "iOS", "All"),
    ("120228989644790318", "India Female", "120209316450690318", 3600, "Adset", "CONV", "India", "Android", "Female"),
    ("120228467191510318", "iOS Purchase", "120228467191530318", 10500, "Adset", "CONV", "India", "iOS", "All"),
    ("120227542434050318", "Telangana Female", "120209316450690318", 1000, "Adset", "CONV", "Telangana", "Android", "Female"),
    ("120226622057440318", "J&K Female", "120209316450690318", 1000, "Adset", "CONV", "J&K", "Android", "Female"),
    ("120225757421360318", "UAE Android", "120225757421370318", 1650, "Adset", "CONV", "UAE", "Android", "All"),
    ("120224689884980318", "Karnataka", "120209316450690318", 2000, "Adset", "CONV", "Karnataka", "Android", "All"),
    ("120219111780410318", "Advantage+ Lookalike", "120215731086760318", 2000, "Campaign (CBO)", "CONV", "India", "All", "All"),
    ("120217652310380318", "Advantage+ India", "120217652310390318", 37000, "Adset", "CONV", "India", "Android", "All"),
    ("120213601599270318", "Tamil Nadu", "120209316450690318", 1400, "Adset", "CONV", "Tamil Nadu", "Android", "All"),
    ("120212196648430318", "Value Campaign", "120212196648420318", 6000, "Adset", "VALUE", "India", "Android", "All"),
    ("120209414239750318", "KSA Android", "120209414239730318", 2500, "Adset", "CONV", "Saudi", "Android", "All"),
    ("23848934971900317", "Bottom Funnel", "23848934971910317", 400, "Adset", "CONV", "India", "All", "Custom"),
    ("23848933924950317", "App Conversions", "23848933924920317", 1000, "Adset", "CONV", "India", "Android", "Custom"),
    ("23848721471100317", "Middle Funnel 3", "23848721470940317", 350, "Adset", "CONV", "India", "All", "Custom"),
    ("23848721350710317", "Middle Funnel 2", "23848721350610317", 150, "Adset", "CONV", "India", "All", "Custom"),
]

# ── Campaign name → ID mapping ─────────────────────────────────────
# Covers name variations across different Meta exports
NAME_TO_ID = {
    "New App promotion oman and bahrain Campaign 22 jan 2026": "120237325237390318",
    "New Awareness campaign 24/10/25 –duplicate (07/11/25)": "120233503097150318",
    "New Awareness campaign 24/10/25 – duplicate (07/11/25)": "120233503097150318",
    "New qatar App promotion Campaign 23 sept 25": "120231060494030318",
    "India IOS app promotion purchase campaign  09/08/25 copy of 9/10/24": "120228467191530318",
    "India IOS app promotion purchase campaign 09/08/25 copy of 9/10/24": "120228467191530318",
    "UAE App promotion Campaign 26 June 25": "120225757421370318",
    "Advantage+ app Purchase campaign 21/12/2024 Campaign – april duplicate": "120217652310390318",
    "Advantage+ shopping campaign 02/03/2025 Campaign": "120215731086760318",
    "New App IOS KSA promotion campaign": "120212785502620318",
    "Advantage+ app campaign maximize value  27/11/2024 Campaign (Duplicate). Campaign": "120212196648420318",
    "Advantage+ app campaign maximize value 27/11/2024 Campaign (Duplicate)": "120212196648420318",
    "Advantage+ KSA app campaign 01/08/2024 Campaign": "120209414239730318",
    "All-Lookalike-manual-registration 27-07-24": "120209316450690318",
    "Bottom Funnel – Copy": "23848934971910317",
    "NF App Conversions": "23848933924920317",
    "Middle Funnel Ad 3(fb-insta)": "23848721470940317",
    "Middle Funnel Ad 3 (fb-insta)": "23848721470940317",
    "Middle Funnel 2(web visit)": "23848721350610317",
    "Middle Funnel 2 (web visit)": "23848721350610317",
}


def normalize_columns(df):
    """
    Normalize column names across different Meta export formats.
    Handles casing differences, missing columns, renamed columns.
    """
    # Lowercase all columns first
    df.columns = [c.lower().strip() for c in df.columns]

    # Standard renaming map — maps any variation to our DB column name
    rename_map = {
        "campaign name": "campaign_name",
        "day": "day",
        "delivery status": "delivery_status",
        "result type": "result_type",
        "results": "results",
        "reach": "reach",
        "frequency": "frequency",
        "cost per result": "cost_per_result",
        "amount spent (inr)": "amount_spent_inr",
        "impressions": "impressions",
        'cpm (cost per 1,000 impressions)': "cpm",
        "link clicks": "link_clicks",
        "cpc (cost per link click)": "cpc_link",
        "ctr (link click-through rate)": "ctr_link",
        "clicks (all)": "clicks_all",
        "ctr (all)": "ctr_all",
        "cpc (all)": "cpc_all",
        "in-app purchases": "in_app_purchases",
        "registrations completed": "registrations_completed",
        "in-app registrations completed": "in_app_registrations",
        "website registrations completed": "website_registrations",
        "cost per registration completed": "cost_per_registration_completed",
        "purchases": "purchases",
        "purchases conversion value": "purchases_conversion_value",
        "in-app purchases conversion value": "in_app_purchases_conversion_value",
        "cost per purchase": "cost_per_purchase",
        "purchase roas (return on ad spend)": "purchase_roas",
        # Handle BOTH naming conventions for app installs
        "app installs": "app_installs",
        "mobile app installs": "mobile_app_installs",
        "cost per app install": "cost_per_app_install",
        "app activiations": "app_activations",
        "in-app sessions": "in_app_sessions",
        "website landing page views": "website_landing_page_views",
        "instagram follows": "instagram_follows",
    }

    df = df.rename(columns=rename_map)

    # Merge "app_installs" and "mobile_app_installs" into one column
    if "mobile_app_installs" in df.columns and "app_installs" not in df.columns:
        df["app_installs"] = df["mobile_app_installs"]
    elif "mobile_app_installs" in df.columns and "app_installs" in df.columns:
        # Fill gaps: use mobile_app_installs where app_installs is null
        df["app_installs"] = df["app_installs"].fillna(df["mobile_app_installs"])

    return df


def ingest_csv(csv_path: str, clear_metrics: bool = False):
    """
    Main ingestion function.
    
    Args:
        csv_path: path to CSV file
        clear_metrics: if True, deletes all existing daily_metrics before loading
    """
    create_tables()
    conn = get_connection()
    cursor = conn.cursor()

    # ── 1. Load campaign metadata ──────────────────────────────
    print("Loading campaign metadata...")
    for row in CAMPAIGN_METADATA:
        cursor.execute("INSERT OR REPLACE INTO campaigns VALUES (?, ?, ?, ?)", row)
    print(f"  → {len(CAMPAIGN_METADATA)} campaigns loaded.")

    # ── 2. Load adset metadata ─────────────────────────────────
    print("Loading adset metadata...")
    for row in ADSET_METADATA:
        cursor.execute("INSERT OR REPLACE INTO adsets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", row)
    print(f"  → {len(ADSET_METADATA)} adsets loaded.")

    # ── 3. Optionally clear old metrics ────────────────────────
    if clear_metrics:
        cursor.execute("DELETE FROM daily_metrics")
        print("  Cleared existing daily_metrics.")

    # ── 4. Load daily metrics from CSV ─────────────────────────
    print(f"Reading CSV: {csv_path}")
    df = pd.read_csv(csv_path)

    # Normalize column names (handles differences between exports)
    df = normalize_columns(df)
    # Fallback: ensure 'day' column exists after normalization
    if "day" not in df.columns:
        for col in df.columns:
            if "day" in col.lower() and "reporting" not in col.lower():
                df = df.rename(columns={col: "day"})
                print(f"  Mapped '{col}' → 'day'")
                break

    # Map campaign names to IDs
    df["campaign_id"] = df["campaign_name"].map(NAME_TO_ID)

    # Check for unmapped campaigns
    unmapped = df[df["campaign_id"].isna()]["campaign_name"].unique()
    if len(unmapped) > 0:
        print(f"  ⚠ Unmapped campaigns: {unmapped}")

    # Parse dates consistently
    df["day"] = pd.to_datetime(df["day"]).dt.strftime("%Y-%m-%d")

    # DB columns we want to insert
    db_columns = [
        "campaign_name", "campaign_id", "day", "delivery_status", "result_type",
        "results", "reach", "frequency", "cost_per_result", "amount_spent_inr",
        "impressions", "cpm", "link_clicks", "cpc_link", "ctr_link",
        "clicks_all", "ctr_all", "cpc_all", "in_app_purchases",
        "registrations_completed", "in_app_registrations", "website_registrations",
        "cost_per_registration_completed", "purchases", "purchases_conversion_value",
        "in_app_purchases_conversion_value", "cost_per_purchase", "purchase_roas",
        "app_installs", "cost_per_app_install", "app_activations",
        "in_app_sessions", "website_landing_page_views", "instagram_follows",
    ]

    available = [c for c in db_columns if c in df.columns]
    df_insert = df[available].where(pd.notnull(df[available]), None)

    placeholders = ", ".join(["?"] * len(available))
    col_names = ", ".join(available)

    inserted = 0
    skipped = 0
    for _, row in df_insert.iterrows():
        try:
            cursor.execute(
                f"INSERT OR REPLACE INTO daily_metrics ({col_names}) VALUES ({placeholders})",
                tuple(row[available])
            )
            inserted += 1
        except Exception as e:
            skipped += 1
            if skipped <= 3:
                print(f"  ⚠ Skipped row: {e}")

    conn.commit()
    conn.close()
    print(f"  → {inserted} daily metric rows loaded ({skipped} skipped).")
    print("Ingestion complete!")


def ingest_multiple(csv_paths: list, fresh: bool = True):
    """
    Ingest multiple CSVs. Use fresh=True to start clean.
    """
    for i, path in enumerate(csv_paths):
        clear = fresh and i == 0  # only clear on first file
        print(f"\n{'='*60}")
        print(f"  Ingesting: {os.path.basename(path)}")
        print(f"{'='*60}")
        ingest_csv(path, clear_metrics=clear)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        # Custom CSV path(s)
        paths = sys.argv[1:]
        ingest_multiple(paths, fresh=True)
    else:
        # Default: ingest both CSVs
        data_dir = os.path.join(os.path.dirname(__file__), "..", "data")
        csvs = sorted([
            os.path.join(data_dir, f) for f in os.listdir(data_dir)
            if f.endswith(".csv")
        ])
        if csvs:
            ingest_multiple(csvs, fresh=True)
        else:
            print("No CSV files found in data/ directory.")