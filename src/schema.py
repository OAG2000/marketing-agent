"""
SCHEMA.PY - Creates the SQLite database tables.

We're building 3 tables:
1. campaigns     - one row per campaign (metadata: name, objective, start date)
2. adsets        - one row per adset (geo, platform, gender, budget details)
3. daily_metrics - one row per campaign per day (the actual performance numbers)

Why SQLite? It's a single file, no server to run, Python has it built-in,
and it's powerful enough for our data size. Later if you need Postgres, 
you only change connection strings — queries stay the same.
"""

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "db", "marketing.db")


def get_connection():
    """Get a connection to the SQLite database."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # lets you access columns by name
    return conn


def create_tables():
    """Create all tables if they don't exist."""
    conn = get_connection()
    cursor = conn.cursor()

    # Table 1: Campaign-level metadata
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS campaigns (
            campaign_id TEXT PRIMARY KEY,
            campaign_name TEXT NOT NULL,
            objective TEXT NOT NULL,
            start_time TEXT
        )
    """)

    # Table 2: Adset-level metadata (child of campaigns)
    # This gives the LLM rich context: "this campaign targets Kerala on Android"
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS adsets (
            adset_id TEXT PRIMARY KEY,
            adset_name TEXT NOT NULL,
            campaign_id TEXT NOT NULL,
            final_budget REAL,
            budget_source TEXT,
            objective TEXT,
            geo TEXT,
            platform TEXT,
            gender TEXT,
            FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id)
        )
    """)

    # Table 3: Daily performance metrics (the main data we analyze)
    # One row = one campaign on one day
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            campaign_name TEXT NOT NULL,
            campaign_id TEXT,
            day TEXT NOT NULL,
            delivery_status TEXT,
            result_type TEXT,
            results REAL,
            reach REAL,
            frequency REAL,
            cost_per_result REAL,
            amount_spent_inr REAL,
            impressions REAL,
            cpm REAL,
            link_clicks REAL,
            cpc_link REAL,
            ctr_link REAL,
            clicks_all REAL,
            ctr_all REAL,
            cpc_all REAL,
            in_app_purchases REAL,
            registrations_completed REAL,
            in_app_registrations REAL,
            website_registrations REAL,
            cost_per_registration_completed REAL,
            purchases REAL,
            purchases_conversion_value REAL,
            in_app_purchases_conversion_value REAL,
            cost_per_purchase REAL,
            purchase_roas REAL,
            app_installs REAL,
            cost_per_app_install REAL,
            app_activations REAL,
            in_app_sessions REAL,
            website_landing_page_views REAL,
            instagram_follows REAL,
            UNIQUE(campaign_id, day)
        )
    """)

    conn.commit()
    conn.close()
    print("Database tables created successfully.")


if __name__ == "__main__":
    create_tables()