"""
Phase 4: Database Setup & Schema Design
Creates a normalized SQLite database with optimized schema,
loads cleaned data, and creates performance indexes.
"""

import pandas as pd
import sqlite3
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR = os.path.join(BASE_DIR, 'output', 'cleaned')
DB_PATH = os.path.join(BASE_DIR, 'ecommerce.db')


# ─── Schema DDL ──────────────────────────────────────────────────────────────

SCHEMA_DDL = """
-- Dimension: Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id TEXT PRIMARY KEY,
    customer_unique_id TEXT NOT NULL,
    customer_zip_code_prefix INTEGER,
    customer_city TEXT,
    customer_state TEXT
);

-- Dimension: Orders
CREATE TABLE IF NOT EXISTS dim_orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT,
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    order_year INTEGER,
    order_month INTEGER,
    order_year_month TEXT,
    order_day_of_week TEXT,
    order_hour INTEGER,
    delivery_days REAL,
    delivery_delay_days REAL,
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id)
);

-- Dimension: Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id TEXT PRIMARY KEY,
    product_category_name TEXT,
    product_category_name_english TEXT,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g REAL,
    product_length_cm REAL,
    product_height_cm REAL,
    product_width_cm REAL,
    product_volume_cm3 REAL
);

-- Dimension: Sellers
CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id TEXT PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city TEXT,
    seller_state TEXT
);

-- Dimension: Reviews
CREATE TABLE IF NOT EXISTS dim_reviews (
    review_id TEXT,
    order_id TEXT,
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    has_comment INTEGER,
    PRIMARY KEY (review_id, order_id),
    FOREIGN KEY (order_id) REFERENCES dim_orders(order_id)
);

-- Fact: Order Items (grain = one line item per order)
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_id TEXT NOT NULL,
    order_item_id INTEGER NOT NULL,
    product_id TEXT NOT NULL,
    seller_id TEXT NOT NULL,
    shipping_limit_date DATETIME,
    price REAL,
    freight_value REAL,
    total_item_value REAL,
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES dim_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id)
);

-- Fact: Payments
CREATE TABLE IF NOT EXISTS fact_payments (
    order_id TEXT NOT NULL,
    payment_sequential INTEGER NOT NULL,
    payment_type TEXT,
    payment_installments INTEGER,
    payment_value REAL,
    PRIMARY KEY (order_id, payment_sequential),
    FOREIGN KEY (order_id) REFERENCES dim_orders(order_id)
);

-- Geolocation reference
CREATE TABLE IF NOT EXISTS ref_geolocation (
    geolocation_zip_code_prefix INTEGER PRIMARY KEY,
    geolocation_lat REAL,
    geolocation_lng REAL,
    geolocation_city TEXT,
    geolocation_state TEXT
);
"""

# ─── Index DDL ───────────────────────────────────────────────────────────────

INDEX_DDL = """
-- Orders indexes for time-series queries
CREATE INDEX IF NOT EXISTS idx_orders_purchase_ts ON dim_orders(order_purchase_timestamp);
CREATE INDEX IF NOT EXISTS idx_orders_year_month ON dim_orders(order_year_month);
CREATE INDEX IF NOT EXISTS idx_orders_status ON dim_orders(order_status);
CREATE INDEX IF NOT EXISTS idx_orders_customer ON dim_orders(customer_id);

-- Customer indexes for segmentation
CREATE INDEX IF NOT EXISTS idx_customers_state ON dim_customers(customer_state);
CREATE INDEX IF NOT EXISTS idx_customers_city ON dim_customers(customer_city);
CREATE INDEX IF NOT EXISTS idx_customers_unique ON dim_customers(customer_unique_id);

-- Product indexes for category analysis
CREATE INDEX IF NOT EXISTS idx_products_category ON dim_products(product_category_name_english);

-- Order items indexes for join performance
CREATE INDEX IF NOT EXISTS idx_items_product ON fact_order_items(product_id);
CREATE INDEX IF NOT EXISTS idx_items_seller ON fact_order_items(seller_id);
CREATE INDEX IF NOT EXISTS idx_items_order ON fact_order_items(order_id);

-- Payments index
CREATE INDEX IF NOT EXISTS idx_payments_order ON fact_payments(order_id);
CREATE INDEX IF NOT EXISTS idx_payments_type ON fact_payments(payment_type);

-- Reviews index
CREATE INDEX IF NOT EXISTS idx_reviews_order ON dim_reviews(order_id);
CREATE INDEX IF NOT EXISTS idx_reviews_score ON dim_reviews(review_score);

-- Sellers index
CREATE INDEX IF NOT EXISTS idx_sellers_state ON dim_sellers(seller_state);

-- Geolocation index
CREATE INDEX IF NOT EXISTS idx_geo_state ON ref_geolocation(geolocation_state);
"""


def create_database():
    """Create SQLite database with optimized schema and indexes."""
    logger.info("Creating database schema...")

    # Remove existing DB
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        logger.info(f"  Removed existing database: {DB_PATH}")

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Enable WAL mode for better concurrent read performance
    cursor.execute("PRAGMA journal_mode=WAL;")
    cursor.execute("PRAGMA foreign_keys=ON;")

    # Create tables
    for statement in SCHEMA_DDL.split(';'):
        statement = statement.strip()
        if statement:
            cursor.execute(statement + ';')
    logger.info("  Tables created successfully")

    # Create indexes
    for statement in INDEX_DDL.split(';'):
        statement = statement.strip()
        if statement and not statement.startswith('--'):
            cursor.execute(statement + ';')
    logger.info("  Indexes created successfully")

    conn.commit()
    conn.close()


def load_data_to_db():
    """Load cleaned CSV data into the SQLite database."""
    logger.info("Loading data into database...")
    conn = sqlite3.connect(DB_PATH)

    table_map = {
        'dim_customers': 'customers_cleaned.csv',
        'dim_orders': 'orders_cleaned.csv',
        'fact_order_items': 'order_items_cleaned.csv',
        'fact_payments': 'payments_cleaned.csv',
        'dim_products': 'products_cleaned.csv',
        'dim_sellers': 'sellers_cleaned.csv',
        'dim_reviews': 'reviews_cleaned.csv',
        'ref_geolocation': 'geolocation_cleaned.csv',
    }

    for table_name, csv_file in table_map.items():
        filepath = os.path.join(CLEANED_DIR, csv_file)
        if not os.path.exists(filepath):
            logger.warning(f"  Missing file: {filepath}")
            continue

        df = pd.read_csv(filepath)

        # Get expected columns from the DB table
        cursor = conn.cursor()
        cursor.execute(f"PRAGMA table_info({table_name})")
        db_columns = [row[1] for row in cursor.fetchall()]

        # Only include columns that exist in both DataFrame and DB table
        common_cols = [col for col in db_columns if col in df.columns]
        df_subset = df[common_cols]

        # Load data
        df_subset.to_sql(table_name, conn, if_exists='replace', index=False)
        logger.info(f"  ✅ {table_name}: {len(df_subset):,} rows loaded ({len(common_cols)} columns)")

    conn.close()


def verify_database():
    """Verify database integrity and print table statistics."""
    logger.info("\n🔍 Verifying database...")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
    tables = [row[0] for row in cursor.fetchall()]

    total_rows = 0
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        total_rows += count
        logger.info(f"  {table}: {count:,} rows")

    # Get all indexes
    cursor.execute("SELECT name FROM sqlite_master WHERE type='index' ORDER BY name;")
    indexes = [row[0] for row in cursor.fetchall()]
    logger.info(f"\n  Total rows: {total_rows:,}")
    logger.info(f"  Total indexes: {len(indexes)}")

    # Database file size
    db_size_mb = os.path.getsize(DB_PATH) / (1024 * 1024)
    logger.info(f"  Database size: {db_size_mb:.1f} MB")

    conn.close()
    return total_rows


def run_database_setup():
    """Main database setup pipeline."""
    logger.info("=" * 60)
    logger.info("PHASE 4: DATABASE SETUP")
    logger.info("=" * 60)

    create_database()
    load_data_to_db()
    total_rows = verify_database()

    logger.info(f"\n{'─' * 40}")
    logger.info("DATABASE SETUP SUMMARY")
    logger.info(f"  Database: {DB_PATH}")
    logger.info(f"  Total rows loaded: {total_rows:,}")
    logger.info(f"{'─' * 40}")
    logger.info("\n✅ Phase 4 Complete: Database Setup\n")


if __name__ == '__main__':
    run_database_setup()
