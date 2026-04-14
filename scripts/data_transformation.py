"""
Phase 2: Data Transformation & Cleaning
Handles missing values, converts timestamps, normalizes categorical data,
creates derived features, and saves cleaned datasets.
"""

import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'Data')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output', 'cleaned')


def load_raw_data():
    """Load all raw CSV files."""
    logger.info("Loading raw datasets...")
    data = {
        'customers': pd.read_csv(os.path.join(DATA_DIR, 'olist_customers_dataset.csv')),
        'orders': pd.read_csv(os.path.join(DATA_DIR, 'olist_orders_dataset.csv')),
        'order_items': pd.read_csv(os.path.join(DATA_DIR, 'olist_order_items_dataset.csv')),
        'payments': pd.read_csv(os.path.join(DATA_DIR, 'olist_order_payments_dataset.csv')),
        'products': pd.read_csv(os.path.join(DATA_DIR, 'olist_products_dataset.csv')),
        'sellers': pd.read_csv(os.path.join(DATA_DIR, 'olist_sellers_dataset.csv')),
        'reviews': pd.read_csv(os.path.join(DATA_DIR, 'olist_order_reviews_dataset.csv')),
        'geolocation': pd.read_csv(os.path.join(DATA_DIR, 'olist_geolocation_dataset.csv')),
        'category_translation': pd.read_csv(os.path.join(DATA_DIR, 'product_category_name_translation.csv')),
    }
    for name, df in data.items():
        logger.info(f"  {name}: {len(df):,} rows")
    return data


def clean_customers(df):
    """Clean customers dataset."""
    logger.info("Cleaning customers...")
    df = df.copy()
    # Standardize city names: lowercase, strip whitespace
    df['customer_city'] = df['customer_city'].str.lower().str.strip()
    # Standardize state: uppercase
    df['customer_state'] = df['customer_state'].str.upper().str.strip()
    logger.info(f"  Customers: {len(df):,} rows after cleaning")
    return df


def clean_orders(df):
    """Clean orders dataset: parse timestamps, handle nulls, create derived features."""
    logger.info("Cleaning orders...")
    df = df.copy()

    # Convert all timestamp columns to datetime
    timestamp_cols = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in timestamp_cols:
        df[col] = pd.to_datetime(df[col], errors='coerce')

    # Fill order_approved_at with purchase_timestamp for delivered orders
    mask = (df['order_approved_at'].isna()) & (df['order_status'] == 'delivered')
    df.loc[mask, 'order_approved_at'] = df.loc[mask, 'order_purchase_timestamp']
    logger.info(f"  Filled {mask.sum()} missing order_approved_at values")

    # Derived time features
    df['order_year'] = df['order_purchase_timestamp'].dt.year
    df['order_month'] = df['order_purchase_timestamp'].dt.month
    df['order_year_month'] = df['order_purchase_timestamp'].dt.to_period('M').astype(str)
    df['order_day_of_week'] = df['order_purchase_timestamp'].dt.day_name()
    df['order_hour'] = df['order_purchase_timestamp'].dt.hour

    # Delivery time in days (only for delivered orders)
    delivered_mask = df['order_delivered_customer_date'].notna()
    df['delivery_days'] = np.nan
    df.loc[delivered_mask, 'delivery_days'] = (
        df.loc[delivered_mask, 'order_delivered_customer_date'] -
        df.loc[delivered_mask, 'order_purchase_timestamp']
    ).dt.total_seconds() / 86400

    # Delivery delay (positive = late, negative = early)
    df['delivery_delay_days'] = np.nan
    mask_both = delivered_mask & df['order_estimated_delivery_date'].notna()
    df.loc[mask_both, 'delivery_delay_days'] = (
        df.loc[mask_both, 'order_delivered_customer_date'] -
        df.loc[mask_both, 'order_estimated_delivery_date']
    ).dt.total_seconds() / 86400

    # Standardize order_status
    df['order_status'] = df['order_status'].str.lower().str.strip()

    logger.info(f"  Orders: {len(df):,} rows, added 7 derived columns")
    return df


def clean_order_items(df):
    """Clean order items dataset."""
    logger.info("Cleaning order items...")
    df = df.copy()
    # Parse shipping limit date
    df['shipping_limit_date'] = pd.to_datetime(df['shipping_limit_date'], errors='coerce')
    # Calculate total item value (price + freight)
    df['total_item_value'] = df['price'] + df['freight_value']
    logger.info(f"  Order items: {len(df):,} rows")
    return df


def clean_payments(df):
    """Clean payments dataset."""
    logger.info("Cleaning payments...")
    df = df.copy()
    # Standardize payment type
    df['payment_type'] = df['payment_type'].str.lower().str.strip()
    # Replace 'not_defined' with 'other'
    df['payment_type'] = df['payment_type'].replace('not_defined', 'other')
    logger.info(f"  Payments: {len(df):,} rows, types: {df['payment_type'].unique().tolist()}")
    return df


def clean_products(df, translation_df):
    """Clean products dataset: handle nulls, translate categories."""
    logger.info("Cleaning products...")
    df = df.copy()

    # Fill missing category names
    null_cat_count = df['product_category_name'].isna().sum()
    df['product_category_name'] = df['product_category_name'].fillna('unknown')
    logger.info(f"  Filled {null_cat_count} missing category names with 'unknown'")

    # Fill missing numeric columns with median
    numeric_cols = ['product_name_lenght', 'product_description_lenght', 'product_photos_qty',
                    'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm']
    for col in numeric_cols:
        if col in df.columns:
            null_count = df[col].isna().sum()
            if null_count > 0:
                median_val = df[col].median()
                df[col] = df[col].fillna(median_val)
                logger.info(f"  Filled {null_count} missing {col} with median ({median_val})")

    # Merge with English translation
    df = df.merge(translation_df, on='product_category_name', how='left')
    df['product_category_name_english'] = df['product_category_name_english'].fillna(
        df['product_category_name']
    )

    # Calculate product volume
    if all(c in df.columns for c in ['product_length_cm', 'product_height_cm', 'product_width_cm']):
        df['product_volume_cm3'] = df['product_length_cm'] * df['product_height_cm'] * df['product_width_cm']

    logger.info(f"  Products: {len(df):,} rows, {df['product_category_name_english'].nunique()} categories")
    return df


def clean_sellers(df):
    """Clean sellers dataset."""
    logger.info("Cleaning sellers...")
    df = df.copy()
    df['seller_city'] = df['seller_city'].str.lower().str.strip()
    df['seller_state'] = df['seller_state'].str.upper().str.strip()
    logger.info(f"  Sellers: {len(df):,} rows")
    return df


def clean_reviews(df):
    """Clean reviews dataset."""
    logger.info("Cleaning reviews...")
    df = df.copy()
    # Fill missing review text
    df['review_comment_title'] = df['review_comment_title'].fillna('')
    df['review_comment_message'] = df['review_comment_message'].fillna('')
    # Convert to string type in case they were read as float
    df['review_comment_title'] = df['review_comment_title'].astype(str)
    df['review_comment_message'] = df['review_comment_message'].astype(str)
    # Parse dates
    df['review_creation_date'] = pd.to_datetime(df['review_creation_date'], errors='coerce')
    df['review_answer_timestamp'] = pd.to_datetime(df['review_answer_timestamp'], errors='coerce')
    # Has comment flag
    df['has_comment'] = (df['review_comment_message'].str.len() > 0).astype(int)
    logger.info(f"  Reviews: {len(df):,} rows, avg score: {df['review_score'].mean():.2f}")
    return df


def clean_geolocation(df):
    """Clean and aggregate geolocation dataset at state level."""
    logger.info("Cleaning geolocation (aggregating to state level)...")
    df = df.copy()
    df['geolocation_city'] = df['geolocation_city'].str.lower().str.strip()
    df['geolocation_state'] = df['geolocation_state'].str.upper().str.strip()

    # Aggregate to zip-code level (take mean of coordinates per zip)
    geo_agg = df.groupby('geolocation_zip_code_prefix').agg(
        geolocation_lat=('geolocation_lat', 'mean'),
        geolocation_lng=('geolocation_lng', 'mean'),
        geolocation_city=('geolocation_city', 'first'),
        geolocation_state=('geolocation_state', 'first'),
    ).reset_index()
    logger.info(f"  Geolocation: {len(df):,} → {len(geo_agg):,} rows (zip-level aggregation)")
    return geo_agg


def save_cleaned_data(datasets):
    """Save all cleaned datasets to output/cleaned/."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for name, df in datasets.items():
        filepath = os.path.join(OUTPUT_DIR, f'{name}_cleaned.csv')
        df.to_csv(filepath, index=False)
        logger.info(f"  Saved {name}_cleaned.csv ({len(df):,} rows)")


def run_transformation():
    """Main transformation pipeline."""
    logger.info("=" * 60)
    logger.info("PHASE 2: DATA TRANSFORMATION & CLEANING")
    logger.info("=" * 60)

    # Load raw data
    raw = load_raw_data()

    # Clean each dataset
    cleaned = {
        'customers': clean_customers(raw['customers']),
        'orders': clean_orders(raw['orders']),
        'order_items': clean_order_items(raw['order_items']),
        'payments': clean_payments(raw['payments']),
        'products': clean_products(raw['products'], raw['category_translation']),
        'sellers': clean_sellers(raw['sellers']),
        'reviews': clean_reviews(raw['reviews']),
        'geolocation': clean_geolocation(raw['geolocation']),
    }

    # Save cleaned datasets
    logger.info("\n💾 Saving cleaned datasets...")
    save_cleaned_data(cleaned)

    # Summary
    logger.info(f"\n{'─' * 40}")
    logger.info("TRANSFORMATION SUMMARY")
    total_rows = sum(len(df) for df in cleaned.values())
    logger.info(f"  Total rows across all tables: {total_rows:,}")
    logger.info(f"  Tables processed: {len(cleaned)}")
    logger.info(f"  Output directory: {OUTPUT_DIR}")
    logger.info(f"{'─' * 40}")
    logger.info("\n✅ Phase 2 Complete: Data Transformation & Cleaning\n")

    return cleaned


if __name__ == '__main__':
    run_transformation()
