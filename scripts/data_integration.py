"""
Phase 3: Data Integration
Merges cleaned datasets into a unified analytical dataset.
Validates join completeness and referential integrity.
"""

import pandas as pd
import numpy as np
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CLEANED_DIR = os.path.join(BASE_DIR, 'output', 'cleaned')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output')


def load_cleaned_data():
    """Load cleaned datasets from output/cleaned/."""
    logger.info("Loading cleaned datasets...")
    data = {}
    files = {
        'customers': 'customers_cleaned.csv',
        'orders': 'orders_cleaned.csv',
        'order_items': 'order_items_cleaned.csv',
        'payments': 'payments_cleaned.csv',
        'products': 'products_cleaned.csv',
        'sellers': 'sellers_cleaned.csv',
        'reviews': 'reviews_cleaned.csv',
    }
    for name, filename in files.items():
        filepath = os.path.join(CLEANED_DIR, filename)
        data[name] = pd.read_csv(filepath)
        logger.info(f"  {name}: {len(data[name]):,} rows")

    # Parse datetime columns back
    datetime_cols_orders = [
        'order_purchase_timestamp', 'order_approved_at',
        'order_delivered_carrier_date', 'order_delivered_customer_date',
        'order_estimated_delivery_date'
    ]
    for col in datetime_cols_orders:
        if col in data['orders'].columns:
            data['orders'][col] = pd.to_datetime(data['orders'][col], errors='coerce')

    return data


def aggregate_payments(payments_df):
    """Aggregate payments to order level."""
    logger.info("Aggregating payments to order level...")
    payment_agg = payments_df.groupby('order_id').agg(
        total_payment_value=('payment_value', 'sum'),
        payment_installments_max=('payment_installments', 'max'),
        payment_type_primary=('payment_type', 'first'),
        num_payment_methods=('payment_sequential', 'max'),
    ).reset_index()
    logger.info(f"  Payment aggregation: {len(payments_df):,} → {len(payment_agg):,} orders")
    return payment_agg


def aggregate_order_items(order_items_df):
    """Aggregate order items to order level."""
    logger.info("Aggregating order items to order level...")
    items_agg = order_items_df.groupby('order_id').agg(
        total_items=('order_item_id', 'max'),
        total_price=('price', 'sum'),
        total_freight=('freight_value', 'sum'),
        avg_item_price=('price', 'mean'),
        num_unique_products=('product_id', 'nunique'),
        num_unique_sellers=('seller_id', 'nunique'),
    ).reset_index()
    items_agg['total_order_value'] = items_agg['total_price'] + items_agg['total_freight']
    logger.info(f"  Items aggregation: {len(order_items_df):,} → {len(items_agg):,} orders")
    return items_agg


def aggregate_reviews(reviews_df):
    """Aggregate reviews to order level (take first review per order)."""
    logger.info("Aggregating reviews to order level...")
    # Some orders may have multiple reviews; keep the first
    reviews_agg = reviews_df.sort_values('review_creation_date').groupby('order_id').agg(
        review_score=('review_score', 'first'),
        has_review_comment=('has_comment', 'max'),
    ).reset_index()
    logger.info(f"  Reviews aggregation: {len(reviews_df):,} → {len(reviews_agg):,} orders")
    return reviews_agg


def build_analytical_dataset(data):
    """
    Build the unified analytical dataset by merging all tables.
    
    Merge strategy:
    orders → customers (customer_id)
           → order_items_agg (order_id)
           → payments_agg (order_id)
           → reviews_agg (order_id)
    """
    logger.info("\n🔗 Building unified analytical dataset...")

    # Start with orders
    df = data['orders'].copy()
    initial_count = len(df)
    logger.info(f"  Base: orders ({len(df):,} rows)")

    # Merge with customers
    df = df.merge(data['customers'], on='customer_id', how='left')
    logger.info(f"  + customers → {len(df):,} rows (orphans: {df['customer_unique_id'].isna().sum()})")

    # Aggregate and merge order items
    items_agg = aggregate_order_items(data['order_items'])
    df = df.merge(items_agg, on='order_id', how='left')
    logger.info(f"  + order_items_agg → {len(df):,} rows (no items: {df['total_price'].isna().sum()})")

    # Aggregate and merge payments
    payment_agg = aggregate_payments(data['payments'])
    df = df.merge(payment_agg, on='order_id', how='left')
    logger.info(f"  + payments_agg → {len(df):,} rows (no payment: {df['total_payment_value'].isna().sum()})")

    # Aggregate and merge reviews
    reviews_agg = aggregate_reviews(data['reviews'])
    df = df.merge(reviews_agg, on='order_id', how='left')
    logger.info(f"  + reviews_agg → {len(df):,} rows (no review: {df['review_score'].isna().sum()})")

    # Fill missing numeric aggregates with 0
    fill_zero_cols = ['total_items', 'total_price', 'total_freight', 'total_order_value',
                      'total_payment_value', 'num_unique_products', 'num_unique_sellers']
    for col in fill_zero_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    logger.info(f"\n  Final analytical dataset: {len(df):,} rows × {len(df.columns)} columns")
    return df


def build_item_level_dataset(data):
    """
    Build an item-level analytical dataset (non-aggregated) for detailed analysis.
    order_items → products → sellers
    """
    logger.info("\n🔗 Building item-level dataset...")
    df = data['order_items'].copy()

    # Merge with products
    df = df.merge(data['products'], on='product_id', how='left')
    logger.info(f"  + products → {len(df):,} rows")

    # Merge with sellers
    df = df.merge(data['sellers'], on='seller_id', how='left')
    logger.info(f"  + sellers → {len(df):,} rows")

    # Merge with order-level info (just key columns)
    order_cols = ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp',
                  'order_year', 'order_month', 'order_year_month', 'order_day_of_week',
                  'delivery_days', 'delivery_delay_days']
    available_cols = [c for c in order_cols if c in data['orders'].columns]
    df = df.merge(data['orders'][available_cols], on='order_id', how='left')
    logger.info(f"  + orders → {len(df):,} rows")

    logger.info(f"  Item-level dataset: {len(df):,} rows × {len(df.columns)} columns")
    return df


def run_integration():
    """Main integration pipeline."""
    logger.info("=" * 60)
    logger.info("PHASE 3: DATA INTEGRATION")
    logger.info("=" * 60)

    # Load cleaned data
    data = load_cleaned_data()

    # Build datasets
    analytical_df = build_analytical_dataset(data)
    item_level_df = build_item_level_dataset(data)

    # Save
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    analytical_path = os.path.join(OUTPUT_DIR, 'analytical_dataset.csv')
    analytical_df.to_csv(analytical_path, index=False)
    logger.info(f"\n💾 Saved analytical_dataset.csv ({len(analytical_df):,} rows)")

    item_path = os.path.join(OUTPUT_DIR, 'item_level_dataset.csv')
    item_level_df.to_csv(item_path, index=False)
    logger.info(f"💾 Saved item_level_dataset.csv ({len(item_level_df):,} rows)")

    # Summary
    logger.info(f"\n{'─' * 40}")
    logger.info("INTEGRATION SUMMARY")
    logger.info(f"  Analytical dataset: {len(analytical_df):,} rows × {len(analytical_df.columns)} cols")
    logger.info(f"  Item-level dataset: {len(item_level_df):,} rows × {len(item_level_df.columns)} cols")
    logger.info(f"{'─' * 40}")
    logger.info("\n✅ Phase 3 Complete: Data Integration\n")

    return analytical_df, item_level_df


if __name__ == '__main__':
    run_integration()
