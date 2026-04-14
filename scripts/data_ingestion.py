"""
Phase 1: Data Ingestion & Validation
Loads all CSV datasets, validates schemas, checks referential integrity,
and generates a data quality report.
"""

import pandas as pd
import numpy as np
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ─── Configuration ───────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'Data')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'output')

# Expected schemas: {filename: {column_name: expected_dtype_category}}
EXPECTED_SCHEMAS = {
    'olist_customers_dataset.csv': {
        'customer_id': 'object',
        'customer_unique_id': 'object',
        'customer_zip_code_prefix': 'int64',
        'customer_city': 'object',
        'customer_state': 'object',
    },
    'olist_orders_dataset.csv': {
        'order_id': 'object',
        'customer_id': 'object',
        'order_status': 'object',
        'order_purchase_timestamp': 'object',
        'order_approved_at': 'object',
        'order_delivered_carrier_date': 'object',
        'order_delivered_customer_date': 'object',
        'order_estimated_delivery_date': 'object',
    },
    'olist_order_items_dataset.csv': {
        'order_id': 'object',
        'order_item_id': 'int64',
        'product_id': 'object',
        'seller_id': 'object',
        'shipping_limit_date': 'object',
        'price': 'float64',
        'freight_value': 'float64',
    },
    'olist_order_payments_dataset.csv': {
        'order_id': 'object',
        'payment_sequential': 'int64',
        'payment_type': 'object',
        'payment_installments': 'int64',
        'payment_value': 'float64',
    },
    'olist_products_dataset.csv': {
        'product_id': 'object',
        'product_category_name': 'object',
    },
    'olist_sellers_dataset.csv': {
        'seller_id': 'object',
        'seller_zip_code_prefix': 'int64',
        'seller_city': 'object',
        'seller_state': 'object',
    },
    'olist_order_reviews_dataset.csv': {
        'review_id': 'object',
        'order_id': 'object',
        'review_score': 'int64',
        'review_creation_date': 'object',
        'review_answer_timestamp': 'object',
    },
    'olist_geolocation_dataset.csv': {
        'geolocation_zip_code_prefix': 'int64',
        'geolocation_lat': 'float64',
        'geolocation_lng': 'float64',
        'geolocation_city': 'object',
        'geolocation_state': 'object',
    },
    'product_category_name_translation.csv': {
        'product_category_name': 'object',
        'product_category_name_english': 'object',
    },
}

# Primary key definitions for uniqueness checks
PRIMARY_KEYS = {
    'olist_customers_dataset.csv': ['customer_id'],
    'olist_orders_dataset.csv': ['order_id'],
    'olist_products_dataset.csv': ['product_id'],
    'olist_sellers_dataset.csv': ['seller_id'],
}

# Foreign key relationships: {child_table: [(child_col, parent_table, parent_col), ...]}
FOREIGN_KEYS = {
    'olist_orders_dataset.csv': [
        ('customer_id', 'olist_customers_dataset.csv', 'customer_id'),
    ],
    'olist_order_items_dataset.csv': [
        ('order_id', 'olist_orders_dataset.csv', 'order_id'),
        ('product_id', 'olist_products_dataset.csv', 'product_id'),
        ('seller_id', 'olist_sellers_dataset.csv', 'seller_id'),
    ],
    'olist_order_payments_dataset.csv': [
        ('order_id', 'olist_orders_dataset.csv', 'order_id'),
    ],
    'olist_order_reviews_dataset.csv': [
        ('order_id', 'olist_orders_dataset.csv', 'order_id'),
    ],
}


# ─── Functions ───────────────────────────────────────────────────────────────

def load_datasets(data_dir):
    """Load all CSV datasets into a dictionary of DataFrames."""
    datasets = {}
    for filename in EXPECTED_SCHEMAS.keys():
        filepath = os.path.join(data_dir, filename)
        if not os.path.exists(filepath):
            logger.warning(f"File not found: {filepath}")
            continue
        logger.info(f"Loading {filename}...")
        df = pd.read_csv(filepath)
        datasets[filename] = df
        logger.info(f"  → {len(df):,} rows, {len(df.columns)} columns")
    return datasets


def validate_schema(datasets):
    """Validate that each dataset has the expected columns and types."""
    report = []
    for filename, expected_cols in EXPECTED_SCHEMAS.items():
        if filename not in datasets:
            report.append({'table': filename, 'check': 'SCHEMA', 'status': 'FAIL', 'detail': 'File not loaded'})
            continue
        df = datasets[filename]
        # Check for missing columns
        missing = set(expected_cols.keys()) - set(df.columns)
        extra = set(df.columns) - set(expected_cols.keys())
        if missing:
            report.append({'table': filename, 'check': 'MISSING_COLS', 'status': 'FAIL', 'detail': str(missing)})
        else:
            report.append({'table': filename, 'check': 'COLUMNS', 'status': 'PASS', 'detail': f'{len(df.columns)} columns found'})
        if extra:
            report.append({'table': filename, 'check': 'EXTRA_COLS', 'status': 'INFO', 'detail': str(extra)})
        # Check data types for key columns
        for col, expected_dtype in expected_cols.items():
            if col in df.columns:
                actual_dtype = str(df[col].dtype)
                if actual_dtype != expected_dtype:
                    report.append({
                        'table': filename, 'check': f'DTYPE_{col}',
                        'status': 'WARN',
                        'detail': f'Expected {expected_dtype}, got {actual_dtype}'
                    })
    return report


def check_primary_keys(datasets):
    """Check primary key uniqueness."""
    report = []
    for filename, pk_cols in PRIMARY_KEYS.items():
        if filename not in datasets:
            continue
        df = datasets[filename]
        duplicates = df.duplicated(subset=pk_cols, keep=False).sum()
        status = 'PASS' if duplicates == 0 else 'FAIL'
        report.append({
            'table': filename, 'check': 'PRIMARY_KEY',
            'status': status,
            'detail': f'PK={pk_cols}, duplicates={duplicates}'
        })
    return report


def check_referential_integrity(datasets):
    """Check foreign key relationships across tables."""
    report = []
    for child_table, fk_list in FOREIGN_KEYS.items():
        if child_table not in datasets:
            continue
        child_df = datasets[child_table]
        for child_col, parent_table, parent_col in fk_list:
            if parent_table not in datasets:
                report.append({
                    'table': child_table, 'check': f'FK_{child_col}',
                    'status': 'SKIP', 'detail': f'Parent table {parent_table} not loaded'
                })
                continue
            parent_df = datasets[parent_table]
            parent_values = set(parent_df[parent_col].dropna().unique())
            child_values = set(child_df[child_col].dropna().unique())
            orphans = child_values - parent_values
            orphan_count = len(orphans)
            status = 'PASS' if orphan_count == 0 else 'WARN'
            report.append({
                'table': child_table, 'check': f'FK_{child_col}→{parent_table}.{parent_col}',
                'status': status,
                'detail': f'{orphan_count} orphan values out of {len(child_values)} unique'
            })
    return report


def check_duplicates(datasets):
    """Check for full-row duplicates."""
    report = []
    for filename, df in datasets.items():
        dup_count = df.duplicated().sum()
        status = 'PASS' if dup_count == 0 else 'WARN'
        report.append({
            'table': filename, 'check': 'DUPLICATES',
            'status': status,
            'detail': f'{dup_count} duplicate rows out of {len(df):,}'
        })
    return report


def generate_null_report(datasets):
    """Generate null value statistics for all datasets."""
    report = []
    for filename, df in datasets.items():
        null_counts = df.isnull().sum()
        for col in df.columns:
            null_count = null_counts[col]
            null_pct = (null_count / len(df)) * 100
            if null_count > 0:
                report.append({
                    'table': filename, 'check': f'NULLS_{col}',
                    'status': 'INFO',
                    'detail': f'{null_count:,} nulls ({null_pct:.1f}%)'
                })
    return report


def run_ingestion():
    """Main ingestion and validation pipeline."""
    logger.info("=" * 60)
    logger.info("PHASE 1: DATA INGESTION & VALIDATION")
    logger.info("=" * 60)

    # Step 1: Load all datasets
    logger.info("\n📥 Step 1: Loading datasets...")
    datasets = load_datasets(DATA_DIR)
    logger.info(f"Loaded {len(datasets)} datasets successfully.\n")

    # Step 2: Schema validation
    logger.info("🔍 Step 2: Validating schemas...")
    all_reports = []
    all_reports.extend(validate_schema(datasets))

    # Step 3: Primary key checks
    logger.info("🔑 Step 3: Checking primary keys...")
    all_reports.extend(check_primary_keys(datasets))

    # Step 4: Referential integrity
    logger.info("🔗 Step 4: Checking referential integrity...")
    all_reports.extend(check_referential_integrity(datasets))

    # Step 5: Duplicate detection
    logger.info("🔄 Step 5: Checking for duplicates...")
    all_reports.extend(check_duplicates(datasets))

    # Step 6: Null analysis
    logger.info("📊 Step 6: Analyzing null values...")
    all_reports.extend(generate_null_report(datasets))

    # Generate and save quality report
    report_df = pd.DataFrame(all_reports)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    report_path = os.path.join(OUTPUT_DIR, 'data_quality_report.csv')
    report_df.to_csv(report_path, index=False)
    logger.info(f"\n📋 Data quality report saved to: {report_path}")

    # Summary
    pass_count = len(report_df[report_df['status'] == 'PASS'])
    warn_count = len(report_df[report_df['status'] == 'WARN'])
    fail_count = len(report_df[report_df['status'] == 'FAIL'])
    info_count = len(report_df[report_df['status'] == 'INFO'])

    logger.info(f"\n{'─' * 40}")
    logger.info(f"QUALITY REPORT SUMMARY")
    logger.info(f"  ✅ PASS: {pass_count}")
    logger.info(f"  ⚠️  WARN: {warn_count}")
    logger.info(f"  ❌ FAIL: {fail_count}")
    logger.info(f"  ℹ️  INFO: {info_count}")
    logger.info(f"{'─' * 40}")

    if fail_count > 0:
        logger.warning("Critical failures detected. Review the quality report.")
        for _, row in report_df[report_df['status'] == 'FAIL'].iterrows():
            logger.warning(f"  FAIL: {row['table']} → {row['check']}: {row['detail']}")

    logger.info("\n✅ Phase 1 Complete: Data Ingestion & Validation\n")
    return datasets


if __name__ == '__main__':
    run_ingestion()
