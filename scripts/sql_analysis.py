"""
Phase 5: SQL-Based Analysis Execution
Executes all SQL queries from sql_queries.sql against the SQLite database
and saves results as CSVs with formatted summaries.
"""

import pandas as pd
import sqlite3
import os
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'ecommerce.db')
SQL_DIR = os.path.join(BASE_DIR, 'sql')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output', 'sql_results')


def parse_sql_file(sql_file_path):
    """Parse the SQL file and extract individual named queries."""
    with open(sql_file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Split by query markers (-- Q<number>_<name>)
    pattern = r'--\s*(Q\d+_\w+)\s*\n(.*?)(?=--\s*Q\d+_\w+|$)'
    matches = re.findall(pattern, content, re.DOTALL)

    queries = {}
    for name, sql in matches:
        sql = sql.strip()
        # Remove comment lines within the query
        sql_lines = [line for line in sql.split('\n') if not line.strip().startswith('--')]
        sql = '\n'.join(sql_lines).strip()
        if sql:
            # Remove trailing semicolons for SQLite compatibility
            sql = sql.rstrip(';')
            queries[name] = sql

    return queries


def execute_queries(queries):
    """Execute all queries and return results as DataFrames."""
    conn = sqlite3.connect(DB_PATH)
    results = {}

    for name, sql in queries.items():
        try:
            logger.info(f"  Executing {name}...")
            df = pd.read_sql_query(sql, conn)
            results[name] = df
            logger.info(f"    → {len(df)} rows returned")
        except Exception as e:
            logger.error(f"    ❌ Error in {name}: {e}")
            results[name] = pd.DataFrame()

    conn.close()
    return results


def save_results(results):
    """Save all query results as CSVs."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for name, df in results.items():
        filepath = os.path.join(OUTPUT_DIR, f'{name.lower()}.csv')
        df.to_csv(filepath, index=False)
        logger.info(f"  💾 Saved {name.lower()}.csv")


def print_summary(results):
    """Print formatted summaries of key findings."""
    logger.info("\n" + "=" * 60)
    logger.info("SQL ANALYSIS — KEY FINDINGS")
    logger.info("=" * 60)

    # Overview
    if 'Q1_OVERVIEW' in results and not results['Q1_OVERVIEW'].empty:
        r = results['Q1_OVERVIEW'].iloc[0]
        logger.info(f"\n📊 BUSINESS OVERVIEW")
        logger.info(f"  Total Orders: {int(r.get('total_orders', 0)):,}")
        logger.info(f"  Total Customers: {int(r.get('total_customers', 0)):,}")
        logger.info(f"  Total Revenue: R$ {r.get('total_revenue', 0):,.2f}")
        logger.info(f"  Total GMV (incl. freight): R$ {r.get('total_gmv', 0):,.2f}")
        logger.info(f"  Avg Item Price: R$ {r.get('avg_item_price', 0):,.2f}")
        logger.info(f"  Unique Products: {int(r.get('unique_products', 0)):,}")
        logger.info(f"  Active Sellers: {int(r.get('active_sellers', 0)):,}")

    # Payment distribution
    if 'Q6_PAYMENT_DISTRIBUTION' in results and not results['Q6_PAYMENT_DISTRIBUTION'].empty:
        logger.info(f"\n💳 PAYMENT METHODS")
        for _, row in results['Q6_PAYMENT_DISTRIBUTION'].iterrows():
            logger.info(f"  {row['payment_type']}: {row['pct_of_transactions']}% of transactions, R$ {row['total_value']:,.2f}")

    # Top categories
    if 'Q5_CATEGORY_PERFORMANCE' in results and not results['Q5_CATEGORY_PERFORMANCE'].empty:
        logger.info(f"\n🏆 TOP 5 CATEGORIES BY REVENUE")
        for _, row in results['Q5_CATEGORY_PERFORMANCE'].head(5).iterrows():
            logger.info(f"  {row['category']}: R$ {row['total_revenue']:,.2f} ({row['revenue_share_pct']}%)")

    # Customer segments
    if 'Q8_CUSTOMER_SEGMENTATION' in results and not results['Q8_CUSTOMER_SEGMENTATION'].empty:
        logger.info(f"\n👥 CUSTOMER SEGMENTS")
        for _, row in results['Q8_CUSTOMER_SEGMENTATION'].iterrows():
            logger.info(f"  {row['segment']}: {int(row['customer_count']):,} customers, R$ {row['total_revenue']:,.2f}")

    # Review distribution
    if 'Q10_REVIEW_ANALYSIS' in results and not results['Q10_REVIEW_ANALYSIS'].empty:
        logger.info(f"\n⭐ REVIEW SCORE DISTRIBUTION")
        for _, row in results['Q10_REVIEW_ANALYSIS'].iterrows():
            stars = '★' * int(row['review_score']) + '☆' * (5 - int(row['review_score']))
            logger.info(f"  {stars}: {int(row['review_count']):,} ({row['pct']}%) | Avg delivery: {row['avg_delivery_days']} days")


def run_sql_analysis():
    """Main SQL analysis pipeline."""
    logger.info("=" * 60)
    logger.info("PHASE 5: SQL-BASED ANALYSIS")
    logger.info("=" * 60)

    # Parse SQL file
    sql_file = os.path.join(SQL_DIR, 'sql_queries.sql')
    logger.info(f"\n📄 Parsing SQL queries from: {sql_file}")
    queries = parse_sql_file(sql_file)
    logger.info(f"  Found {len(queries)} queries\n")

    # Execute queries
    logger.info("🔄 Executing queries...")
    results = execute_queries(queries)

    # Save results
    logger.info("\n💾 Saving results...")
    save_results(results)

    # Print summary
    print_summary(results)

    logger.info(f"\n{'─' * 40}")
    logger.info("SQL ANALYSIS SUMMARY")
    logger.info(f"  Queries executed: {len(results)}")
    logger.info(f"  Results saved to: {OUTPUT_DIR}")
    logger.info(f"{'─' * 40}")
    logger.info("\n✅ Phase 5 Complete: SQL-Based Analysis\n")

    return results


if __name__ == '__main__':
    run_sql_analysis()
