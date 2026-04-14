"""
Phase 8 (data): Generate Dashboard Data
Precomputes all dashboard metrics from SQLite DB and exports as JSON.
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'ecommerce.db')
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')


def get_conn():
    return sqlite3.connect(DB_PATH)


def compute_kpis():
    """Compute KPI card values."""
    conn = get_conn()
    q = """
    SELECT
        ROUND(SUM(fi.price), 2) AS total_revenue,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
        ROUND(SUM(fi.price) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value,
        ROUND(AVG(r.review_score), 2) AS avg_review_score,
        COUNT(DISTINCT fi.product_id) AS unique_products,
        COUNT(DISTINCT fi.seller_id) AS active_sellers,
        ROUND(SUM(fi.freight_value), 2) AS total_freight,
        ROUND(AVG(o.delivery_days), 1) AS avg_delivery_days
    FROM dim_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    JOIN fact_order_items fi ON o.order_id = fi.order_id
    LEFT JOIN dim_reviews r ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
    """
    result = pd.read_sql(q, conn).iloc[0].to_dict()
    conn.close()

    # Convert numpy types to Python native for JSON serialization
    return {k: float(v) if isinstance(v, (np.floating, np.integer)) else v for k, v in result.items()}


def compute_monthly_trends():
    """Compute monthly revenue and order trends."""
    conn = get_conn()
    q = """
    SELECT
        o.order_year_month AS month,
        ROUND(SUM(fi.price), 2) AS revenue,
        COUNT(DISTINCT o.order_id) AS orders,
        COUNT(DISTINCT o.customer_id) AS customers
    FROM dim_orders o
    JOIN fact_order_items fi ON o.order_id = fi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_year_month
    ORDER BY o.order_year_month
    """
    df = pd.read_sql(q, conn)
    conn.close()
    # Remove edge months (possibly incomplete)
    if len(df) > 2:
        df = df.iloc[1:-1]
    return df.to_dict(orient='list')


def compute_category_performance():
    """Compute top categories by revenue."""
    conn = get_conn()
    q = """
    SELECT
        p.product_category_name_english AS category,
        ROUND(SUM(fi.price), 2) AS revenue,
        COUNT(DISTINCT fi.order_id) AS orders
    FROM fact_order_items fi
    JOIN dim_products p ON fi.product_id = p.product_id
    JOIN dim_orders o ON fi.order_id = o.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY p.product_category_name_english
    ORDER BY revenue DESC
    LIMIT 15
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df.to_dict(orient='list')


def compute_regional_data():
    """Compute revenue by state."""
    conn = get_conn()
    q = """
    SELECT
        c.customer_state AS state,
        ROUND(SUM(fi.price), 2) AS revenue,
        COUNT(DISTINCT o.order_id) AS orders,
        COUNT(DISTINCT c.customer_unique_id) AS customers
    FROM dim_customers c
    JOIN dim_orders o ON c.customer_id = o.customer_id
    JOIN fact_order_items fi ON o.order_id = fi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY c.customer_state
    ORDER BY revenue DESC
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df.to_dict(orient='list')


def compute_payment_distribution():
    """Compute payment method breakdown."""
    conn = get_conn()
    q = """
    SELECT
        payment_type AS type,
        COUNT(*) AS count,
        ROUND(SUM(payment_value), 2) AS value
    FROM fact_payments
    GROUP BY payment_type
    ORDER BY value DESC
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df.to_dict(orient='list')


def compute_customer_segments():
    """Compute RFM-based customer segmentation."""
    conn = get_conn()
    q = """
    WITH customer_rfm AS (
        SELECT
            c.customer_unique_id,
            JULIANDAY('2018-10-01') - JULIANDAY(MAX(o.order_purchase_timestamp)) AS recency,
            COUNT(DISTINCT o.order_id) AS frequency,
            ROUND(SUM(fi.price), 2) AS monetary
        FROM dim_customers c
        JOIN dim_orders o ON c.customer_id = o.customer_id
        JOIN fact_order_items fi ON o.order_id = fi.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY c.customer_unique_id
    ),
    rfm_scores AS (
        SELECT *,
            CASE WHEN recency <= 30 THEN 5 WHEN recency <= 90 THEN 4
                 WHEN recency <= 180 THEN 3 WHEN recency <= 365 THEN 2 ELSE 1 END AS r,
            CASE WHEN frequency >= 5 THEN 5 WHEN frequency >= 3 THEN 4
                 WHEN frequency >= 2 THEN 3 ELSE 1 END AS f,
            CASE WHEN monetary >= 500 THEN 5 WHEN monetary >= 200 THEN 4
                 WHEN monetary >= 100 THEN 3 WHEN monetary >= 50 THEN 2 ELSE 1 END AS m
        FROM customer_rfm
    )
    SELECT
        CASE
            WHEN r >= 4 AND f >= 3 AND m >= 4 THEN 'Champions'
            WHEN r >= 3 AND f >= 2 AND m >= 3 THEN 'Loyal'
            WHEN r >= 4 AND f <= 2 THEN 'Recent'
            WHEN r >= 3 AND m >= 3 THEN 'Potential'
            WHEN r <= 2 AND f >= 2 THEN 'At Risk'
            WHEN r <= 2 AND f <= 1 AND m <= 2 THEN 'Lost'
            ELSE 'Others'
        END AS segment,
        COUNT(*) AS count,
        ROUND(SUM(monetary), 2) AS revenue
    FROM rfm_scores
    GROUP BY segment
    ORDER BY revenue DESC
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df.to_dict(orient='list')


def compute_review_distribution():
    """Review score breakdown."""
    conn = get_conn()
    q = """
    SELECT review_score AS score, COUNT(*) AS count
    FROM dim_reviews
    GROUP BY review_score
    ORDER BY review_score
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df.to_dict(orient='list')


def compute_delivery_metrics():
    """On-time delivery metrics."""
    conn = get_conn()
    q = """
    SELECT
        SUM(CASE WHEN delivery_delay_days <= 0 THEN 1 ELSE 0 END) AS on_time,
        SUM(CASE WHEN delivery_delay_days > 0 THEN 1 ELSE 0 END) AS late,
        COUNT(*) AS total,
        ROUND(AVG(delivery_days), 1) AS avg_delivery_days,
        ROUND(AVG(delivery_delay_days), 1) AS avg_delay
    FROM dim_orders
    WHERE order_status = 'delivered' AND delivery_delay_days IS NOT NULL
    """
    result = pd.read_sql(q, conn).iloc[0].to_dict()
    conn.close()
    result['on_time_rate'] = round(result['on_time'] / result['total'] * 100, 1) if result['total'] > 0 else 0
    return {k: float(v) if isinstance(v, (np.floating, np.integer)) else v for k, v in result.items()}


def compute_dow_patterns():
    """Day-of-week order patterns."""
    conn = get_conn()
    q = """
    SELECT
        order_day_of_week AS day,
        COUNT(DISTINCT order_id) AS orders
    FROM dim_orders
    WHERE order_status = 'delivered'
    GROUP BY order_day_of_week
    """
    df = pd.read_sql(q, conn)
    conn.close()
    # Reorder
    order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    df['day'] = pd.Categorical(df['day'], categories=order, ordered=True)
    df = df.sort_values('day')
    return df.to_dict(orient='list')


def compute_yearly_summary():
    """Yearly summary for YoY comparison."""
    conn = get_conn()
    q = """
    SELECT
        o.order_year AS year,
        ROUND(SUM(fi.price), 2) AS revenue,
        COUNT(DISTINCT o.order_id) AS orders
    FROM dim_orders o
    JOIN fact_order_items fi ON o.order_id = fi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_year
    ORDER BY o.order_year
    """
    df = pd.read_sql(q, conn)
    conn.close()
    return df.to_dict(orient='list')


def run_dashboard_data_generation():
    """Generate all dashboard data and save as JSON."""
    logger.info("=" * 60)
    logger.info("PHASE 8: DASHBOARD DATA GENERATION")
    logger.info("=" * 60)

    os.makedirs(DASHBOARD_DIR, exist_ok=True)

    logger.info("\n📊 Computing dashboard metrics...")

    dashboard_data = {
        'kpis': compute_kpis(),
        'monthly_trends': compute_monthly_trends(),
        'category_performance': compute_category_performance(),
        'regional_data': compute_regional_data(),
        'payment_distribution': compute_payment_distribution(),
        'customer_segments': compute_customer_segments(),
        'review_distribution': compute_review_distribution(),
        'delivery_metrics': compute_delivery_metrics(),
        'dow_patterns': compute_dow_patterns(),
        'yearly_summary': compute_yearly_summary(),
    }

    # Save as JS file to avoid CORS issues on local file:// protocol
    output_path = os.path.join(DASHBOARD_DIR, 'dashboard_data.js')
    with open(output_path, 'w', encoding='utf-8') as f:
        f.write("window.DASHBOARD_DATA = ")
        json.dump(dashboard_data, f, indent=2, default=str)
        f.write(";")

    logger.info(f"\n💾 Dashboard data saved to: {output_path}")
    logger.info(f"  File size: {os.path.getsize(output_path) / 1024:.1f} KB")
    logger.info(f"  Sections: {list(dashboard_data.keys())}")

    logger.info("\n✅ Phase 8 (data): Dashboard Data Generation Complete\n")
    return dashboard_data


if __name__ == '__main__':
    run_dashboard_data_generation()
