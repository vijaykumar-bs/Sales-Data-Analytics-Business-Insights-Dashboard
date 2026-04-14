import sqlite3
import pandas as pd
import json
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'ecommerce.db')
DASHBOARD_DIR = os.path.join(BASE_DIR, 'dashboard')

def export_transactions():
    conn = sqlite3.connect(DB_PATH)
    # Aggregated dataset to minimize size while keeping all filter dimensions
    q = '''
    SELECT 
        o.order_year as yr,
        o.order_year_month as mo,
        c.customer_state as st,
        SUBSTR(p.product_category_name_english, 1, 20) as cat,
        pm.payment_type as pay,
        COUNT(DISTINCT fi.order_id) as orders,
        ROUND(SUM(fi.price), 2) as rev,
        ROUND(AVG(r.review_score), 2) as rev_score,
        COUNT(DISTINCT o.customer_id) as cust
    FROM dim_orders o
    JOIN dim_customers c ON o.customer_id = c.customer_id
    JOIN fact_order_items fi ON o.order_id = fi.order_id
    LEFT JOIN dim_products p ON fi.product_id = p.product_id
    LEFT JOIN fact_payments pm ON o.order_id = pm.order_id
    LEFT JOIN dim_reviews r ON o.order_id = r.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_year, o.order_year_month, c.customer_state, p.product_category_name_english, pm.payment_type
    '''
    df = pd.read_sql(q, conn)
    conn.close()

    df = df.fillna('Unknown')
    
    out_path = os.path.join(DASHBOARD_DIR, 'filter_data.json')
    df.to_json(out_path, orient='records')
    
    size_mb = os.path.getsize(out_path) / (1024*1024)
    print(f"Exported {len(df)} rows to filter_data.json ({size_mb:.2f} MB)")

if __name__ == '__main__':
    export_transactions()
