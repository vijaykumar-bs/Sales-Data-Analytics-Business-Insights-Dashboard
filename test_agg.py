import sqlite3
import pandas as pd
import json
import os

conn = sqlite3.connect('ecommerce.db')
q = '''
SELECT 
    o.order_year as year,
    o.order_year_month as month,
    c.customer_state as state,
    p.product_category_name_english as category,
    COUNT(DISTINCT o.order_id) as orders,
    ROUND(SUM(fi.price), 2) as revenue
FROM dim_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN fact_order_items fi ON o.order_id = fi.order_id
LEFT JOIN dim_products p ON fi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY o.order_year, o.order_year_month, c.customer_state, p.product_category_name_english
'''
df = pd.read_sql(q, conn)
conn.close()

print(f'Rows: {len(df)}')
df.to_json('dashboard/filter_test.json', orient='records')
print(f'Size: {os.path.getsize("dashboard/filter_test.json") / 1024:.2f} KB')
