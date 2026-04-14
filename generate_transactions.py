import sqlite3
import pandas as pd
import json

conn = sqlite3.connect('ecommerce.db')
q = '''
SELECT 
    o.order_year_month as m,
    c.customer_state as s,
    SUBSTR(p.product_category_name_english, 1, 15) as c,
    ROUND(SUM(fi.price), 2) as r,
    COUNT(DISTINCT fi.order_id) as o
FROM dim_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
JOIN fact_order_items fi ON o.order_id = fi.order_id
LEFT JOIN dim_products p ON fi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY o.order_year_month, c.customer_state, p.product_category_name_english
'''
df = pd.read_sql(q, conn)
conn.close()

# Keep it small
df = df.fillna('Other')
df.to_json('dashboard/transactions.json', orient='records', separators=(',', ':'))

import os
print(f"Size: {os.path.getsize('dashboard/transactions.json') / 1024:.2f} KB")
