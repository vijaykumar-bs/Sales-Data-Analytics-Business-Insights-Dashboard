-- ============================================================================
-- E-Commerce Data Analytics: SQL Queries
-- Database: ecommerce.db (SQLite)
-- Schema: Star schema with dim_* and fact_* tables
-- ============================================================================

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 1: Total Revenue & Order Volume Overview
-- ────────────────────────────────────────────────────────────────────────────
-- Q1_OVERVIEW
SELECT
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(DISTINCT o.customer_id) AS total_customers,
    ROUND(SUM(fi.price), 2) AS total_revenue,
    ROUND(SUM(fi.freight_value), 2) AS total_freight,
    ROUND(SUM(fi.total_item_value), 2) AS total_gmv,
    ROUND(AVG(fi.price), 2) AS avg_item_price,
    COUNT(DISTINCT fi.product_id) AS unique_products,
    COUNT(DISTINCT fi.seller_id) AS active_sellers
FROM dim_orders o
JOIN fact_order_items fi ON o.order_id = fi.order_id
WHERE o.order_status = 'delivered';

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 2: Monthly Sales Trends
-- ────────────────────────────────────────────────────────────────────────────
-- Q2_MONTHLY_TRENDS
SELECT
    o.order_year_month,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(fi.price), 2) AS revenue,
    ROUND(SUM(fi.freight_value), 2) AS freight_revenue,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(AVG(fi.price), 2) AS avg_order_value
FROM dim_orders o
JOIN fact_order_items fi ON o.order_id = fi.order_id
WHERE o.order_status = 'delivered'
GROUP BY o.order_year_month
ORDER BY o.order_year_month;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 3: Yearly Sales Summary
-- ────────────────────────────────────────────────────────────────────────────
-- Q3_YEARLY_SUMMARY
SELECT
    o.order_year,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(fi.price), 2) AS revenue,
    COUNT(DISTINCT o.customer_id) AS unique_customers,
    ROUND(SUM(fi.price) / COUNT(DISTINCT o.order_id), 2) AS avg_order_value
FROM dim_orders o
JOIN fact_order_items fi ON o.order_id = fi.order_id
WHERE o.order_status = 'delivered'
GROUP BY o.order_year
ORDER BY o.order_year;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 4: Top 10 Customers by Lifetime Value
-- ────────────────────────────────────────────────────────────────────────────
-- Q4_TOP_CUSTOMERS
SELECT
    c.customer_unique_id,
    c.customer_city,
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS total_orders,
    ROUND(SUM(fi.price), 2) AS total_spent,
    ROUND(AVG(fi.price), 2) AS avg_item_value,
    ROUND(AVG(r.review_score), 1) AS avg_review_score
FROM dim_customers c
JOIN dim_orders o ON c.customer_id = o.customer_id
JOIN fact_order_items fi ON o.order_id = fi.order_id
LEFT JOIN dim_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id
ORDER BY total_spent DESC
LIMIT 10;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 5: Product Category Performance (Top 20)
-- ────────────────────────────────────────────────────────────────────────────
-- Q5_CATEGORY_PERFORMANCE
SELECT
    p.product_category_name_english AS category,
    COUNT(DISTINCT fi.order_id) AS order_count,
    SUM(fi.order_item_id) AS items_sold,
    ROUND(SUM(fi.price), 2) AS total_revenue,
    ROUND(AVG(fi.price), 2) AS avg_price,
    ROUND(SUM(fi.price) * 100.0 / (SELECT SUM(price) FROM fact_order_items), 2) AS revenue_share_pct
FROM fact_order_items fi
JOIN dim_products p ON fi.product_id = p.product_id
JOIN dim_orders o ON fi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name_english
ORDER BY total_revenue DESC
LIMIT 20;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 6: Payment Method Distribution
-- ────────────────────────────────────────────────────────────────────────────
-- Q6_PAYMENT_DISTRIBUTION
SELECT
    fp.payment_type,
    COUNT(*) AS transaction_count,
    ROUND(SUM(fp.payment_value), 2) AS total_value,
    ROUND(AVG(fp.payment_value), 2) AS avg_value,
    ROUND(AVG(fp.payment_installments), 1) AS avg_installments,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM fact_payments), 2) AS pct_of_transactions
FROM fact_payments fp
GROUP BY fp.payment_type
ORDER BY total_value DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 7: Average Delivery Time by State
-- ────────────────────────────────────────────────────────────────────────────
-- Q7_DELIVERY_BY_STATE
SELECT
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(AVG(o.delivery_days), 1) AS avg_delivery_days,
    ROUND(AVG(o.delivery_delay_days), 1) AS avg_delay_days,
    ROUND(SUM(CASE WHEN o.delivery_delay_days <= 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS on_time_pct
FROM dim_orders o
JOIN dim_customers c ON o.customer_id = c.customer_id
WHERE o.order_status = 'delivered'
  AND o.delivery_days IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_days;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 8: Customer Segmentation (RFM-based)
-- ────────────────────────────────────────────────────────────────────────────
-- Q8_CUSTOMER_SEGMENTATION
WITH customer_rfm AS (
    SELECT
        c.customer_unique_id,
        JULIANDAY('2018-10-01') - JULIANDAY(MAX(o.order_purchase_timestamp)) AS recency_days,
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
        CASE
            WHEN recency_days <= 30 THEN 5
            WHEN recency_days <= 90 THEN 4
            WHEN recency_days <= 180 THEN 3
            WHEN recency_days <= 365 THEN 2
            ELSE 1
        END AS r_score,
        CASE
            WHEN frequency >= 5 THEN 5
            WHEN frequency >= 3 THEN 4
            WHEN frequency >= 2 THEN 3
            ELSE 1
        END AS f_score,
        CASE
            WHEN monetary >= 500 THEN 5
            WHEN monetary >= 200 THEN 4
            WHEN monetary >= 100 THEN 3
            WHEN monetary >= 50 THEN 2
            ELSE 1
        END AS m_score
    FROM customer_rfm
)
SELECT
    CASE
        WHEN r_score >= 4 AND f_score >= 3 AND m_score >= 4 THEN 'Champions'
        WHEN r_score >= 3 AND f_score >= 2 AND m_score >= 3 THEN 'Loyal Customers'
        WHEN r_score >= 4 AND f_score <= 2 THEN 'Recent Customers'
        WHEN r_score >= 3 AND m_score >= 3 THEN 'Potential Loyalists'
        WHEN r_score <= 2 AND f_score >= 2 THEN 'At Risk'
        WHEN r_score <= 2 AND f_score <= 1 AND m_score <= 2 THEN 'Lost'
        ELSE 'Others'
    END AS segment,
    COUNT(*) AS customer_count,
    ROUND(AVG(recency_days), 0) AS avg_recency,
    ROUND(AVG(frequency), 1) AS avg_frequency,
    ROUND(AVG(monetary), 2) AS avg_monetary,
    ROUND(SUM(monetary), 2) AS total_revenue
FROM rfm_scores
GROUP BY segment
ORDER BY total_revenue DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 9: Seller Performance Rankings
-- ────────────────────────────────────────────────────────────────────────────
-- Q9_SELLER_PERFORMANCE
SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    COUNT(DISTINCT fi.order_id) AS orders_fulfilled,
    ROUND(SUM(fi.price), 2) AS total_revenue,
    ROUND(AVG(fi.price), 2) AS avg_item_price,
    COUNT(DISTINCT fi.product_id) AS unique_products,
    ROUND(AVG(r.review_score), 2) AS avg_review_score
FROM dim_sellers s
JOIN fact_order_items fi ON s.seller_id = fi.seller_id
JOIN dim_orders o ON fi.order_id = o.order_id
LEFT JOIN dim_reviews r ON o.order_id = r.order_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_id
ORDER BY total_revenue DESC
LIMIT 15;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 10: Review Score Distribution with Delivery Correlation
-- ────────────────────────────────────────────────────────────────────────────
-- Q10_REVIEW_ANALYSIS
SELECT
    r.review_score,
    COUNT(*) AS review_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_reviews), 1) AS pct,
    ROUND(AVG(o.delivery_days), 1) AS avg_delivery_days,
    ROUND(AVG(o.delivery_delay_days), 1) AS avg_delay_days,
    SUM(r.has_comment) AS with_comments
FROM dim_reviews r
JOIN dim_orders o ON r.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY r.review_score
ORDER BY r.review_score;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 11: Revenue by State (Regional Performance)
-- ────────────────────────────────────────────────────────────────────────────
-- Q11_REGIONAL_PERFORMANCE
SELECT
    c.customer_state,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(DISTINCT c.customer_unique_id) AS unique_customers,
    ROUND(SUM(fi.price), 2) AS total_revenue,
    ROUND(AVG(fi.price), 2) AS avg_order_value,
    ROUND(SUM(fi.price) * 100.0 / (SELECT SUM(price) FROM fact_order_items), 2) AS revenue_share_pct
FROM dim_customers c
JOIN dim_orders o ON c.customer_id = o.customer_id
JOIN fact_order_items fi ON o.order_id = fi.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_revenue DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 12: Day-of-Week Purchase Patterns
-- ────────────────────────────────────────────────────────────────────────────
-- Q12_DOW_PATTERNS
SELECT
    o.order_day_of_week,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(fi.price), 2) AS revenue,
    ROUND(AVG(fi.price), 2) AS avg_item_price
FROM dim_orders o
JOIN fact_order_items fi ON o.order_id = fi.order_id
WHERE o.order_status = 'delivered'
GROUP BY o.order_day_of_week
ORDER BY
    CASE o.order_day_of_week
        WHEN 'Monday' THEN 1
        WHEN 'Tuesday' THEN 2
        WHEN 'Wednesday' THEN 3
        WHEN 'Thursday' THEN 4
        WHEN 'Friday' THEN 5
        WHEN 'Saturday' THEN 6
        WHEN 'Sunday' THEN 7
    END;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 13: Hourly Purchase Distribution
-- ────────────────────────────────────────────────────────────────────────────
-- Q13_HOURLY_PATTERNS
SELECT
    o.order_hour,
    COUNT(DISTINCT o.order_id) AS order_count,
    ROUND(SUM(fi.price), 2) AS revenue
FROM dim_orders o
JOIN fact_order_items fi ON o.order_id = fi.order_id
WHERE o.order_status = 'delivered'
GROUP BY o.order_hour
ORDER BY o.order_hour;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 14: Order Cancellation Analysis
-- ────────────────────────────────────────────────────────────────────────────
-- Q14_CANCELLATION_ANALYSIS
SELECT
    o.order_status,
    COUNT(*) AS order_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM dim_orders), 2) AS pct
FROM dim_orders o
GROUP BY o.order_status
ORDER BY order_count DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 15: Freight Cost Analysis by State
-- ────────────────────────────────────────────────────────────────────────────
-- Q15_FREIGHT_ANALYSIS
SELECT
    c.customer_state,
    ROUND(AVG(fi.freight_value), 2) AS avg_freight,
    ROUND(SUM(fi.freight_value), 2) AS total_freight,
    ROUND(AVG(fi.freight_value / NULLIF(fi.price, 0)) * 100, 1) AS freight_pct_of_price,
    COUNT(*) AS item_count
FROM dim_customers c
JOIN dim_orders o ON c.customer_id = o.customer_id
JOIN fact_order_items fi ON o.order_id = fi.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY avg_freight DESC;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 16: Year-over-Year Growth Rate
-- ────────────────────────────────────────────────────────────────────────────
-- Q16_YOY_GROWTH
WITH yearly AS (
    SELECT
        o.order_year,
        ROUND(SUM(fi.price), 2) AS revenue,
        COUNT(DISTINCT o.order_id) AS orders
    FROM dim_orders o
    JOIN fact_order_items fi ON o.order_id = fi.order_id
    WHERE o.order_status = 'delivered'
    GROUP BY o.order_year
)
SELECT
    y1.order_year,
    y1.revenue,
    y1.orders,
    ROUND((y1.revenue - y2.revenue) * 100.0 / y2.revenue, 1) AS revenue_growth_pct,
    ROUND((y1.orders - y2.orders) * 100.0 / y2.orders, 1) AS order_growth_pct
FROM yearly y1
LEFT JOIN yearly y2 ON y1.order_year = y2.order_year + 1
ORDER BY y1.order_year;

-- ────────────────────────────────────────────────────────────────────────────
-- QUERY 17: Product Category Review Analysis
-- ────────────────────────────────────────────────────────────────────────────
-- Q17_CATEGORY_REVIEWS
SELECT
    p.product_category_name_english AS category,
    COUNT(DISTINCT r.review_id) AS review_count,
    ROUND(AVG(r.review_score), 2) AS avg_score,
    SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) AS negative_reviews,
    ROUND(SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS satisfaction_rate
FROM dim_reviews r
JOIN dim_orders o ON r.order_id = o.order_id
JOIN fact_order_items fi ON o.order_id = fi.order_id
JOIN dim_products p ON fi.product_id = p.product_id
WHERE o.order_status = 'delivered'
GROUP BY p.product_category_name_english
HAVING review_count >= 50
ORDER BY avg_score DESC
LIMIT 20;
