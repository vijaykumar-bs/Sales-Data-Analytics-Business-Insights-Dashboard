"""
Phase 6: Exploratory Data Analysis (EDA)
Generates publication-quality visualizations using Matplotlib and Seaborn.
Covers distributions, trends, correlations, geographic analysis, and anomaly detection.
"""

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import seaborn as sns
import sqlite3
import os
import warnings
import logging

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'ecommerce.db')
OUTPUT_DIR = os.path.join(BASE_DIR, 'output', 'visualizations')

# ─── Style Configuration ────────────────────────────────────────────────────

COLORS = {
    'primary': '#6366f1',
    'secondary': '#8b5cf6',
    'accent': '#06b6d4',
    'success': '#10b981',
    'warning': '#f59e0b',
    'danger': '#ef4444',
    'dark': '#1e1b4b',
    'bg': '#0f0e1a',
    'text': '#e2e8f0',
    'grid': '#2d2b55',
}

PALETTE = ['#6366f1', '#8b5cf6', '#06b6d4', '#10b981', '#f59e0b', '#ef4444',
           '#ec4899', '#14b8a6', '#f97316', '#3b82f6']


def setup_style():
    """Configure global plot styling."""
    plt.rcParams.update({
        'figure.facecolor': COLORS['bg'],
        'axes.facecolor': '#16142d',
        'axes.edgecolor': COLORS['grid'],
        'axes.labelcolor': COLORS['text'],
        'text.color': COLORS['text'],
        'xtick.color': COLORS['text'],
        'ytick.color': COLORS['text'],
        'grid.color': COLORS['grid'],
        'grid.alpha': 0.3,
        'font.family': 'sans-serif',
        'font.size': 11,
        'axes.titlesize': 14,
        'axes.titleweight': 'bold',
        'figure.titlesize': 16,
    })


def get_data():
    """Load data from SQLite database."""
    conn = sqlite3.connect(DB_PATH)

    orders = pd.read_sql("SELECT * FROM dim_orders", conn)
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])

    items = pd.read_sql("SELECT * FROM fact_order_items", conn)
    customers = pd.read_sql("SELECT * FROM dim_customers", conn)
    products = pd.read_sql("SELECT * FROM dim_products", conn)
    payments = pd.read_sql("SELECT * FROM fact_payments", conn)
    reviews = pd.read_sql("SELECT * FROM dim_reviews", conn)
    sellers = pd.read_sql("SELECT * FROM dim_sellers", conn)

    conn.close()
    return orders, items, customers, products, payments, reviews, sellers


# ─── Visualization Functions ────────────────────────────────────────────────

def plot_monthly_revenue_trends(orders, items):
    """Plot 1: Monthly revenue and order volume trends."""
    logger.info("  📈 Plotting monthly revenue trends...")
    merged = orders.merge(items, on='order_id')
    merged = merged[merged['order_status'] == 'delivered']
    monthly = merged.groupby('order_year_month').agg(
        revenue=('price', 'sum'),
        orders=('order_id', 'nunique'),
    ).reset_index()
    monthly = monthly.sort_values('order_year_month')
    # Remove incomplete months at edges
    monthly = monthly.iloc[1:-1]

    fig, ax1 = plt.subplots(figsize=(14, 6))
    ax2 = ax1.twinx()

    ax1.fill_between(range(len(monthly)), monthly['revenue'], alpha=0.3, color=COLORS['primary'])
    ax1.plot(range(len(monthly)), monthly['revenue'], color=COLORS['primary'], linewidth=2.5, marker='o', markersize=4, label='Revenue')
    ax2.plot(range(len(monthly)), monthly['orders'], color=COLORS['accent'], linewidth=2, linestyle='--', marker='s', markersize=3, label='Orders')

    ax1.set_xlabel('Month')
    ax1.set_ylabel('Revenue (R$)', color=COLORS['primary'])
    ax2.set_ylabel('Order Count', color=COLORS['accent'])
    ax1.set_title('Monthly Revenue & Order Volume Trends', pad=15)

    # X-axis labels
    tick_positions = list(range(0, len(monthly), max(1, len(monthly)//10)))
    ax1.set_xticks(tick_positions)
    ax1.set_xticklabels([monthly.iloc[i]['order_year_month'] for i in tick_positions], rotation=45, ha='right', fontsize=8)

    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}K'))
    ax1.grid(True, alpha=0.2)
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '01_monthly_revenue_trends.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_revenue_by_category(items, products):
    """Plot 2: Top 15 categories by revenue."""
    logger.info("  📊 Plotting category performance...")
    merged = items.merge(products, on='product_id')
    cat_rev = merged.groupby('product_category_name_english')['price'].sum().sort_values(ascending=True).tail(15)

    fig, ax = plt.subplots(figsize=(12, 7))
    bars = ax.barh(range(len(cat_rev)), cat_rev.values, color=PALETTE[:len(cat_rev)][::-1], edgecolor='none', height=0.7)

    ax.set_yticks(range(len(cat_rev)))
    ax.set_yticklabels(cat_rev.index, fontsize=10)
    ax.set_xlabel('Revenue (R$)')
    ax.set_title('Top 15 Product Categories by Revenue', pad=15)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'R$ {x/1000:.0f}K'))
    ax.grid(axis='x', alpha=0.2)

    # Value labels
    for bar, val in zip(bars, cat_rev.values):
        ax.text(val + cat_rev.max() * 0.01, bar.get_y() + bar.get_height()/2,
                f'R$ {val/1000:.1f}K', va='center', fontsize=8, color=COLORS['text'])

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '02_category_revenue.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_payment_distribution(payments):
    """Plot 3: Payment method distribution."""
    logger.info("  💳 Plotting payment distribution...")
    pay_dist = payments.groupby('payment_type')['payment_value'].agg(['sum', 'count']).reset_index()
    pay_dist.columns = ['type', 'total_value', 'count']
    pay_dist = pay_dist.sort_values('total_value', ascending=False)

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Pie chart - by value
    colors = [COLORS['primary'], COLORS['accent'], COLORS['success'], COLORS['warning'], COLORS['danger']]
    wedges, texts, autotexts = ax1.pie(pay_dist['total_value'], labels=pay_dist['type'],
                                        autopct='%1.1f%%', colors=colors[:len(pay_dist)],
                                        textprops={'color': COLORS['text'], 'fontsize': 9},
                                        pctdistance=0.8, startangle=90)
    ax1.set_title('Payment Methods by Value', pad=15)

    # Bar chart - by count
    ax2.bar(pay_dist['type'], pay_dist['count'], color=colors[:len(pay_dist)], edgecolor='none')
    ax2.set_title('Payment Methods by Transaction Count', pad=15)
    ax2.set_ylabel('Transactions')
    ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))
    ax2.grid(axis='y', alpha=0.2)
    plt.setp(ax2.get_xticklabels(), rotation=30, ha='right')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '03_payment_distribution.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_review_distribution(reviews, orders):
    """Plot 4: Review score distribution and delivery correlation."""
    logger.info("  ⭐ Plotting review analysis...")
    merged = reviews.merge(orders[['order_id', 'delivery_days', 'order_status']], on='order_id')
    merged = merged[merged['order_status'] == 'delivered']

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Score distribution
    score_counts = merged['review_score'].value_counts().sort_index()
    colors_map = {1: COLORS['danger'], 2: '#f97316', 3: COLORS['warning'], 4: '#a3e635', 5: COLORS['success']}
    bar_colors = [colors_map.get(s, COLORS['primary']) for s in score_counts.index]
    ax1.bar(score_counts.index, score_counts.values, color=bar_colors, edgecolor='none', width=0.6)
    ax1.set_xlabel('Review Score')
    ax1.set_ylabel('Count')
    ax1.set_title('Review Score Distribution', pad=15)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))
    ax1.grid(axis='y', alpha=0.2)

    # Delivery time vs review score (box plot)
    delivery_data = [merged[merged['review_score'] == s]['delivery_days'].dropna() for s in range(1, 6)]
    bp = ax2.boxplot(delivery_data, labels=['1★', '2★', '3★', '4★', '5★'],
                     patch_artist=True, showfliers=False,
                     medianprops={'color': COLORS['warning'], 'linewidth': 2})
    for patch, color in zip(bp['boxes'], bar_colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
    ax2.set_xlabel('Review Score')
    ax2.set_ylabel('Delivery Time (Days)')
    ax2.set_title('Delivery Time vs Review Score', pad=15)
    ax2.grid(axis='y', alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '04_review_analysis.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_regional_performance(orders, items, customers):
    """Plot 5: Revenue by state."""
    logger.info("  🗺️  Plotting regional performance...")
    merged = orders.merge(customers, on='customer_id').merge(items, on='order_id')
    merged = merged[merged['order_status'] == 'delivered']

    state_rev = merged.groupby('customer_state').agg(
        revenue=('price', 'sum'),
        orders=('order_id', 'nunique')
    ).sort_values('revenue', ascending=True).tail(15).reset_index()

    fig, ax = plt.subplots(figsize=(12, 7))

    # Color gradient based on revenue
    norm = plt.Normalize(state_rev['revenue'].min(), state_rev['revenue'].max())
    colors = plt.cm.viridis(norm(state_rev['revenue']))

    bars = ax.barh(state_rev['customer_state'], state_rev['revenue'], color=colors, edgecolor='none', height=0.6)

    ax.set_xlabel('Revenue (R$)')
    ax.set_title('Top 15 States by Revenue', pad=15)
    ax.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'R$ {x/1e6:.1f}M'))
    ax.grid(axis='x', alpha=0.2)

    # Add order count annotations
    for bar, rev, orders_count in zip(bars, state_rev['revenue'], state_rev['orders']):
        ax.text(rev + state_rev['revenue'].max() * 0.01, bar.get_y() + bar.get_height()/2,
                f'{orders_count:,} orders', va='center', fontsize=8, color=COLORS['text'])

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '05_regional_performance.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_correlation_heatmap(orders, items, reviews):
    """Plot 6: Correlation heatmap of numeric variables."""
    logger.info("  🔥 Plotting correlation heatmap...")
    merged = orders.merge(items, on='order_id')
    merged = merged.merge(reviews[['order_id', 'review_score']], on='order_id', how='left')
    merged = merged[merged['order_status'] == 'delivered']

    numeric_cols = ['price', 'freight_value', 'delivery_days', 'delivery_delay_days', 'review_score']
    corr_data = merged[numeric_cols].dropna()
    corr_matrix = corr_data.corr()

    fig, ax = plt.subplots(figsize=(8, 7))
    mask = np.triu(np.ones_like(corr_matrix, dtype=bool), k=1)
    sns.heatmap(corr_matrix, mask=mask, annot=True, fmt='.2f', cmap='RdYlBu_r',
                center=0, vmin=-1, vmax=1, square=True, linewidths=1,
                linecolor=COLORS['grid'], ax=ax,
                annot_kws={'fontsize': 11, 'fontweight': 'bold'},
                cbar_kws={'shrink': 0.8})
    ax.set_title('Correlation Matrix: Key Metrics', pad=15)
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '06_correlation_heatmap.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_customer_distribution(orders, items, customers):
    """Plot 7: Customer purchase frequency and value distributions."""
    logger.info("  👥 Plotting customer distributions...")
    merged = orders.merge(items, on='order_id').merge(customers, on='customer_id')
    merged = merged[merged['order_status'] == 'delivered']

    customer_stats = merged.groupby('customer_unique_id').agg(
        total_orders=('order_id', 'nunique'),
        total_spent=('price', 'sum'),
    ).reset_index()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Purchase frequency
    freq = customer_stats['total_orders'].value_counts().sort_index().head(10)
    ax1.bar(freq.index.astype(str), freq.values, color=COLORS['primary'], edgecolor='none')
    ax1.set_xlabel('Number of Orders')
    ax1.set_ylabel('Customer Count')
    ax1.set_title('Customer Purchase Frequency', pad=15)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'{x/1000:.0f}K'))
    ax1.grid(axis='y', alpha=0.2)

    # Spending distribution (log scale)
    ax2.hist(customer_stats['total_spent'], bins=50, color=COLORS['secondary'], edgecolor='none', alpha=0.8)
    ax2.set_xlabel('Total Spent (R$)')
    ax2.set_ylabel('Customer Count')
    ax2.set_title('Customer Lifetime Value Distribution', pad=15)
    ax2.set_yscale('log')
    ax2.grid(axis='y', alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '07_customer_distributions.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_day_of_week_patterns(orders, items):
    """Plot 8: Day-of-week and hourly purchase patterns."""
    logger.info("  📅 Plotting temporal patterns...")
    merged = orders.merge(items, on='order_id')
    merged = merged[merged['order_status'] == 'delivered']

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Day of week
    dow_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow_rev = merged.groupby('order_day_of_week')['price'].sum()
    dow_rev = dow_rev.reindex(dow_order)
    colors_dow = [COLORS['primary'] if d in ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
                  else COLORS['accent'] for d in dow_order]
    ax1.bar(range(7), dow_rev.values, color=colors_dow, edgecolor='none')
    ax1.set_xticks(range(7))
    ax1.set_xticklabels(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
    ax1.set_ylabel('Revenue (R$)')
    ax1.set_title('Revenue by Day of Week', pad=15)
    ax1.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'R$ {x/1e6:.1f}M'))
    ax1.grid(axis='y', alpha=0.2)

    # Hourly pattern
    hourly = merged.groupby('order_hour')['order_id'].nunique()
    ax2.fill_between(hourly.index, hourly.values, alpha=0.3, color=COLORS['secondary'])
    ax2.plot(hourly.index, hourly.values, color=COLORS['secondary'], linewidth=2)
    ax2.set_xlabel('Hour of Day')
    ax2.set_ylabel('Order Count')
    ax2.set_title('Order Volume by Hour', pad=15)
    ax2.set_xticks(range(0, 24, 2))
    ax2.grid(True, alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '08_temporal_patterns.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_delivery_performance(orders, customers):
    """Plot 9: Delivery performance analysis."""
    logger.info("  🚚 Plotting delivery performance...")
    delivered = orders[orders['order_status'] == 'delivered'].copy()
    delivered = delivered.merge(customers[['customer_id', 'customer_state']], on='customer_id')

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Delivery time distribution
    delivery_times = delivered['delivery_days'].dropna()
    ax1.hist(delivery_times, bins=50, color=COLORS['primary'], edgecolor='none', alpha=0.8)
    median_val = delivery_times.median()
    ax1.axvline(median_val, color=COLORS['warning'], linestyle='--', linewidth=2, label=f'Median: {median_val:.1f} days')
    ax1.set_xlabel('Delivery Time (Days)')
    ax1.set_ylabel('Order Count')
    ax1.set_title('Delivery Time Distribution', pad=15)
    ax1.set_xlim(0, 60)
    ax1.legend()
    ax1.grid(axis='y', alpha=0.2)

    # On-time vs late by state (top 10)
    state_delivery = delivered.groupby('customer_state').agg(
        on_time=('delivery_delay_days', lambda x: (x <= 0).sum()),
        late=('delivery_delay_days', lambda x: (x > 0).sum()),
    ).reset_index()
    state_delivery['total'] = state_delivery['on_time'] + state_delivery['late']
    state_delivery = state_delivery.nlargest(10, 'total')
    state_delivery['on_time_pct'] = state_delivery['on_time'] / state_delivery['total'] * 100

    x = range(len(state_delivery))
    ax2.bar(x, state_delivery['on_time'], label='On Time', color=COLORS['success'], edgecolor='none')
    ax2.bar(x, state_delivery['late'], bottom=state_delivery['on_time'], label='Late', color=COLORS['danger'], edgecolor='none')
    ax2.set_xticks(x)
    ax2.set_xticklabels(state_delivery['customer_state'], fontsize=9)
    ax2.set_ylabel('Orders')
    ax2.set_title('On-Time vs Late Deliveries (Top 10 States)', pad=15)
    ax2.legend()
    ax2.grid(axis='y', alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '09_delivery_performance.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_price_outliers(items):
    """Plot 10: Price distribution and outlier detection."""
    logger.info("  🔍 Plotting outlier analysis...")
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Price distribution
    prices = items['price']
    q1 = prices.quantile(0.25)
    q3 = prices.quantile(0.75)
    iqr = q3 - q1
    upper_bound = q3 + 1.5 * iqr
    outlier_count = (prices > upper_bound).sum()

    ax1.hist(prices[prices <= upper_bound], bins=50, color=COLORS['primary'], edgecolor='none', alpha=0.8, label='Normal')
    ax1.axvline(upper_bound, color=COLORS['danger'], linestyle='--', linewidth=2, label=f'Outlier threshold: R$ {upper_bound:.0f}')
    ax1.set_xlabel('Price (R$)')
    ax1.set_ylabel('Count')
    ax1.set_title(f'Price Distribution ({outlier_count:,} outliers detected)', pad=15)
    ax1.legend(fontsize=9)
    ax1.grid(axis='y', alpha=0.2)

    # Freight vs Price scatter
    sample = items.sample(min(5000, len(items)), random_state=42)
    ax2.scatter(sample['price'], sample['freight_value'], alpha=0.3, s=5, color=COLORS['accent'])
    ax2.set_xlabel('Price (R$)')
    ax2.set_ylabel('Freight Value (R$)')
    ax2.set_title('Price vs Freight Value', pad=15)
    ax2.set_xlim(0, 1000)
    ax2.set_ylim(0, 200)
    ax2.grid(True, alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '10_outlier_analysis.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_seller_analysis(items, sellers):
    """Plot 11: Seller performance analysis."""
    logger.info("  🏪 Plotting seller analysis...")
    merged = items.merge(sellers, on='seller_id')
    seller_stats = merged.groupby('seller_id').agg(
        revenue=('price', 'sum'),
        orders=('order_id', 'nunique'),
        items=('order_item_id', 'count'),
    ).reset_index()

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    # Revenue concentration (Pareto)
    seller_sorted = seller_stats.sort_values('revenue', ascending=False)
    cumulative_pct = seller_sorted['revenue'].cumsum() / seller_sorted['revenue'].sum() * 100
    seller_pct = np.arange(1, len(seller_sorted) + 1) / len(seller_sorted) * 100

    ax1.plot(seller_pct, cumulative_pct, color=COLORS['primary'], linewidth=2)
    ax1.fill_between(seller_pct, cumulative_pct, alpha=0.2, color=COLORS['primary'])
    ax1.axhline(80, color=COLORS['warning'], linestyle='--', alpha=0.7, label='80% revenue')
    pct_at_80 = seller_pct[np.argmax(cumulative_pct >= 80)]
    ax1.axvline(pct_at_80, color=COLORS['warning'], linestyle='--', alpha=0.7)
    ax1.set_xlabel('% of Sellers')
    ax1.set_ylabel('% of Revenue (Cumulative)')
    ax1.set_title(f'Revenue Concentration: {pct_at_80:.0f}% of sellers = 80% revenue', pad=15)
    ax1.legend()
    ax1.grid(True, alpha=0.2)

    # Top 10 seller states
    state_rev = merged.groupby('seller_state')['price'].sum().sort_values(ascending=True).tail(10)
    ax2.barh(state_rev.index, state_rev.values, color=COLORS['secondary'], edgecolor='none', height=0.6)
    ax2.set_xlabel('Revenue (R$)')
    ax2.set_title('Top 10 Seller States by Revenue', pad=15)
    ax2.xaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: f'R$ {x/1e6:.1f}M'))
    ax2.grid(axis='x', alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '11_seller_analysis.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_order_status(orders):
    """Plot 12: Order status breakdown."""
    logger.info("  📋 Plotting order status...")
    status_counts = orders['order_status'].value_counts()

    fig, ax = plt.subplots(figsize=(10, 6))
    colors_status = [COLORS['success'], COLORS['accent'], COLORS['danger'],
                     COLORS['warning'], COLORS['primary'], COLORS['secondary'],
                     '#ec4899', '#14b8a6']

    bars = ax.bar(status_counts.index, status_counts.values,
                  color=colors_status[:len(status_counts)], edgecolor='none')
    ax.set_ylabel('Count')
    ax.set_title('Order Status Distribution', pad=15)
    ax.set_yscale('log')
    ax.grid(axis='y', alpha=0.2)
    plt.setp(ax.get_xticklabels(), rotation=30, ha='right')

    for bar, val in zip(bars, status_counts.values):
        ax.text(bar.get_x() + bar.get_width()/2, val * 1.1,
                f'{val:,}', ha='center', va='bottom', fontsize=9, color=COLORS['text'])

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '12_order_status.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_installment_analysis(payments):
    """Plot 13: Payment installment analysis."""
    logger.info("  💰 Plotting installment analysis...")
    credit = payments[payments['payment_type'] == 'credit_card']
    inst_dist = credit.groupby('payment_installments').agg(
        count=('payment_value', 'count'),
        avg_value=('payment_value', 'mean')
    ).reset_index()
    inst_dist = inst_dist[inst_dist['payment_installments'] <= 12]

    fig, ax1 = plt.subplots(figsize=(12, 6))
    ax2 = ax1.twinx()

    ax1.bar(inst_dist['payment_installments'], inst_dist['count'],
            color=COLORS['primary'], alpha=0.7, edgecolor='none', label='Transaction Count')
    ax2.plot(inst_dist['payment_installments'], inst_dist['avg_value'],
             color=COLORS['warning'], linewidth=2.5, marker='o', markersize=6, label='Avg Value')

    ax1.set_xlabel('Number of Installments')
    ax1.set_ylabel('Transaction Count', color=COLORS['primary'])
    ax2.set_ylabel('Average Transaction Value (R$)', color=COLORS['warning'])
    ax1.set_title('Credit Card Installment Analysis', pad=15)
    ax1.set_xticks(range(1, 13))
    ax1.grid(axis='y', alpha=0.2)
    ax1.legend(loc='upper left')
    ax2.legend(loc='upper right')

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '13_installment_analysis.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_category_review_scores(items, products, reviews, orders):
    """Plot 14: Category satisfaction analysis."""
    logger.info("  📊 Plotting category reviews...")
    merged = items.merge(products[['product_id', 'product_category_name_english']], on='product_id')
    merged = merged.merge(orders[['order_id', 'order_status']], on='order_id')
    merged = merged[merged['order_status'] == 'delivered']
    merged = merged.merge(reviews[['order_id', 'review_score']], on='order_id')

    cat_scores = merged.groupby('product_category_name_english').agg(
        avg_score=('review_score', 'mean'),
        count=('review_score', 'count'),
        revenue=('price', 'sum'),
    ).reset_index()
    cat_scores = cat_scores[cat_scores['count'] >= 100].sort_values('avg_score', ascending=True).tail(15)

    fig, ax = plt.subplots(figsize=(12, 7))
    colors_cat = plt.cm.RdYlGn(plt.Normalize(1, 5)(cat_scores['avg_score']))
    bars = ax.barh(cat_scores['product_category_name_english'], cat_scores['avg_score'],
                   color=colors_cat, edgecolor='none', height=0.6)
    ax.set_xlabel('Average Review Score')
    ax.set_title('Top 15 Categories by Customer Satisfaction (min 100 reviews)', pad=15)
    ax.set_xlim(0, 5)
    ax.axvline(cat_scores['avg_score'].mean(), color=COLORS['warning'], linestyle='--', alpha=0.7,
               label=f'Overall avg: {cat_scores["avg_score"].mean():.2f}')
    ax.legend()
    ax.grid(axis='x', alpha=0.2)

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '14_category_satisfaction.png'), dpi=150, bbox_inches='tight')
    plt.close()


def plot_freight_analysis(items, orders, customers):
    """Plot 15: Freight cost analysis."""
    logger.info("  📦 Plotting freight analysis...")
    merged = items.merge(orders[['order_id', 'order_status', 'customer_id']], on='order_id')
    merged = merged[merged['order_status'] == 'delivered']
    merged = merged.merge(customers[['customer_id', 'customer_state']], on='customer_id')

    state_freight = merged.groupby('customer_state').agg(
        avg_freight=('freight_value', 'mean'),
        freight_ratio=('freight_value', lambda x: (x / merged.loc[x.index, 'price']).mean() * 100)
    ).sort_values('avg_freight', ascending=True).tail(15).reset_index()

    fig, ax = plt.subplots(figsize=(12, 7))
    norm = plt.Normalize(state_freight['avg_freight'].min(), state_freight['avg_freight'].max())
    colors_f = plt.cm.YlOrRd(norm(state_freight['avg_freight']))
    bars = ax.barh(state_freight['customer_state'], state_freight['avg_freight'],
                   color=colors_f, edgecolor='none', height=0.6)
    ax.set_xlabel('Average Freight Cost (R$)')
    ax.set_title('Average Freight Cost by State (Top 15)', pad=15)
    ax.grid(axis='x', alpha=0.2)

    for bar, val in zip(bars, state_freight['avg_freight']):
        ax.text(val + 0.5, bar.get_y() + bar.get_height()/2,
                f'R$ {val:.1f}', va='center', fontsize=8, color=COLORS['text'])

    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, '15_freight_analysis.png'), dpi=150, bbox_inches='tight')
    plt.close()


def run_eda():
    """Main EDA pipeline."""
    logger.info("=" * 60)
    logger.info("PHASE 6: EXPLORATORY DATA ANALYSIS")
    logger.info("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    setup_style()

    logger.info("\n📥 Loading data from database...")
    orders, items, customers, products, payments, reviews, sellers = get_data()

    logger.info(f"\n🎨 Generating visualizations...\n")

    plot_monthly_revenue_trends(orders, items)
    plot_revenue_by_category(items, products)
    plot_payment_distribution(payments)
    plot_review_distribution(reviews, orders)
    plot_regional_performance(orders, items, customers)
    plot_correlation_heatmap(orders, items, reviews)
    plot_customer_distribution(orders, items, customers)
    plot_day_of_week_patterns(orders, items)
    plot_delivery_performance(orders, customers)
    plot_price_outliers(items)
    plot_seller_analysis(items, sellers)
    plot_order_status(orders)
    plot_installment_analysis(payments)
    plot_category_review_scores(items, products, reviews, orders)
    plot_freight_analysis(items, orders, customers)

    chart_count = len([f for f in os.listdir(OUTPUT_DIR) if f.endswith('.png')])
    logger.info(f"\n{'─' * 40}")
    logger.info("EDA SUMMARY")
    logger.info(f"  Visualizations generated: {chart_count}")
    logger.info(f"  Output directory: {OUTPUT_DIR}")
    logger.info(f"{'─' * 40}")
    logger.info("\n✅ Phase 6 Complete: Exploratory Data Analysis\n")


if __name__ == '__main__':
    run_eda()
