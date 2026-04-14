# Olist E-Commerce Data Analytics & Engineering Pipeline

A production-level, end-to-end data analytics solution built on the **Brazilian E-Commerce (Olist)** dataset. Demonstrates strong skills in Python, SQL, data engineering, and business intelligence.

## Project Overview

This project processes, transforms, and analyzes structured data from 9 relational CSV sources to generate actionable business insights through an automated pipeline.

### Key Metrics Discovered
- **R$ 13.2M** total revenue across **96,478** delivered orders
- **96,478** unique customers across all 27 Brazilian states
- **32,216** products from **2,970** active sellers
- **21.1%** year-over-year revenue growth (2017-2018)
- **4.1/5** average customer satisfaction score

## Architecture

```
Raw CSVs (9 files, ~125 MB)
    |
    v
[Phase 1] Data Ingestion & Validation
    |
    v
[Phase 2] Data Transformation & Cleaning
    |
    v
[Phase 3] Data Integration (Unified Dataset)
    |
    v
[Phase 4] SQLite Database (Star Schema)
    |
    v
[Phase 5] SQL Analysis (17 Queries)
    |
    v
[Phase 6] EDA Visualizations (15 Charts)
    |
    v
[Phase 8] Interactive Dashboard (HTML/JS)
```

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Language | Python 3.11 |
| Data Processing | Pandas, NumPy |
| Database | SQLite 3.43 + SQLAlchemy |
| Visualization | Matplotlib, Seaborn, Chart.js |
| Dashboard | HTML5, CSS3, JavaScript |
| SQL | 17 optimized analytical queries |

## Project Structure

```
Dash-Project/
|-- Data/                          # Raw CSV datasets (Olist)
|-- scripts/
|   |-- data_ingestion.py          # Phase 1: Schema validation, integrity checks
|   |-- data_transformation.py     # Phase 2: Cleaning, timestamps, derived features
|   |-- data_integration.py        # Phase 3: Table merging, unified dataset
|   |-- database_setup.py          # Phase 4: SQLite star-schema, indexes
|   |-- sql_analysis.py            # Phase 5: Execute 17 SQL queries
|   |-- eda_analysis.py            # Phase 6: 15 publication-quality charts
|   |-- generate_dashboard_data.py # Phase 8: Pre-compute dashboard JSON
|-- sql/
|   |-- sql_queries.sql            # 17 optimized analytical queries
|-- output/
|   |-- cleaned/                   # 8 cleaned CSV files
|   |-- sql_results/               # 17 query result CSVs
|   |-- visualizations/            # 15 EDA chart PNGs
|   |-- analytical_dataset.csv     # Unified order-level dataset
|   |-- item_level_dataset.csv     # Item-level analytical dataset
|   |-- business_insights.md       # Business recommendations report
|-- dashboard/
|   |-- index.html                 # Interactive analytics dashboard
|   |-- dashboard.css              # Premium dark-theme styling
|   |-- dashboard.js               # Chart.js visualizations
|   |-- dashboard_data.json        # Pre-computed metrics
|-- ecommerce.db                   # SQLite database (71 MB)
|-- run_pipeline.py                # Master pipeline orchestrator
|-- README.md                      # This file
```

## Quick Start

### Run the Full Pipeline
```bash
python run_pipeline.py
```
This executes all phases sequentially (~4 minutes).

### Run Individual Phases
```bash
python scripts/data_ingestion.py          # Phase 1
python scripts/data_transformation.py     # Phase 2
python scripts/data_integration.py        # Phase 3
python scripts/database_setup.py          # Phase 4
python scripts/sql_analysis.py            # Phase 5
python scripts/eda_analysis.py            # Phase 6
python scripts/generate_dashboard_data.py # Phase 8
```

### View the Dashboard
Open `dashboard/index.html` in any modern browser. The dashboard loads data from `dashboard_data.json`.

## Database Schema

Star-schema design with fact and dimension tables:

- **Fact Tables**: `fact_order_items`, `fact_payments`
- **Dimension Tables**: `dim_customers`, `dim_orders`, `dim_products`, `dim_sellers`, `dim_reviews`
- **Reference**: `ref_geolocation`
- **16+ indexes** for query performance optimization

## SQL Analysis Coverage

17 optimized queries covering:
1. Total revenue & order volume overview
2. Monthly sales trends
3. Yearly sales summary
4. Top customers by lifetime value
5. Product category performance
6. Payment method distribution
7. Delivery time by state
8. Customer segmentation (RFM)
9. Seller performance rankings
10. Review score analysis
11. Regional performance
12. Day-of-week patterns
13. Hourly purchase distribution
14. Order cancellation analysis
15. Freight cost analysis
16. Year-over-year growth rates
17. Category satisfaction analysis

## EDA Visualizations

15 publication-quality dark-themed charts:
- Monthly revenue & order trends
- Category revenue breakdown
- Payment method distribution
- Review score distribution & delivery correlation
- Regional performance heatmap
- Correlation matrix
- Customer purchase frequency & LTV distribution
- Day-of-week & hourly patterns
- Delivery performance analysis
- Price outlier detection
- Seller concentration (Pareto)
- Order status breakdown
- Credit card installment analysis
- Category satisfaction scores
- Freight cost by state

## Key Business Insights

1. **Customer Retention Crisis**: 35,857 customers classified as "Lost" - massive reactivation opportunity
2. **Delivery = Satisfaction**: Strong inverse correlation between delivery time and review scores
3. **Geographic Concentration**: Sao Paulo drives 37.3% of all revenue
4. **Payment Dominance**: Credit cards account for 73.9%, with avg 3.5 installments
5. **Growth Trajectory**: 21%+ YoY growth sustained into 2018

See `output/business_insights.md` for the full report with recommendations.

## Dataset

**Brazilian E-Commerce Public Dataset by Olist**
- 9 interrelated CSV files
- ~550K total rows across all tables
- Period: September 2016 - October 2018
- Source: [Kaggle](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

## License

This project is for educational and portfolio purposes. The dataset is provided by Olist under CC BY-NC-SA 4.0.
