# Sales Data Pipeline – End-to-End Data Engineering Project

This project implements a complete data pipeline using Python, BigQuery, and Looker Studio.  
It simulates a real-world sales analytics workflow, including transactional modeling, time-series aggregation, growth metrics, and dashboard creation.

---

## 1. Project Overview

The purpose of this project is to demonstrate practical data engineering skills through the creation of an end-to-end ETL workflow.  
The pipeline ingests synthetic sales data, transforms it into a business-ready schema, calculates monthly revenue metrics, and loads it into a cloud data warehouse for visualization.

This project is part of a professional data engineering portfolio.

---

## 2. Architecture

The pipeline follows a layered data architecture:


Raw Data → Cleaned Data → BigQuery Warehouse → Looker Studio Dashboard

- **Extract:** Generate synthetic transactional data.
- **Transform:** Clean data, compute revenue, monthly aggregates, and MoM/YoY growth.
- **Load:** Push tables into BigQuery using the Python BigQuery client.
- **Visualize:** Build a financial-style analytics dashboard.

---

## 3. Repository Structure

sales-data-pipeline/
│
├── scripts/
│ ├── extract.py # Generates raw sales dataset
│ ├── transform.py # Cleans data and computes business metrics
│ └── load_bigquery.py # Loads tables into BigQuery
│
├── data/
│ ├── raw/ # Auto-generated raw CSV
│ └── clean/ # Processed datasets used for loading
│
├── dashboard/
│ ├── screenshots/ # Images of the Looker Studio dashboard
└── README.md


---

## 4. Pipeline Steps

### 4.1 Extract  
A synthetic dataset of sales transactions is generated with fields:

- `order_id`
- `order_date`
- `customer_id`
- `product`
- `category`
- `price`
- `quantity`

Output:  
`data/raw/sales_raw.csv`

---

### 4.2 Transform  
Business metrics are computed:

- Revenue per transaction  
- Year, month, and year-month keys  
- Monthly revenue per product/category  
- Month-over-month (MoM) growth  
- Year-over-year (YoY) growth  

Outputs:

data/clean/sales_transactions_clean.csv
data/clean/sales_monthly_agg.csv


---

### 4.3 Load  
Two tables are loaded into BigQuery:

- `sales.transactions`  
- `sales.monthly_revenue`

The BigQuery client uses schema autodetection and overwrite mode.

---

## 5. Dashboard (Looker Studio)

The dashboard contains:

### KPIs
- Total revenue  
- Total orders  
- Quantity sold  
- Average monthly revenue  

### Visuals
- Revenue over time (time-series)  
- Revenue by category  
- Top 10 products by revenue  
- Month-over-month and year-over-year growth table  

A minimalist, finance-style theme was applied: white background, neutral text, blue accent, no borders or shadows.

Screenshots are included in:
dashboard/screenshots/



---

## 6. Technologies Used

- Python  
- Pandas  
- Google BigQuery  
- Looker Studio  
- Data modeling (fact & aggregate tables)  
- Cloud-based ETL  

---

## 7. Purpose

This project demonstrates:

- Pipeline design and automation  
- Time-series analytics  
- Cloud data warehousing  
- Business metric engineering  
- Dashboard development  

It is intended to serve as a complete example of real-world data engineering work.

---

## 8. Contact

For inquiries regarding this project or portfolio, please reach out via GitHub.














