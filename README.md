# E-commerce Offline Data Warehouse

![Python](https://img.shields.io/badge/Python-3.x-blue)
![Hive](https://img.shields.io/badge/Hive-Data%20Warehouse-orange)
![Spark](https://img.shields.io/badge/Spark-SQL-red)
![HDFS](https://img.shields.io/badge/HDFS-Storage-yellow)
![Dataset](https://img.shields.io/badge/Dataset-35M%2B%20Records-green)

![Project Overview](docs/mermaid-diagram_2.png)

An end-to-end offline data engineering pipeline simulating a real-world e-commerce analytics platform.

This project demonstrates a complete data engineering workflow including:

- large-scale synthetic data generation
- layered data warehouse architecture
- Spark / Hive analytical processing
- business metric aggregation
- BI dashboard visualization

Dataset generator:  
https://github.com/learner2638/ecommerce-data-simulator

Dataset scale:  
10M Orders | 25M Order Items | 35M+ Records

---

# System Architecture

![System Architecture](docs/mermaid-diagram.png)

Synthetic Data Generator  
↓  
ODS Layer (Raw Data)  
↓  
DWD Layer (Detail Fact Tables)  
↓  
DWS Layer (Aggregated Metrics)  
↓  
ADS Layer (Business Analytics)  
↓  
BI Dashboard  

This architecture follows the standard offline data warehouse pattern used in industry systems.

---

# Dataset

The dataset is generated using a custom E-commerce Data Simulator.

The simulator produces realistic data including:

- users
- shops
- SKUs
- orders
- order items
- refunds

---

# Dataset Scale

| Entity | Count |
|------|------|
| Orders | 10,000,000 |
| Order Items | ~25,000,000 |
| Users | 120,000 |
| Shops | 8,000 |
| SKUs | 35,000 |

Total records exceed 35 million rows.

---

# Data Generation Configuration

order_cnt = 10_000_000  
user_cnt = 120_000  
shop_cnt = 8_000  
sku_cnt = 35_000  
p_refund_given_paid = 0.18  
batch_size = 300_000  
workers = 6  

---

# Tech Stack

| Layer | Technology |
|------|------|
| Data Generation | Python |
| Storage | Hadoop / HDFS |
| Data Warehouse | Hive |
| Query Engine | Hive SQL / Spark SQL |
| Data Modeling | ODS → DWD → DWS → ADS |
| Visualization | Python + Pyecharts |

---

# Data Warehouse Design

ODS → DWD → DWS → ADS

---

## ODS Layer

ods_orders  
ods_order_items  
ods_user_dim  
ods_shop_dim  
ods_sku_dim  

---

## DWD Layer

dwd_trade_order  
dwd_trade_order_detail  
dwd_trade_refund_detail  

---

## DWS Layer

dws_trade_day_summary  
dws_trade_category_day_summary  
dws_trade_shop_day_summary  

---

## ADS Layer

ads_trade_overview  
ads_trade_top_category  
ads_trade_top_shop  

---

# 🚀 Engineering Enhancements

These enhancements simulate real-world data warehouse engineering practices beyond basic ETL pipelines.

## 1. Data Skew Optimization

- salted two-stage aggregation  
- broadcast join (MapJoin)  
- hot key split strategy  

Effect:

- reduced long-tail tasks  
- improved parallelism  
- more stable execution  

---

## 2. Partition Design

dt = substr(created_time, 1, 10)

Benefits:

- partition pruning  
- incremental processing (T+1)  
- efficient backfill  

---

## 3. Metric Definition

Core metrics include:

- GMV  
- order_cnt  
- user_cnt  
- total_paid  
- total_refund  

Ensures:

- consistency  
- traceability  
- interpretability  

---

## 4. Data Quality Framework

Includes:

- uniqueness check (order_id)  
- not-null validation  
- business rule validation  
- time logic validation  
- ODS ↔ DWD reconciliation  

Output table:

ads_data_quality_report_prod  

Provides:

- unified monitoring  
- early anomaly detection  
- improved reliability  

---

## 5. Workflow Scheduling

ODS → DWD → DWS → ADS → Dashboard  

Features:

- dependency control  
- failure retry  
- automated scheduling  

---

## Summary

The project evolves from:

a basic ETL demo  

to:

a production-oriented data warehouse system with performance optimization, data governance, and workflow orchestration

---

# Partition Strategy

PARTITIONED BY (dt)

---

# BI Dashboard

Python + Pyecharts

Includes:

- GMV Trend  
- Order Count  
- User Count  
- Refund Trend  
- Top Categories  
- Top Shops  

---

# Dashboard Preview

![Dashboard](docs/dashboard_preview_1.png)

---

# Analytical Metrics

GMV  
Order Count  
User Count  
Refund Amount  
Average Order Value  
Refund Rate  

---

# BI Pipeline

ADS → CSV → Pandas → Pyecharts → HTML

---

# Repository Structure

ecommerce-data-warehouse  
├── sql  
├── build_dashboard.py  
├── dashboard.html  
├── README.md  
└── docs  

---

# How to Run

1. Generate dataset  
2. Upload to HDFS  
3. Run SQL (ODS → DWD → DWS → ADS)  
4. Export data  
5. Build dashboard  

---

# Practical Skills Demonstrated

- data warehouse modeling  
- Spark / Hive processing  
- data skew optimization  
- data quality design  
- metric system design  
- workflow orchestration  

---

# Future Improvements

Kafka / Flink  
Trino / ClickHouse  
Superset / Tableau  
Docker  

---

# Data Source

https://github.com/learner2638/ecommerce-data-simulator

---

# Author

Built as a data engineering project focusing on end-to-end pipeline and engineering practices.
