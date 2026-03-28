# Workflow Scheduling (DolphinScheduler)

## 1. Overview

To make the data warehouse pipeline more production-oriented, this project introduces a scheduling layer based on DolphinScheduler.

Instead of manually executing SQL and Python scripts, the entire pipeline is organized as a DAG (Directed Acyclic Graph) workflow with clear dependencies and execution order.

---

## 2. Why Scheduling Is Needed

In the original implementation:

- SQL scripts were executed manually
- Execution order relied on human control
- There was no dependency management between layers
- Failures could lead to incomplete downstream data

This introduces risks in real scenarios.

By adding a scheduling system:

- task dependencies are enforced automatically
- execution becomes repeatable and controllable
- failures can be retried without rerunning everything manually

---

## 3. Workflow Architecture

The scheduling workflow follows the warehouse layering design:

ODS → DWD → DWS → ADS → Dashboard

### DAG Structure

ods_prepare  
→ dwd_trade_order  
→ dws_trade_day_summary  
→ ads_trade_overview  
→ build_dashboard  

### Extended DAG

ods_prepare  
→ dwd_trade_order / dwd_trade_order_detail  
→ dws_trade_day_summary / dws_trade_shop_day_summary / dws_trade_category_day_summary  
→ ads_trade_overview / ads_trade_top_shop / ads_trade_top_category  
→ build_dashboard  

---

## 4. Task Implementation

Each node in the workflow is implemented as a task that triggers scripts:

- Shell Task → executes Spark SQL / Hive SQL scripts
- Spark SQL Task → directly runs SQL logic
- Python Task → builds dashboard output

Example:

- DWD layer → Spark SQL scripts
- DWS layer → aggregation SQL
- ADS layer → business-level metrics
- Dashboard → Python visualization

---

## 5. Dependency Control

- downstream tasks only run after upstream success
- prevents partial or inconsistent data generation
- ensures data correctness across layers

---

## 6. Scheduling Strategy

Designed for daily batch processing:

1. refresh ODS data
2. run DWD ETL
3. run DWS aggregations
4. generate ADS tables
5. build dashboard

Supports:

- scheduled execution
- manual rerun
- failure retry

---

## 7. Project Value

This upgrade transforms the project from:

"SQL script collection"

into:

"a workflow-driven data warehouse pipeline"

Key improvements:

- closer to real enterprise data warehouse systems
- clear task orchestration and dependency management
- improved reliability and maintainability

---

## 8. Interview Talking Points

This scheduling layer demonstrates:

- understanding of data workflow orchestration
- ability to design DAG-based pipelines
- awareness of dependency management in data engineering
- transition from demo project to production-like system

The key idea is not just running SQL, but organizing the entire data pipeline in a controlled and automated way.
