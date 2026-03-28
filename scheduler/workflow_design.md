# Workflow Design

## 1. Goal

To make the offline data warehouse pipeline more production-like, this project adds a scheduling layer based on DolphinScheduler.

The goal is to turn the original manually executed SQL/Python pipeline into a workflow with clear task dependencies, execution order, and retry capability.

---

## 2. Workflow Scope

The scheduling workflow covers the main offline warehouse process:

ODS -> DWD -> DWS -> ADS -> Dashboard

This means:

- ODS layer provides raw source data
- DWD layer performs cleaning and detail-layer standardization
- DWS layer performs daily subject-oriented aggregation
- ADS layer outputs business-facing analysis tables
- Dashboard layer generates final visual results

---

## 3. DAG Design

### Core workflow

ods_prepare
   ↓
dwd_trade_order
   ↓
dws_trade_day_summary
   ↓
ads_trade_overview
   ↓
build_dashboard

### Extended workflow

ods_prepare
   ↓
dwd_trade_order
dwd_trade_order_detail
   ↓
dws_trade_day_summary
dws_trade_shop_day_summary
dws_trade_category_day_summary
   ↓
ads_trade_overview
ads_trade_top_shop
ads_trade_top_category
   ↓
build_dashboard

---

## 4. Task Dependency Logic

- `ods_prepare` must finish successfully before downstream tasks start
- `dwd_trade_order` and `dwd_trade_order_detail` depend on ODS data preparation
- DWS tasks depend on DWD task completion
- ADS tasks depend on DWS aggregation results
- `build_dashboard` depends on ADS output tables

This ensures the downstream layer will not run on incomplete upstream data.

---

## 5. Scheduling Strategy

This workflow is designed to support daily scheduling.

Typical daily execution process:

1. Prepare or refresh ODS data
2. Run DWD detail-layer ETL
3. Run DWS daily aggregations
4. Generate ADS business tables
5. Build dashboard/report output

---

## 6. Engineering Value

Compared with manually running SQL scripts one by one, introducing scheduling and dependency management provides:

- clearer execution order
- stronger workflow controllability
- reduced manual operation errors
- easier retry and rerun management
- a more production-oriented warehouse pipeline

---

## 7. Implementation Plan

In this project, DolphinScheduler is used as the workflow orchestration entry.

Each node can be implemented as a Shell task that triggers:

- Spark SQL scripts
- Hive SQL scripts
- Python dashboard scripts

Example task types:

- Shell Task
- Spark SQL Task
- Python Task

---

## 8. Interview Talking Points

This scheduling layer mainly solves the problem of "manual execution and weak dependency management" in offline warehouse projects.

By introducing DolphinScheduler, the project evolves from a collection of SQL scripts into a DAG-based data workflow with dependency control.

This makes the project closer to real enterprise warehouse scheduling scenarios.
