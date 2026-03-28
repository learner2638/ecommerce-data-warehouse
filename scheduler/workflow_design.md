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

```text
ods_prepare
   ↓
dwd_trade_order
   ↓
dws_trade_day_summary
   ↓
ads_trade_overview
   ↓
build_dashboard
