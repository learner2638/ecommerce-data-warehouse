# DAG Example

## Core DAG

ods_prepare  
→ dwd_trade_order  
→ dws_trade_day_summary  
→ ads_trade_overview  
→ build_dashboard  

---

## Extended DAG

ods_prepare  
→ dwd_trade_order  
→ dwd_trade_order_detail  
→ dws_trade_day_summary  
→ dws_trade_shop_day_summary  
→ dws_trade_category_day_summary  
→ ads_trade_overview  
→ ads_trade_top_shop  
→ ads_trade_top_category  
→ build_dashboard  

---

## Description

This DAG represents the dependency chain of the offline data warehouse workflow.

- ODS is the raw input layer
- DWD is the cleaned detail layer
- DWS is the summarized service layer
- ADS is the business-facing application layer
- Dashboard is the final visualization output layer

The workflow ensures that downstream tasks are triggered only after upstream tasks complete successfully.
