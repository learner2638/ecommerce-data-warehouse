#!/bin/bash
set -e

echo "===== DWS START ====="

spark-sql -f ./sql/dws/dws_trade_day_summary.sql
spark-sql -f ./sql/dws/dws_trade_shop_day_summary.sql
spark-sql -f ./sql/dws/dws_trade_category_day_summary.sql

echo "===== DWS END ====="
