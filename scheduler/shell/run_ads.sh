#!/bin/bash
set -e

echo "===== ADS START ====="

spark-sql -f ./sql/ads/ads_trade_overview.sql
spark-sql -f ./sql/ads/ads_trade_top_shop.sql
spark-sql -f ./sql/ads/ads_trade_top_category.sql

echo "===== ADS END ====="
