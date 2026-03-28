#!/bin/bash
set -e

echo "===== DWD START ====="

spark-sql -f ./sql/dwd/dwd_trade_order.sql
spark-sql -f ./sql/dwd/dwd_trade_order_detail.sql

echo "===== DWD END ====="
