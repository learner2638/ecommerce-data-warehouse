#!/bin/bash
set -e

echo "===== ODS PREPARE START ====="

# 这里通常是数据准备或校验
# 你的项目里可以简单模拟一下，比如检查表是否存在

hive -e "
SHOW TABLES LIKE 'ods_orders';
SHOW TABLES LIKE 'ods_order_items';
"

echo "===== ODS PREPARE END ====="
