#!/bin/bash

if [ -n "$1" ]; then
  do_date=$1
else
  do_date=$(date -d "-1 day" +%F)
fi

echo "Running data quality checks for date: ${do_date}"

hive -f sql/quality/create_ads_data_quality_report.sql

hive -f sql/quality/dq_order_pk_unique.sql -hivevar do_date=${do_date}
hive -f sql/quality/dq_order_core_not_null.sql -hivevar do_date=${do_date}
hive -f sql/quality/dq_order_amount_valid.sql -hivevar do_date=${do_date}
hive -f sql/quality/dq_order_time_logic.sql -hivevar do_date=${do_date}
hive -f sql/quality/dq_ods_dwd_order_count_reconcile.sql -hivevar do_date=${do_date}
hive -f sql/quality/dq_ods_dwd_paid_amount_reconcile.sql -hivevar do_date=${do_date}

echo "Data quality checks finished for ${do_date}"
