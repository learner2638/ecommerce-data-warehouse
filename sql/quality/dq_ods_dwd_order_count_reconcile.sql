use ecommerce_dw;

insert into table ads_data_quality_report_prod partition (dt='${hivevar:do_date}')
select
    '${hivevar:do_date}',
    'ods_orders_prod_vs_dwd_trade_order_prod',
    'reconciliation',
    'order_row_count_reconcile',
    concat('ods_cnt=', ods_cnt, ', dwd_cnt=', dwd_cnt),
    'diff = 0',
    cast(abs(ods_cnt - dwd_cnt) as string),
    case when abs(ods_cnt - dwd_cnt) = 0 then 'PASS' else 'FAIL' end,
    abs(ods_cnt - dwd_cnt),
    case when abs(ods_cnt - dwd_cnt) = 0 then 'INFO' else 'ERROR' end,
    current_timestamp()
from (
    select
        (select count(*)
         from ods_orders_prod
         where substr(created_time,1,10)='${hivevar:do_date}'
           and order_id is not null
           and cast(order_id as string) <> 'order_id') as ods_cnt,
        (select count(*)
         from dwd_trade_order_prod
         where dt='${hivevar:do_date}') as dwd_cnt
) t;
