use ecommerce_dw;

insert into table ads_data_quality_report_prod partition (dt='${hivevar:do_date}')
select
    '${hivevar:do_date}',
    'dwd_trade_order_prod',
    'null_check',
    'order_core_columns_not_null',
    cast(null_cnt as string),
    '0',
    cast(null_cnt as string),
    case when null_cnt = 0 then 'PASS' else 'FAIL' end,
    null_cnt,
    case when null_cnt = 0 then 'INFO' else 'ERROR' end,
    current_timestamp()
from (
    select count(*) as null_cnt
    from dwd_trade_order_prod
    where dt='${hivevar:do_date}'
      and (
            order_id is null
         or user_id is null
         or shop_id is null
         or created_time is null
         or total_amount is null
         or paid_amount is null
      )
) t;
