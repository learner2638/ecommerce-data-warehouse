use ecommerce_dw;

insert into table ads_data_quality_report_prod partition (dt='${hivevar:do_date}')
select
    '${hivevar:do_date}',
    'dwd_trade_order_prod',
    'validity',
    'order_amount_valid_check',
    cast(abnormal_cnt as string),
    '0',
    cast(abnormal_cnt as string),
    case when abnormal_cnt = 0 then 'PASS' else 'FAIL' end,
    abnormal_cnt,
    case when abnormal_cnt = 0 then 'INFO' else 'ERROR' end,
    current_timestamp()
from (
    select count(*) as abnormal_cnt
    from dwd_trade_order_prod
    where dt='${hivevar:do_date}'
      and (
            total_amount < 0
         or discount_amount < 0
         or paid_amount < 0
         or refund_amount < 0
      )
) t;
