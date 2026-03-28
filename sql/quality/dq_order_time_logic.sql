use ecommerce_dw;

insert into table ads_data_quality_report_prod partition (dt='${hivevar:do_date}')
select
    '${hivevar:do_date}',
    'dwd_trade_order_prod',
    'logic_check',
    'order_time_logic_check',
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
            (pay_time is not null and created_time is not null and pay_time < created_time)
         or (ship_time is not null and pay_time is not null and ship_time < pay_time)
         or (complete_time is not null and ship_time is not null and complete_time < ship_time)
         or (refund_time is not null and pay_time is not null and refund_time < pay_time)
      )
) t;
