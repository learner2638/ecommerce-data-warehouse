use ecommerce_dw;

insert into table ads_data_quality_report_prod partition (dt='${hivevar:do_date}')
select
    '${hivevar:do_date}',
    'ods_orders_prod_vs_dwd_trade_order_prod',
    'reconciliation',
    'paid_amount_sum_reconcile',
    concat('ods_amt=', cast(ods_amt as string), ', dwd_amt=', cast(dwd_amt as string)),
    'diff <= 0.01',
    cast(abs(ods_amt - dwd_amt) as string),
    case when abs(ods_amt - dwd_amt) <= 0.01 then 'PASS' else 'FAIL' end,
    case when abs(ods_amt - dwd_amt) <= 0.01 then 0 else 1 end,
    case when abs(ods_amt - dwd_amt) <= 0.01 then 'INFO' else 'ERROR' end,
    current_timestamp()
from (
    select
        coalesce((
            select cast(sum(coalesce(paid_amount,0)) as decimal(20,2))
            from ods_orders_prod
            where substr(created_time,1,10)='${hivevar:do_date}'
              and order_id is not null
              and cast(order_id as string) <> 'order_id'
        ), 0) as ods_amt,
        coalesce((
            select cast(sum(coalesce(paid_amount,0)) as decimal(20,2))
            from dwd_trade_order_prod
            where dt='${hivevar:do_date}'
        ), 0) as dwd_amt
) t;
