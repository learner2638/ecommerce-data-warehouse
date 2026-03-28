use ecommerce_dw;

insert overwrite table ads_data_quality_report_prod partition (dt='${hivevar:do_date}')
select
    '${hivevar:do_date}' as check_date,
    'dwd_trade_order_prod' as table_name,
    'uniqueness' as check_type,
    'order_id_unique_check' as check_name,
    cast(dup_cnt as string) as actual_value,
    '0' as expected_value,
    cast(dup_cnt as string) as diff_value,
    case when dup_cnt = 0 then 'PASS' else 'FAIL' end as check_result,
    dup_cnt as error_count,
    case when dup_cnt = 0 then 'INFO' else 'ERROR' end as severity,
    current_timestamp() as create_time
from (
    select count(*) as dup_cnt
    from (
        select order_id
        from dwd_trade_order_prod
        where dt='${hivevar:do_date}'
        group by order_id
        having count(*) > 1
    ) t
) s;
