insert overwrite local directory '/home/xu/ecommerce_dw/bi_export/daily_order_cnt'
row format delimited
fields terminated by ','
select
    dt,
    order_cnt
from dws_trade_day_summary_prod
order by dt;
