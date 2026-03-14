insert overwrite local directory '/home/xu/ecommerce_dw/bi_export/daily_refund'
row format delimited
fields terminated by ','
select
    dt,
    total_refund
from dws_trade_day_summary_prod
order by dt;
