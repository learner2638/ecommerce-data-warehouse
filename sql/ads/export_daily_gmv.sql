insert overwrite local directory '/home/xu/ecommerce_dw/bi_export/daily_gmv'
row format delimited
fields terminated by ','
select
    dt,
    gmv
from dws_trade_day_summary_prod
order by dt;
