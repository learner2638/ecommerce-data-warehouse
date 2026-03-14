insert overwrite local directory '/home/xu/ecommerce_dw/bi_export/top_category_gmv'
row format delimited
fields terminated by ','
select
    category,
    gmv
from ads_trade_top_category_prod
where dt = '2025-01-01'
order by rank_no
limit 10;
