insert overwrite local directory '/home/xu/ecommerce_dw/bi_export/top_shop_gmv'
row format delimited
fields terminated by ','
select
    shop_id,
    shop_type,
    gmv
from ads_trade_top_shop_prod
where dt = '2025-01-01'
order by rank_no
limit 10;
