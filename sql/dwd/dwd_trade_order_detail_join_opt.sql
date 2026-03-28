SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;

SELECT /*+ MAPJOIN(shop) */
    od.order_id,
    od.user_id,
    od.shop_id,
    shop.shop_name,
    od.sku_id,
    od.buy_num,
    od.pay_amount,
    od.dt
FROM dwd_trade_order_detail_prod od
LEFT JOIN dim_shop shop
ON od.shop_id = shop.shop_id;
