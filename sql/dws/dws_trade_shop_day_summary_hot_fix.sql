WITH hot_shop AS (
    SELECT shop_id
    FROM dwd_trade_order_detail_prod
    GROUP BY shop_id
    ORDER BY count(*) DESC
    LIMIT 10
),

hot_data AS (
    SELECT *
    FROM dwd_trade_order_detail_prod
    WHERE shop_id IN (SELECT shop_id FROM hot_shop)
),

normal_data AS (
    SELECT *
    FROM dwd_trade_order_detail_prod
    WHERE shop_id NOT IN (SELECT shop_id FROM hot_shop)
),

hot_result AS (
    SELECT
        dt,
        shop_id,
        count(*) as order_cnt,
        sum(pay_amount) as total_paid
    FROM hot_data
    GROUP BY dt, shop_id
),

normal_result AS (
    SELECT
        dt,
        shop_id,
        count(*) as order_cnt,
        sum(pay_amount) as total_paid
    FROM normal_data
    GROUP BY dt, shop_id
)

SELECT * FROM hot_result
UNION ALL
SELECT * FROM normal_result;
