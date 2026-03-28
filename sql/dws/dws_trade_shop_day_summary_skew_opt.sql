SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

SET spark.sql.adaptive.enabled=true;
SET spark.sql.adaptive.skewJoin.enabled=true;
SET spark.sql.shuffle.partitions=200;

WITH tmp_salted AS (
    SELECT
        dt,
        concat(cast(floor(rand() * 8) as string), '_', cast(shop_id as string)) as salted_shop_id,
        shop_id,
        order_id,
        user_id,
        sku_id,
        buy_num,
        pay_amount,
        refund_amount
    FROM dwd_trade_order_detail_prod
),
tmp_partial AS (
    SELECT
        dt,
        salted_shop_id,
        max(shop_id) as shop_id,
        count(distinct order_id) as order_cnt,
        count(distinct user_id) as user_cnt,
        count(distinct sku_id) as sku_cnt,
        sum(buy_num) as total_qty,
        sum(pay_amount) as total_paid,
        sum(refund_amount) as total_refund
    FROM tmp_salted
    GROUP BY dt, salted_shop_id
),
tmp_final AS (
    SELECT
        dt,
        shop_id,
        sum(order_cnt) as order_cnt,
        sum(user_cnt) as user_cnt,
        sum(sku_cnt) as sku_cnt,
        sum(total_qty) as total_qty,
        sum(total_paid) as total_paid,
        sum(total_refund) as total_refund
    FROM tmp_partial
    GROUP BY dt, shop_id
)

INSERT OVERWRITE TABLE dws_trade_shop_day_summary_prod PARTITION (dt)
SELECT
    shop_id,
    order_cnt,
    user_cnt,
    sku_cnt,
    total_qty,
    total_paid,
    total_refund,
    dt
FROM tmp_final;
