use ecommerce_dw;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.sources.partitionOverwriteMode=dynamic;
set spark.sql.shuffle.partitions=24;

-- =========================
-- DWS 1: 全站日汇总
-- =========================
insert overwrite table dws_trade_day_summary_prod partition (dt)
select
    o.order_cnt,
    o.user_cnt,
    d.sku_cnt,
    d.total_qty,
    d.gmv,
    o.total_paid,
    o.total_refund,
    o.dt
from
(
    select
        dt,
        count(distinct order_id) as order_cnt,
        count(distinct user_id) as user_cnt,
        cast(sum(coalesce(paid_amount, 0)) as decimal(20,2)) as total_paid,
        cast(sum(coalesce(refund_amount, 0)) as decimal(20,2)) as total_refund
    from dwd_trade_order_prod
    group by dt
) o
join
(
    select
        dt,
        count(distinct sku_id) as sku_cnt,
        cast(sum(coalesce(item_qty, 0)) as bigint) as total_qty,
        cast(sum(coalesce(item_amount, 0)) as decimal(20,2)) as gmv
    from dwd_trade_order_detail_prod
    group by dt
) d
on o.dt = d.dt;

-- =========================
-- DWS 2: 店铺日汇总
-- =========================
insert overwrite table dws_trade_shop_day_summary_prod partition (dt)
select
    shop_id,
    shop_type,
    count(distinct order_id) as order_cnt,
    count(distinct user_id) as user_cnt,
    cast(sum(coalesce(total_qty, 0)) as bigint) as total_qty,
    cast(sum(coalesce(total_amount, 0)) as decimal(20,2)) as gmv,
    cast(sum(coalesce(paid_amount, 0)) as decimal(20,2)) as total_paid,
    cast(sum(coalesce(refund_amount, 0)) as decimal(20,2)) as total_refund,
    dt
from dwd_trade_order_prod
group by shop_id, shop_type, dt;

-- =========================
-- DWS 3: 类目日汇总
-- =========================
insert overwrite table dws_trade_category_day_summary_prod partition (dt)
select
    category,
    count(distinct order_id) as order_cnt,
    count(distinct user_id) as user_cnt,
    cast(sum(coalesce(item_qty, 0)) as bigint) as total_qty,
    cast(sum(coalesce(item_amount, 0)) as decimal(20,2)) as gmv,
    dt
from dwd_trade_order_detail_prod
group by category, dt;
