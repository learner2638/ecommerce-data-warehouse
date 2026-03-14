use ecommerce_dw;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.sources.partitionOverwriteMode=dynamic;
set spark.sql.shuffle.partitions=24;

-- =========================
-- ADS 1: 全站核心指标
-- =========================
insert overwrite table ads_trade_overview_prod partition (dt)
select
    cast(gmv as decimal(20,2)) as gmv,
    order_cnt,
    user_cnt,
    sku_cnt,
    total_qty,
    cast(total_paid as decimal(20,2)) as total_paid,
    cast(total_refund as decimal(20,2)) as total_refund,
    cast(case when order_cnt = 0 then 0 else gmv / order_cnt end as decimal(20,2)) as avg_order_value,
    cast(case when gmv = 0 then 0 else total_refund / gmv end as decimal(20,4)) as refund_rate,
    dt
from dws_trade_day_summary_prod;

-- =========================
-- ADS 2: 每日店铺GMV排行
-- =========================
insert overwrite table ads_trade_top_shop_prod partition (dt)
select
    shop_id,
    shop_type,
    cast(gmv as decimal(20,2)) as gmv,
    order_cnt,
    user_cnt,
    total_qty,
    rank_no,
    dt
from
(
    select
        shop_id,
        shop_type,
        gmv,
        order_cnt,
        user_cnt,
        total_qty,
        dt,
        row_number() over(partition by dt order by gmv desc, order_cnt desc) as rank_no
    from dws_trade_shop_day_summary_prod
) t
where rank_no <= 10;

-- =========================
-- ADS 3: 每日类目GMV排行
-- =========================
insert overwrite table ads_trade_top_category_prod partition (dt)
select
    category,
    cast(gmv as decimal(20,2)) as gmv,
    order_cnt,
    user_cnt,
    total_qty,
    rank_no,
    dt
from
(
    select
        category,
        gmv,
        order_cnt,
        user_cnt,
        total_qty,
        dt,
        row_number() over(partition by dt order by gmv desc, order_cnt desc) as rank_no
    from dws_trade_category_day_summary_prod
) t
where rank_no <= 10;
