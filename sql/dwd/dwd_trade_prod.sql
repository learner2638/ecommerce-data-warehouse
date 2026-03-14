use ecommerce_dw;

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set spark.sql.sources.partitionOverwriteMode=dynamic;
set spark.sql.shuffle.partitions=24;

-- =========================
-- DWD 1: 订单事实表
-- =========================
insert overwrite table dwd_trade_order_prod partition (dt)
select
    o.order_id,
    o.user_id,
    o.shop_id,
    o.status,
    o.refund_type,
    o.created_time,
    o.pay_time,
    o.cancel_time,
    o.ship_time,
    o.complete_time,
    o.refund_time,
    u.city,
    s.shop_type,
    s.shop_weight,
    o.total_qty,
    o.total_amount,
    o.discount_amount,
    o.paid_amount,
    o.refund_amount,
    substr(o.created_time, 1, 10) as dt
from ods_orders_prod o
left join ods_user_dim_prod u
    on o.user_id = u.user_id
left join ods_shop_dim_prod s
    on o.shop_id = s.shop_id
where o.order_id is not null
  and cast(o.order_id as string) <> 'order_id';

-- =========================
-- DWD 2: 订单明细事实表
-- =========================
insert overwrite table dwd_trade_order_detail_prod partition (dt)
select
    oi.order_id,
    oi.order_item_id,
    oi.user_id,
    oi.shop_id,
    oi.sku_id,
    o.status,
    o.refund_type,
    o.created_time,
    o.pay_time,
    o.cancel_time,
    o.ship_time,
    o.complete_time,
    o.refund_time,
    u.city,
    s.shop_type,
    s.shop_weight,
    k.category,
    o.total_qty,
    o.total_amount,
    o.discount_amount,
    o.paid_amount,
    o.refund_amount,
    oi.item_qty,
    oi.sku_price,
    oi.item_amount,
    substr(o.created_time, 1, 10) as dt
from ods_order_items_prod oi
left join ods_orders_prod o
    on oi.order_id = o.order_id
left join ods_user_dim_prod u
    on o.user_id = u.user_id
left join ods_shop_dim_prod s
    on oi.shop_id = s.shop_id
left join ods_sku_dim_prod k
    on oi.sku_id = k.sku_id
where oi.order_item_id is not null
  and cast(oi.order_item_id as string) <> 'order_item_id'
  and oi.order_id is not null;

-- =========================
-- DWD 3: 退款明细事实表
-- =========================
insert overwrite table dwd_trade_refund_detail_prod partition (dt)
select
    oi.order_id,
    oi.order_item_id,
    oi.user_id,
    oi.shop_id,
    oi.sku_id,
    o.status,
    o.refund_type,
    o.created_time,
    o.pay_time,
    o.cancel_time,
    o.ship_time,
    o.complete_time,
    o.refund_time,
    u.city,
    s.shop_type,
    s.shop_weight,
    k.category,
    o.total_qty,
    o.total_amount,
    o.discount_amount,
    o.paid_amount,
    o.refund_amount,
    oi.item_qty,
    oi.sku_price,
    oi.item_amount,
    substr(o.created_time, 1, 10) as dt
from ods_order_items_prod oi
left join ods_orders_prod o
    on oi.order_id = o.order_id
left join ods_user_dim_prod u
    on o.user_id = u.user_id
left join ods_shop_dim_prod s
    on oi.shop_id = s.shop_id
left join ods_sku_dim_prod k
    on oi.sku_id = k.sku_id
where oi.order_item_id is not null
  and cast(oi.order_item_id as string) <> 'order_item_id'
  and oi.order_id is not null
  and (
      coalesce(o.refund_amount, 0) > 0
      or o.refund_time is not null
      or (o.refund_type is not null and o.refund_type <> '')
  );
