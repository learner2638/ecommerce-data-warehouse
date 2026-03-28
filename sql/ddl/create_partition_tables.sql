USE ecommerce_dw;

-- =========================
-- ODS 层
-- 说明：
-- 1) ODS 表这里按你现有下游 SQL 反推字段
-- 2) 如果你的原始 CSV 就是逗号分隔，这版可直接用
-- 3) ODS 当前保留为普通表，分区不强制；你也可以后续升级为分区表
-- =========================

DROP TABLE IF EXISTS ods_orders_prod;
CREATE TABLE IF NOT EXISTS ods_orders_prod (
    order_id           BIGINT COMMENT '订单ID',
    user_id            BIGINT COMMENT '用户ID',
    shop_id            BIGINT COMMENT '店铺ID',
    status             STRING COMMENT '订单状态',
    refund_type        STRING COMMENT '退款类型',
    created_time       STRING COMMENT '下单时间',
    pay_time           STRING COMMENT '支付时间',
    cancel_time        STRING COMMENT '取消时间',
    ship_time          STRING COMMENT '发货时间',
    complete_time      STRING COMMENT '完成时间',
    refund_time        STRING COMMENT '退款时间',
    total_qty          BIGINT COMMENT '订单总件数',
    total_amount       DECIMAL(20,2) COMMENT '订单总金额',
    discount_amount    DECIMAL(20,2) COMMENT '优惠金额',
    paid_amount        DECIMAL(20,2) COMMENT '实付金额',
    refund_amount      DECIMAL(20,2) COMMENT '退款金额'
)
COMMENT 'ODS-订单原始表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS ods_order_items_prod;
CREATE TABLE IF NOT EXISTS ods_order_items_prod (
    order_item_id      BIGINT COMMENT '订单明细ID',
    order_id           BIGINT COMMENT '订单ID',
    user_id            BIGINT COMMENT '用户ID',
    shop_id            BIGINT COMMENT '店铺ID',
    sku_id             BIGINT COMMENT '商品ID',
    item_qty           BIGINT COMMENT '明细购买件数',
    sku_price          DECIMAL(20,2) COMMENT '商品单价',
    item_amount        DECIMAL(20,2) COMMENT '明细金额'
)
COMMENT 'ODS-订单明细原始表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS ods_user_dim_prod;
CREATE TABLE IF NOT EXISTS ods_user_dim_prod (
    user_id            BIGINT COMMENT '用户ID',
    city               STRING COMMENT '城市'
)
COMMENT 'ODS-用户维表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS ods_shop_dim_prod;
CREATE TABLE IF NOT EXISTS ods_shop_dim_prod (
    shop_id            BIGINT COMMENT '店铺ID',
    shop_type          STRING COMMENT '店铺类型',
    shop_weight        STRING COMMENT '店铺权重等级'
)
COMMENT 'ODS-店铺维表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;

DROP TABLE IF EXISTS ods_sku_dim_prod;
CREATE TABLE IF NOT EXISTS ods_sku_dim_prod (
    sku_id             BIGINT COMMENT '商品ID',
    category           STRING COMMENT '商品类目'
)
COMMENT 'ODS-SKU维表'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE;


-- =========================
-- DWD 层
-- 字段根据 dwd_trade_prod.sql 精确反推
-- =========================

DROP TABLE IF EXISTS dwd_trade_order_prod;
CREATE TABLE IF NOT EXISTS dwd_trade_order_prod (
    order_id           BIGINT COMMENT '订单ID',
    user_id            BIGINT COMMENT '用户ID',
    shop_id            BIGINT COMMENT '店铺ID',
    status             STRING COMMENT '订单状态',
    refund_type        STRING COMMENT '退款类型',
    created_time       STRING COMMENT '下单时间',
    pay_time           STRING COMMENT '支付时间',
    cancel_time        STRING COMMENT '取消时间',
    ship_time          STRING COMMENT '发货时间',
    complete_time      STRING COMMENT '完成时间',
    refund_time        STRING COMMENT '退款时间',
    city               STRING COMMENT '用户城市',
    shop_type          STRING COMMENT '店铺类型',
    shop_weight        STRING COMMENT '店铺权重等级',
    total_qty          BIGINT COMMENT '订单总件数',
    total_amount       DECIMAL(20,2) COMMENT '订单总金额',
    discount_amount    DECIMAL(20,2) COMMENT '优惠金额',
    paid_amount        DECIMAL(20,2) COMMENT '实付金额',
    refund_amount      DECIMAL(20,2) COMMENT '退款金额'
)
COMMENT 'DWD-交易域订单事实表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;

DROP TABLE IF EXISTS dwd_trade_order_detail_prod;
CREATE TABLE IF NOT EXISTS dwd_trade_order_detail_prod (
    order_id           BIGINT COMMENT '订单ID',
    order_item_id      BIGINT COMMENT '订单明细ID',
    user_id            BIGINT COMMENT '用户ID',
    shop_id            BIGINT COMMENT '店铺ID',
    sku_id             BIGINT COMMENT '商品ID',
    status             STRING COMMENT '订单状态',
    refund_type        STRING COMMENT '退款类型',
    created_time       STRING COMMENT '下单时间',
    pay_time           STRING COMMENT '支付时间',
    cancel_time        STRING COMMENT '取消时间',
    ship_time          STRING COMMENT '发货时间',
    complete_time      STRING COMMENT '完成时间',
    refund_time        STRING COMMENT '退款时间',
    city               STRING COMMENT '用户城市',
    shop_type          STRING COMMENT '店铺类型',
    shop_weight        STRING COMMENT '店铺权重等级',
    category           STRING COMMENT '商品类目',
    total_qty          BIGINT COMMENT '订单总件数',
    total_amount       DECIMAL(20,2) COMMENT '订单总金额',
    discount_amount    DECIMAL(20,2) COMMENT '优惠金额',
    paid_amount        DECIMAL(20,2) COMMENT '实付金额',
    refund_amount      DECIMAL(20,2) COMMENT '退款金额',
    item_qty           BIGINT COMMENT '明细件数',
    sku_price          DECIMAL(20,2) COMMENT '商品单价',
    item_amount        DECIMAL(20,2) COMMENT '明细金额'
)
COMMENT 'DWD-交易域订单明细事实表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;

DROP TABLE IF EXISTS dwd_trade_refund_detail_prod;
CREATE TABLE IF NOT EXISTS dwd_trade_refund_detail_prod (
    order_id           BIGINT COMMENT '订单ID',
    order_item_id      BIGINT COMMENT '订单明细ID',
    user_id            BIGINT COMMENT '用户ID',
    shop_id            BIGINT COMMENT '店铺ID',
    sku_id             BIGINT COMMENT '商品ID',
    status             STRING COMMENT '订单状态',
    refund_type        STRING COMMENT '退款类型',
    created_time       STRING COMMENT '下单时间',
    pay_time           STRING COMMENT '支付时间',
    cancel_time        STRING COMMENT '取消时间',
    ship_time          STRING COMMENT '发货时间',
    complete_time      STRING COMMENT '完成时间',
    refund_time        STRING COMMENT '退款时间',
    city               STRING COMMENT '用户城市',
    shop_type          STRING COMMENT '店铺类型',
    shop_weight        STRING COMMENT '店铺权重等级',
    category           STRING COMMENT '商品类目',
    total_qty          BIGINT COMMENT '订单总件数',
    total_amount       DECIMAL(20,2) COMMENT '订单总金额',
    discount_amount    DECIMAL(20,2) COMMENT '优惠金额',
    paid_amount        DECIMAL(20,2) COMMENT '实付金额',
    refund_amount      DECIMAL(20,2) COMMENT '退款金额',
    item_qty           BIGINT COMMENT '退款明细件数',
    sku_price          DECIMAL(20,2) COMMENT '商品单价',
    item_amount        DECIMAL(20,2) COMMENT '退款明细金额'
)
COMMENT 'DWD-交易域退款明细事实表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;


-- =========================
-- DWS 层
-- 字段根据 dws_trade_prod.sql 精确反推
-- =========================

DROP TABLE IF EXISTS dws_trade_day_summary_prod;
CREATE TABLE IF NOT EXISTS dws_trade_day_summary_prod (
    order_cnt          BIGINT COMMENT '订单数',
    user_cnt           BIGINT COMMENT '下单用户数',
    sku_cnt            BIGINT COMMENT '商品数',
    total_qty          BIGINT COMMENT '总销量',
    gmv                DECIMAL(20,2) COMMENT 'GMV',
    total_paid         DECIMAL(20,2) COMMENT '总实付金额',
    total_refund       DECIMAL(20,2) COMMENT '总退款金额'
)
COMMENT 'DWS-交易域全站日汇总表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;

DROP TABLE IF EXISTS dws_trade_shop_day_summary_prod;
CREATE TABLE IF NOT EXISTS dws_trade_shop_day_summary_prod (
    shop_id            BIGINT COMMENT '店铺ID',
    shop_type          STRING COMMENT '店铺类型',
    order_cnt          BIGINT COMMENT '订单数',
    user_cnt           BIGINT COMMENT '下单用户数',
    total_qty          BIGINT COMMENT '总销量',
    gmv                DECIMAL(20,2) COMMENT 'GMV',
    total_paid         DECIMAL(20,2) COMMENT '总实付金额',
    total_refund       DECIMAL(20,2) COMMENT '总退款金额'
)
COMMENT 'DWS-交易域店铺日汇总表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;

DROP TABLE IF EXISTS dws_trade_category_day_summary_prod;
CREATE TABLE IF NOT EXISTS dws_trade_category_day_summary_prod (
    category           STRING COMMENT '类目',
    order_cnt          BIGINT COMMENT '订单数',
    user_cnt           BIGINT COMMENT '下单用户数',
    total_qty          BIGINT COMMENT '总销量',
    gmv                DECIMAL(20,2) COMMENT 'GMV'
)
COMMENT 'DWS-交易域类目日汇总表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;


-- =========================
-- ADS 层
-- 字段根据 ads_trade_prod.sql 精确反推
-- =========================

DROP TABLE IF EXISTS ads_trade_overview_prod;
CREATE TABLE IF NOT EXISTS ads_trade_overview_prod (
    gmv                DECIMAL(20,2) COMMENT 'GMV',
    order_cnt          BIGINT COMMENT '订单数',
    user_cnt           BIGINT COMMENT '下单用户数',
    sku_cnt            BIGINT COMMENT '商品数',
    total_qty          BIGINT COMMENT '总销量',
    total_paid         DECIMAL(20,2) COMMENT '总实付金额',
    total_refund       DECIMAL(20,2) COMMENT '总退款金额',
    avg_order_value    DECIMAL(20,2) COMMENT '客单价',
    refund_rate        DECIMAL(20,4) COMMENT '退款率'
)
COMMENT 'ADS-交易概览表'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;

DROP TABLE IF EXISTS ads_trade_top_shop_prod;
CREATE TABLE IF NOT EXISTS ads_trade_top_shop_prod (
    shop_id            BIGINT COMMENT '店铺ID',
    shop_type          STRING COMMENT '店铺类型',
    gmv                DECIMAL(20,2) COMMENT 'GMV',
    order_cnt          BIGINT COMMENT '订单数',
    user_cnt           BIGINT COMMENT '下单用户数',
    total_qty          BIGINT COMMENT '总销量',
    rank_no            BIGINT COMMENT '排名'
)
COMMENT 'ADS-店铺GMV TOP榜单'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;

DROP TABLE IF EXISTS ads_trade_top_category_prod;
CREATE TABLE IF NOT EXISTS ads_trade_top_category_prod (
    category           STRING COMMENT '类目',
    gmv                DECIMAL(20,2) COMMENT 'GMV',
    order_cnt          BIGINT COMMENT '订单数',
    user_cnt           BIGINT COMMENT '下单用户数',
    total_qty          BIGINT COMMENT '总销量',
    rank_no            BIGINT COMMENT '排名'
)
COMMENT 'ADS-类目GMV TOP榜单'
PARTITIONED BY (dt STRING COMMENT '业务日期')
STORED AS PARQUET;


-- =========================
-- 数据质量结果表
-- 字段直接来自 create_ads_data_quality_report.sql
-- =========================

DROP TABLE IF EXISTS ads_data_quality_report_prod;
CREATE TABLE IF NOT EXISTS ads_data_quality_report_prod (
    check_date         STRING COMMENT '业务日期',
    table_name         STRING COMMENT '表名',
    check_type         STRING COMMENT '校验类型',
    check_name         STRING COMMENT '校验名称',
    actual_value       STRING COMMENT '实际值',
    expected_value     STRING COMMENT '期望值',
    diff_value         STRING COMMENT '差异值',
    check_result       STRING COMMENT 'PASS/FAIL',
    error_count        BIGINT COMMENT '异常数量',
    severity           STRING COMMENT 'INFO/WARN/ERROR',
    create_time        STRING COMMENT '创建时间'
)
COMMENT '数据质量结果表'
PARTITIONED BY (dt STRING COMMENT '分区日期')
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;
