# 指标口径说明

## 1. 文档目的

本文档用于说明本项目当前版本中各核心指标的实际计算口径、来源表及适用范围。

说明重点：

1. 本文档描述的是当前仓库 SQL 已实现的真实口径  
2. 不对现有 DWD / DWS / ADS 加工逻辑做改动  
3. 主要用于项目说明、README 补充和面试表达  
4. 若后续需要升级为更严格的企业级统一口径，可在此基础上继续抽象标准指标层  

---

## 2. 分层说明

本项目采用典型离线数仓分层：

ODS -> DWD -> DWS -> ADS

其中：

- ODS：原始明细数据层  
- DWD：明细事实层，完成基础清洗与维度补充  
- DWS：汇总服务层，按主题沉淀统计指标  
- ADS：应用分析层，面向 BI 展示和业务分析输出最终结果  

---

## 3. 当前版本时间口径

本项目当前 DWD 层订单事实表、订单明细事实表的分区字段 dt，均取自订单创建时间 created_time 的日期部分：

```sql
substr(created_time, 1, 10) as dt
```

因此当前版本中：

所有日统计均基于订单创建日期

---

## 4. 指标来源总览

### 4.1 全站日汇总（dws_trade_day_summary_prod）

订单侧指标来源：

- 表：dwd_trade_order_prod  
- 指标：order_cnt、user_cnt、total_paid、total_refund  

明细侧指标来源：

- 表：dwd_trade_order_detail_prod  
- 指标：sku_cnt、total_qty、gmv  

最终按 dt 关联汇总。

---

### 4.2 店铺日汇总（dws_trade_shop_day_summary_prod）

来源表：

- dwd_trade_order_prod  

维度：

- shop_id  
- shop_type  
- dt  

指标：

- order_cnt  
- user_cnt  
- total_qty  
- gmv  
- total_paid  
- total_refund  

---

### 4.3 类目日汇总（dws_trade_category_day_summary_prod）

来源表：

- dwd_trade_order_detail_prod  

维度：

- category  
- dt  

指标：

- order_cnt  
- user_cnt  
- total_qty  
- gmv  

---

### 4.4 ADS 层（ads_trade_overview_prod）

基于全站日汇总生成：

- gmv  
- order_cnt  
- user_cnt  
- sku_cnt  
- total_qty  
- total_paid  
- total_refund  
- avg_order_value  
- refund_rate  

---

## 5. 核心指标定义

### 5.1 订单数（order_cnt）

定义：

```sql
count(distinct order_id)
```

来源：

- 全站、店铺：dwd_trade_order_prod  
- 类目：dwd_trade_order_detail_prod  

说明：

- 未对订单状态做额外筛选  
- 基于订单创建时间进行统计  

---

### 5.2 用户数（user_cnt）

定义：

```sql
count(distinct user_id)
```

来源：

- 全站、店铺：dwd_trade_order_prod  
- 类目：dwd_trade_order_detail_prod  

含义：

- 去重下单用户数  

---

### 5.3 商品数（sku_cnt）

定义：

```sql
count(distinct sku_id)
```

来源：

- dwd_trade_order_detail_prod  

说明：

- 当前仅在全站汇总中提供  

---

### 5.4 商品件数（total_qty）

全站：

```sql
sum(item_qty)
```

来源：

- dwd_trade_order_detail_prod  

店铺：

```sql
sum(total_qty)
```

来源：

- dwd_trade_order_prod  

类目：

```sql
sum(item_qty)
```

来源：

- dwd_trade_order_detail_prod  

---

### 5.5 GMV

全站：

```sql
sum(item_amount)
```

来源：

- dwd_trade_order_detail_prod  

店铺：

```sql
sum(total_amount)
```

来源：

- dwd_trade_order_prod  

类目：

```sql
sum(item_amount)
```

来源：

- dwd_trade_order_detail_prod  

说明：

当前 GMV 在不同粒度下来源字段不同：

- 全站：item_amount  
- 店铺：total_amount  
- 类目：item_amount  

---

### 5.6 实付金额（total_paid）

定义：

```sql
sum(paid_amount)
```

来源：

- dwd_trade_order_prod  

---

### 5.7 退款金额（total_refund）

定义：

```sql
sum(refund_amount)
```

来源：

- dwd_trade_order_prod  

---

### 5.8 客单价（avg_order_value）

定义：

```sql
gmv / order_cnt
```

说明：

- 当前为平均每单 GMV  

---

### 5.9 退款率（refund_rate）

定义：

```sql
total_refund / gmv
```

说明：

- 分母使用 GMV  

---

## 6. 当前版本口径特点

优点：

- 已形成完整数仓分层  
- 指标来源清晰  
- 支持多维分析  
- 可用于 BI 展示  

限制：

- GMV 来源不统一  
- 日统计基于创建时间  
- 未区分订单状态  
- 退款未细分  

---



## 7. 总结

在不改动现有 SQL 的前提下，对指标体系进行了统一梳理，使指标具备：

- 明确定义  
- 可追溯来源  
- 可解释口径  
