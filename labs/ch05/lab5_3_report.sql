-- 实验 5-3：按 ODS → DWD → DWS → ADS 加工“每日各省销售日报”
USE xiaoxiang;

-- DWD 明细层：只留已完成的订单，补上品类和会员等级，加上日期分区
CREATE TABLE IF NOT EXISTS dwd_order_detail (
  order_id STRING, user_id STRING, level STRING, product_id STRING,
  category STRING, amount DECIMAL(10,2), province STRING, pay_type STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE dwd_order_detail PARTITION (dt)
SELECT o.order_id, o.user_id, u.level, o.product_id, p.category,
       o.amount, o.province, o.pay_type,
       DATE_FORMAT(o.order_time, 'yyyy-MM-dd') AS dt
FROM ods_orders o
LEFT JOIN ods_users u    ON o.user_id = u.user_id
LEFT JOIN ods_products p ON o.product_id = p.product_id
WHERE o.status = '已完成';

-- DWS 汇总层：每天每省一行
CREATE TABLE IF NOT EXISTS dws_province_day (
  province STRING, order_cnt BIGINT, buyer_cnt BIGINT, gmv DECIMAL(14,2)
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

INSERT OVERWRITE TABLE dws_province_day PARTITION (dt)
SELECT province, COUNT(*), COUNT(DISTINCT user_id), SUM(amount), dt
FROM dwd_order_detail
GROUP BY dt, province;

-- ADS 应用层：日报 = 当天排名 + 客单价 + 和前一天比的环比
CREATE TABLE IF NOT EXISTS ads_province_daily_report STORED AS ORC AS
SELECT dt, province, order_cnt, gmv,
       ROUND(gmv / buyer_cnt, 2)                                   AS per_buyer,
       RANK() OVER (PARTITION BY dt ORDER BY gmv DESC)              AS gmv_rank,
       ROUND((gmv - LAG(gmv) OVER (PARTITION BY province ORDER BY dt))
             / LAG(gmv) OVER (PARTITION BY province ORDER BY dt) * 100, 1) AS gmv_dod_pct
FROM dws_province_day;

-- 看 9 月 30 日销售额前 5 的省份（第一句让列名不带表名前缀，显示更短）
SET hive.resultset.use.unique.column.names=false;
SELECT * FROM ads_province_daily_report
WHERE dt = '2026-09-30' AND gmv_rank <= 5
ORDER BY gmv_rank;
