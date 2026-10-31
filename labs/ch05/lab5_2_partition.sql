-- 实验 5-2：分区表
USE xiaoxiang;

-- ① 300 万行订单的外部表（不分区），数据在 /xiaoxiang/ods/orders_big
CREATE EXTERNAL TABLE IF NOT EXISTS ods_orders_big (
  order_id STRING, user_id STRING, product_id STRING, quantity INT,
  amount DECIMAL(10,2), order_time TIMESTAMP, province STRING,
  pay_type STRING, status STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/xiaoxiang/ods/orders_big'
TBLPROPERTIES ('skip.header.line.count'='1');

-- ② 按日期分区的订单表：dt 是分区字段，不写在括号里
CREATE TABLE IF NOT EXISTS orders_part (
  order_id STRING, user_id STRING, product_id STRING, quantity INT,
  amount DECIMAL(10,2), order_time TIMESTAMP, province STRING,
  pay_type STRING, status STRING
)
PARTITIONED BY (dt STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- ③ 动态分区：按每行的日期自动放进对应分区，分区字段放在 SELECT 的最后
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE orders_part PARTITION (dt)
SELECT order_id, user_id, product_id, quantity, amount, order_time,
       province, pay_type, status,
       DATE_FORMAT(order_time, 'yyyy-MM-dd') AS dt
FROM ods_orders_big;

SHOW PARTITIONS orders_part;
