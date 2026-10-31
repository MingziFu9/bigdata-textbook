-- 实验 5-1 第 3 步：建库，建三张外部表，指向 HDFS 上已有的 CSV 文件
CREATE DATABASE IF NOT EXISTS xiaoxiang;
USE xiaoxiang;

-- 订单表：数据在 /xiaoxiang/ods/orders 目录下
CREATE EXTERNAL TABLE IF NOT EXISTS ods_orders (
  order_id    STRING,
  user_id     STRING,
  product_id  STRING,
  quantity    INT,
  amount      DECIMAL(10,2),
  order_time  TIMESTAMP,
  province    STRING,
  pay_type    STRING,
  status      STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/xiaoxiang/ods/orders'
-- skip.header.line.count=1：跳过第一行表头
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS ods_users (
  user_id        STRING,
  gender         STRING,
  age            INT,
  province       STRING,
  city           STRING,
  register_date  DATE,
  level          STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/xiaoxiang/ods/users'
TBLPROPERTIES ('skip.header.line.count'='1');

CREATE EXTERNAL TABLE IF NOT EXISTS ods_products (
  product_id    STRING,
  product_name  STRING,
  category      STRING,
  price         DECIMAL(10,2),
  shop_id       STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/xiaoxiang/ods/products'
TBLPROPERTIES ('skip.header.line.count'='1');

SHOW TABLES;
DESCRIBE ods_orders;
SELECT * FROM ods_orders LIMIT 3;
SELECT COUNT(*) FROM ods_orders;
