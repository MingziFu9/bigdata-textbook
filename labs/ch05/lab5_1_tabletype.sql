-- 实验 5-1 第 5 步：内部表和外部表，删表时有什么不同
-- 运行前先在 master 容器里执行：hdfs dfs -put -f /labs/data/xiaoxiang/products.csv /tmp/
USE xiaoxiang;

-- 内部表：不写 EXTERNAL，用 LOAD DATA 把 HDFS 上的文件“搬进”表的目录
CREATE TABLE products_managed (
  product_id STRING, product_name STRING, category STRING,
  price DECIMAL(10,2), shop_id STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
TBLPROPERTIES ('skip.header.line.count'='1');

LOAD DATA INPATH '/tmp/products.csv' INTO TABLE products_managed;
SELECT COUNT(*) FROM products_managed;
DESCRIBE FORMATTED products_managed;

-- 外部表：和 ods_products 指向同一个目录
CREATE EXTERNAL TABLE products_ext (
  product_id STRING, product_name STRING, category STRING,
  price DECIMAL(10,2), shop_id STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
LOCATION '/xiaoxiang/ods/products'
TBLPROPERTIES ('skip.header.line.count'='1');

-- 两张表都删掉，然后到 HDFS 上看文件还在不在
DROP TABLE products_managed;
DROP TABLE products_ext;
