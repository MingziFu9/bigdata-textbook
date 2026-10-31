"""实验 8-3（第 1 部分）：Spark 读 Hive 表，统计后写回 Hive，并写进 MySQL。

需要先启动 Hive（第 5 章）和 MySQL，并下载 MySQL 的 JDBC 驱动到 labs/jars/（见本章 README）。在 spark 容器中运行：
    spark-submit --jars /labs/jars/mysql-connector-j-8.4.0.jar /labs/ch08/lab8_3_hive_mysql.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName("实验8-3 读写 Hive 与 MySQL")
         .enableHiveSupport()                      # 打开 Hive 支持：表名直接指向 Hive 的表
         .getOrCreate())

# ① 直接查 Hive 中第 5 章建的表
spark.sql("SHOW TABLES IN xiaoxiang").show(truncate=False)
orders = spark.table("xiaoxiang.ods_orders")
products = spark.table("xiaoxiang.ods_products")
print("① ods_orders 行数", orders.count())
# Spark 读 Hive 的文本表时不认 skip.header.line.count，CSV 的表头会被当成一行数据，要自己去掉
orders = orders.filter(F.col("order_id") != "order_id")
products = products.filter(F.col("product_id") != "product_id")
print("   去掉表头后", orders.count())

# ② 每天每个品类的销售额（只算已完成）
daily = (orders.filter(F.col("status") == "已完成")
               .join(products.select("product_id", "category"), "product_id")
               .groupBy(F.to_date("order_time").alias("dt"), "category")
               .agg(F.round(F.sum("amount"), 2).alias("gmv"), F.count("*").alias("orders"))
               .orderBy("dt", "category"))
print("② 结果行数", daily.count())
daily.show(5)

# ③ 写回 Hive：成为数据仓库 ADS 层的一张表，在 beeline 中也能查
daily.write.mode("overwrite").format("parquet").saveAsTable("xiaoxiang.ads_category_daily")
print("③ 已写入 Hive 表 xiaoxiang.ads_category_daily")

# ④ 写进 MySQL：报表系统、看板一般从 MySQL 这样的数据库取数
url = "jdbc:mysql://mysql:3306/xiaoxiang"
props = {"user": "root", "password": "xiaoxiang", "driver": "com.mysql.cj.jdbc.Driver"}
daily.write.mode("overwrite").jdbc(url, "category_daily", properties=props)
back = spark.read.jdbc(url, "category_daily", properties=props)
print("④ 已写入 MySQL 表 category_daily，读回来", back.count(), "行")
spark.stop()
