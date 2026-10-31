"""实验 12-2 第 1 步：把看板要用的数据写进 MySQL，交给 Superset 查询。

先完成实验 7-3（订单宽表）和实验 10-2（用户分群），并启动 MySQL 容器，再在 spark 容器中运行：
    spark-submit --jars /labs/jars/mysql-connector-j-8.4.0.jar /labs/ch12/lab12_2_to_mysql.py
结果：MySQL 库 xiaoxiang 中的两张表 dash_orders、dash_user_cluster
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验12-2 看板数据").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
DATA = "file:///labs/data"
URL = "jdbc:mysql://mysql:3306/xiaoxiang?useUnicode=true&characterEncoding=utf8"
PROPS = {"user": "root", "password": "xiaoxiang", "driver": "com.mysql.cj.jdbc.Driver"}

# ① 订单明细（9 月，10 万行左右）。看板只需要这些列，用户编号以外不放任何个人信息
orders = (spark.read.parquet(f"{DATA}/xiaoxiang_clean/order_wide")
               .select("order_id", "user_id", "order_time", "dt", "province", "category",
                       "quantity", F.col("amount").cast("double").alias("amount"),
                       "pay_type", "status", "level"))
orders.write.mode("overwrite").jdbc(URL, "dash_orders", properties=PROPS)
print("① dash_orders：", orders.count(), "行")

# ② 实验 10-2 的用户分群
cluster = spark.read.parquet(f"{DATA}/xiaoxiang_clean/user_cluster").select(
    "user_id", "cluster_name", "R", "F", F.col("M").cast("double").alias("M"))
cluster.write.mode("overwrite").jdbc(URL, "dash_user_cluster", properties=PROPS)
print("② dash_user_cluster：", cluster.count(), "行")

back = spark.read.jdbc(URL, "(SELECT category, ROUND(SUM(amount)/10000, 1) AS gmv_wan FROM dash_orders "
                            "WHERE status = '已完成' GROUP BY category ORDER BY gmv_wan DESC) t", properties=PROPS)
print("③ 从 MySQL 读回来核对：")
back.show()
spark.stop()
