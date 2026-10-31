"""实验 7-2：用正则表达式把访问日志拆成字段，整理成表，保存为 Parquet。

先在本机运行 python ch07/make_dirty.py 生成带坏行的日志，再在 spark 容器中运行：
    spark-submit /labs/ch07/lab7_2_parse_log.py
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验7-2 解析访问日志").getOrCreate()
DATA = "file:///labs/data"

# 一行日志：2026-09-30T08:00:03 183.6.12.34 u0001234 GET /item/p01024 200 5320 Android
PATTERN = r"^(\S+T\S+) (\d+\.\d+\.\d+\.\d+) (\S+) (GET|POST) (\S+) (\d{3}) (\d+) (PC|Android|iPhone|MiniProgram)$"

lines = spark.read.text(f"{DATA}/xiaoxiang_dirty/access_dirty.log")      # 一列 value，每行一条
print("① 原始行数", lines.count())

ok = lines.filter(F.col("value").rlike(PATTERN))
print("② 不符合格式的行", lines.count() - ok.count())
lines.filter(~F.col("value").rlike(PATTERN)).show(5, truncate=60)        # 看看坏行长什么样

names = ["ts", "ip", "user_id", "method", "url", "status", "bytes", "device"]
logs = ok.select(*[F.regexp_extract("value", PATTERN, i + 1).alias(n) for i, n in enumerate(names)])
logs = (logs
        .withColumn("ts", F.to_timestamp("ts", "yyyy-MM-dd'T'HH:mm:ss"))
        .withColumn("status", F.col("status").cast("int"))
        .withColumn("bytes", F.col("bytes").cast("int"))
        .withColumn("user_id", F.when(F.col("user_id") == "-", None).otherwise(F.col("user_id")))  # 未登录记为空
        .withColumn("hour", F.hour("ts"))
        .withColumn("product_id", F.regexp_extract("url", r"^/item/(p\d+)$", 1))                  # 商品详情页的商品编号
        .withColumn("product_id", F.when(F.col("product_id") == "", None).otherwise(F.col("product_id"))))
logs.printSchema()
logs.show(5, truncate=False)
print("③ 解析后行数", logs.count(), "，其中登录用户的访问", logs.filter(F.col("user_id").isNotNull()).count(),
      "，商品详情页的访问", logs.filter(F.col("product_id").isNotNull()).count())

out = "/labs/data/xiaoxiang_clean/access_log"
logs.write.mode("overwrite").parquet("file://" + out)
size = sum(os.path.getsize(os.path.join(out, f)) for f in os.listdir(out) if f.endswith(".parquet"))
print(f"④ 原始文本 {os.path.getsize('/labs/data/xiaoxiang_dirty/access_dirty.log') / 1048576:.1f} MB，Parquet {size / 1048576:.1f} MB")

# ⑤ 存成 Parquet 后再读：只读用到的列
t = spark.read.parquet("file://" + out)
t.groupBy("device").agg(F.count("*").alias("pv"), F.countDistinct("user_id").alias("uv")).orderBy(F.desc("pv")).show()
spark.stop()
