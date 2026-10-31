"""实验 8-3（第 3 部分）：Spark（本地模式）处理不同数据量要多久。在 spark 容器中运行：
    spark-submit /labs/ch08/lab8_3_spark_time.py
任务与 lab8_3_pandas_time.py 完全相同。
"""
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

t_start = time.time()
spark = SparkSession.builder.appName("实验8-3 Spark 计时").getOrCreate()
print(f"启动 Spark 用时 {time.time() - t_start:.1f} 秒")
for n in [10_000, 100_000, 1_000_000, 3_000_000]:
    path = f"/labs/data/scale/orders_{n}.csv"
    if not os.path.exists(path):
        print(f"{n:>9,} 行：没有找到 {path}")
        continue
    t0 = time.time()
    df = spark.read.csv("file://" + path, header=True, inferSchema=True)
    result = (df.filter(F.col("status") == "已完成").groupBy("province")
                .agg(F.sum("amount").alias("gmv")).orderBy(F.desc("gmv")).collect())
    t1 = time.time()
    print(f"{n:>9,} 行：合计 {t1 - t0:5.2f} 秒，第一名 {result[0]['province']} {result[0]['gmv']:,.2f}")
spark.stop()
