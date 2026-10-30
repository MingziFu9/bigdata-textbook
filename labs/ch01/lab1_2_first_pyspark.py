"""实验 1-2：第一次运行 PySpark，统计小象优选各省的订单量和销售额。

在 labs/ 目录下运行（先运行 python data/make_data.py 生成数据）：
    python ch01/lab1_2_first_pyspark.py
需要 Java 17 或 21，以及 pip install pyspark==4.1.4。运行期间可以打开 http://localhost:4040 查看 Spark 的网页。
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

t0 = time.time()
spark = (SparkSession.builder
         .appName("小象优选-各省订单统计")
         .master("local[*]")                        # 本地模式：用本机所有 CPU 核模拟一个小集群
         .config("spark.ui.showConsoleProgress", "false")   # 不显示进度条
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")              # 只显示错误信息，屏幕清爽一些
print(f"Spark {spark.version} 启动完成，用时 {time.time() - t0:.1f} 秒，"
      f"并行度 {spark.sparkContext.defaultParallelism}")

orders = spark.read.csv("data/xiaoxiang/orders.csv", header=True, inferSchema=True)
print(f"订单表共 {orders.count():,} 行，字段：{orders.columns}")

t = time.time()
result = (orders.filter(F.col("status") == "已完成")         # 只统计已完成的订单
          .groupBy("province")                               # 按省份分组
          .agg(F.count("*").alias("订单数"),
               F.sum("amount").cast("decimal(14,2)").alias("销售额"))
          .orderBy(F.desc("销售额")))
result.show(10)
print(f"统计用时 {time.time() - t:.1f} 秒，共 {result.count()} 个省份")

spark.stop()
