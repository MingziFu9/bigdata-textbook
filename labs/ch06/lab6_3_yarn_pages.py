"""实验 6-3：把作业提交到 YARN 集群——统计 1000 万行日志中最热门的 10 个页面。
和第 4 章实验 4-3 的 MapReduce 作业做同样的事，比较耗时。

在 spark 容器中运行：
    spark-submit --master yarn --num-executors 2 /labs/ch06/lab6_3_yarn_pages.py
（不写 --master yarn 就在 spark 容器里以本地模式运行，可用来对比。）
"""
import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("实验6-3 热门页面(YARN)").getOrCreate()
sc = spark.sparkContext
print("运行方式：", sc.master, "，应用编号：", sc.applicationId)
t0 = time.time()

lines = sc.textFile("/xiaoxiang/logs_big/access.log")       # 不写 file://，读的是 HDFS
print("分区数（= 第一阶段的任务数）：", lines.getNumPartitions())
top10 = (lines.map(lambda line: line.split())
              .filter(lambda f: len(f) == 8 and f[5] == "200")
              .map(lambda f: (f[4], 1))
              .reduceByKey(lambda a, b: a + b)
              .takeOrdered(10, key=lambda kv: -kv[1]))
for page, n in top10:
    print(f"{page}\t{n}")
print(f"计算用时 {time.time() - t0:.1f} 秒")
spark.stop()
