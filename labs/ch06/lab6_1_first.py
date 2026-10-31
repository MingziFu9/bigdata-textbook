"""实验 6-1：第一个 Spark 程序——体会转换和行动、惰性执行和缓存。

在 spark 容器中运行：
    spark-submit /labs/ch06/lab6_1_first.py
程序结束前会暂停 60 秒，这时在浏览器打开 http://localhost:4040 查看 Spark UI。
"""
import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("实验6-1 第一个Spark程序").getOrCreate()
sc = spark.sparkContext
print("Spark 版本", spark.version, "，运行方式", sc.master, "，默认并行度", sc.defaultParallelism)

# 1. 转换（transformation）：只记下“要做什么”，并不马上执行
t = time.time()
lines = sc.textFile("file:///labs/data/xiaoxiang/access.log")
pages = lines.map(lambda line: line.split()[4])          # 取第 5 个字段：页面
counts = pages.map(lambda p: (p, 1)).reduceByKey(lambda a, b: a + b)
print(f"定义三个转换用时 {time.time() - t:.3f} 秒（还没有读文件）")

# 2. 行动（action）：要结果了，Spark 才真正读文件、计算
t = time.time()
print("不同页面的个数：", counts.count())
print(f"第一次 count 用时 {time.time() - t:.1f} 秒")

print("访问最多的 3 个页面：", counts.takeOrdered(3, key=lambda kv: -kv[1]))

# 3. 缓存：把 pages 留在内存里，后面再用就不必重新读文件
pages.cache()
t = time.time(); n1 = pages.count(); a = time.time() - t       # 这一次读文件并放进缓存
t = time.time(); n2 = pages.count(); b = time.time() - t       # 这一次直接用缓存
print(f"pages 共 {n1} 行：缓存前 count 用时 {a:.1f} 秒，缓存后 {b:.2f} 秒")

print("现在打开 http://localhost:4040 查看 Jobs 和 Stages，60 秒后程序结束……")
time.sleep(60)
spark.stop()
