"""实验 6-2：用 RDD 统计小象优选的访问日志（每小时 PV、UV，热门页面前 10）。
和第 4 章实验 4-2 的 MapReduce 程序做同样的事，对照结果和代码量。

在 spark 容器中运行（本地模式）：
    spark-submit /labs/ch06/lab6_2_log_rdd.py
日志每行 8 个字段：时间 IP 用户 请求方法 页面 状态码 字节数 终端
"""
import time

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("实验6-2 用RDD统计访问日志").getOrCreate()
sc = spark.sparkContext
t0 = time.time()

# 读文件：每行是 RDD 的一个元素。file:// 表示容器里的本地文件，不加则读 HDFS
lines = sc.textFile("file:///labs/data/xiaoxiang/access.log")
fields = lines.map(lambda line: line.split()).filter(lambda f: len(f) == 8)
fields.cache()                     # 下面要用三次，先缓存在内存里

# ① 每小时 PV：(小时, 1) → 按小时求和
pv = fields.map(lambda f: (f[0][11:13], 1)).reduceByKey(lambda a, b: a + b)

# ② 每小时 UV：只要登录用户，(小时, 用户) 去重后再按小时计数
uv = (fields.filter(lambda f: f[2] != "-")
            .map(lambda f: (f[0][11:13], f[2]))
            .distinct()
            .map(lambda kv: (kv[0], 1))
            .reduceByKey(lambda a, b: a + b))

# ③ 热门页面：只要状态码 200 的访问，(页面, 1) 求和后按次数从大到小取前 10
top10 = (fields.filter(lambda f: f[5] == "200")
               .map(lambda f: (f[4], 1))
               .reduceByKey(lambda a, b: a + b)
               .sortBy(lambda kv: kv[1], ascending=False)
               .take(10))

print("小时  PV     UV")
for hour, (p, u) in sorted(pv.join(uv).collect()):      # join：按小时把 PV 和 UV 拼在一起
    print(f"{hour}\t{p}\t{u}")
print("热门页面前 10：")
for page, n in top10:
    print(f"{page}\t{n}")
print(f"日志总行数 {fields.count()}，用时 {time.time() - t0:.1f} 秒")
spark.stop()
