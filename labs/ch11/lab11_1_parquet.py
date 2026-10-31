"""实验 11-1：CSV 换成 Parquet，比较文件大小和查询耗时；再看看小文件太多会怎样。

先用实验 1-1 生成 300 万行订单（python ch01/lab1_1_data_size.py --max 3000000），再在 spark 容器中运行：
    spark-submit --py-files /labs/ch11/ui.py /labs/ch11/lab11_1_parquet.py
结果：data/scale/orders_parquet/（后面两个实验都用它）
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ui import timed, stats

spark = SparkSession.builder.appName("实验11-1 CSV 与 Parquet").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
CSV = "file:///labs/data/scale/orders_3000000.csv"
PQ = "file:///labs/data/scale/orders_parquet"
SMALL = "file:///labs/data/scale/orders_small_files"
SCHEMA = ("order_id STRING, user_id STRING, product_id STRING, quantity INT, amount DOUBLE, "
          "order_time TIMESTAMP, province STRING, pay_type STRING, status STRING")


def du(path):
    """本地目录或文件的大小（MB）和文件个数（不算 _SUCCESS 和 .crc）"""
    p = path.replace("file://", "")
    files = [os.path.join(p, f) for f in os.listdir(p)] if os.path.isdir(p) else [p]
    files = [f for f in files if not os.path.basename(f).startswith(("_", "."))]
    return sum(os.path.getsize(f) for f in files) / 2**20, len(files)


# ① 读 CSV：让 Spark 自己猜类型（inferSchema）要先把文件完整读一遍，比直接给出类型慢
t1, _ = timed(spark, "csv_infer", lambda: spark.read.csv(CSV, header=True, inferSchema=True).count())
t2, _ = timed(spark, "csv_schema", lambda: spark.read.csv(CSV, header=True, schema=SCHEMA).count())
print(f"① 数行数：CSV + inferSchema {t1:.1f} 秒，CSV + 指定类型 {t2:.1f} 秒")

# ② 转成 Parquet（默认 Snappy 压缩），比较大小
df = spark.read.csv(CSV, header=True, schema=SCHEMA)
df.write.mode("overwrite").parquet(PQ)
(csv_mb, _), (pq_mb, pq_n) = du(CSV), du(PQ)
print(f"② CSV {csv_mb:.1f} MB → Parquet {pq_mb:.1f} MB（{pq_n} 个文件），只有原来的 {pq_mb / csv_mb:.0%}")

# ③ 同一个查询：各省已完成订单的销售额
def by_province(d):
    return lambda: (d.filter(F.col("status") == "已完成").groupBy("province")
                     .agg(F.round(F.sum("amount"), 2).alias("gmv")).orderBy(F.desc("gmv")).collect())

csv_df, pq_df = spark.read.csv(CSV, header=True, schema=SCHEMA), spark.read.parquet(PQ)
t_csv, r1 = timed(spark, "q_csv", by_province(csv_df))
t_pq, r2 = timed(spark, "q_pq", by_province(pq_df))
print(f"③ 各省销售额：CSV {t_csv:.2f} 秒（读入 {stats(spark, 'q_csv')['input_mb']:.1f} MB），"
      f"Parquet {t_pq:.2f} 秒；结果相同：{r1 == r2}")
print("   前 3 名：", [(r["province"], r["gmv"]) for r in r2[:3]])

# ④ 看执行计划：Parquet 只读了 3 列（ReadSchema），过滤条件也推给了文件（PushedFilters）
plan = pq_df.filter(F.col("status") == "已完成").groupBy("province").agg(F.sum("amount"))
print("④ Parquet 扫描的关键信息：")
for line in plan._jdf.queryExecution().executedPlan().toString().split("\n"):
    if "FileScan" in line:
        for key in ("PushedFilters", "ReadSchema"):
            i = line.find(key)
            print("  ", line[i:line.find("]", i) + 1] if key == "PushedFilters" else line[i:])

# 查询只用到 3 列。把这 3 列单独存一份，看看 Parquet 实际只需要读多少
pq_df.select("province", "status", "amount").write.mode("overwrite").parquet(PQ + "_3cols")
print(f"   查询用到的 3 列单独存放只有 {du(PQ + '_3cols')[0]:.1f} MB，这就是 Parquet 要读的量")

# ⑤ 小文件：每个文件最多 1500 行，故意写成 2000 个小文件；再合并成 4 个，比较同一个查询
pq_df.write.option("maxRecordsPerFile", 1500).mode("overwrite").parquet(SMALL)
small_mb, small_n = du(SMALL)
t_small, _ = timed(spark, "q_small", by_province(spark.read.parquet(SMALL)))
print(f"⑤ 2000 个小文件：共 {small_mb:.1f} MB，平均每个 {small_mb / small_n * 1024:.0f} KB，"
      f"查询 {t_small:.2f} 秒，任务数 {stats(spark, 'q_small')['tasks']}")
spark.read.parquet(SMALL).coalesce(4).write.mode("overwrite").parquet(SMALL + "_merged")
m_mb, m_n = du(SMALL + "_merged")
t_merged, _ = timed(spark, "q_merged", by_province(spark.read.parquet(SMALL + "_merged")))
print(f"   合并成 {m_n} 个文件：共 {m_mb:.1f} MB，查询 {t_merged:.2f} 秒，任务数 {stats(spark, 'q_merged')['tasks']}")
spark.stop()
