"""实验 8-2：RFM 用户价值分层。

先完成实验 7-1（生成 data/xiaoxiang_clean/orders/），再在 spark 容器中运行：
    spark-submit /labs/ch08/lab8_2_rfm.py
结果：data/xiaoxiang_clean/rfm/（每个用户一行，Parquet）
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验8-2 RFM").getOrCreate()
DATA = "file:///labs/data"
TODAY = "2026-10-01"          # 分析日：9 月数据统计完的第二天

done = spark.read.parquet(f"{DATA}/xiaoxiang_clean/orders").filter(F.col("status") == "已完成")

# ① 每个用户的 R、F、M
rfm = (done.groupBy("user_id")
           .agg(F.datediff(F.lit(TODAY), F.max("order_time")).alias("R"),   # 最近一次购买距今几天
                F.count("*").alias("F"),                                     # 购买次数
                F.round(F.sum("amount"), 2).alias("M")))                     # 购买金额
print("① 用户数", rfm.count())
rfm.orderBy("user_id").show(5)
rfm.describe("R", "F", "M").show()

# ② 和平均值比高低：R 越小越好（最近买过），F、M 越大越好
avg = rfm.agg(F.avg("R"), F.avg("F"), F.avg("M")).first()
print(f"② 平均值：R {avg[0]:.1f} 天，F {avg[1]:.2f} 次，M {avg[2]:.2f} 元")
# 注意：Spark 的列名不区分大小写，新列不能叫 r、f、m，否则会覆盖 R、F、M
rfm = (rfm.withColumn("R_level", F.when(F.col("R") <= avg[0], "高").otherwise("低"))
          .withColumn("F_level", F.when(F.col("F") >= avg[1], "高").otherwise("低"))
          .withColumn("M_level", F.when(F.col("M") >= avg[2], "高").otherwise("低")))

# ③ 按 R、F、M 的高低组合分成 8 类
SEGMENTS = {
    "高高高": "重要价值用户", "低高高": "重要唤回用户", "高低高": "重要发展用户", "低低高": "重要挽留用户",
    "高高低": "一般价值用户", "低高低": "一般唤回用户", "高低低": "一般发展用户", "低低低": "一般挽留用户",
}
mapping = F.create_map(*[F.lit(x) for kv in SEGMENTS.items() for x in kv])
rfm = rfm.withColumn("segment", mapping[F.concat("R_level", "F_level", "M_level")])

total_m = rfm.agg(F.sum("M")).first()[0]
print("③ 各类用户")
(rfm.groupBy("segment", "R_level", "F_level", "M_level")
    .agg(F.count("*").alias("users"), F.round(F.avg("R"), 1).alias("avg_R"),
         F.round(F.avg("F"), 2).alias("avg_F"), F.round(F.avg("M"), 0).alias("avg_M"),
         F.round(F.sum("M") / total_m * 100, 1).alias("m_pct"))
    .orderBy(F.desc("m_pct")).show(truncate=False))

rfm.write.mode("overwrite").parquet(f"{DATA}/xiaoxiang_clean/rfm")
print("已保存到 data/xiaoxiang_clean/rfm/")
spark.stop()
