"""第 10 章共用：从订单宽表做出“每个用户一行”的特征表。

实验 10-1、10-3 都用它，运行时用 --py-files 带上：
    spark-submit --py-files /labs/ch10/features.py /labs/ch10/lab10_1_repurchase.py
"""
from pyspark.sql import functions as F

DATA = "file:///labs/data"


def user_features(spark, start, cut):
    """用 [start, cut) 这段时间的已完成订单，给每个下过单的用户算特征。

    R   最近一次购买距 cut 几天      F   购买次数         M   购买金额
    avg_amount 平均每单金额          categories 买过几个品类
    days_since_first 第一次购买距 cut 几天
    gender、age、level 来自用户表（宽表里已经有）
    """
    wide = (spark.read.parquet(f"{DATA}/xiaoxiang_clean/order_wide")
                 .filter((F.col("status") == "已完成")
                         & (F.col("order_time") >= F.lit(start).cast("timestamp"))
                         & (F.col("order_time") < F.lit(cut).cast("timestamp"))))
    return (wide.groupBy("user_id", "gender", "age", "level")
                .agg(F.datediff(F.lit(cut), F.max("order_time")).alias("R"),
                     F.count("*").alias("F"),
                     F.sum("amount").cast("double").alias("M"),
                     F.countDistinct("category").alias("categories"),
                     F.datediff(F.lit(cut), F.min("order_time")).alias("days_since_first"))
                .withColumn("avg_amount", F.round(F.col("M") / F.col("F"), 2)))


def buyers(spark, start, end):
    """[start, end) 这段时间下过已完成订单的用户，带一列 label=1。"""
    return (spark.read.parquet(f"{DATA}/xiaoxiang_clean/orders")
                 .filter((F.col("status") == "已完成")
                         & (F.col("order_time") >= F.lit(start).cast("timestamp"))
                         & (F.col("order_time") < F.lit(end).cast("timestamp")))
                 .select("user_id").distinct().withColumn("label", F.lit(1.0)))
