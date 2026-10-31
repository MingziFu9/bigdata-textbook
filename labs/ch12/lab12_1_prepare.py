"""实验 12-1 第 1 步：用 Spark 算好报告要用的 5 张小表，存成 CSV 交给 pyecharts 画图。

先完成第 7 章的实验（生成 data/xiaoxiang_clean/），再在 spark 容器中运行：
    spark-submit /labs/ch12/lab12_1_prepare.py
结果：data/report/ 下 5 个目录，每个目录里一个 CSV 文件
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验12-1 报告数据").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
DATA = "file:///labs/data"
OUT = f"{DATA}/report"

wide = spark.read.parquet(f"{DATA}/xiaoxiang_clean/order_wide").filter(F.col("status") == "已完成")
logs = spark.read.parquet(f"{DATA}/xiaoxiang_clean/access_log")


def save(df, name):
    """结果很小，合成一个 CSV 文件，方便本机的 pandas 读取"""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{OUT}/{name}")
    print(f"{name}：{df.count()} 行")
    df.show(5, truncate=False)


# ① 比较：各品类销售额
save(wide.groupBy("category").agg(F.round(F.sum("amount") / 1e4, 1).alias("gmv_wan"))
         .orderBy(F.desc("gmv_wan")), "category")
# ② 趋势：每天的销售额和订单数
save(wide.groupBy("dt").agg(F.round(F.sum("amount") / 1e4, 1).alias("gmv_wan"), F.count("*").alias("orders"))
         .orderBy("dt"), "daily")
# ③ 地区：各省销售额
save(wide.groupBy("province").agg(F.round(F.sum("amount") / 1e4, 1).alias("gmv_wan"))
         .orderBy(F.desc("gmv_wan")), "province")
# ④ 构成：各支付方式的订单数
save(wide.groupBy("pay_type").count().orderBy(F.desc("count")), "pay_type")
# ⑤ 漏斗：9 月 30 日登录用户 看商品 → 加购物车 → 提交订单 → 支付，每一步只算上一步留下来的人
ok = logs.filter(F.col("user_id").isNotNull() & (F.col("status") == 200))
step_urls = [("浏览商品", "/item/%"), ("加入购物车", "/cart"), ("提交订单", "/order/submit"), ("完成支付", "/pay")]
users, rows = None, []
for name, pattern in step_urls:
    u = ok.filter(F.col("url").like(pattern)).select("user_id").distinct()
    users = u if users is None else users.join(u, "user_id")
    rows.append((name, users.count()))
save(spark.createDataFrame(rows, "step string, users long"), "funnel")
spark.stop()
