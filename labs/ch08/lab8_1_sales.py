"""实验 8-1：小象优选 9 月销售多维分析。

先完成实验 7-1、7-3（生成 data/xiaoxiang_clean/order_wide/），再在 spark 容器中运行：
    spark-submit /labs/ch08/lab8_1_sales.py
"""
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验8-1 销售多维分析").getOrCreate()
DATA = "file:///labs/data"

wide = spark.read.parquet(f"{DATA}/xiaoxiang_clean/order_wide")
done = wide.filter(F.col("status") == "已完成").cache()        # 销售额只算已完成的订单
done.createOrReplaceTempView("orders")                        # 注册成临时表，下面可以写 SQL

# ① 整体指标
total = done.agg(F.sum("amount").alias("gmv"), F.count("*").alias("orders"),
                 F.countDistinct("user_id").alias("buyers")).first()
repeat = done.groupBy("user_id").count().filter("count >= 2").count()
print(f"① 销售额 {total.gmv:,.2f} 元，订单 {total.orders} 笔，下单用户 {total.buyers} 人")
print(f"   客单价 {total.gmv / total.buyers:,.2f} 元，笔单价 {total.gmv / total.orders:,.2f} 元，"
      f"复购率 {repeat / total.buyers:.1%}（买了 2 次及以上的 {repeat} 人）")

# ② 按品类：同一个问题，DataFrame 写法和 SQL 写法
by_cat = (done.groupBy("category")
              .agg(F.sum("amount").alias("gmv"), F.count("*").alias("orders"),
                   F.countDistinct("user_id").alias("buyers"))
              .withColumn("per_buyer", F.round(F.col("gmv") / F.col("buyers"), 2))
              .orderBy(F.desc("gmv")))
print("② 按品类（DataFrame 写法）"); by_cat.show()
print("② 按品类（SQL 写法）")
spark.sql("""
    SELECT category, SUM(amount) AS gmv, COUNT(*) AS orders, COUNT(DISTINCT user_id) AS buyers,
           ROUND(SUM(amount) / COUNT(DISTINCT user_id), 2) AS per_buyer
    FROM orders GROUP BY category ORDER BY gmv DESC
""").show()

# ③ 透视表：行是会员等级，列是品类，格子里是销售额（万元）
print("③ 会员等级 × 品类 销售额（万元）")
LEVELS = ["普通", "白银", "黄金", "钻石"]
(done.groupBy("level").pivot("category").agg(F.round(F.sum("amount") / 10000, 1))
     .orderBy(F.array_position(F.array(*[F.lit(x) for x in LEVELS]), F.col("level")))   # 按等级从低到高排
     .show(truncate=False))

# ④ 窗口函数：每个省份销售额前 3 的品类
w_rank = Window.partitionBy("province").orderBy(F.desc("gmv"))
top3 = (done.groupBy("province", "category").agg(F.sum("amount").alias("gmv"))
            .withColumn("rank", F.row_number().over(w_rank))
            .filter("rank <= 3"))
print("④ 广东、浙江销售额前 3 的品类")
top3.filter(F.col("province").isin("广东", "浙江")).orderBy("province", "rank").show()

# ⑤ 窗口函数：每日销售额、环比、累计、7 日移动平均
w_day = Window.orderBy("dt")
daily = (done.groupBy("dt").agg(F.round(F.sum("amount"), 2).alias("gmv"))
             .withColumn("prev", F.lag("gmv", 1).over(w_day))
             .withColumn("dod", F.round((F.col("gmv") - F.col("prev")) / F.col("prev") * 100, 1))
             .withColumn("cum", F.round(F.sum("gmv").over(w_day.rowsBetween(Window.unboundedPreceding, 0)), 2))
             .withColumn("ma7", F.round(F.avg("gmv").over(w_day.rowsBetween(-6, 0)), 2))
             .drop("prev"))
print("⑤ 每日销售额（dod：环比 %；cum：累计；ma7：7 日移动平均）")
daily.orderBy("dt").show(30)

# ⑥ 转化率：9 月 30 日访问过的登录用户中，有多少当天下了单（含未完成的订单）
logs = spark.read.parquet(f"{DATA}/xiaoxiang_clean/access_log")
visitors = logs.filter(F.col("user_id").isNotNull()).select("user_id").distinct()
buyers = wide.filter(F.col("dt") == "2026-09-30").select("user_id").distinct()
n_v, n_b = visitors.count(), visitors.join(buyers, "user_id").count()
print(f"⑥ 9 月 30 日：访问的登录用户 {n_v} 人，其中当天下单 {n_b} 人，转化率 {n_b / n_v:.2%}")
spark.stop()
