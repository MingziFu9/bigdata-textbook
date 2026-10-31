"""实验 7-1：清洗小象优选的“脏”订单数据。

先在本机 labs/ 目录下运行 python ch07/make_dirty.py 生成脏数据，再在 spark 容器中运行：
    spark-submit /labs/ch07/lab7_1_clean_orders.py
清洗结果按日期分区存成 Parquet：labs/data/xiaoxiang_clean/orders/
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验7-1 清洗订单").getOrCreate()
DATA = "file:///labs/data"

# ---------- 0. 读入：先全部当字符串读，免得格式不对的值读进来就出错 ----------
raw = spark.read.csv(f"{DATA}/xiaoxiang_dirty/orders_dirty.csv", header=True)
raw.printSchema()
print("① 原始行数", raw.count())

# ---------- 1. 去重：整行完全相同的只留一行 ----------
df = raw.dropDuplicates()
print("② 去掉完全重复的行后", df.count())

# ---------- 2. 统一格式 ----------
df = (df
      .withColumn("user_id", F.lower(F.trim("user_id")))                         # U0071359 → u0071359
      .withColumn("pay_type", F.trim("pay_type"))                                # 去掉前后空格
      .withColumn("province", F.regexp_replace(F.trim("province"), "(省|市|壮族自治区|回族自治区|维吾尔自治区|自治区)$", ""))  # 广东省 → 广东
      .withColumn("amount", F.regexp_replace("amount", "¥", "").try_cast("decimal(10,2)"))  # ¥59.80 → 59.80
      .withColumn("quantity", F.col("quantity").try_cast("int"))
      # 四种时间写法，逐一尝试，哪种能解析就用哪种
      .withColumn("order_time", F.coalesce(
          F.try_to_timestamp("order_time", F.lit("yyyy-MM-dd HH:mm:ss")),
          F.try_to_timestamp("order_time", F.lit("yyyy/M/d HH:mm:ss")),
          F.try_to_timestamp("order_time", F.lit("yyyy-MM-dd'T'HH:mm:ss")),
          F.try_to_timestamp("order_time", F.lit("yyyyMMdd HHmmss")))))
print("③ 时间无法解析的行", df.filter(F.col("order_time").isNull()).count())

# ---------- 3. 缺失值 ----------
print("④ 缺失情况：", {c: df.filter(F.col(c).isNull()).count() for c in ["province", "amount", "pay_type"]})
users = spark.read.csv(f"{DATA}/xiaoxiang/users.csv", header=True).select("user_id", F.col("province").alias("u_province"))
products = spark.read.csv(f"{DATA}/xiaoxiang/products.csv", header=True, inferSchema=True).select("product_id", "price")
df = (df.join(users, "user_id", "left")
        .join(products, "product_id", "left")
        .withColumn("province", F.coalesce("province", "u_province"))                    # 省份：按用户表补
        .withColumn("amount", F.coalesce("amount", (F.col("price") * F.col("quantity")).cast("decimal(10,2)")))  # 金额：单价×数量
        .fillna({"pay_type": "未知"})                                                     # 支付方式：填“未知”
        .drop("u_province", "price"))

# ---------- 4. 异常值：数量、金额必须大于 0，时间必须在 2026 年 9 月 ----------
bad_qty = F.col("quantity") <= 0
bad_amt = F.col("amount") <= 0
bad_time = ~F.col("order_time").between("2026-09-01 00:00:00", "2026-09-30 23:59:59")
for name, cond in [("数量不大于 0", bad_qty), ("金额不大于 0", bad_amt), ("时间不在 9 月", bad_time)]:
    print(f"⑤ 异常：{name}", df.filter(cond).count())
df = df.filter(~(bad_qty | bad_amt | bad_time))

# ---------- 5. 同一个订单号只能出现一次（格式统一后可能又出现重复） ----------
df = df.dropDuplicates(["order_id"])
print("⑥ 清洗后行数", df.count())

# ---------- 6. 保存：按日期分区写成 Parquet ----------
clean = df.withColumn("dt", F.date_format("order_time", "yyyy-MM-dd")).select(
    "order_id", "user_id", "product_id", "quantity", "amount", "order_time", "province", "pay_type", "status", "dt")
clean.write.mode("overwrite").partitionBy("dt").parquet(f"{DATA}/xiaoxiang_clean/orders")
print("已保存到 data/xiaoxiang_clean/orders/")

# ---------- 7. 核对：和生成脏数据之前的原始订单逐行比较 ----------
orig = spark.read.csv(f"{DATA}/xiaoxiang/orders.csv", header=True, inferSchema=True)
check = clean.join(orig.select("order_id", F.col("amount").cast("decimal(10,2)").alias("a0"),
                               F.col("province").alias("p0"), F.col("pay_type").alias("t0")), "order_id")
diff = check.filter((F.col("amount") != F.col("a0")) | (F.col("province") != F.col("p0")) |
                    ((F.col("pay_type") != F.col("t0")) & (F.col("pay_type") != "未知")))
print("⑦ 与原始数据核对：能对上的行", check.count(), "，金额、省份或支付方式不一致的行", diff.count())
clean.groupBy("pay_type").count().orderBy(F.desc("count")).show()
spark.stop()
