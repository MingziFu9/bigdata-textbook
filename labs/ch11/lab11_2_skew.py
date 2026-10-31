"""实验 11-2：一个热点店铺引起的数据倾斜，以及用“加盐”打散。

先完成实验 11-1（生成 data/scale/orders_parquet/），再在 spark 容器中运行：
    spark-submit --py-files /labs/ch11/ui.py /labs/ch11/lab11_2_skew.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ui import timed, slowest_stage

spark = (SparkSession.builder.appName("实验11-2 数据倾斜")
         .config("spark.sql.adaptive.enabled", "false")         # 先关掉自适应执行（AQE），让倾斜原样暴露
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # 假设店铺表很大、不能广播（广播见实验 11-3）
         .config("spark.sql.shuffle.partitions", "200")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
DATA = "file:///labs/data"

# ① 模拟“秒杀日”：头部旗舰店 s001 搞秒杀，一半订单都是它的
orders = spark.read.parquet(f"{DATA}/scale/orders_parquet")
products = spark.read.csv(f"{DATA}/xiaoxiang/products.csv", header=True, inferSchema=True)
flash = (orders.join(F.broadcast(products.select("product_id", "shop_id", "category")), "product_id")
               .withColumn("shop_id", F.when(F.rand(seed=11) < 0.5, "s001").otherwise(F.col("shop_id"))))
flash.cache()
total = flash.count()
cnt = flash.groupBy("shop_id").count()
median = cnt.approxQuantile("count", [0.5], 0)[0]
hot = cnt.orderBy(F.desc("count")).first()
print(f"① 秒杀日 {total} 笔订单，每店订单数的中位数 {median:.0f}；"
      f"{hot['shop_id']} 有 {hot['count']} 笔，占 {hot['count'] / total:.0%}，是中位数的 {hot['count'] / median:.0f} 倍")

# 店铺表：每个店铺一行，佣金比例 3%～8%（用店铺编号的哈希值算出，每次运行都一样）
shops = (products.select("shop_id").distinct()
                 .withColumn("rate", F.lit(0.03) + F.abs(F.hash("shop_id")) % 51 / 1000))


def commission(o, s, keys):
    """关联店铺表，按品类算平台佣金"""
    return lambda: (o.join(s, keys).groupBy("category")
                     .agg(F.round(F.sum(F.col("amount") * F.col("rate")), 2).alias("fee"))
                     .orderBy("category").collect())


def report(tag, label, t):
    s = slowest_stage(spark, label)
    print(f"{tag}{t:.2f} 秒。关联阶段 {s['tasks']} 个任务，每个任务读入的行数：中位数 {s['rows_median']:.0f}，"
          f"最多 {s['rows_max']:.0f}；任务耗时：中位数 {s['time_median_ms']:.0f} 毫秒，最长 {s['time_max_ms']:.0f} 毫秒")


# ② 直接关联：s001 的 150 万笔订单都被送到同一个任务
t1, r1 = timed(spark, "plain", commission(flash, shops, "shop_id"))
report("② 直接关联：", "plain", t1)

# ③ 加盐：订单随机加一个 0～19 的盐；店铺表每行复制 20 份，盐分别是 0～19；按（店铺, 盐）关联
N = 20
salted_orders = flash.withColumn("salt", (F.rand(seed=7) * N).cast("int"))
salted_shops = shops.withColumn("salt", F.explode(F.sequence(F.lit(0), F.lit(N - 1))))
t2, r2 = timed(spark, "salted", commission(salted_orders, salted_shops, ["shop_id", "salt"]))
report("③ 加盐以后：", "salted", t2)

# ④ 不加盐，打开 AQE（Spark 4 默认是打开的），看看它做了什么
spark.conf.set("spark.sql.adaptive.enabled", "true")
t3, r3 = timed(spark, "aqe", commission(flash, shops, "shop_id"))
report("④ 打开 AQE：", "aqe", t3)

# 浮点数相加的先后顺序不同，最后一位可能差几分钱，所以按“相差不到 1 元”比较
same = all(abs(a["fee"] - b["fee"]) < 1 and abs(a["fee"] - c["fee"]) < 1 for a, b, c in zip(r1, r2, r3))
print(f"⑤ 三种做法的结果相同：{same}")
for r in r1[:3]:
    print(f"   {r['category']}：佣金 {r['fee']} 元")
spark.stop()
