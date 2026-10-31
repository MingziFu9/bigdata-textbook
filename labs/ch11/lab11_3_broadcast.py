"""实验 11-3：广播连接。比较打开和关闭广播时，大表关联小表的耗时和 Shuffle 量。

先完成实验 11-1（生成 data/scale/orders_parquet/），再在 spark 容器中运行：
    spark-submit --py-files /labs/ch11/ui.py /labs/ch11/lab11_3_broadcast.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ui import timed, stats

spark = SparkSession.builder.appName("实验11-3 广播连接").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
DATA = "file:///labs/data"

orders = spark.read.parquet(f"{DATA}/scale/orders_parquet")                       # 大表：300 万行
products = spark.read.csv(f"{DATA}/xiaoxiang/products.csv", header=True, inferSchema=True)   # 小表：2000 行
print(f"① 订单 {orders.count()} 行，商品 {products.count()} 行；"
      f"自动广播的门槛 spark.sql.autoBroadcastJoinThreshold = {spark.conf.get('spark.sql.autoBroadcastJoinThreshold')}")

q = lambda: (orders.join(products, "product_id").groupBy("category")
                   .agg(F.round(F.sum("amount"), 2).alias("gmv")).orderBy(F.desc("gmv")))


def join_type():
    """从执行计划里找出用的是哪种关联"""
    plan = q()._jdf.queryExecution().executedPlan().toString()
    return next(k for k in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin") if k in plan)


def run(tag, label):
    t, rows = timed(spark, label, lambda: q().collect())
    s = stats(spark, label)
    print(f"{tag}{join_type()}，{t:.2f} 秒，Shuffle 写出 {s['shuffle_mb']:.1f} MB，任务 {s['tasks']} 个")
    return rows


# ② 默认：商品表很小，Spark 自动把它广播到每个执行器，订单表不用 Shuffle
r1 = run("② 默认（自动广播）：", "auto")

# ③ 关闭自动广播（门槛设为 -1），只能两边都按 product_id Shuffle 再排序合并
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
r2 = run("③ 关闭广播：", "off")

# ④ 自动广播关着，但在代码里用 F.broadcast() 明确要求广播小表
q = lambda: (orders.join(F.broadcast(products), "product_id").groupBy("category")
                   .agg(F.round(F.sum("amount"), 2).alias("gmv")).orderBy(F.desc("gmv")))
r3 = run("④ 用 broadcast() 指定：", "hint")

# 浮点数相加的先后顺序不同，最后一位可能差几分钱，所以按“相差不到 1 元”比较
same = all(a["category"] == b["category"] == c["category"] and abs(a["gmv"] - b["gmv"]) < 1
           and abs(a["gmv"] - c["gmv"]) < 1 for a, b, c in zip(r1, r2, r3))
print(f"⑤ 三种做法的结果相同：{same}")
for r in r1[:3]:
    print(f"   {r['category']}：{r['gmv']}")
spark.stop()
