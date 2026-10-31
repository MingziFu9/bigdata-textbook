"""实验 9-3：水位线设成多少合适？比较不同水位线下每分钟成交额与标准答案（在 spark 容器中运行）。

先在本机发一遍带迟到订单的数据（只需一次）：
    python ch09/order_producer.py --late 0.05 --topic orders_late
再在 spark 容器里分别用三种水位线运行：
    spark-submit --jars $KAFKA_JARS ch09/lab9_3_late.py "10 seconds"
    spark-submit --jars $KAFKA_JARS ch09/lab9_3_late.py "2 minutes"
    spark-submit --jars $KAFKA_JARS ch09/lab9_3_late.py "5 minutes"
程序从头“重放”主题里的消息，每批只取 60 条，按发送顺序模拟数据陆续到达；处理完自动停止。
"""
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType

WATERMARK = sys.argv[1] if len(sys.argv) > 1 else "2 minutes"

spark = SparkSession.builder.appName("lab9_3_late").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 4)

schema = (StructType()
          .add("order_id", StringType()).add("user_id", StringType())
          .add("product_id", StringType()).add("category", StringType())
          .add("quantity", IntegerType()).add("amount", DecimalType(10, 2))
          .add("province", StringType()).add("pay_type", StringType())
          .add("event_time", StringType()))


def parse(df):
    return (df.select(F.from_json(F.col("value").cast("string"), schema).alias("o"))
              .select("o.*").withColumn("event_time", F.to_timestamp("event_time")))


def per_minute(df):
    return (df.groupBy(F.window("event_time", "1 minute"))
              .agg(F.count("*").alias("orders"), F.sum("amount").alias("gmv"))
              .select(F.date_format("window.start", "HH:mm").alias("minute"), "orders", "gmv"))


kafka = {"kafka.bootstrap.servers": "kafka:9092", "subscribe": "orders_late", "startingOffsets": "earliest"}

# 标准答案：把主题里的全部消息当成一张普通的表（批处理）一次算完
truth = {r.minute: (r.orders, r.gmv) for r in per_minute(parse(spark.read.format("kafka").options(**kafka).load())).collect()}

# 流处理：同样的消息分批到达，每批 60 条
stream = parse(spark.readStream.format("kafka").options(**kafka).option("maxOffsetsPerTrigger", 60).load())
result = {}                                   # 每个窗口最后一次输出的值


def keep(batch_df, batch_id):
    for r in batch_df.collect():
        result[r.minute] = (r.orders, r.gmv)


query = (per_minute(stream.withWatermark("event_time", WATERMARK))
         .writeStream.outputMode("update")
         .foreachBatch(keep)
         .trigger(availableNow=True)          # 把现有的消息分批处理完就停止
         .start())
query.awaitTermination()

ops = [p["stateOperators"][0] for p in query.recentProgress if p["stateOperators"]]
print(f"\n水位线 {WATERMARK}：共处理 {query.lastProgress['batchId'] + 1} 批，"
      f"状态中最多同时保存 {max(o['numRowsTotal'] for o in ops)} 个窗口")
print("分钟    标准答案(笔)  流计算(笔)  少算(笔)   标准答案(元)   流计算(元)")
lost = 0
for m in sorted(truth):
    t, s = truth[m], result.get(m, (0, 0))
    lost += t[0] - s[0]
    print(f"{m}   {t[0]:>8}  {s[0]:>10}  {t[0] - s[0]:>7}   {t[1]:>12}  {s[1]:>12}")
print(f"合计少算 {lost} 笔")
