"""实验 9-2：用 Structured Streaming 实时统计每分钟的成交额和订单数（在 spark 容器中运行）。

运行（spark 容器里，labs 目录下）：
    spark-submit --jars $KAFKA_JARS ch09/lab9_2_stream.py
（KAFKA_JARS 是 docker-compose.spark.yml 中设好的环境变量，列出了 labs/jars/ 下的 4 个 jar）
然后在本机另开一个窗口运行 python ch09/order_producer.py 发订单。按 Ctrl + C 停止。
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DecimalType

spark = SparkSession.builder.appName("lab9_2_stream").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.shuffle.partitions", 4)   # 每批数据很少，分区少一些更快

# 订单消息的结构（和 order_producer.py 发出的 JSON 对应）
schema = (StructType()
          .add("order_id", StringType()).add("user_id", StringType())
          .add("product_id", StringType()).add("category", StringType())
          .add("quantity", IntegerType()).add("amount", DecimalType(10, 2))
          .add("province", StringType()).add("pay_type", StringType())
          .add("event_time", StringType()))

# ① 读：订阅 Kafka 主题 orders，得到一张“不断变长的表”
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "kafka:9092")
       .option("subscribe", "orders")
       .option("startingOffsets", "latest")          # 只处理启动之后新来的消息
       .load())

# ② 解析：value 列是字节，转成字符串后按 JSON 拆成各列
orders = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("o"))
             .select("o.*")
             .withColumn("event_time", F.to_timestamp("event_time")))

# ③ 统计：按下单时间每 1 分钟一个窗口，求成交额和订单数
per_minute = (orders
              .withWatermark("event_time", "1 minute")   # 最多等迟到 1 分钟的数据
              .groupBy(F.window("event_time", "1 minute"))
              .agg(F.count("*").alias("orders"), F.sum("amount").alias("gmv"))
              .select(F.date_format("window.start", "HH:mm").alias("minute"), "orders", "gmv"))

# ④ 输出：每 10 秒算一批，把有变化的窗口打印出来
query = (per_minute.writeStream
         .outputMode("update")                 # 只输出这一批中有变化的窗口
         .format("console")
         .option("truncate", False)
         .option("checkpointLocation", "/xiaoxiang/checkpoint/lab9_2")   # 检查点（在 HDFS 上）：记下读到哪里、算到哪里
         .trigger(processingTime="10 seconds")
         .start())
try:
    query.awaitTermination()
except (KeyboardInterrupt, Exception):          # 按 Ctrl + C：停止查询，检查点里记下的进度不会丢
    query.stop()
    print("查询已停止，下次启动从检查点接着算")
