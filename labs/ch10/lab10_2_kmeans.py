"""实验 10-2：用 K-Means 给用户分群。

先完成实验 8-2（生成 data/xiaoxiang_clean/rfm/），再在 spark 容器中运行：
    spark-submit /labs/ch10/lab10_2_kmeans.py
结果：data/xiaoxiang_clean/user_cluster/（每个用户一行，带群号和群名）
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator

spark = SparkSession.builder.appName("实验10-2 K-Means 分群").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
DATA = "file:///labs/data"

# ① 读实验 8-2 的 RFM 结果。F、M 少数人特别大，先取对数把“长尾”压短
rfm = (spark.read.parquet(f"{DATA}/xiaoxiang_clean/rfm")
            .select("user_id", "R", "F", F.col("M").cast("double").alias("M"), "segment")
            .withColumn("logF", F.log1p("F"))
            .withColumn("logM", F.log1p("M")))
rfm.cache()
print("① 用户数", rfm.count())
rfm.select("R", "F", "M").summary("min", "25%", "50%", "75%", "max").show()

# ② 组装特征并标准化（R 是天数、M 是元，单位不同，不标准化的话 M 一个人说了算）
assembler = VectorAssembler(inputCols=["R", "logF", "logM"], outputCol="raw")
scaler = StandardScaler(inputCol="raw", outputCol="features", withMean=True)
prep = Pipeline(stages=[assembler, scaler]).fit(rfm)
X = prep.transform(rfm)
X.cache()

# ③ 试 k = 2～8，看轮廓系数（越接近 1 分得越开）
evaluator = ClusteringEvaluator(featuresCol="features")
for k in range(2, 9):
    m = KMeans(k=k, seed=1, featuresCol="features").fit(X)
    print(f"③ k = {k}  轮廓系数 {evaluator.evaluate(m.transform(X)):.3f}  "
          f"簇内距离平方和 {m.summary.trainingCost:.0f}")

# ④ 结合业务选 k = 4（运营一般只想管理三五类人），看每一群的平均 R、F、M
K = 4
model = KMeans(k=K, seed=1, featuresCol="features").fit(X)
out = model.transform(X)
total_m = rfm.agg(F.sum("M")).first()[0]
profile = (out.groupBy("prediction")
              .agg(F.count("*").alias("users"),
                   F.round(F.avg("R"), 1).alias("avg_R"),
                   F.round(F.avg("F"), 2).alias("avg_F"),
                   F.round(F.avg("M"), 0).alias("avg_M"),
                   F.round(F.sum("M") / total_m * 100, 1).alias("m_pct"))
              .orderBy("prediction"))
print(f"④ k = {K} 时各群的画像")
profile.show()

# ⑤ 给每一群起名字：看上面的画像，按“最近买没买、买得多不多”来起。
#    群号是随机的，换了数据或 seed 要重新对照画像改这张表
NAMES = {0: "忠实大客户", 1: "沉睡客户", 2: "小额尝鲜客", 3: "近期活跃客户"}
names = F.create_map(*[F.lit(x) for kv in NAMES.items() for x in kv])
out = out.withColumn("cluster_name", names[F.col("prediction")])

# ⑥ 和实验 8-2 的 RFM 八类对照：每一群主要由哪几类人组成
print("⑥ K-Means 群与 RFM 分类的对照（人数）")
(out.groupBy("cluster_name").pivot("segment").count().fillna(0)
    .orderBy("cluster_name").show(truncate=False))

(out.select("user_id", "R", "F", "M", F.col("prediction").alias("cluster"), "cluster_name")
    .write.mode("overwrite").parquet(f"{DATA}/xiaoxiang_clean/user_cluster"))
print("⑦ 分群结果已保存到 data/xiaoxiang_clean/user_cluster/")
spark.stop()
