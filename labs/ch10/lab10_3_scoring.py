"""实验 10-3：加载保存的模型，给全部老用户打分，输出营销名单。

先完成实验 10-1（保存 data/models/repurchase/）和实验 10-2（生成 data/xiaoxiang_clean/user_cluster/），
再在 spark 容器中运行：
    spark-submit --py-files /labs/ch10/features.py /labs/ch10/lab10_3_scoring.py
结果：data/xiaoxiang_clean/marketing_list/（一个 CSV 文件）
"""
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

from features import user_features, DATA

spark = SparkSession.builder.appName("实验10-3 批量打分").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ① 加载实验 10-1 保存的整条流水线（编号、独热、标准化、逻辑回归都在里面）
model = PipelineModel.load(f"{DATA}/models/repurchase")
print("① 加载模型：", [type(s).__name__ for s in model.stages])

# ② 用 9 月整月的订单算特征。注意：特征的算法必须和训练时一模一样，
#    只是时间往后挪了一周（训练时截止 9 月 24 日，现在截止 10 月 1 日），预测的是 10 月 1～7 日
users = user_features(spark, "2026-09-01", "2026-10-01")
print(f"② 待打分的老用户 {users.count()} 个")

# ③ 批量打分：transform 一下就行。probability 是向量，取第 2 个数（复购的概率）
t0 = time.time()
scored = (model.transform(users)
               .withColumn("p_buy", F.round(vector_to_array("probability")[1], 4))
               .select("user_id", "R", "F", "M", "p_buy"))
scored.cache()
print(f"③ 打分完成，{scored.count()} 人，用时 {time.time() - t0:.1f} 秒")
scored.select("p_buy").summary("min", "25%", "50%", "75%", "max").show()

# ④ 按概率分 5 档看人数，挑出前 20% 作为营销名单
q80 = scored.approxQuantile("p_buy", [0.8], 0.001)[0]
print(f"④ 前 20% 的分数线：p_buy ≥ {q80:.4f}")
cluster = spark.read.parquet(f"{DATA}/xiaoxiang_clean/user_cluster").select("user_id", "cluster_name")
rfm = spark.read.parquet(f"{DATA}/xiaoxiang_clean/rfm").select("user_id", "segment")
target = (scored.filter(F.col("p_buy") >= q80)
                .join(cluster, "user_id", "left")
                .join(rfm, "user_id", "left")
                .orderBy(F.desc("p_buy")))
print(f"   营销名单 {target.count()} 人，前 10 名：")
target.show(10, truncate=False)

# ⑤ 名单里都是哪些人：按实验 10-2 的分群统计
print("⑤ 名单按 K-Means 分群统计")
target.groupBy("cluster_name").agg(F.count("*").alias("users"),
                                   F.round(F.avg("p_buy"), 3).alias("avg_p")).orderBy(F.desc("users")).show()

# ⑥ 写成一个 CSV 文件交给运营（coalesce(1) 合成一个文件；只放用户编号和需要的列）
(target.select("user_id", "p_buy", "cluster_name", "segment", "R", "F", "M")
       .coalesce(1).write.mode("overwrite").option("header", True)
       .csv(f"{DATA}/xiaoxiang_clean/marketing_list"))
print("⑥ 营销名单已保存到 data/xiaoxiang_clean/marketing_list/")
spark.stop()
