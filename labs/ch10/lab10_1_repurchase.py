"""实验 10-1：预测用户下周会不会复购（Pipeline + 逻辑回归）。

先完成实验 7-3（生成 data/xiaoxiang_clean/order_wide/），再在 spark 容器中运行：
    spark-submit --py-files /labs/ch10/features.py /labs/ch10/lab10_1_repurchase.py
结果：模型保存到 data/models/repurchase/
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator

from features import user_features, buyers, DATA

spark = SparkSession.builder.appName("实验10-1 复购预测").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ① 做样本：用 9 月 1～23 日的订单算特征，看 9 月 24～30 日这 7 天有没有再买（label）
feat = user_features(spark, "2026-09-01", "2026-09-24")
data = (feat.join(buyers(spark, "2026-09-24", "2026-10-01"), "user_id", "left")
            .fillna({"label": 0.0}))
data.cache()
n, pos = data.count(), data.filter("label = 1").count()
print(f"① 样本 {n} 个用户，其中下周复购 {pos} 个，占 {pos / n:.1%}")
data.select("user_id", "gender", "age", "level", "R", "F", "M", "categories", "label").show(5)

# ② 按 8:2 分成训练集和测试集
train, test = data.randomSplit([0.8, 0.2], seed=10)
print(f"② 训练集 {train.count()} 个，测试集 {test.count()} 个")

# ③ 搭流水线：文字列编号 → 独热编码 → 拼成特征向量 → 标准化 → 逻辑回归
indexer = StringIndexer(inputCols=["gender", "level"], outputCols=["gender_idx", "level_idx"])
encoder = OneHotEncoder(inputCols=["gender_idx", "level_idx"], outputCols=["gender_vec", "level_vec"])
assembler = VectorAssembler(
    inputCols=["R", "F", "M", "avg_amount", "categories", "days_since_first", "age",
               "gender_vec", "level_vec"],
    outputCol="raw_features")
scaler = StandardScaler(inputCol="raw_features", outputCol="features")
lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=50)
pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler, lr])
model = pipeline.fit(train)
print("③ 流水线的 5 个步骤：", [type(s).__name__ for s in model.stages])

# ④ 在测试集上评估
pred = model.transform(test)
pred.select("user_id", "R", "F", "label", "probability", "prediction").show(5, truncate=False)
auc = BinaryClassificationEvaluator(labelCol="label").evaluate(pred)     # 默认指标就是 AUC
acc = pred.filter("label = prediction").count() / pred.count()
print(f"④ AUC = {auc:.3f}，准确率 = {acc:.1%}")
print("   混淆矩阵（行是真实，列是预测）：")
pred.groupBy("label").pivot("prediction", [0.0, 1.0]).count().orderBy("label").show()

# ⑤ 换个用法：按复购概率从高到低排，看前 20% 的用户里真复购的比例
p1 = F.udf(lambda v: float(v[1]), "double")("probability")
ranked = pred.withColumn("p", p1)
top_n = int(ranked.count() * 0.2)
top = ranked.orderBy(F.desc("p")).limit(top_n)
hit = top.filter("label = 1").count()
base = ranked.filter("label = 1").count() / ranked.count()
print(f"⑤ 概率最高的 {top_n} 人里有 {hit} 人复购，命中率 {hit / top_n:.1%}，"
      f"是随便挑（{base:.1%}）的 {hit / top_n / base:.1f} 倍")

# ⑥ 看各特征的系数（标准化以后才能比大小）
names = ["R", "F", "M", "avg_amount", "categories", "days_since_first", "age"]
coef = model.stages[-1].coefficients.toArray()[:len(names)]
print("⑥ 逻辑回归系数：", ", ".join(f"{k} {v:+.3f}" for k, v in zip(names, coef)))

# ⑦ 只换最后一步，比较随机森林
rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=50, maxDepth=5, seed=10)
rf_model = Pipeline(stages=[indexer, encoder, assembler, scaler, rf]).fit(train)
rf_auc = BinaryClassificationEvaluator(labelCol="label").evaluate(rf_model.transform(test))
imp = rf_model.stages[-1].featureImportances.toArray()[:len(names)]
print(f"⑦ 随机森林 AUC = {rf_auc:.3f}")
print("   特征重要性：", ", ".join(f"{k} {v:.2f}" for k, v in sorted(zip(names, imp), key=lambda x: -x[1])))

# ⑧ 交叉验证挑正则化参数 regParam（3 折 × 3 个候选值 = 训练 9 次）
grid = ParamGridBuilder().addGrid(lr.regParam, [0.0, 0.01, 0.1]).build()
cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid,
                    evaluator=BinaryClassificationEvaluator(labelCol="label"), numFolds=3, seed=10)
cv_model = cv.fit(train)
for params, m in zip(grid, cv_model.avgMetrics):
    print(f"⑧ regParam = {list(params.values())[0]:<5} 平均 AUC = {m:.3f}")
best = cv_model.bestModel
best_auc = BinaryClassificationEvaluator(labelCol="label").evaluate(best.transform(test))
print(f"   最好的模型在测试集上 AUC = {best_auc:.3f}")

# ⑨ 保存整条流水线（包括编号、标准化的参数），实验 10-3 直接加载
best.write().overwrite().save(f"{DATA}/models/repurchase")
print("⑨ 模型已保存到 data/models/repurchase/")
spark.stop()
