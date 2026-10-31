# 第 10 章 Spark MLlib 机器学习应用：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `features.py` | 共用：从订单宽表算出每个用户的 R、F、M、平均每单金额、品类数、首购距今天数，以及某段时间内下过单的用户 | 被下面两个程序导入，运行时用 `--py-files` 带上 |
| `lab10_1_repurchase.py` | 实验 10-1：复购预测。Pipeline + 逻辑回归，AUC、混淆矩阵、前 20% 命中率，比较随机森林，交叉验证，模型保存到 `data/models/repurchase/` | spark 容器：`spark-submit --py-files /labs/ch10/features.py /labs/ch10/lab10_1_repurchase.py`（约 5 分钟） |
| `lab10_2_kmeans.py` | 实验 10-2：在 RFM 结果上做 K-Means 分群，结果存到 `data/xiaoxiang_clean/user_cluster/` | spark 容器：`spark-submit /labs/ch10/lab10_2_kmeans.py` |
| `lab10_3_scoring.py` | 实验 10-3：加载模型给全部老用户打分，前 20% 写成 `data/xiaoxiang_clean/marketing_list/` 下的 CSV | spark 容器：`spark-submit --py-files /labs/ch10/features.py /labs/ch10/lab10_3_scoring.py` |

准备工作：

- 先完成实验 7-3（订单宽表 `order_wide/`）和实验 8-2（`rfm/`）。
- 本章起 spark 容器要用加装了 NumPy 的镜像（`cluster/spark/Dockerfile`），第一次运行前重新构建：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.spark.yml up -d --build spark
```
