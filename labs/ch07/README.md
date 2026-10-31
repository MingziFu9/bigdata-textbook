# 第 7 章 用 Spark 做数据清洗：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `make_dirty.py` | 从实验 1-1 生成的原始数据“弄脏”出 `data/xiaoxiang_dirty/orders_dirty.csv`（102000 行）和 `access_dirty.log`（1001000 行），随机种子固定 | 本机 `labs` 目录：`python ch07/make_dirty.py` |
| `lab7_1_clean_orders.py` | 实验 7-1：去重、统一格式、类型转换、日期解析、补缺失值、删异常值，按 dt 分区存成 Parquet，并与原始订单逐行核对 | spark 容器：`spark-submit /labs/ch07/lab7_1_clean_orders.py` |
| `lab7_2_parse_log.py` | 实验 7-2：用正则表达式解析带坏行的访问日志，存成 Parquet，比较文件大小 | spark 容器：`spark-submit /labs/ch07/lab7_2_parse_log.py` |
| `lab7_3_merge_report.py` | 实验 7-3：订单与用户、商品合成宽表，生成 `output/清洗报告.md` | spark 容器：`spark-submit /labs/ch07/lab7_3_merge_report.py`（先完成实验 7-1） |

输出位置（都在本机 `labs/` 下，可以随时删除后重新生成）：

- `data/xiaoxiang_dirty/`：脏数据
- `data/xiaoxiang_clean/orders/`、`access_log/`、`order_wide/`：清洗结果（Parquet）
- `ch07/output/清洗报告.md`：实验 7-3 生成的报告

启动并进入 spark 容器：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.spark.yml up -d
docker compose -f docker-compose.yml -f docker-compose.spark.yml exec spark bash
```

本章只用本地模式，不需要启动 Hive。
