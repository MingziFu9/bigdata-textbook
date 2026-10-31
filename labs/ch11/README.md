# 第 11 章 常见问题与调优：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `ui.py` | 共用：把一个查询运行 3 次取最短用时；从 Spark UI 的接口读出读入量、Shuffle 量、任务数，以及最慢阶段的任务耗时和读入行数的中位数、最大值 | 被下面三个程序导入，运行时用 `--py-files /labs/ch11/ui.py` 带上 |
| `lab11_1_parquet.py` | 实验 11-1：300 万行订单 CSV 转 Parquet，比较大小、查询耗时和执行计划；2000 个小文件与合并后的比较。结果写到 `data/scale/orders_parquet/` 等目录 | spark 容器：`spark-submit --py-files /labs/ch11/ui.py /labs/ch11/lab11_1_parquet.py` |
| `lab11_2_skew.py` | 实验 11-2：模拟秒杀日热点店铺，比较直接关联、加盐、打开 AQE 三种做法 | spark 容器：`spark-submit --py-files /labs/ch11/ui.py /labs/ch11/lab11_2_skew.py` |
| `lab11_3_broadcast.py` | 实验 11-3：订单关联商品，比较自动广播、关闭广播、`F.broadcast()` 指定广播 | spark 容器：`spark-submit --py-files /labs/ch11/ui.py /labs/ch11/lab11_3_broadcast.py` |

准备工作：在本机 `labs` 目录运行 `python ch01/lab1_1_data_size.py --max 3000000`，生成 `data/scale/orders_3000000.csv`。实验 11-2、11-3 要先完成实验 11-1。

做完本章实验后，可以删除 `data/scale/orders_small_files*` 和 `data/scale/orders_parquet_3cols/` 节省空间。
