# 第 1 章 大数据概述与技术生态：实验代码与数据

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `lab1_1_data_size.py` | 实验 1-1：生成 1 万～100 万行订单（`--max 3000000` 再加 300 万行），比较文件大小、pandas 读取时间和内存，推算 1 亿行 | 本机 `python ch01/lab1_1_data_size.py`，生成的文件在 `data/scale/`（不进仓库） |
| `lab1_2_first_pyspark.py` | 实验 1-2：本地模式运行 PySpark，统计各省份已完成订单的订单数和销售额 | 本机 `python ch01/lab1_2_first_pyspark.py`，需要 Java 17 或 21 |

实验 1-3 是画图作业，不需要代码。都在 `labs/` 目录下运行，先 `python data/make_data.py` 生成数据。
