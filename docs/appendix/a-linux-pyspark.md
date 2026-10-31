---
title: 附录 A Linux 与 PySpark 速查
description: 本书实验中常用的 Linux 命令、管道写法和 PySpark 接口速查
---

# 附录 A Linux 与 PySpark 速查

本附录汇总本书实验中最常用的命令和接口，供做实验时随时查阅。它不是完整的教程，Linux 的系统学习请参考“Linux 操作系统”课程。

!!! note "编写中"
    目前收录了第 1～4 章用到的 Linux 命令和 PySpark 入门写法；PySpark 的其余部分随第 6～10 章补充。

## A.1 本书用到的 15 条命令

第 2 章 2.1 节的速记表，按用途排列：

| 用途 | 命令 |
|---|---|
| 目录 | `pwd` 显示当前目录 · `ls -lh` 列出文件 · `cd 目录` 进入目录 · `mkdir -p a/b` 建目录 |
| 文件 | `cp` 复制 · `mv` 移动或改名 · `rm -r` 删除（无法恢复） |
| 查看内容 | `cat` 全部 · `head -n 3` / `tail -n 2` 开头或结尾几行 · `grep 关键词` 找行 · `wc -l` 数行数 |
| 系统状态 | `df -h` 磁盘空间 · `free -h` 内存 · `ps aux` 进程 |
| 网络 | `curl -I http://localhost:9870` 检查网页服务 |

## A.2 查看文件

```bash
ls -lh data/xiaoxiang/        # 列出文件，-h 用 KB、MB 显示大小
head -3 access.log            # 看前 3 行
tail -n +2 orders.csv         # 从第 2 行开始显示（去掉表头）
wc -l access.log              # 数一数有多少行
```

## A.3 管道与文本处理

管道 `|` 把前一个命令的输出交给后一个命令当输入。本书用它在本机模拟 Hadoop Streaming：

```bash
cat 文件 | python3 mapper.py | sort | python3 reducer.py
```

| 命令 | 作用 | 例子 |
|---|---|---|
| `cut -d, -f5` | 按分隔符取第几列 | 取 CSV 的第 5 列 |
| `sort` | 排序（默认按整行的字符顺序） | 模拟 Shuffle 的排序 |
| `sort -t$'\t' -k2 -nr` | 以 Tab 分列，按第 2 列的数值从大到小排 | 按次数排行 |
| `uniq -c` | 统计相邻重复行的个数（先 `sort`） | `sort | uniq -c` |
| `grep 关键词` | 只保留包含关键词的行 | `grep "/cart" access.log` |

## A.4 PySpark 入门写法

第 1 章实验 1-2 用到的写法，在本机以本地模式运行（需要 Java 17 或 21 和 `pip install pyspark==4.1.4`）：

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("作业名").master("local[*]").getOrCreate()
df = spark.read.csv("data/xiaoxiang/orders.csv", header=True, inferSchema=True)
(df.filter(F.col("status") == "已完成")
   .groupBy("province")
   .agg(F.count("*").alias("订单数"), F.sum("amount").cast("decimal(14,2)").alias("销售额"))
   .orderBy(F.desc("销售额"))
   .show(10))
spark.stop()
```

| pandas | PySpark |
|---|---|
| `pd.read_csv(路径)` | `spark.read.csv(路径, header=True, inferSchema=True)` |
| `df[df["status"] == "已完成"]` | `df.filter(F.col("status") == "已完成")` |
| `df.groupby("a").agg(...)` | `df.groupBy("a").agg(...)` |
| `df.sort_values("x", ascending=False)` | `df.orderBy(F.desc("x"))` |
| `df.head(10)` | `df.show(10)` |

## A.5 DataFrame 常用操作（第 7、8 章）

先导入：`from pyspark.sql import functions as F` 和 `from pyspark.sql import Window`。

| 想做什么 | 写法 | 见 |
|---|---|---|
| 读 CSV（全部当字符串） | `spark.read.csv(路径, header=True)` | 7.2.2 |
| 读文本、Parquet | `spark.read.text(路径)`、`spark.read.parquet(路径)` | 7.2.2 |
| 看结构、前几行、概况 | `df.printSchema()`、`df.show(5)`、`df.describe("列").show()` | 7.2.3 |
| 选列、过滤、新增列 | `df.select(...)`、`df.filter(条件)`、`df.withColumn("名", 表达式)` | 7.2.3 |
| 多个条件 | <code>(条件1) &amp; (条件2)</code>、<code>(条件1) &#124; (条件2)</code>、<code>~条件</code> | 7.2.3 |
| 每列空值个数 | `df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns])` | 7.3.2 |
| 去重 | `df.dropDuplicates()`、`df.dropDuplicates(["order_id"])` | 7.3.1 |
| 缺失值 | `df.dropna(subset=[...])`、`df.fillna({"列": 值})`、`F.coalesce(列1, 列2)` | 7.3.2 |
| 类型转换（不报错） | `F.col("amount").try_cast("decimal(10,2)")` | 7.3.3 |
| 字符串 | `F.trim`、`F.lower`、`F.regexp_replace(列, 模式, 替换)`、`F.regexp_extract(列, 模式, 组号)`、`列.rlike(模式)` | 7.3.4 |
| 解析时间 | `F.try_to_timestamp("列", F.lit("yyyy/M/d HH:mm:ss"))` | 7.3.5 |
| 连接 | `df.join(另一个, "列名", "left")` | 7.4 |
| 写 Parquet（按日期分区） | `df.write.mode("overwrite").partitionBy("dt").parquet(路径)` | 7.5 |
| 分组聚合 | `df.groupBy("a").agg(F.sum("x").alias("s"), F.countDistinct("u"))` | 8.2.1 |
| 透视表 | `df.groupBy("行").pivot("列").agg(F.sum("x"))` | 8.2.3 |
| 写 SQL | `df.createOrReplaceTempView("t")`，`spark.sql("SELECT ... FROM t")` | 8.1.2 |
| 分组排名 | `F.row_number().over(Window.partitionBy("a").orderBy(F.desc("x")))` | 8.3 |
| 累计、移动平均、环比 | `.rowsBetween(Window.unboundedPreceding, 0)`、`.rowsBetween(-6, 0)`、`F.lag("x", 1)` | 8.3 |
