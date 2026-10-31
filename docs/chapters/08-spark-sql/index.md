---
title: 第 8 章 DataFrame 与 Spark SQL 数据分析
description: 用 DataFrame 函数和 SQL 两种写法完成分组聚合、透视表、多表连接和窗口函数，计算销售额、客单价、复购率、转化率、留存率等常用指标，用 RFM 模型给用户分层，并让 Spark 读写 Hive 表和 MySQL、比较 pandas 与 Spark
---

# 第 8 章 DataFrame 与 Spark SQL 数据分析

!!! abstract "本章学时"
    理论 6 学时 · 实验 1 学时（本章共 3 个实验，教师可任选）

## 学习目标

学完本章，你应该能够：

1. **说出** Spark SQL 的作用，用 DataFrame 函数和 SQL 两种写法表达同一个分析问题（理解、应用）；
2. **使用**分组聚合、透视表、多表连接、排序取前几名完成多维分析（应用）；
3. **使用**窗口函数完成分组排名、累计、移动平均和环比计算（应用）；
4. **计算**销售额、客单价、复购率、转化率、留存率等常用指标，用 RFM 模型完成用户分层并提出运营建议（应用、分析）；
5. **完成** Spark 与 Hive、MySQL 之间的数据读写，根据数据量在 pandas 和 Spark 之间做出选择（应用、评价）。

!!! info "先修要求"
    学习本章前，你需要掌握：

    - DataFrame 的读入、`select`、`filter`、`withColumn`、`join`，以及实验 7-1、7-3 生成的清洗结果（[第 7 章 用 Spark 做数据清洗](../07-data-cleaning/index.md)）；
    - HiveQL 的 `GROUP BY`、窗口函数的基本写法，数据仓库的 ODS、ADS 分层（[第 5 章 Hive 数据仓库](../05-hive/index.md)）；
    - pandas 的 `read_csv`、`groupby`（“Python 程序设计”课程）。

??? abstract "本章概念清单（14 个，点击查看定义和例子）"
    1. [Spark SQL](../../appendix/glossary.md#g-Spark-SQL)
    2. [临时视图](../../appendix/glossary.md#g-临时视图)
    3. [透视表](../../appendix/glossary.md#g-透视表)
    4. [移动平均](../../appendix/glossary.md#g-移动平均)
    5. [环比](../../appendix/glossary.md#g-环比)
    6. [同比](../../appendix/glossary.md#g-同比)
    7. [GMV](../../appendix/glossary.md#g-GMV)
    8. [客单价](../../appendix/glossary.md#g-客单价)
    9. [复购率](../../appendix/glossary.md#g-复购率)
    10. [转化率](../../appendix/glossary.md#g-转化率)
    11. [留存率](../../appendix/glossary.md#g-留存率)
    12. [RFM 模型](../../appendix/glossary.md#g-RFM-模型)
    13. [用户分层](../../appendix/glossary.md#g-用户分层)
    14. [JDBC](../../appendix/glossary.md#g-JDBC)

## 情景导入

数据洗干净了，运营部的问题接踵而来：“9 月哪个品类卖得最好？每个省前三名的品类是什么？”“这个月有多少老客户又回来买了？”“国庆要发优惠券，预算只够发给一万人，发给谁？”老板还想要一个每天自动更新的销售看板，数据要放进公司的 MySQL 数据库，看板从那里取数。

这些问题，第 5 章用 Hive 写 SQL 也能做一部分，但每条 SQL 都要等 Tez 作业启动，写复杂的用户分层还要串好几张临时表。小林决定用 Spark：清洗、分析、写数据库都在一个程序里完成，SQL 和 Python 可以混着写。

本章先学 DataFrame 和 SQL 两种写法，再学分组聚合、透视表和窗口函数；然后弄清电商常用指标的算法，用 RFM 模型给用户分层；最后让 Spark 读写 Hive 表和 MySQL，并比较一下：同样的统计，什么时候用 pandas 就够了，什么时候该用 Spark。

## 8.1 两种写法：DataFrame 与 SQL

### 8.1.1 Spark SQL 是什么

[**Spark SQL**](../../appendix/glossary.md#g-Spark-SQL "Spark 处理结构化数据的模块，可以用 DataFrame 函数或 SQL 语句完成同样的计算，两者共用一个执行引擎"){ .term } 是 Spark 处理结构化数据的模块。第 7 章用的 DataFrame 就属于它。它有两种用法：调用 DataFrame 的函数（`groupBy`、`agg`、`join`……），或者直接写 SQL 语句。两种写法交给同一个执行引擎，Spark 会把它们优化成同样的执行计划，速度没有差别。<sup class="cite">[[1]](#ref8-1)</sup><sup class="cite">[[2]](#ref8-2)</sup>

| 比较项 | DataFrame 函数 | SQL 语句 |
|---|---|---|
| 写起来 | Python 代码，可以拆成多步、加注释、用变量和循环 | 一条完整的语句，熟悉 SQL 的人上手快 |
| 适合 | 步骤多、需要和 Python 逻辑结合的处理 | 一次性的统计查询，从 Hive 迁移过来的 SQL |
| 出错时 | 列名写错，运行到这一步就报错 | 整条语句一起检查 |

实际工作中两种写法常常混用：用 DataFrame 读数据、清洗，用 SQL 写统计，再用 DataFrame 写出结果。

### 8.1.2 同一个问题两种写法

要用 SQL 查 DataFrame，先把它注册成一个[**临时视图**](../../appendix/glossary.md#g-临时视图 "把 DataFrame 注册成一个表名，供 SQL 语句查询，程序结束后消失"){ .term }，也就是给它起一个表名。临时视图只在本程序中有效，不会写进 Hive：

```python
wide = spark.read.parquet("file:///labs/data/xiaoxiang_clean/order_wide")   # 实验 7-3 的宽表
done = wide.filter(F.col("status") == "已完成")       # 销售额只算已完成的订单
done.createOrReplaceTempView("orders")               # 注册成临时视图 orders
```

问题：9 月每个品类的销售额、订单数、下单用户数、客单价各是多少？

DataFrame 写法：

```python
(done.groupBy("category")
     .agg(F.sum("amount").alias("gmv"),
          F.count("*").alias("orders"),
          F.countDistinct("user_id").alias("buyers"))
     .withColumn("per_buyer", F.round(F.col("gmv") / F.col("buyers"), 2))
     .orderBy(F.desc("gmv"))
     .show())
```

SQL 写法：

```python
spark.sql("""
    SELECT category, SUM(amount) AS gmv, COUNT(*) AS orders,
           COUNT(DISTINCT user_id) AS buyers,
           ROUND(SUM(amount) / COUNT(DISTINCT user_id), 2) AS per_buyer
    FROM orders
    GROUP BY category
    ORDER BY gmv DESC
""").show()
```

两种写法在实验 8-1 中得到完全相同的结果：

```text
+--------+-----------+------+------+---------+
|category|        gmv|orders|buyers|per_buyer|
+--------+-----------+------+------+---------+
|手机数码|97597310.90| 18534| 14741|  6620.81|
|家用电器|22003274.00| 13014| 10917|  2015.51|
|服饰鞋包|10441059.90| 13308| 11057|   944.29|
|家居日用| 3208242.00| 10534|  9044|   354.74|
|美妆个护| 3085825.00|  7533|  6659|   463.41|
|母婴玩具| 2814263.90|  4844|  4411|   638.01|
|食品生鲜| 2018578.70| 16996| 13823|   146.03|
|图书文具|  468084.40|  4877|  4421|   105.88|
+--------+-----------+------+------+---------+
```

对照着看，DataFrame 的每个函数都能在 SQL 中找到对应的部分：

| SQL | DataFrame |
|---|---|
| `SELECT a, b` | `.select("a", "b")` |
| `WHERE 条件` | `.filter(条件)` |
| `GROUP BY a` + 聚合函数 | `.groupBy("a").agg(...)` |
| `HAVING 条件` | `.agg(...)` 之后再 `.filter(条件)` |
| `ORDER BY a DESC` | `.orderBy(F.desc("a"))` |
| `LIMIT 10` | `.limit(10)` |
| `JOIN ... ON` | `.join(另一个, "列名", "连接方式")` |
| `表达式 AS 名字` | `F.表达式.alias("名字")` 或 `.withColumn("名字", 表达式)` |

`spark.sql(...)` 返回的也是 DataFrame，可以接着用函数处理，或者 `show()`、写文件。

## 8.2 常用分析

### 8.2.1 分组聚合

分组聚合是最常用的分析：按一个或几个维度分组，对每组算指标。`agg` 里可以同时写多个聚合函数：

| 函数 | 作用 | 注意 |
|---|---|---|
| `F.sum`、`F.avg`、`F.min`、`F.max` | 求和、平均、最小、最大 | 空值不参与计算 |
| `F.count("*")` | 行数 | |
| `F.count("列")` | 该列不为空的行数 | 实操题用它数商品详情页的访问 |
| `F.countDistinct("列")` | 不同取值的个数 | 数下单用户数要用它，不能用 `count` |
| `F.round(表达式, 2)` | 四舍五入到 2 位小数 | |

按两个维度分组只要在 `groupBy` 中写两个列名，比如 `groupBy("province", "category")` 得到每个省、每个品类一行。

### 8.2.2 多表连接与排序取前几名

第 7 章已经把订单和用户、商品合成了宽表，分析时直接用就行。如果还要别的维度，比如店铺所在城市，再 `join` 一张店铺表即可，写法和 7.4 节一样。

“销售额最高的 10 个省份”这类问题叫 TopN（第 4 章用 MapReduce 做过），在 DataFrame 中只要排序后取前几行：

```python
done.groupBy("province").agg(F.sum("amount").alias("gmv")).orderBy(F.desc("gmv")).limit(10).show()
```

如果要的是“**每个**省份销售额前 3 的品类”，`orderBy` 加 `limit` 就不够了，这要用 8.3 节的窗口函数。

### 8.2.3 透视表

[**透视表**](../../appendix/glossary.md#g-透视表 "把某一列的不同取值展开成多列的汇总表，行、列各是一个维度"){ .term }把某一列的不同取值展开成多列，行和列各是一个维度，和 Excel 的数据透视表一样。DataFrame 用 `groupBy(行).pivot(列).agg(值)`：

```python
done.groupBy("level").pivot("category").agg(F.round(F.sum("amount") / 10000, 1)).show()
```

实验 8-1 的结果（单位万元）：

```text
+-----+--------+--------+--------+--------+--------+--------+--------+--------+
|level|图书文具|家居日用|家用电器|手机数码|服饰鞋包|母婴玩具|美妆个护|食品生鲜|
+-----+--------+--------+--------+--------+--------+--------+--------+--------+
|普通 |26.3    |180.8   |1197.6  |5384.5  |579.1   |153.9   |170.8   |111.0   |
|白银 |11.3    |80.1    |555.9   |2456.6  |252.4   |71.0    |72.3    |51.0    |
|黄金 |7.1     |45.1    |333.4   |1450.3  |162.1   |43.9    |49.1    |29.8    |
|钻石 |2.2     |14.8    |113.4   |468.3   |50.6    |12.6    |16.5    |10.0    |
+-----+--------+--------+--------+--------+--------+--------+--------+--------+
```

一眼就能看出：各等级会员都是手机数码买得最多；普通会员人数最多，销售额也最高。透视表的列数等于这一列的不同取值个数，所以只适合取值不多的列（品类、支付方式、月份），不要对用户编号这样的列做透视。

## 8.3 窗口函数

第 5 章用 HiveQL 写过窗口函数，Spark SQL 的写法完全相同。这里系统地学一遍。**窗口函数**和 `GROUP BY` 的区别是：`GROUP BY` 把一组行合成一行，窗口函数保留每一行，只是给每一行多算一列，算的时候可以“看到”同组的其他行。<sup class="cite">[[3]](#ref8-3)</sup>

一个窗口由三部分说明：

| 部分 | SQL | DataFrame | 意思 |
|---|---|---|---|
| 分组 | `PARTITION BY province` | `Window.partitionBy("province")` | 每个省份各算各的；不写就是全部数据一组 |
| 排序 | `ORDER BY gmv DESC` | `.orderBy(F.desc("gmv"))` | 组内按什么顺序排 |
| 范围 | `ROWS BETWEEN 6 PRECEDING AND CURRENT ROW` | `.rowsBetween(-6, 0)` | 计算当前行时用哪几行：这里是前 6 行到当前行 |

DataFrame 写法要先导入 `Window`：`from pyspark.sql import Window`。常用的四类窗口计算：

**① 分组排名：每个省份销售额前 3 的品类。**

```python
w = Window.partitionBy("province").orderBy(F.desc("gmv"))
top3 = (done.groupBy("province", "category").agg(F.sum("amount").alias("gmv"))
            .withColumn("rank", F.row_number().over(w))
            .filter("rank <= 3"))
```

```text
+--------+--------+----------+----+
|province|category|       gmv|rank|
+--------+--------+----------+----+
|    广东|手机数码|8809804.70|   1|
|    广东|家用电器|2090733.20|   2|
|    广东|服饰鞋包| 966275.40|   3|
|    浙江|手机数码|4590529.60|   1|
|    浙江|家用电器| 988286.50|   2|
|    浙江|服饰鞋包| 483050.00|   3|
+--------+--------+----------+----+
```

排名函数有三个，区别在遇到并列时。编者用 4 家店铺的例子试了一下（B、C 销售额相同）：

```text
+----+---+----------+----+----------+
|shop|gmv|row_number|rank|dense_rank|
+----+---+----------+----+----------+
|   A|300|         1|   1|         1|
|   B|200|         2|   2|         2|
|   C|200|         3|   2|         2|
|   D|100|         4|   4|         3|
+----+---+----------+----+----------+
```

`row_number` 不管并列，依次编号；`rank` 并列的名次相同，后面跳号；`dense_rank` 并列的名次相同，后面不跳号。“每组只取 3 个”用 `row_number`，“并列的都要列出来”用 `rank` 或 `dense_rank`。

**② 累计：截至每一天的累计销售额。**范围是从第一行到当前行：

```python
w_day = Window.orderBy("dt")
daily = done.groupBy("dt").agg(F.sum("amount").alias("gmv"))
daily = daily.withColumn("cum", F.sum("gmv").over(w_day.rowsBetween(Window.unboundedPreceding, 0)))
```

**③ 移动平均：每天的 7 日平均销售额。**每天的销售额有高有低，看不出趋势；[**移动平均**](../../appendix/glossary.md#g-移动平均 "当前行和前面若干行的平均值，用来平滑每天的波动，看清趋势"){ .term }用当前行和前 6 行的平均值，把波动“抹平”：

```python
daily = daily.withColumn("ma7", F.avg("gmv").over(w_day.rowsBetween(-6, 0)))
```

**④ 环比：今天比昨天增长了多少。**`F.lag("gmv", 1)` 取上一行的值，[**环比**](../../appendix/glossary.md#g-环比 "本期与上一期相比的变化率：（本期 − 上期）÷ 上期"){ .term } = （本期 − 上期）÷ 上期。和去年同一时期比叫[**同比**](../../appendix/glossary.md#g-同比 "本期与去年同期相比的变化率"){ .term }，算法一样，只是“上期”换成去年同期：

```python
daily = (daily.withColumn("prev", F.lag("gmv", 1).over(w_day))
              .withColumn("dod", F.round((F.col("gmv") - F.col("prev")) / F.col("prev") * 100, 1)))
```

实验 8-1 前 5 天的结果：

```text
+----------+----------+-----+------------+----------+
|        dt|       gmv|  dod|         cum|       ma7|
+----------+----------+-----+------------+----------+
|2026-09-01|4471291.70| NULL|  4471291.70|4471291.70|
|2026-09-02|5019066.20| 12.3|  9490357.90|4745178.95|
|2026-09-03|4660418.80| -7.1| 14150776.70|4716925.57|
|2026-09-04|4644829.80| -0.3| 18795606.50|4698901.63|
|2026-09-05|4820731.80|  3.8| 23616338.30|4723267.66|
+----------+----------+-----+------------+----------+
```

注意两个边界：9 月 1 日前面没有行，所以环比是 NULL；前 6 天凑不满 7 天，移动平均只用了已有的几天，解读时要心里有数。

!!! tip "不分组的窗口会有警告"
    `Window.orderBy("dt")` 没有 `partitionBy`，运行时会出现警告 `No Partition Defined for Window operation! Moving all data to a single partition`：所有数据要放到一个分区里排序。这里只有 30 行，没有问题；如果对上亿行数据这样做，会非常慢，应该先聚合到较少的行，或者加上分组。

下面的动画用实验 8-1 的真实数据，逐行演示四类窗口计算：

<iframe class="sim-frame" src="../../sims/window-functions/main.html" height="1060" loading="lazy" title="窗口函数"></iframe>

先在“分组排名”中按“下一行”走到浙江，看编号怎样重新从 1 开始；再取消“按省份分组”比较一下；然后切换到“移动平均”，看前几行的窗口为什么不满 7 天。[全屏打开](../../sims/window-functions/main.html){ target=_blank }

## 8.4 常用指标与用户分层

### 8.4.1 销售额、客单价、复购率

运营部天天说的几个指标，算法要先说清楚。同一个名字，不同公司的口径可能不一样（比如销售额算不算已退款的订单），写报告时一定要注明口径。本书的口径如下：

| 指标 | 算法 | 实验 8-1 的结果（9 月） |
|---|---|---|
| 销售额（[**GMV**](../../appendix/glossary.md#g-GMV "成交总额，一段时间内成交订单的金额之和；本书只算已完成的订单"){ .term }） | 已完成订单的金额之和 | 141,636,638.80 元 |
| 订单数 | 已完成订单的笔数 | 89640 笔 |
| 下单用户数 | 有已完成订单的不同用户数 | 48644 人 |
| [**客单价**](../../appendix/glossary.md#g-客单价 "销售额 ÷ 下单用户数，平均每个用户花了多少钱"){ .term } | 销售额 ÷ 下单用户数 | 2,911.70 元 |
| 笔单价 | 销售额 ÷ 订单数 | 1,580.06 元 |
| [**复购率**](../../appendix/glossary.md#g-复购率 "一段时间内购买 2 次及以上的用户占下单用户的比例"){ .term } | 购买 2 次及以上的用户数 ÷ 下单用户数 | 37.7%（18358 人） |

复购率的写法值得看一下：先按用户分组数订单，再数“订单数 ≥ 2”的用户有多少：

```python
repeat = done.groupBy("user_id").count().filter("count >= 2").count()
```

### 8.4.2 转化率与留存率

[**转化率**](../../appendix/glossary.md#g-转化率 "完成目标行为（如下单）的用户占访问用户的比例"){ .term }衡量“来了多少人，有多少人买了”：下单用户数 ÷ 访问用户数。我们只有 9 月 30 日一天的访问日志，所以计算这一天的转化率：当天访问过的登录用户中，有多少人当天下了单。实验 8-1 的结果是 99046 人访问、3078 人下单，转化率 3.11%。计算方法是把两份用户名单做连接：

```python
visitors = logs.filter(F.col("user_id").isNotNull()).select("user_id").distinct()
buyers = wide.filter(F.col("dt") == "2026-09-30").select("user_id").distinct()
n_both = visitors.join(buyers, "user_id").count()      # 既访问又下单的用户
```

[**留存率**](../../appendix/glossary.md#g-留存率 "某一时期的用户中，在之后某个时期仍然活跃的比例"){ .term }衡量“用户还回不回来”：某一时期的用户中，之后某个时期仍然活跃的比例。编者用同样的连接方法算了一个：9 月第一周（1 日至 7 日）有已完成订单的 16323 个用户中，第二周（8 日至 14 日）又下单的有 4156 人，周留存率 25.5%。APP 常用的“次日留存”“7 日留存”，算法相同，只是把“下单”换成“登录”。

### 8.4.3 案例：RFM 用户价值分层

国庆的优惠券该发给谁？[**RFM 模型**](../../appendix/glossary.md#g-RFM-模型 "用最近一次购买距今天数（R）、购买次数（F）、购买金额（M）衡量用户价值并分层的方法"){ .term }是电商和零售业最常用的[**用户分层**](../../appendix/glossary.md#g-用户分层 "按价值或行为把用户分成若干类，对不同的类采取不同的运营办法"){ .term }方法，用三个数衡量一个用户的价值：<sup class="cite">[[4]](#ref8-4)</sup>

| 指标 | 含义 | 怎么算 | 越……越好 |
|---|---|---|---|
| R（Recency） | 最近一次购买距今几天 | 分析日 − 最后一次下单的日期 | 越小越好：最近还买过 |
| F（Frequency） | 购买次数 | 订单数 | 越大越好：买得勤 |
| M（Monetary） | 购买金额 | 金额之和 | 越大越好：花得多 |

做法分三步：

1. **算出每个用户的 R、F、M。**分析日取 10 月 1 日：

    ```python
    rfm = (done.groupBy("user_id")
               .agg(F.datediff(F.lit("2026-10-01"), F.max("order_time")).alias("R"),
                    F.count("*").alias("F"),
                    F.round(F.sum("amount"), 2).alias("M")))
    ```

2. **每个指标和平均值比，分出高低。**实验 8-2 中，平均 R 为 13.0 天、F 为 1.84 次、M 为 2911.70 元。R 不超过平均值算“高”（最近买过），F、M 不低于平均值算“高”。也可以按五等分打 1～5 分，原理相同。

3. **按高低组合分成 8 类。**比如三项都高的是“重要价值用户”，M 高但很久没来（R 低）、F 也低的是“重要挽留用户”。

实验 8-2 的结果（按贡献的销售额从高到低排列）：

| 用户类型 | R | F | M | 人数 | 平均 R（天） | 平均 F（次） | 平均 M（元） | 销售额占比 | 运营建议 |
|---|---|---|---|---|---|---|---|---|---|
| 重要价值用户 | 高 | 高 | 高 | 6911 | 5.4 | 4.62 | 10432 | 50.9% | 专属客服、新品优先，重点维护 |
| 重要挽留用户 | 低 | 低 | 高 | 3117 | 22.0 | 1.0 | 6329 | 13.9% | 买过大件但很久没来，大额券召回 |
| 重要发展用户 | 高 | 低 | 高 | 2424 | 7.0 | 1.0 | 6400 | 11.0% | 新近买了大件，推荐配件、促进复购 |
| 重要唤回用户 | 低 | 高 | 高 | 1778 | 18.7 | 2.41 | 7434 | 9.3% | 以前常买，最近少来，短信、推送唤回 |
| 一般价值用户 | 高 | 高 | 低 | 6942 | 6.1 | 2.47 | 996 | 4.9% | 买得勤但金额小，满减券提升客单价 |
| 一般挽留用户 | 低 | 低 | 低 | 14031 | 22.0 | 1.0 | 467 | 4.6% | 人数最多、价值最低，低成本触达即可 |
| 一般发展用户 | 高 | 低 | 低 | 10714 | 7.0 | 1.0 | 472 | 3.6% | 新客，新人券引导第二次购买 |
| 一般唤回用户 | 低 | 高 | 低 | 2727 | 18.9 | 2.19 | 936 | 1.8% | 视预算决定是否唤回 |

14% 的“重要价值用户”贡献了一半的销售额。优惠券只能发一万张的话，优先发给“重要挽留”和“重要唤回”用户（合计 4895 人）：他们花钱多，但最近没来，最需要拉回来；“重要价值用户”本来就常买，发券的作用反而不大。

!!! warning "分层之前看一眼极端值"
    实验 8-2 的 `describe()` 显示，F 的最大值是 386：有一个用户一个月买了 386 次，可能是企业采购或代购。这样的用户会把平均值拉高，影响分层的分界线。实际工作中，要先把这类用户单独拿出来，再对其余用户分层。

## 8.5 连接 Hive 与 MySQL

### 8.5.1 读写 Hive 表

第 5 章在 Hive 中建了 `xiaoxiang` 数据库和 ODS、DWD、DWS、ADS 各层的表。Spark 可以直接读写这些表，只要创建 SparkSession 时打开 Hive 支持：<sup class="cite">[[5]](#ref8-5)</sup>

```python
spark = SparkSession.builder.appName("...").enableHiveSupport().getOrCreate()
orders = spark.table("xiaoxiang.ods_orders")           # 读 Hive 表，得到 DataFrame
spark.sql("SELECT COUNT(*) FROM xiaoxiang.ods_orders")  # 也可以直接写 SQL
daily.write.mode("overwrite").format("parquet").saveAsTable("xiaoxiang.ads_category_daily")  # 写成 Hive 表
```

Spark 通过 Hive 的 Metastore（第 5 章）知道每张表的结构和数据在 HDFS 上的位置，然后自己读写 HDFS 上的文件，不经过 Hive 的计算引擎。随书 spark 容器已经在 `labs/cluster/spark-conf/` 中配好了 Metastore 的地址和版本，只要第 5 章的 Hive 容器在运行就能用（附录 C 有说明）。用 Spark 写进 Hive 的表，在 beeline 中也能查到。

编者实测发现一个坑：第 5 章的 `ods_orders` 是指向 CSV 文件的文本表，用 `skip.header.line.count` 跳过表头。Hive 认这个设置，Spark 却不认：在 Spark 中 `count()` 得到 100001 行，多出来的一行就是表头 `order_id,user_id,...`。所以用 Spark 读这类表时，要自己加一句 `filter(F.col("order_id") != "order_id")` 去掉表头。

### 8.5.2 把结果写进 MySQL

报表系统、销售看板、业务系统一般不直接读 HDFS，而是从 MySQL 这样的关系数据库取数。所以分析结果算好后，常常要写进 MySQL。Spark 通过 [**JDBC**](../../appendix/glossary.md#g-JDBC "Java 程序访问关系数据库的标准接口，Spark 通过它读写 MySQL 等数据库"){ .term }（Java 程序访问数据库的标准接口）读写数据库，需要先准备好 MySQL 的 JDBC 驱动（一个 jar 文件），运行时用 `--jars` 交给 Spark：<sup class="cite">[[6]](#ref8-6)</sup>

```python
url = "jdbc:mysql://mysql:3306/xiaoxiang"          # 主机 mysql（容器名）、端口 3306、数据库 xiaoxiang
props = {"user": "root", "password": "xiaoxiang", "driver": "com.mysql.cj.jdbc.Driver"}
daily.write.mode("overwrite").jdbc(url, "category_daily", properties=props)   # 写
back = spark.read.jdbc(url, "category_daily", properties=props)              # 读
```

`mode("overwrite")` 会先删掉再重建 MySQL 中的表，Spark 按 DataFrame 的类型建表：实验 8-3 中，日期列建成 `date`，品类建成 `longtext`，金额建成 `decimal(21,2)`。如果对表结构有要求（比如要加索引），应先在 MySQL 中建好表，再用 `mode("append")` 追加数据。

### 8.5.3 用 pandas 接着画图

写进 MySQL 的结果只有 240 行，用 pandas 处理正合适。在本机用 pandas 从 MySQL 读出来，透视成“日期 × 品类”的表，再用 matplotlib 画图：

```python
from sqlalchemy import create_engine
engine = create_engine("mysql+pymysql://root:xiaoxiang@localhost:3306/xiaoxiang?charset=utf8mb4")
df = pd.read_sql("SELECT dt, category, gmv FROM category_daily", engine)
table = df.pivot_table(index="dt", columns="category", values="gmv", aggfunc="sum") / 10000
```

![9 月各品类销售额和每日销售额](img/category_daily.png)

这就是大数据分析中常见的分工：**Spark 把大数据算小，pandas 把小结果画出来**。第 12 章还会用 pyecharts 和 Superset 做交互式的看板。

## 8.6 pandas 和 Spark 怎么选

pandas 简单好用，Spark 能处理大数据，到底该用哪个？编者用实验 1-1 生成的四个不同大小的订单文件，做了同一个统计（只留已完成的订单，按省份求销售额，排序），比较两者的耗时：

| 数据量 | CSV 大小 | pandas 用时 | pandas 占内存 | Spark 用时（本地模式） |
|---|---|---|---|---|
| 1 万行 | 0.8 MB | 0.02 秒 | 4.6 MB | 3.9 秒 |
| 10 万行 | 8.0 MB | 0.19 秒 | 45.5 MB | 1.1 秒 |
| 100 万行 | 80 MB | 2.2 秒 | 455 MB | 2.0 秒 |
| 300 万行 | 240 MB | 6.4 秒 | 1366 MB | 2.5 秒 |

表中 pandas 在本机运行，Spark 在 spark 容器中运行，不含 Spark 启动的约 3 秒；1 万行时 Spark 的第一次查询还包含了预热时间。从表中可以看出：

1. **数据量小（几十万行以内），pandas 快得多。**Spark 每次都要拆分任务、调度，这些固定开销比计算本身还长；加上启动，`spark-submit` 跑完整个程序要 15 秒。
2. **数据量到百万行，两者差不多；再往上，Spark 越来越占优。**Spark 用 4 个核并行读文件，pandas 只用 1 个核。
3. **更要紧的是内存。**pandas 要把整个数据装进内存，300 万行已经占了 1.3 GB；按这个比例，3000 万行要 13 GB，普通笔记本就装不下了。pandas 官方文档也建议，数据接近内存大小时，要只读需要的列、用更省内存的类型、分块处理，或者换用能并行和分布式处理的工具。<sup class="cite">[[7]](#ref8-7)</sup> Spark 不需要把数据全部装进内存，还可以把作业提交到集群，用很多台机器一起算。

选择的经验：

| 情况 | 选择 |
|---|---|
| 数据能轻松装进一台电脑的内存（几百万行以内），做探索、画图 | pandas |
| 数据有几千万行以上，或者存放在 HDFS、Hive 上 | Spark |
| 每天定时运行、数据会越来越多的生产任务 | Spark |
| 大数据算出的小结果（汇总表、报表） | 交给 pandas 画图、做报告 |

## 8.7 前沿视窗

**用自然语言问数据：Text-to-SQL。**第 1 章提到过 Text-to-SQL：用户用中文问“9 月哪个省的手机卖得最好”，大模型把问题翻译成 SQL，交给 Spark 或数据库执行。国内外的 BI 产品都在做这样的“智能问数”功能，让不会写 SQL 的运营人员也能自己查数据。

它现在能做到什么程度？研究者在 BIRD 评测集上做了测试：这个评测集有 95 个真实规模的数据库、12751 个问题，覆盖 37 个行业。结果 GPT-4 生成的 SQL 只有 54.89% 能执行出正确结果，而人类专家能达到 92.96%。<sup class="cite">[[8]](#ref8-8)</sup> 难点恰恰是本章反复强调的东西：业务口径（“销售额”算不算退款）、字段的含义和取值（“已完成”在表里怎么写）、多表之间怎样关联。这些知识写在数据人员的脑子里和文档里，不在表结构里。

所以 Text-to-SQL 用得好的团队，都会先把指标口径、字段说明整理成文档交给模型，并让数据人员检查生成的 SQL。会写 SQL、懂指标口径的人，在大模型时代不是更不重要，而是更重要了。

想系统地学习 DataFrame 的聚合与窗口函数，可以读《Spark 权威指南》中聚合一章；<sup class="cite">[[9]](#ref8-9)</sup> 想看更多 DataFrame 操作和读写 MySQL 的中文例子，可以参考林子雨《Spark 编程基础（Python 版）》中 Spark SQL 一章的课件。<sup class="cite">[[10]](#ref8-10)</sup>

## 本章实验

!!! abstract "实验总览"
    | 实验 | 类型 | 建议时长 | 内容 |
    |---|---|---|---|
    | 实验 8-1 | ★ 基础 | 40 分钟 | 用实验 7-3 的宽表计算整体指标，按品类、会员等级、省份、日期做多维分析，用窗口函数求分组排名、环比、累计和移动平均，计算转化率 |
    | 实验 8-2 | ★ 基础 | 30 分钟 | 计算每个用户的 R、F、M，分成 8 类用户，提出发券建议 |
    | 实验 8-3 | ☆ 拓展 | 40 分钟 | Spark 读 Hive 表、写回 Hive 并写进 MySQL，本机用 pandas 读 MySQL 画图；比较 pandas 和 Spark 处理不同数据量的耗时 |

    实验 8-1、8-2 只用 spark 容器（本地模式），要先完成实验 7-1 和 7-3。实验 8-3 还要启动第 5 章的 Hive 容器和本章的 MySQL 容器，Docker 至少分配 8 GB 内存。实验代码见配套资源 `labs/ch08/` 目录。编者的运行环境：4 核 CPU、16 GB 内存的 Linux 主机，Docker 29.9，Spark 4.1.4、Hive 4.2.1、MySQL 8.4 镜像。

### 实验 8-1 ★ 小象优选销售多维分析

**实验目的**

1. 用 DataFrame 和 SQL 两种写法完成分组聚合，计算销售额、客单价、复购率等指标；
2. 用透视表和窗口函数完成多维分析：会员等级 × 品类、各省前 3 的品类、每日环比、累计和 7 日移动平均；
3. 把访问日志和订单两份数据结合起来计算转化率。

**实验环境**：第 6 章的 spark 容器，本地模式。

**实验数据**：实验 7-3 生成的宽表 `labs/data/xiaoxiang_clean/order_wide/`，实验 7-2 生成的日志表 `labs/data/xiaoxiang_clean/access_log/`。

**实验步骤与代码**

第 1 步，确认已完成实验 7-1、7-2、7-3，`labs/data/xiaoxiang_clean/` 下有 `order_wide` 和 `access_log` 两个目录。

第 2 步，打开 `labs/ch08/lab8_1_sales.py`，对照 8.1～8.4 节找到 ①～⑥ 每一部分对应的内容，然后运行：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.spark.yml up -d
docker compose -f docker-compose.yml -f docker-compose.spark.yml exec spark spark-submit /labs/ch08/lab8_1_sales.py
```

第 3 步，进入 pyspark，把程序中的语句逐段粘贴进去试验。比如把 ④ 中的 `row_number` 换成 `rank`、`dense_rank`，或者把 ⑤ 中的 7 日移动平均改成 3 日，观察结果的变化。

**运行结果**

程序用时约 18 秒，输出如下（警告信息已删去，② 的 SQL 写法、③ 的透视表、④ 的分组排名与正文 8.1～8.3 节相同，这里省略；⑤ 只列前 5 天和最后 3 天）：

```text
① 销售额 141,636,638.80 元，订单 89640 笔，下单用户 48644 人
   客单价 2,911.70 元，笔单价 1,580.06 元，复购率 37.7%（买了 2 次及以上的 18358 人）
② 按品类（DataFrame 写法）
+--------+-----------+------+------+---------+
|category|        gmv|orders|buyers|per_buyer|
+--------+-----------+------+------+---------+
|手机数码|97597310.90| 18534| 14741|  6620.81|
|家用电器|22003274.00| 13014| 10917|  2015.51|
|服饰鞋包|10441059.90| 13308| 11057|   944.29|
|家居日用| 3208242.00| 10534|  9044|   354.74|
|美妆个护| 3085825.00|  7533|  6659|   463.41|
|母婴玩具| 2814263.90|  4844|  4411|   638.01|
|食品生鲜| 2018578.70| 16996| 13823|   146.03|
|图书文具|  468084.40|  4877|  4421|   105.88|
+--------+-----------+------+------+---------+
……
⑤ 每日销售额（dod：环比 %；cum：累计；ma7：7 日移动平均）
+----------+----------+-----+------------+----------+
|        dt|       gmv|  dod|         cum|       ma7|
+----------+----------+-----+------------+----------+
|2026-09-01|4471291.70| NULL|  4471291.70|4471291.70|
|2026-09-02|5019066.20| 12.3|  9490357.90|4745178.95|
|2026-09-03|4660418.80| -7.1| 14150776.70|4716925.57|
|2026-09-04|4644829.80| -0.3| 18795606.50|4698901.63|
|2026-09-05|4820731.80|  3.8| 23616338.30|4723267.66|
……
|2026-09-28|5025595.40|  5.6|132020657.90|4747932.50|
|2026-09-29|4897527.30| -2.5|136918185.20|4783810.99|
|2026-09-30|4718453.60| -3.7|141636638.80|4753512.53|
+----------+----------+-----+------------+----------+

⑥ 9 月 30 日：访问的登录用户 99046 人，其中当天下单 3078 人，转化率 3.11%
```

累计销售额的最后一行 141,636,638.80 元正好等于 ① 的总销售额，也等于实验 7-3 清洗报告中的数字，三处可以互相核对。9 月每天的销售额在约 436 万到 503 万元之间，环比降得最多的是 9 月 25 日（−10.6%）。

**实验记录表**

| 观察内容 | 你的结果 |
|---|---|
| 销售额、订单数、下单用户数 | |
| 客单价、笔单价、复购率 | |
| 客单价最高和最低的品类 | |
| 你所在省份销售额前 3 的品类 | |
| 环比增长最多和下降最多的日期 | |
| 9 月 30 日的转化率 | |

**常见问题排查**

| 现象 | 原因 | 解决办法 |
|---|---|---|
| `Path does not exist: .../xiaoxiang_clean/order_wide` | 没有完成实验 7-3 | 依次运行实验 7-1、7-2、7-3 的程序 |
| `[TABLE_OR_VIEW_NOT_FOUND] The table or view orders cannot be found` | 写 SQL 前没有注册临时视图，或视图名写错 | 先 `df.createOrReplaceTempView("orders")` |
| `NameError: name 'Window' is not defined` | 没有导入 `Window` | `from pyspark.sql import Window` |
| 下单用户数比订单数还大 | 用了 `F.count("user_id")` | 数不同用户要用 `F.countDistinct("user_id")` |
| 警告 `No Partition Defined for Window operation!` | 窗口没有 `partitionBy`，所有数据放到一个分区 | 数据量小时可以忽略；数据量大时先聚合或加分组 |

**思考与练习**

1. 把复购率的口径改成“9 月内在 2 个不同日期下过单的用户”，程序要怎样改？结果和 37.7% 相比是高还是低？
2. ⑥ 中转化率的分子用的是所有状态的订单，如果只算已完成的订单，结果会怎样变化？哪种口径更合理？

**实验报告要求**

提交程序输出截图、实验记录表，回答思考与练习的两个问题。

### 实验 8-2 ★ RFM 用户分层

**实验目的**

1. 用分组聚合计算每个用户的 R、F、M；
2. 按与平均值比较的高低，把用户分成 8 类，统计各类的人数和销售额占比；
3. 根据分层结果提出发放优惠券的建议。

**实验环境**：同实验 8-1。

**实验数据**：实验 7-1 生成的 `labs/data/xiaoxiang_clean/orders/`。

**实验步骤与代码**

第 1 步，运行程序：

```bash
docker compose -f docker-compose.yml -f docker-compose.spark.yml exec spark spark-submit /labs/ch08/lab8_2_rfm.py
```

程序先算出每个用户的 R、F、M 并用 `describe()` 查看分布；再和平均值比较，得到 `R_level`、`F_level`、`M_level` 三列“高 / 低”；最后用一个字典把“高高高”等 8 种组合对应到用户类型，统计各类的人数、平均值和销售额占比，并把每个用户的结果存成 Parquet。

第 2 步，在 pyspark 中读回 `labs/data/xiaoxiang_clean/rfm/`，找出“重要挽留用户”中 M 最高的 10 个人。

**运行结果**

程序用时约 18 秒，输出如下（警告信息已删去）：

```text
① 用户数 48644
+--------+---+---+-------+
| user_id|  R|  F|      M|
+--------+---+---+-------+
|u0000006| 17|  1|  21.40|
|u0000008| 16|  2|3256.80|
|u0000009| 22|  1| 582.90|
|u0000010| 19|  1| 173.90|
|u0000011|  3|  4|1445.30|
+--------+---+---+-------+
only showing top 5 rows
+-------+-----------------+------------------+-----------------+
|summary|                R|                 F|                M|
+-------+-----------------+------------------+-----------------+
|  count|            48644|             48644|            48644|
|   mean|13.02923279335581| 1.842776087492805|      2911.698026|
| stddev|8.569580387328228|3.5011886390678724|6865.638965092135|
|    min|                1|                 1|             5.00|
|    max|               30|               386|        557739.50|
+-------+-----------------+------------------+-----------------+

② 平均值：R 13.0 天，F 1.84 次，M 2911.70 元
③ 各类用户
+------------+-------+-------+-------+-----+-----+-----+-----+-----+
|segment     |R_level|F_level|M_level|users|avg_R|avg_F|avg_M|m_pct|
+------------+-------+-------+-------+-----+-----+-----+-----+-----+
|重要价值用户|高     |高     |高     |6911 |5.4  |4.62 |10432|50.9 |
|重要挽留用户|低     |低     |高     |3117 |22.0 |1.0  |6329 |13.9 |
|重要发展用户|高     |低     |高     |2424 |7.0  |1.0  |6400 |11.0 |
|重要唤回用户|低     |高     |高     |1778 |18.7 |2.41 |7434 |9.3  |
|一般价值用户|高     |高     |低     |6942 |6.1  |2.47 |996  |4.9  |
|一般挽留用户|低     |低     |低     |14031|22.0 |1.0  |467  |4.6  |
|一般发展用户|高     |低     |低     |10714|7.0  |1.0  |472  |3.6  |
|一般唤回用户|低     |高     |低     |2727 |18.9 |2.19 |936  |1.8  |
+------------+-------+-------+-------+-----+-----+-----+-----+-----+

已保存到 data/xiaoxiang_clean/rfm/
```

用户数 48644 与实验 8-1 的下单用户数一致；8 类用户的人数之和也是 48644，销售额占比之和为 100%。平均 F 是 1.84，所以“F 高”就是买了 2 次及以上，F 高的四类用户合计 18358 人，正是实验 8-1 的复购用户数。

**实验记录表**

| 观察内容 | 你的结果 |
|---|---|
| 用户数；R、F、M 的平均值 | |
| F 的最大值，你认为这是什么样的用户 | |
| 人数最多的一类、销售额占比最高的一类 | |
| “重要价值用户”的人数占比和销售额占比 | |
| 你建议优先给哪几类用户发券，为什么 | |

**常见问题排查**

| 现象 | 原因 | 解决办法 |
|---|---|---|
| R、F、M 三列的值变成了“高”“低” | Spark 列名不区分大小写，新列叫 `r`、`f`、`m` 会覆盖 `R`、`F`、`M` | 新列换个名字，如 `R_level` |
| `[CAST_INVALID_INPUT] The value '低' ... cannot be cast to "DOUBLE"` | 同上，`M` 列被覆盖成文字后再求和 | 同上 |
| R 出现负数 | 分析日写得比最后一次下单还早 | 分析日取数据最后一天的第二天 |
| `segment` 列全是 NULL | 字典的键与 `concat` 出来的字符串不一致 | 检查“高”“低”两字和拼接顺序 R、F、M |

**思考与练习**

1. 把分层的分界线从平均值改成中位数（`F.percentile_approx("M", 0.5)`），各类用户的人数会怎样变化？为什么 M 的中位数比平均值小得多？
2. 先把 F 大于 50 的用户排除，再重新分层，“重要价值用户”的人数有什么变化？

**实验报告要求**

提交程序输出截图、实验记录表，以及一段不少于 100 字的发券建议。

### 实验 8-3 ☆ 读写 Hive 与 MySQL，比较 pandas 和 Spark 的耗时

**实验目的**

1. 用 Spark 读 Hive 表，把统计结果写回 Hive，并写进 MySQL；
2. 在本机用 pandas 读取 MySQL 中的结果并画图；
3. 用不同大小的数据比较 pandas 和 Spark 的耗时与内存，总结两者的适用场合。

**实验环境**：随书 Hadoop 集群 + 第 5 章的 Hive 容器 + spark 容器 + 本章的 MySQL 容器（`docker-compose.mysql.yml`）；本机 Python 需另外安装 `pymysql`、`sqlalchemy`、`matplotlib`（见 `labs/requirements.txt`）。

**实验数据**：第 5 章实验 5-1 建的 Hive 表 `xiaoxiang.ods_orders`、`ods_products`；实验 1-1 生成的 `labs/data/scale/orders_*.csv`。

**实验步骤与代码**

第 1 步，下载 MySQL 的 JDBC 驱动，放到 `labs/jars/` 目录。在本机 `labs` 目录下执行（地址很长，注意不要换行）：

```bash
mkdir jars
# Linux、macOS：
curl -o jars/mysql-connector-j-8.4.0.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar
# Windows PowerShell：
Invoke-WebRequest -Uri https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar -OutFile jars\mysql-connector-j-8.4.0.jar
```

下载慢的话，可以把地址中的 `https://repo1.maven.org/maven2/` 换成阿里云镜像 `https://maven.aliyun.com/repository/public/`，也可以用浏览器打开地址下载。文件约 2.5 MB。

第 2 步，启动全部容器（Hive 要等约 1 分钟才能连上）：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.hive.yml -f docker-compose.spark.yml -f docker-compose.mysql.yml up -d
```

第 3 步，运行读写 Hive 与 MySQL 的程序：

```bash
docker compose -f docker-compose.yml -f docker-compose.spark.yml exec spark spark-submit --jars /labs/jars/mysql-connector-j-8.4.0.jar /labs/ch08/lab8_3_hive_mysql.py
```

第 4 步，分别在 beeline 和 MySQL 中查看写入的表：

```bash
docker compose -f docker-compose.yml -f docker-compose.hive.yml exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n root
#   SELECT category, SUM(gmv) AS gmv FROM xiaoxiang.ads_category_daily GROUP BY category ORDER BY gmv DESC LIMIT 3;
docker compose -f docker-compose.yml -f docker-compose.mysql.yml exec mysql mysql -uroot -pxiaoxiang xiaoxiang
#   SELECT category, SUM(gmv) AS gmv, SUM(orders) AS orders FROM category_daily GROUP BY category ORDER BY gmv DESC;
```

第 5 步，在本机 `labs` 目录下安装库并画图：

```bash
pip install pymysql sqlalchemy matplotlib -i https://pypi.tuna.tsinghua.edu.cn/simple
python ch08/lab8_3_plot.py
```

第 6 步，比较耗时。先在本机运行 pandas 版，再在 spark 容器中运行 Spark 版，两个程序做的是同一个统计：

```bash
python ch08/lab8_3_pandas_time.py
cd cluster
docker compose -f docker-compose.yml -f docker-compose.spark.yml exec spark spark-submit /labs/ch08/lab8_3_spark_time.py
```

**运行结果**

第 3 步用时约 16 秒，输出（表格只列前几行）：

```text
+---------+-------------------------+-----------+
|namespace|tableName                |isTemporary|
+---------+-------------------------+-----------+
|xiaoxiang|ods_users                |false      |
|xiaoxiang|dwd_order_detail         |false      |
|xiaoxiang|dws_province_day         |false      |
|xiaoxiang|ods_orders_big           |false      |
|xiaoxiang|ods_products             |false      |
|xiaoxiang|ads_province_daily_report|false      |
|xiaoxiang|ods_orders               |false      |
|xiaoxiang|orders_part              |false      |
+---------+-------------------------+-----------+

① ods_orders 行数 100001
   去掉表头后 100000
② 结果行数 240
+----------+--------+----------+------+
|        dt|category|       gmv|orders|
+----------+--------+----------+------+
|2026-09-01|图书文具|  14745.00|   147|
|2026-09-01|家居日用| 100163.00|   337|
|2026-09-01|家用电器| 720358.60|   430|
|2026-09-01|手机数码|3056515.60|   603|
|2026-09-01|服饰鞋包| 349953.90|   423|
+----------+--------+----------+------+
only showing top 5 rows
③ 已写入 Hive 表 xiaoxiang.ads_category_daily
④ 已写入 MySQL 表 category_daily，读回来 240 行
```

Hive 中能看到第 5 章建的 8 张表；结果是 30 天 × 8 个品类 = 240 行。第 4 步在 MySQL 中的查询结果：

```text
+--------------+-------------+--------+
| category     | gmv         | orders |
+--------------+-------------+--------+
| 手机数码     | 97860811.40 |  18587 |
| 家用电器     | 22053946.80 |  13056 |
| 服饰鞋包     | 10461943.70 |  13338 |
| 家居日用     |  3221405.30 |  10563 |
| 美妆个护     |  3093701.20 |   7556 |
| 母婴玩具     |  2821223.50 |   4858 |
| 食品生鲜     |  2027059.00 |  17062 |
| 图书文具     |   469955.50 |   4888 |
+--------------+-------------+--------+
```

这与第 5 章实验 5-1 用 HiveQL 统计的品类销售额完全一致（读的是同一份未弄脏的原始订单），比实验 8-1 用清洗后数据算出的略高，因为清洗时删掉了 300 个异常订单。beeline 中查 `ads_category_daily` 的前 3 名也是同样的数字。

第 5 步生成的图片见 8.5.3 节。第 6 步的输出（编者各运行了两次，取第二次）：

```text
   10,000 行：读入  0.02 秒，统计  0.00 秒，合计  0.02 秒，占内存    4.6 MB，第一名 广东 1,313,195.80
  100,000 行：读入  0.17 秒，统计  0.02 秒，合计  0.19 秒，占内存   45.5 MB，第一名 广东 13,021,535.20
1,000,000 行：读入  1.79 秒，统计  0.44 秒，合计  2.22 秒，占内存  455.2 MB，第一名 广东 135,808,057.10
3,000,000 行：读入  5.69 秒，统计  0.68 秒，合计  6.37 秒，占内存 1365.5 MB，第一名 广东 399,436,862.70
```

```text
启动 Spark 用时 2.9 秒
   10,000 行：合计  4.02 秒，第一名 广东 1,313,195.80
  100,000 行：合计  1.03 秒，第一名 广东 13,021,535.20
1,000,000 行：合计  1.98 秒，第一名 广东 135,808,057.10
3,000,000 行：合计  2.83 秒，第一名 广东 399,436,862.70
```

两边算出的结果完全相同。耗时每次会有些差别（编者第一次运行 pandas 时，100 万行用了 4.7 秒），但规律不变：数据量小时 pandas 快，数据量大时 Spark 快，分界点大约在 100 万行。8.6 节的表格就是根据这次结果整理的。

**实验记录表**

| 观察内容 | 你的结果 |
|---|---|
| Spark 读 `ods_orders` 的行数，去掉表头后的行数 | |
| MySQL 中 `category_daily` 的行数和销售额前 3 的品类 | |
| MySQL 为各列建的类型（`DESCRIBE category_daily;`） | |
| pandas 和 Spark 处理 1 万、100 万、300 万行的用时 | |
| pandas 处理 300 万行占用的内存 | |
| 你的电脑上两者的分界点大约在多少行 | |

**常见问题排查**

| 现象 | 原因 | 解决办法 |
|---|---|---|
| `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | 没有下载 JDBC 驱动，或 `--jars` 路径写错 | 检查 `labs/jars/` 下有 jar 文件，路径写 `/labs/jars/...` |
| `Communications link failure` | MySQL 没有启动，或刚启动还在初始化 | `docker compose ... ps` 查看，等 20 秒再试 |
| `[TABLE_OR_VIEW_NOT_FOUND] ... xiaoxiang.ods_orders` | Hive 的 Metastore 没有启动，或没有做过实验 5-1 | 启动 Hive 容器，必要时重做实验 5-1 第 3 步 |
| 读 Hive 表的行数多 1 | Spark 不认 `skip.header.line.count`，表头被当成数据 | 用 `filter` 去掉表头行 |
| MySQL 中的中文显示为 `????` | 客户端字符集不是 UTF-8 | 进入 mysql 时加 `--default-character-set=utf8mb4` |
| 本机 `pd.read_sql` 连不上 | 本机访问容器要用 `localhost:3306`，不能用容器名 `mysql` | 连接串写 `@localhost:3306` |
| 图中的中文显示为方框 | matplotlib 找不到中文字体 | 程序已依次尝试微软雅黑、黑体等；Linux 可安装文泉驿字体 |

**思考与练习**

1. 把 `lab8_3_hive_mysql.py` 中写 MySQL 的 `mode("overwrite")` 改成 `mode("append")`，连续运行两次，`category_daily` 有多少行？在什么场合应该用 `append`？
2. 本实验在 Spark 中读的是 CSV。如果 300 万行订单事先存成了 Parquet（第 7 章），你认为 pandas 和 Spark 的用时会怎样变化？说说理由。

**实验报告要求**

提交第 3、4 步的输出截图、第 5 步生成的图片、第 6 步两个程序的输出截图、实验记录表，回答思考与练习的两个问题。

## 本章小结

- **Spark SQL** 让同一个分析既可以用 DataFrame 函数写，也可以用 SQL 写，两者共用一个执行引擎。DataFrame 注册成**临时视图**后就能用 `spark.sql()` 查询，结果仍是 DataFrame。
- 常用分析：`groupBy().agg()` 分组聚合（数用户用 `countDistinct`），`orderBy().limit()` 取前几名，`groupBy().pivot().agg()` 做**透视表**，`join` 关联维度。
- **窗口函数**保留每一行、多算一列，由分组（`partitionBy`）、排序（`orderBy`）、范围（`rowsBetween`）三部分说明。`row_number`、`rank`、`dense_rank` 用于分组排名；`sum` 加“从第一行到当前行”求累计；`avg` 加“前 6 行到当前行”求 7 日**移动平均**；`lag` 取上一行算**环比**，和去年同期比是**同比**。
- 常用指标要先说清口径：**GMV**（已完成订单金额之和）、**客单价**（销售额 ÷ 下单用户数）、**复购率**（买 2 次及以上的用户占比）、**转化率**（下单用户 ÷ 访问用户）、**留存率**（之后仍活跃的比例）。9 月 GMV 1.42 亿元、客单价 2911.70 元、复购率 37.7%。
- **RFM 模型**用最近一次购买距今天数、购买次数、购买金额给用户打分，按高低组合分成 8 类，是最常用的**用户分层**方法。实验中 14% 的重要价值用户贡献了 51% 的销售额；分层前要先看极端值。
- `enableHiveSupport()` 后 Spark 可以直接读写 Hive 表，但不认 `skip.header.line.count`；通过 **JDBC** 把结果写进 MySQL，供报表和看板使用；小结果交给 pandas 画图。
- 几十万行以内用 pandas 更快；数据量上百万行、存放在 HDFS 或 Hive、需要每天定时运行的任务用 Spark。Spark 把大数据算小，pandas 把小结果画出来。

```mermaid
mindmap
  root((第 8 章<br/>Spark SQL 分析))
    两种写法
      DataFrame 函数
      SQL 语句
      临时视图
    常用分析
      groupBy agg
      countDistinct
      TopN
      透视表 pivot
    窗口函数
      partitionBy orderBy
      row_number rank dense_rank
      累计 sum
      移动平均 avg
      环比 lag 同比
    指标与分层
      GMV 客单价
      复购率 转化率 留存率
      RFM 八类用户
      口径要写清
    读写与选择
      Hive 表 enableHiveSupport
      MySQL JDBC
      pandas 画图
      pandas 还是 Spark
```

**下一章预告**

到目前为止，我们处理的都是“已经发生了的”数据：9 月的订单、9 月 30 日的日志，算一次出一份报表。可是国庆当天，老板想在大屏上看到“这一分钟卖了多少钱”，风控部门想在可疑订单付款的几秒内拦下来。这就需要实时处理：数据一边产生，一边计算。下一章学习 Kafka 和 Spark 的 Structured Streaming，把源源不断的订单当成一张不断变长的表，按分钟统计成交额，并看看迟到的数据该怎么处理。

## 参考文献

正文中的上标编号（如<sup class="cite">[[1]](#ref8-1)</sup>）对应下面的文献。带链接的可以直接打开；书籍可在图书馆或电子书平台查阅。

<div class="refs" markdown>

**教材与专著**

- <span id="ref8-9"></span>[9] CHAMBERS B, ZAHARIA M. Spark: the definitive guide: big data processing made simple[M]. Sebastopol: O'Reilly Media, 2018.
    <span class="ref-note">第 7 章“聚合”系统讲解分组、透视、窗口函数等操作，例子与本章 8.2、8.3 节对应。</span>

**经典论文**

- <span id="ref8-1"></span>[1] ARMBRUST M, XIN R S, LIAN C, et al. Spark SQL: relational data processing in Spark[C]//Proceedings of the 2015 ACM SIGMOD International Conference on Management of Data. Melbourne: ACM, 2015: 1383-1394.
    <span class="ref-note">介绍 Spark SQL 的设计：DataFrame 接口把关系查询和过程式代码结合起来，两种写法交给同一个优化器生成执行计划，对应 8.1 节。</span>
- <span id="ref8-4"></span>[4] FADER P S, HARDIE B G S, LEE K L. RFM and CLV: using iso-value curves for customer base analysis[J]. Journal of Marketing Research, 2005, 42(4): 415-430.
    <span class="ref-note">研究只用 R、F、M 三个量估计用户终身价值的方法，说明了 RFM 在用户价值分析中的地位，对应 8.4.3 节。</span>
- <span id="ref8-8"></span>[8] LI J, HUI B, QU G, et al. Can LLM already serve as a database interface? A BIg bench for large-scale database grounded text-to-SQLs[C]//Advances in Neural Information Processing Systems 36 (NeurIPS 2023), Datasets and Benchmarks Track. 2023.
    <span class="ref-note">BIRD 评测集：95 个数据库、12751 个问题，GPT-4 的执行准确率为 54.89%，人类为 92.96%，并指出数据库中的取值和业务知识是难点，对应 8.7 节。</span>

**在线资源与数据**

- <span id="ref8-2"></span>[2] Apache Software Foundation. Spark SQL, DataFrames and Datasets guide (Spark 4.1.4)[EB/OL]. [2026-10-31]. <https://spark.apache.org/docs/4.1.4/sql-programming-guide.html>.
    <span class="ref-note">Spark SQL 的官方指南：可以用 SQL 或 DataFrame 接口与 Spark SQL 交互，无论用哪种写法都使用同一个执行引擎，对应 8.1 节。</span>
- <span id="ref8-3"></span>[3] Apache Software Foundation. Window functions (Spark 4.1.4)[EB/OL]. [2026-10-31]. <https://spark.apache.org/docs/4.1.4/sql-ref-syntax-qry-select-window.html>.
    <span class="ref-note">窗口函数的语法：排名函数 ROW_NUMBER、RANK、DENSE_RANK，分析函数 LAG、LEAD，以及 ROWS BETWEEN 范围的写法，对应 8.3 节。</span>
- <span id="ref8-5"></span>[5] Apache Software Foundation. Hive tables (Spark 4.1.4)[EB/OL]. [2026-10-31]. <https://spark.apache.org/docs/4.1.4/sql-data-sources-hive-tables.html>.
    <span class="ref-note">用 `enableHiveSupport()` 读写 Hive 表，以及 `spark.sql.hive.metastore.version`、`spark.sql.hive.metastore.jars` 等连接不同版本 Metastore 的配置，对应 8.5.1 节和附录 C。</span>
- <span id="ref8-6"></span>[6] Apache Software Foundation. JDBC to other databases (Spark 4.1.4)[EB/OL]. [2026-10-31]. <https://spark.apache.org/docs/4.1.4/sql-data-sources-jdbc.html>.
    <span class="ref-note">通过 JDBC 读写其他数据库：驱动 jar 要放进 Spark 的类路径，`read.jdbc`、`write.jdbc` 的用法和各项参数，对应 8.5.2 节和实验 8-3。</span>
- <span id="ref8-7"></span>[7] pandas development team. Scaling to large datasets[EB/OL]. [2026-10-31]. <https://pandas.pydata.org/docs/user_guide/scale.html>.
    <span class="ref-note">pandas 官方文档对大数据量的建议：只读需要的列、使用更省内存的类型、分块处理，或改用支持并行和分布式计算的库，对应 8.6 节。</span>
- <span id="ref8-10"></span>[10] 林子雨. Spark 编程基础（Python 版）：第 5 章 Spark SQL[EB/OL]. 厦门大学数据库实验室，2020. [2026-10-31]. <https://dblab.xmu.edu.cn/wp-content/uploads/2019/12/Chapter5-厦门大学-林子雨-Spark编程基础Python版-第5章-Spark-SQL（2020年1月版）.pdf>.
    <span class="ref-note">Spark SQL 的中文课件，包括 DataFrame 的创建、保存、常用操作，以及用 JDBC 读写 MySQL，可作为本章的补充练习，对应 8.7 节。</span>

</div>

## 课后练习

选择题和判断题可以直接点击选项作答，系统会马上告诉你对错，并展开答案与解析；多选题选好后点“提交答案”。简答和实操题请先自己作答，再点开“答案与解析”核对。每道题的解析下方都有“回看”链接，做错了可以直接跳回正文复习。

**一、单项选择题**

<div class="quiz" data-answer="C" markdown>

**1.** 关于 Spark SQL 的两种写法，下列说法正确的是（　　）。

- A. SQL 写法比 DataFrame 写法快得多
- B. DataFrame 写法只能处理小数据
- C. 两种写法交给同一个执行引擎，可以混合使用
- D. 用 SQL 写法必须先把数据存进 Hive

</div>

??? tip "答案与解析"
    **答案：C**

    DataFrame 函数和 SQL 语句最终由同一个执行引擎优化执行，速度没有差别；DataFrame 注册成临时视图就能用 SQL 查，不需要存进 Hive。

    考查：Spark SQL · 认知层次：理解 · [回看 8.1.1](#811-spark-sql)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**2.** 要在 `spark.sql("SELECT ... FROM orders")` 中查询 DataFrame `done`，事先要执行（　　）。

- A. `done.cache()`
- B. `done.createOrReplaceTempView("orders")`
- C. `done.write.saveAsTable("orders")`
- D. `spark.table("orders")`

</div>

??? tip "答案与解析"
    **答案：B**

    `createOrReplaceTempView` 把 DataFrame 注册成临时视图 orders，只在本程序中有效。`saveAsTable` 会真的写成 Hive 表，`spark.table` 是读表。

    考查：临时视图 · 认知层次：应用 · [回看 8.1.2](#812)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**3.** 计算每个品类的下单用户数，应该用（　　）。

- A. `F.count("*")`
- B. `F.count("user_id")`
- C. `F.sum("user_id")`
- D. `F.countDistinct("user_id")`

</div>

??? tip "答案与解析"
    **答案：D**

    同一个用户可能下多个订单，`count` 数的是订单行数，`countDistinct` 才是不同用户的个数。

    考查：分组聚合 · 认知层次：应用 · [回看 8.2.1](#821)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**4.** `done.groupBy("level").pivot("category").agg(F.sum("amount"))` 得到的结果是（　　）。

- A. 每个会员等级一行，每个品类一列，格子里是销售额
- B. 每个品类一行，每个会员等级一列
- C. 每个订单一行，多出一列销售额
- D. 只有一行，是全部销售额

</div>

??? tip "答案与解析"
    **答案：A**

    `groupBy` 的列成为行，`pivot` 的列的每个取值展开成一列，`agg` 算格子里的值。

    考查：透视表 · 认知层次：理解 · [回看 8.2.3](#823)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**5.** 窗口函数与 GROUP BY 最主要的区别是（　　）。

- A. 窗口函数只能在 Hive 中使用
- B. 窗口函数不能分组
- C. 窗口函数保留每一行，只多算一列；GROUP BY 把一组合成一行
- D. 窗口函数不能排序

</div>

??? tip "答案与解析"
    **答案：C**

    窗口函数可以分组（PARTITION BY）和排序（ORDER BY），但不会减少行数，每一行都多出一列结果。

    考查：窗口函数 · 认知层次：理解 · [回看 8.3](#83)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**6.** 销售额为 300、200、200、100 的四家店铺，按销售额从高到低用 `rank()` 排名，结果依次是（　　）。

- A. 1、2、3、4
- B. 1、2、2、4
- C. 1、2、2、3
- D. 1、1、2、3

</div>

??? tip "答案与解析"
    **答案：B**

    `rank` 并列的名次相同，后面跳号；`row_number` 是 1、2、3、4；`dense_rank` 是 1、2、2、3。

    考查：窗口函数 · 认知层次：应用 · [回看 8.3](#83)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**7.** 计算 7 日移动平均，窗口范围应写成（　　）。

- A. `rowsBetween(0, 6)`
- B. `rowsBetween(Window.unboundedPreceding, 0)`
- C. `rowsBetween(-7, 0)`
- D. `rowsBetween(-6, 0)`

</div>

??? tip "答案与解析"
    **答案：D**

    前 6 行加当前行共 7 行。`(-7, 0)` 是 8 行；`(0, 6)` 是当前行和后 6 行；`unboundedPreceding` 到 0 是累计。

    考查：移动平均 · 认知层次：应用 · [回看 8.3](#83)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**8.** 9 月销售额 141,636,638.80 元，下单用户 48644 人，订单 89640 笔，客单价约为（　　）。

- A. 2911.70 元
- B. 1580.06 元
- C. 48644 元
- D. 1.84 元

</div>

??? tip "答案与解析"
    **答案：A**

    客单价 = 销售额 ÷ 下单用户数 = 141,636,638.80 ÷ 48644 ≈ 2911.70 元；1580.06 元是笔单价（销售额 ÷ 订单数）。

    考查：客单价 · 认知层次：应用 · [回看 8.4.1](#841)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**9.** RFM 分层中，“R 低、F 低、M 高”的用户是“重要挽留用户”，对这类用户较合适的做法是（　　）。

- A. 不用管，他们会自己回来
- B. 只推送新人优惠券
- C. 用大额优惠券、专属提醒把他们召回
- D. 从用户表中删除

</div>

??? tip "答案与解析"
    **答案：C**

    这类用户花钱多，但很久没来、只买过一次，流失风险高、价值大，值得花成本召回。新人券适合“一般发展用户”。

    考查：RFM 模型 · 认知层次：应用 · [回看 8.4.3](#843-rfm)
    { .quiz-meta }

**二、多项选择题**

<div class="quiz quiz-multi" data-answer="ABD" markdown>

**1.** 下列 SQL 与 DataFrame 写法对应正确的有（　　）。

- A. `WHERE amount > 0` 对应 `.filter(F.col("amount") > 0)`
- B. `GROUP BY province` 加聚合对应 `.groupBy("province").agg(...)`
- C. `ORDER BY gmv DESC` 对应 `.orderBy("gmv")`
- D. `LIMIT 10` 对应 `.limit(10)`

</div>

??? tip "答案与解析"
    **答案：ABD**

    `.orderBy("gmv")` 默认从小到大，从大到小要写 `.orderBy(F.desc("gmv"))`。

    考查：两种写法 · 认知层次：应用 · [回看 8.1.2](#812)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="BCD" markdown>

**2.** 下列需求中，适合用窗口函数完成的有（　　）。

- A. 计算 9 月的总销售额
- B. 每个省份销售额前 3 的品类
- C. 截至每一天的累计销售额
- D. 每天销售额的环比

</div>

??? tip "答案与解析"
    **答案：BCD**

    总销售额是一个数，用 `agg(F.sum(...))` 即可；分组排名、累计、环比都要在保留每一行的同时“看到”其他行，适合窗口函数。

    考查：窗口函数 · 认知层次：分析 · [回看 8.3](#83)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ACD" markdown>

**3.** 用 Spark 读写 Hive 表和 MySQL 时，下列说法正确的有（　　）。

- A. 创建 SparkSession 时要调用 `enableHiveSupport()`
- B. Spark 会自动跳过 Hive 文本表的表头行
- C. 写 MySQL 要提供 JDBC 驱动，运行时用 `--jars` 指定
- D. 用 `mode("overwrite")` 写 MySQL 时，Spark 会按 DataFrame 的类型重新建表

</div>

??? tip "答案与解析"
    **答案：ACD**

    Spark 不认 Hive 表的 `skip.header.line.count`，实验 8-3 中读出 100001 行，要自己去掉表头。

    考查：读写 Hive 与 MySQL · 认知层次：理解 · [回看 8.5.1](#851-hive)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="AB" markdown>

**4.** 根据实验 8-3 的结果，下列判断正确的有（　　）。

- A. 1 万行的数据，pandas 比 Spark 快得多
- B. 300 万行的数据，Spark 比 pandas 快，pandas 占用内存已超过 1 GB
- C. 无论数据多大，都应该用 Spark
- D. Spark 算出的小结果不能再交给 pandas 处理

</div>

??? tip "答案与解析"
    **答案：AB**

    小数据时 Spark 的调度开销比计算本身还大；大数据时 Spark 并行、且不必把数据全部装进内存。大数据用 Spark 算小，小结果交给 pandas 画图，是常见的分工。

    考查：pandas 与 Spark · 认知层次：评价 · [回看 8.6](#86-pandas-spark)
    { .quiz-meta }

**三、判断题**

<div class="quiz quiz-judge" data-answer="√" markdown>

**1.** `spark.sql(...)` 的返回值是一个 DataFrame，可以继续调用 `filter`、`show` 等函数。

</div>

??? tip "答案与解析"
    **答案：√**

    SQL 和 DataFrame 可以随时互相切换。

    考查：Spark SQL · 认知层次：理解 · [回看 8.1.2](#812)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**2.** 用 `F.lag("gmv", 1)` 计算环比时，第一天的环比结果是 0。

</div>

??? tip "答案与解析"
    **答案：×**

    第一天前面没有行，`lag` 得到 NULL，环比也是 NULL，而不是 0。

    考查：环比 · 认知层次：理解 · [回看 8.3](#83)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**3.** 复购率 = 订单数 ÷ 下单用户数。

</div>

??? tip "答案与解析"
    **答案：×**

    复购率是购买 2 次及以上的用户数 ÷ 下单用户数；订单数 ÷ 下单用户数是人均订单数（实验 8-2 中平均 F 为 1.84）。

    考查：复购率 · 认知层次：理解 · [回看 8.4.1](#841)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**4.** 在 DataFrame 中新建一列叫 `m`，会覆盖已有的列 `M`，因为 Spark 的列名默认不区分大小写。

</div>

??? tip "答案与解析"
    **答案：√**

    实验 8-2 的常见问题之一。新列要换一个不重复的名字，如 `M_level`。

    考查：RFM 模型 · 认知层次：应用 · [回看 实验 8-2](#8-2-rfm)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**5.** 有了 Text-to-SQL，数据分析人员不再需要了解指标口径和字段含义。

</div>

??? tip "答案与解析"
    **答案：×**

    BIRD 评测中 GPT-4 只有约 55% 的 SQL 能得到正确结果，难点正是业务口径和字段取值。要先把口径整理成文档，并由懂业务的人检查生成的 SQL。

    考查：Text-to-SQL · 认知层次：评价 · [回看 8.7](#87)
    { .quiz-meta }

**四、简答题**

**1.** 用自己的话说明 `row_number`、`rank`、`dense_rank` 的区别，并各举一个适合使用的场合。

??? tip "答案与解析"
    **参考答案：** 三者都是在分组内按顺序编号，区别在遇到并列时：`row_number` 不管并列，依次编 1、2、3、4；`rank` 并列的名次相同，后面跳号，如 1、2、2、4；`dense_rank` 并列的名次相同，后面不跳号，如 1、2、2、3。场合：每个省只取 3 个品类做展示，用 `row_number`，保证每组正好 3 行；评选销售冠军，并列的都要获奖、下一名按实际名次计，用 `rank`；只想知道“销售额处在第几档”，用 `dense_rank`。

    **评分要点：** 三者区别 3 分，场合合理 3 分。

    考查：窗口函数 · 认知层次：理解 · [回看 8.3](#83)
    { .quiz-meta }

**2.** 什么是 RFM 模型？说明实验 8-2 的分层步骤，并说说为什么分层前要先看极端值。

??? tip "答案与解析"
    **参考答案：** RFM 模型用三个量衡量用户价值：R 是最近一次购买距今天数（越小越好），F 是购买次数，M 是购买金额（越大越好）。步骤：① 按用户分组，算出每个用户的 R、F、M；② 每个指标和平均值比较，分出高低；③ 按高低组合分成 8 类用户，统计各类人数和销售额占比，制定运营策略。极端值会把平均值拉高或拉低，从而移动分层的分界线：实验中有一个用户一个月买了 386 次，可能是企业采购，应先单独处理，再对其余用户分层。

    **评分要点：** R、F、M 含义 2 分，三个步骤 2 分，极端值的影响 2 分。

    考查：RFM 模型 · 认知层次：理解 · [回看 8.4.3](#843-rfm)
    { .quiz-meta }

**3.** 一个数据任务，原始数据有 5000 万行、存放在 HDFS 上，结果是 30 行的每日汇总，最后要画成折线图。你会怎样分配 Spark 和 pandas 的工作？说明理由。

??? tip "答案与解析"
    **参考答案：** 用 Spark 读 HDFS 上的 5000 万行数据，完成清洗、聚合，得到 30 行的汇总结果，写进 MySQL 或保存成小文件；再用 pandas 读这 30 行，用 matplotlib 画折线图。理由：5000 万行数据装进 pandas 需要十几到几十 GB 内存，普通电脑装不下，而且 pandas 只用一个核，Spark 可以并行、可以提交到集群，也能直接读 HDFS；而 30 行的结果用 Spark 处理反而有调度开销，pandas 画图更方便。

    **评分要点：** 分工正确 3 分，从数据量、内存、并行、便利性说明理由 3 分。

    考查：pandas 与 Spark · 认知层次：评价 · [回看 8.6](#86-pandas-spark)
    { .quiz-meta }

**五、实操题**

**1.** 用实验 7-3 的宽表，统计每种支付方式的销售额、订单数和笔单价（只算已完成的订单），分别用 DataFrame 写法和 SQL 写法完成，按销售额从高到低排列。写出代码和结果。

??? tip "答案与解析"
    **参考答案：**

    ```python
    from pyspark.sql import functions as F
    wide = spark.read.parquet("file:///labs/data/xiaoxiang_clean/order_wide")
    done = wide.filter(F.col("status") == "已完成")
    (done.groupBy("pay_type")
         .agg(F.round(F.sum("amount"), 2).alias("gmv"), F.count("*").alias("orders"))
         .withColumn("per_order", F.round(F.col("gmv") / F.col("orders"), 2))
         .orderBy(F.desc("gmv")).show())

    done.createOrReplaceTempView("orders")
    spark.sql("""
        SELECT pay_type, ROUND(SUM(amount), 2) AS gmv, COUNT(*) AS orders,
               ROUND(SUM(amount) / COUNT(*), 2) AS per_order
        FROM orders GROUP BY pay_type ORDER BY gmv DESC
    """).show()
    ```

    编者的结果（两种写法相同）：

    ```text
    +--------+-----------+------+---------+
    |pay_type|        gmv|orders|per_order|
    +--------+-----------+------+---------+
    |微信支付|63394401.40| 40214|  1576.43|
    |  支付宝|53422235.10| 34090|  1567.09|
    |花呗分期|14125219.90|  8789|  1607.15|
    |  银行卡|10278661.90|  6272|  1638.82|
    |    未知|  416120.50|   275|  1513.17|
    +--------+-----------+------+---------+
    ```

    “未知”是第 7 章清洗时填补的支付方式。5 行的订单数之和为 89640，销售额之和为 141,636,638.80 元，与实验 8-1 一致。

    **评分要点：** DataFrame 写法 2 分，SQL 写法 2 分，结果正确并能用总数核对 2 分。

    考查：两种写法 · 认知层次：应用 · [回看 8.1.2](#812)
    { .quiz-meta }

**2.** 在实验 8-1 程序的基础上，计算 9 月每天的下单用户数，以及它的 7 日移动平均，找出下单用户数最多的一天。写出新增的代码和结论。

??? tip "答案与解析"
    **参考答案：**

    ```python
    w = Window.orderBy("dt")
    users_daily = (done.groupBy("dt").agg(F.countDistinct("user_id").alias("buyers"))
                       .withColumn("ma7", F.round(F.avg("buyers").over(w.rowsBetween(-6, 0)), 1)))
    users_daily.orderBy("dt").show(30)
    users_daily.orderBy(F.desc("buyers")).show(1)
    ```

    编者的结果：每天的下单用户数在 2624～2956 人之间，最多的是 9 月 15 日（2956 人，当天的 7 日移动平均为 2812.7 人）。注意每天的用户数不能相加得到月下单用户数（48644），因为同一个用户可能在多天下单。

    **评分要点：** 每日用户数用 `countDistinct` 2 分，移动平均窗口正确 2 分，找出最多的一天并说明不能相加 2 分。

    考查：窗口函数 · 认知层次：应用 · [回看 8.3](#83)
    { .quiz-meta }
