---
title: 在线模拟卷
description: 按期末闭卷考试的题型和分值，从各章课后练习中组成的两套在线模拟卷，可以直接作答并查看解析
---

# 在线模拟卷

本课程期末为闭卷考试。下面两套模拟卷按期末考试的题型和分值，从各章课后练习中抽题组成，覆盖全书 12 章：

| 题型 | 题数 | 每题分值 | 合计 |
|---|---|---|---|
| 单项选择题 | 20 | 2 | 40 |
| 多项选择题 | 5 | 4 | 20 |
| 判断题 | 10 | 1 | 10 |
| 简答题 | 3 | 10 | 30 |

建议限时 90 分钟，先独立做完一整套，再展开“答案与解析”核对。简答题解析中的“评分要点”沿用各章练习的原始分值，自评时按比例换算成 10 分。选择题和判断题可以直接点击作答；每道题解析下方的“回看”链接可以跳回对应章节复习。

## 模拟卷一

**一、单项选择题**（每题 2 分）

<div class="quiz" data-answer="C" markdown>

**1.** 大促期间，运营要求在大屏上每隔几秒更新一次成交额。这主要体现了大数据 5V 特征中的（　　）。

- A. Volume 量大
- B. Variety 多样
- C. Velocity 快
- D. Veracity 真实性

</div>

??? tip "答案与解析"
    **答案：C**

    Velocity 既指数据产生得快，也指要求处理得快。每隔几秒更新成交额，需要实时处理（第 9 章）。

    考查：5V 特征 · 认知层次：理解 · [回看 1.1.2](chapters/01-overview/index.md#112-5v)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**2.** 要查看 `access.log` 文件一共有多少行，应使用的命令是（　　）。

- A. `head access.log`
- B. `wc -l access.log`
- C. `grep access.log`
- D. `df -h access.log`

</div>

??? tip "答案与解析"
    **答案：B**

    `wc -l` 数行数。`head` 显示开头几行，`grep` 查找包含某个词的行，`df -h` 查看磁盘空间。

    考查：Linux 常用命令 · 认知层次：记忆 · [回看 2.1](chapters/02-environment/index.md#21-linux-15)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**3.** 在 HDFS 中，记录“每个文件由哪些块组成、每个块存在哪几台机器上”的是（　　）。

- A. NameNode
- B. DataNode
- C. ResourceManager
- D. HttpFS

</div>

??? tip "答案与解析"
    **答案：A**

    NameNode 管理元数据，相当于图书馆的总目录台；DataNode 只负责存取数据块。

    考查：NameNode · 认知层次：记忆 · [回看 3.2.1](chapters/03-hdfs/index.md#321-namenode-datanode)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**4.** 在词频统计作业中，Map 函数处理“耳机 耳机 手机”这一行，输出的是（　　）。

- A. (耳机, 2) (手机, 1)
- B. (耳机, 1) (耳机, 1) (手机, 1)
- C. (耳机 耳机 手机, 1)
- D. (1, 耳机) (1, 耳机) (1, 手机)

</div>

??? tip "答案与解析"
    **答案：B**

    Map 只负责“拆”，每遇到一个词就输出一个 (词, 1)，相同的词由 Reduce 汇总。A 是加了 Combiner 以后的结果；C 把整行当成了一个键；D 把键和值写反了。

    考查：Map 阶段 · 认知层次：理解 · [回看 4.2.2](chapters/04-mapreduce-yarn/index.md#422)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**5.** 关于 Hive，下列说法正确的是（　　）。

- A. Hive 是一种新的分布式文件系统，用来代替 HDFS
- B. Hive 让人用类似 SQL 的语言查询 HDFS 上的数据，在背后把 SQL 翻译成计算任务
- C. Hive 适合处理“查一个用户的订单、毫秒级返回”这类请求
- D. 用 Hive 之前必须把数据从 HDFS 导出到 MySQL

</div>

??? tip "答案与解析"
    **答案：B**

    Hive 本身不存数据，数据仍是 HDFS 上的文件；它适合批量统计，不适合毫秒级的单条查询。

    考查：Hive · 认知层次：理解 · [回看 5.1.1](chapters/05-hive/index.md#511-sql-hdfs)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**6.** Spark 比 MapReduce 快，最主要的原因是（　　）。

- A. Spark 只能处理很小的数据
- B. Spark 不需要读取 HDFS 上的数据
- C. 多个步骤连成一个流程，中间结果尽量放在内存中，不必每一步都写进 HDFS
- D. Spark 用 Python 编写，Python 比 Java 快

</div>

??? tip "答案与解析"
    **答案：C**

    MapReduce 每个作业的结果都要写进 HDFS，下一个作业再读出来；Spark 把多个步骤组织在一个流程中，中间数据尽量留在内存里。Spark 本身主要用 Scala 编写，与 Python 快慢无关。

    考查：Spark 与 MapReduce · 认知层次：理解 · [回看 6.1.1](chapters/06-spark-rdd/index.md#611-mapreduce)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**7.** 订单表中同一个省份既有“广东”又有“广东省”，这属于哪类数据质量问题？（　　）

- A. 缺失值
- B. 格式不一
- C. 重复值
- D. 异常值

</div>

??? tip "答案与解析"
    **答案：B**

    同一个东西有两种写法，是格式不一，会让分组统计把一个省份拆成两行，影响数据的一致性和规范性。

    考查：脏数据 · 认知层次：理解 · [回看 7.1.1](chapters/07-data-cleaning/index.md#711)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**8.** 关于 Spark SQL 的两种写法，下列说法正确的是（　　）。

- A. SQL 写法比 DataFrame 写法快得多
- B. DataFrame 写法只能处理小数据
- C. 两种写法交给同一个执行引擎，可以混合使用
- D. 用 SQL 写法必须先把数据存进 Hive

</div>

??? tip "答案与解析"
    **答案：C**

    DataFrame 函数和 SQL 语句最终由同一个执行引擎优化执行，速度没有差别；DataFrame 注册成临时视图就能用 SQL 查，不需要存进 Hive。

    考查：Spark SQL · 认知层次：理解 · [回看 8.1.1](chapters/08-spark-sql/index.md#811-spark-sql)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**9.** 下列需求中，最需要用实时处理来做的是（　　）。

- A. 每月给财务部门的各省销售额报表
- B. 大促期间每几秒刷新一次的交易大屏
- C. 统计上一年度每个品类的退货率
- D. 每周一发给店长的上周销量排行

</div>

??? tip "答案与解析"
    **答案：B**

    A、C、D 都是“算过去”，第二天或更晚出结果完全够用，用批处理更简单。交易大屏要几秒内反映最新的成交，晚了就没有意义，适合实时处理。

    考查：实时处理 · 认知层次：理解 · [回看 9.1.1](chapters/09-streaming/index.md#911)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**10.** 下列问题中，属于聚类问题的是（　　）。

- A. 预测一个用户下周会不会复购
- B. 预测下个月全平台的销售额
- C. 在没有任何类别标注的情况下，把用户分成几群
- D. 判断一条评价是不是刷评

</div>

??? tip "答案与解析"
    **答案：C**

    A、D 有“是 / 否”的标签，是分类；B 预测的是数值，是回归；C 没有标签，让算法自己把相似的用户归到一起，是聚类。

    考查：机器学习 · 认知层次：理解 · [回看 10.1.1](chapters/10-mllib/index.md#1011)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**11.** 一个 Spark 阶段有 200 个任务，其中 199 个在 1 秒内完成，最后 1 个用了 5 分钟。这个阶段的用时大约是（　　）。

- A. 1 秒
- B. 5 分钟
- C. 200 个任务用时的平均值
- D. 200 个任务用时的中位数

</div>

??? tip "答案与解析"
    **答案：B**

    一个阶段的所有任务都完成，下一个阶段才能开始，所以阶段的用时取决于最慢的那个任务。

    考查：Spark UI · 认知层次：理解 · [回看 11.1.1](chapters/11-tuning/index.md#1111)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**12.** 要向老板展示 9 月每天销售额的变化，最合适的图表是（　　）。

- A. 饼图
- B. 漏斗图
- C. 折线图
- D. 散点图

</div>

??? tip "答案与解析"
    **答案：C**

    看数值随时间怎么变，属于“趋势”，用折线图，横轴是时间。

    考查：数据可视化 · 认知层次：应用 · [回看 12.1.1](chapters/12-visualization-project/index.md#1211)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**13.** 下列小象优选的数据中，属于半结构化数据的是（　　）。

- A. 订单表
- B. 网站访问日志
- C. 商品图片
- D. 客服通话录音

</div>

??? tip "答案与解析"
    **答案：B**

    访问日志每行有固定的字段顺序，但不是现成的表格，要先按空格切开才能当表格用，属于半结构化数据。订单表是结构化数据，图片和录音是非结构化数据。

    考查：数据类型 · 认知层次：理解 · [回看 1.1.4](chapters/01-overview/index.md#114)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**14.** 关于 Docker 镜像和容器，下列说法正确的是（　　）。

- A. 一个镜像只能启动一个容器
- B. 删除容器时，用来启动它的镜像也会被删除
- C. 同一个镜像可以启动多个互不干扰的容器
- D. 容器是只读的，镜像可以随意修改

</div>

??? tip "答案与解析"
    **答案：C**

    镜像是只读的模具，可以启动任意多个容器；删除容器不影响镜像。随书集群就是用同一个 Hadoop 镜像启动的 3 个容器。

    考查：镜像与容器 · 认知层次：理解 · [回看 2.2.2](chapters/02-environment/index.md#222)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**15.** 块大小为 128 MB，一个 300 MB 的文件在 HDFS 中被切成（　　）。

- A. 2 块，每块 150 MB
- B. 3 块，每块 128 MB
- C. 3 块，前两块 128 MB，最后一块 44 MB
- D. 300 块

</div>

??? tip "答案与解析"
    **答案：C**

    300 ÷ 128 ≈ 2.34，向上取整为 3 块。前两块各 128 MB，最后一块 300 − 256 = 44 MB，只占用 44 MB 的空间。

    考查：数据块 · 认知层次：应用 · [回看 3.2.3](chapters/03-hdfs/index.md#323)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**16.** 一个 MapReduce 作业中，通常最费时、也最值得优化的阶段是（　　）。

- A. 切分输入分片
- B. Map 阶段
- C. Shuffle 阶段
- D. 把结果写成 part 文件

</div>

??? tip "答案与解析"
    **答案：C**

    Shuffle 要把 Map 的全部输出写磁盘、经网络传给 Reduce、再排序合并，是唯一需要机器之间大量交换数据的步骤。实验 4-3 中，不加 Combiner 时 Shuffle 要搬运 122.4 MB。

    考查：Shuffle · 认知层次：理解 · [回看 4.2.1](chapters/04-mapreduce-yarn/index.md#421)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**17.** 在 Hive 中，保存“表有哪些列、每列什么类型、数据文件在哪个目录”的是（　　）。

- A. HiveServer2
- B. DataNode
- C. Metastore
- D. Beeline

</div>

??? tip "答案与解析"
    **答案：C**

    Metastore 保存表结构等元数据；HiveServer2 接收和翻译 SQL；Beeline 是客户端；DataNode 存 HDFS 的数据块。

    考查：Metastore · 认知层次：记忆 · [回看 5.3.1](chapters/05-hive/index.md#531-hive-hdfs)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**18.** 在 Spark 程序中，运行你写的主程序、把计算拆成任务并收集结果的是（　　）。

- A. Driver
- B. Executor
- C. NameNode
- D. NodeManager

</div>

??? tip "答案与解析"
    **答案：A**

    Driver 运行主程序、拆分和分派任务；Executor 在工作节点上运行任务。NameNode、NodeManager 是 HDFS 和 YARN 的角色。

    考查：Driver · 认知层次：记忆 · [回看 6.2.1](chapters/06-spark-rdd/index.md#621-driver-executor)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**19.** 按 GB/T 36344—2018，“应该有值的字段都有值”是在评价数据质量的（　　）。

- A. 准确性
- B. 一致性
- C. 完整性
- D. 时效性

</div>

??? tip "答案与解析"
    **答案：C**

    完整性看必填的字段是否都有值；准确性看数据是否反映真实情况，一致性看同一事物在不同地方是否矛盾，时效性看数据是否及时。

    考查：数据质量 · 认知层次：记忆 · [回看 7.1.2](chapters/07-data-cleaning/index.md#712)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**20.** 要在 `spark.sql("SELECT ... FROM orders")` 中查询 DataFrame `done`，事先要执行（　　）。

- A. `done.cache()`
- B. `done.createOrReplaceTempView("orders")`
- C. `done.write.saveAsTable("orders")`
- D. `spark.table("orders")`

</div>

??? tip "答案与解析"
    **答案：B**

    `createOrReplaceTempView` 把 DataFrame 注册成临时视图 orders，只在本程序中有效。`saveAsTable` 会真的写成 Hive 表，`spark.table` 是读表。

    考查：临时视图 · 认知层次：应用 · [回看 8.1.2](chapters/08-spark-sql/index.md#812)
    { .quiz-meta }

**二、多项选择题**（每题 4 分）

<div class="quiz quiz-multi" data-answer="ABD" markdown>

**1.** 关于用集群处理数据，下列说法正确的有（　　）。

- A. 集群每次计算都要花时间启动，数据很小时可能比单机还慢
- B. 一台机器坏了，其他机器可以接着工作
- C. 机器越多，总耗时一定越短，而且会一直按比例缩短
- D. 需要专门的软件来协调各台机器

</div>

??? tip "答案与解析"
    **答案：ABD**

    C 错：启动开销是集群耗时的下限，机器加到一定数量后再加，总时间几乎不再减少（见 1.2.4 节的动画）。

    考查：横向扩展与集群 · 认知层次：理解 · [回看 1.2.4](chapters/01-overview/index.md#124)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ABD" markdown>

**2.** 关于 HDFS 的副本，下列说法正确的有（　　）。

- A. 同一个块的几份副本放在不同的机器上
- B. 副本数越多，能同时承受损坏的机器越多，占用空间也越大
- C. 副本数可以设成任意大，不受 DataNode 台数限制
- D. 已有文件的副本数可以用 `hdfs dfs -setrep` 修改

</div>

??? tip "答案与解析"
    **答案：ABD**

    C 错：同一块的副本必须放在不同的机器上，副本数大于在线的 DataNode 台数时，块会一直处于“副本不足”状态。

    考查：副本 · 认知层次：理解 · [回看 3.2.3](chapters/03-hdfs/index.md#323)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ABD" markdown>

**3.** 关于数据库和数据仓库，下列说法正确的有（　　）。

- A. 数据库主要支撑业务系统运行，数据仓库主要支撑分析决策
- B. 数据仓库通常保存多个系统汇集来的历史数据
- C. 数据仓库要求每条查询在毫秒级返回
- D. 企业常常两者都用，定期把业务库的数据同步到数据仓库

</div>

??? tip "答案与解析"
    **答案：ABD**

    C 错：数据仓库一次扫描大量数据做汇总，几秒到几分钟出结果都可以接受，毫秒级响应是数据库的要求。

    考查：数据仓库 · 认知层次：理解 · [回看 5.1.2](chapters/05-hive/index.md#512)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ABD" markdown>

**4.** 与 CSV 相比，Parquet 格式的优点有（　　）。

- A. 列式存储，查询时只读用到的列
- B. 同一列的数据放在一起，压缩率高
- C. 可以直接用记事本打开查看
- D. 自带列名和类型，读进来不用再转换

</div>

??? tip "答案与解析"
    **答案：ABD**

    Parquet 是二进制的列式存储格式，不能用记事本查看；实验 7-2 中 70.2 MB 的日志存成 Parquet 只有 19.1 MB。

    考查：Parquet · 认知层次：理解 · [回看 7.5](chapters/07-data-cleaning/index.md#75-parquet)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ABD" markdown>

**5.** 关于 Kafka，下列说法正确的有（　　）。

- A. 一个主题可以分成多个分区，分散在不同的 Broker 上
- B. 偏移量是消息在分区中的序号
- C. 消息被一个消费者组读走后就从 Kafka 中删除
- D. 不同的消费者组可以各自把同一个主题完整地读一遍

</div>

??? tip "答案与解析"
    **答案：ABD**

    Kafka 的消息读后不删除，而是按主题设定的保留时间保存，所以不同的组可以各读一遍，出了问题也能回头重读。

    考查：Kafka · 认知层次：理解 · [回看 9.2](chapters/09-streaming/index.md#92-kafka)
    { .quiz-meta }

**三、判断题**（每题 1 分）

<div class="quiz quiz-judge" data-answer="×" markdown>

**1.** 只要数据量大，就可以称为大数据，与数据的种类、产生速度无关。

</div>

??? tip "答案与解析"
    **答案：×**

    量大只是 5V 之一。数据产生快、种类多、价值密度低、真假混杂，都会让传统单机软件难以处理。

    考查：5V 特征 · 认知层次：理解 · [回看 1.1.2](chapters/01-overview/index.md#112-5v)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**2.** Docker 容器和第 4 章 YARN 中的“容器”是同一个东西。

</div>

??? tip "答案与解析"
    **答案：×**

    Docker 容器是用镜像启动的一个隔离的运行环境；YARN 的容器是分给任务的一份内存和 CPU。随书集群中，YARN 的容器运行在 worker 这个 Docker 容器里面。

    考查：Docker 容器 · 认知层次：理解 · [回看 2.2.2](chapters/02-environment/index.md#222)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**3.** NameNode 除了记录元数据，还负责保存所有文件的内容，所以它的硬盘必须特别大。

</div>

??? tip "答案与解析"
    **答案：×**

    文件内容存在 DataNode 上。NameNode 只保存元数据，而且为了查得快，元数据放在内存里，所以 NameNode 更需要的是大内存。

    考查：NameNode · 认知层次：理解 · [回看 3.2.1](chapters/03-hdfs/index.md#321-namenode-datanode)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**4.** Combiner 的输出还会被 Reduce 再处理一次，所以任何 reducer 程序都可以直接拿来当 Combiner。

</div>

??? tip "答案与解析"
    **答案：×**

    只有“分几次算和一次算结果相同”的计算（求和、计数、最大最小值）才能直接当 Combiner；求平均、去重计数都不行。

    考查：Combiner · 认知层次：理解 · [回看 4.2.4](chapters/04-mapreduce-yarn/index.md#424-combiner)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**5.** Hive 把表的数据存放在 Metastore 的数据库里，所以 Metastore 需要很大的硬盘。

</div>

??? tip "答案与解析"
    **答案：×**

    Metastore 只存表结构等元数据，数据是 HDFS 上的文件。

    考查：Metastore · 认知层次：理解 · [回看 5.3.1](chapters/05-hive/index.md#531-hive-hdfs)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**6.** 程序中有 4 个行动，运行时 Spark UI 上就会出现 4 个 Job。

</div>

??? tip "答案与解析"
    **答案：√**

    每个行动触发一个 Job。实验 6-1 的程序有 4 个行动，Jobs 页面列出 4 个 Job。

    考查：Job · 认知层次：理解 · [回看 6.2.4](chapters/06-spark-rdd/index.md#624-jobstage-task)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**7.** 数据清洗就是把看着不对的行都删掉，删得越多，数据越干净。

</div>

??? tip "答案与解析"
    **答案：×**

    清洗要先定规则：能补的补，能改的改，只有确实错误又无法修复的才删。直接 `dropna()` 会让 102000 行只剩 100681 行，删掉了 1300 多个真实订单。

    考查：数据清洗 · 认知层次：理解 · [回看 7.3.2](chapters/07-data-cleaning/index.md#732)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**8.** `spark.sql(...)` 的返回值是一个 DataFrame，可以继续调用 `filter`、`show` 等函数。

</div>

??? tip "答案与解析"
    **答案：√**

    SQL 和 DataFrame 可以随时互相切换。

    考查：Spark SQL · 认知层次：理解 · [回看 8.1.2](chapters/08-spark-sql/index.md#812)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**9.** 有了实时处理，企业就不再需要每天凌晨运行的批处理了。

</div>

??? tip "答案与解析"
    **答案：×**

    大多数报表“算昨天”就够了，批处理更简单、更省资源，还常用来把实时结果中因迟到数据而不准的部分修正过来。

    考查：实时处理 · 认知层次：评价 · [回看 9.1.1](chapters/09-streaming/index.md#911)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**10.** 模型在训练集上的成绩越高，说明它在新数据上的效果一定越好。

</div>

??? tip "答案与解析"
    **答案：×**

    模型可能把训练数据中的偶然细节也背了下来，这就是过拟合：训练集成绩很高，测试集和新数据上反而变差。判断模型效果要看测试集。

    考查：过拟合 · 认知层次：理解 · [回看 10.1.2](chapters/10-mllib/index.md#1012)
    { .quiz-meta }

**四、简答题**（每题 10 分）

**1.** 用自己的话说明 Shuffle 阶段做了哪些事，为什么它通常是作业中最费时的一步，以及 Combiner 怎样减轻它的负担。

??? tip "答案与解析"
    **参考答案：** Shuffle 位于 Map 和 Reduce 之间：每个 Map 任务把输出按键分区、在分区内排序后写到本机磁盘；每个 Reduce 任务再到所有 Map 任务那里拉取属于自己分区的数据，合并排序，让相同的键挨在一起。它费时是因为要把 Map 的全部输出写磁盘、通过网络在机器之间传输、再排序合并，数据量往往很大。Combiner 在 Map 端先把相同键的值汇总一次，减少要写和要传的记录数；实验 4-3 中，Shuffle 数据从 122.4 MB 降到 0.2 MB。

    **评分要点：** 分区排序写磁盘（1 分），Reduce 拉取合并（1 分），费时原因（2 分），Combiner 的作用并举例（2 分）。

    考查：Shuffle 与 Combiner · 认知层次：理解 · [回看 4.2.1](chapters/04-mapreduce-yarn/index.md#421)
    { .quiz-meta }

**2.** 用自己的话说明 `row_number`、`rank`、`dense_rank` 的区别，并各举一个适合使用的场合。

??? tip "答案与解析"
    **参考答案：** 三者都是在分组内按顺序编号，区别在遇到并列时：`row_number` 不管并列，依次编 1、2、3、4；`rank` 并列的名次相同，后面跳号，如 1、2、2、4；`dense_rank` 并列的名次相同，后面不跳号，如 1、2、2、3。场合：每个省只取 3 个品类做展示，用 `row_number`，保证每组正好 3 行；评选销售冠军，并列的都要获奖、下一名按实际名次计，用 `rank`；只想知道“销售额处在第几档”，用 `dense_rank`。

    **评分要点：** 三者区别 3 分，场合合理 3 分。

    考查：窗口函数 · 认知层次：理解 · [回看 8.3](chapters/08-spark-sql/index.md#83)
    { .quiz-meta }

**3.** 写出“比较、趋势、构成、分布、关系、转化”六种目的各自推荐的图表，并各举一个小象优选的例子。

??? tip "答案与解析"
    **参考答案：** 比较用柱状图，如各品类销售额；趋势用折线图，如每日销售额；构成用饼图或环形图（类别不超过 5～6 个），如各支付方式的订单占比；分布用直方图，如订单金额的分布；关系用散点图，如每天订单数与销售额；转化用漏斗图，如浏览 → 加购 → 下单 → 支付的人数。

    **评分要点：** 每种目的的图表和例子各 1 分，共 6 分。

    考查：数据可视化 · 认知层次：理解 · [回看 12.1.1](chapters/12-visualization-project/index.md#1211)
    { .quiz-meta }

## 模拟卷二

**一、单项选择题**（每题 2 分）

<div class="quiz" data-answer="D" markdown>

**1.** 一个文件大小为 2 GB，换算成 MB 是（　　）。

- A. 2000 MB
- B. 1024 MB
- C. 20480 MB
- D. 2048 MB

</div>

??? tip "答案与解析"
    **答案：D**

    1 GB = 1024 MB，2 GB = 2048 MB。日常生活中常说“1 GB 约等于 1000 MB”，但在计算机存储中按 1024 换算。

    考查：数据量单位 · 认知层次：应用 · [回看 1.1.3](chapters/01-overview/index.md#113)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**2.** 执行 `docker run -d -p 18080:80 --name web nginx:1.29` 后，在浏览器中访问 nginx 的地址是（　　）。

- A. `http://localhost:18080`
- B. `http://localhost:80`
- C. `http://localhost:1.29`
- D. `http://web:18080`

</div>

??? tip "答案与解析"
    **答案：A**

    `-p` 的格式是“宿主机端口:容器端口”，宿主机的 18080 端口转到容器的 80 端口，所以在本机浏览器访问 18080。

    考查：端口映射 · 认知层次：应用 · [回看 2.2.2](chapters/02-environment/index.md#222)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**3.** 副本数为 3，上传一个 2 GB 的文件，大约占用 HDFS 多少存储空间？（　　）

- A. 2 GB
- B. 2.67 GB
- C. 5 GB
- D. 6 GB

</div>

??? tip "答案与解析"
    **答案：D**

    每个块存 3 份，占用空间 = 文件大小 × 副本数 = 2 × 3 = 6 GB。`hdfs dfs -du` 的第二列显示的就是含副本的占用空间。

    考查：副本 · 认知层次：应用 · [回看 3.2.3](chapters/03-hdfs/index.md#323)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**4.** 小周提交了一个设置 `-D mapreduce.job.reduces=3` 的作业，作业成功后，输出目录中的 part 文件有（　　）。

- A. 1 个
- B. 2 个
- C. 和 Map 任务一样多
- D. 3 个

</div>

??? tip "答案与解析"
    **答案：D**

    每个 Reduce 任务写一个 part 文件，3 个 Reduce 就有 part-00000～part-00002 三个文件（有的可能是空文件），此外还有一个 `_SUCCESS` 标记文件。

    考查：分区与输出文件 · 认知层次：应用 · [回看 4.2.3](chapters/04-mapreduce-yarn/index.md#423-reduce)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**5.** 小象优选从业务库原样同步过来、没有做任何修改的订单数据，应该放在数据仓库的（　　）。

- A. ODS 层
- B. DWD 层
- C. DWS 层
- D. ADS 层

</div>

??? tip "答案与解析"
    **答案：A**

    ODS 层原样存放同步来的数据；清洗后的明细在 DWD 层，汇总数据在 DWS 层，给报表的结果在 ADS 层。

    考查：数仓分层 · 认知层次：理解 · [回看 5.2.1](chapters/05-hive/index.md#521)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**6.** 下列 RDD 操作中，属于行动的是（　　）。

- A. `map`
- B. `filter`
- C. `reduceByKey`
- D. `count`

</div>

??? tip "答案与解析"
    **答案：D**

    `count` 要返回一个具体的数，是行动；`map`、`filter`、`reduceByKey` 都生成新的 RDD，是转换。

    考查：转换与行动 · 认知层次：理解 · [回看 6.2.3](chapters/06-spark-rdd/index.md#623)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**7.** 读入一份含有 `¥233.8` 这类金额的脏订单 CSV，推荐的做法是（　　）。

- A. `spark.read.csv(路径, header=True)`，先全部当字符串读入，清洗时再转换类型
- B. `spark.read.csv(路径, header=True, inferSchema=True)`，让 Spark 把金额推断成小数
- C. 先用记事本把 ¥ 全部手工删掉
- D. 用 `spark.read.text` 读入，再自己按逗号切分

</div>

??? tip "答案与解析"
    **答案：A**

    列里只要有一个 `¥233.8`，`inferSchema` 就只能把整列推断成字符串，推断也没有意义；先全部按字符串读入，读入这一步不会出错，再按规则清洗和转换。手工修改无法复现，也不适合大数据。

    考查：DataFrame 读入 · 认知层次：应用 · [回看 7.2.2](chapters/07-data-cleaning/index.md#722)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**8.** 计算每个品类的下单用户数，应该用（　　）。

- A. `F.count("*")`
- B. `F.count("user_id")`
- C. `F.sum("user_id")`
- D. `F.countDistinct("user_id")`

</div>

??? tip "答案与解析"
    **答案：D**

    同一个用户可能下多个订单，`count` 数的是订单行数，`countDistinct` 才是不同用户的个数。

    考查：分组聚合 · 认知层次：应用 · [回看 8.2.1](chapters/08-spark-sql/index.md#821)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**9.** 生产者用用户编号做键发送订单消息，下列说法正确的是（　　）。

- A. 同一个用户的订单一定在同一个分区，并且在分区内按发送的先后排列
- B. 同一个用户的订单会平均分到各个分区
- C. 所有订单在整个主题内严格按时间排序
- D. 键只用于显示，不影响消息进哪个分区

</div>

??? tip "答案与解析"
    **答案：A**

    Kafka 按键的哈希值除以分区数取余决定分区，键相同就进同一个分区；分区内有序，不同分区之间不保证顺序。

    考查：分区 · 认知层次：理解 · [回看 9.2.2](chapters/09-streaming/index.md#922)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**10.** 下列情况中，最适合用 Spark MLlib 而不是 scikit-learn 的是（　　）。

- A. 班级 50 名同学的成绩，预测谁会挂科
- B. 一个 20 MB 的 CSV 文件，做一次性的分析
- C. 想尝试几十种不同的模型，数据只有几万行
- D. 几亿条用户行为记录存放在 Hive 中，预测结果还要写回 Hive

</div>

??? tip "答案与解析"
    **答案：D**

    数据一台电脑放得下就用 scikit-learn，它更简单、模型更多，在小数据上更快。D 的数据量大、本来就在集群上，结果还要写回集群，用 MLlib 可以避免把数据搬来搬去。

    考查：机器学习 · 认知层次：评价 · [回看 10.2](chapters/10-mllib/index.md#102-spark)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**11.** 下列做法中，最容易产生大量小文件的是（　　）。

- A. 流处理程序每 5 秒写一批结果，每批按 200 个分区写出
- B. 每天凌晨把当天的订单合并成一个 Parquet 文件
- C. 写结果前先 `coalesce(4)`
- D. 按天分区存放订单，每天几百 MB

</div>

??? tip "答案与解析"
    **答案：A**

    每 5 秒写 200 个文件，一天就是 345 万个文件，每个只有几 KB。B、C 是合并文件的做法，D 每个分区几百 MB，大小合适。

    考查：小文件问题 · 认知层次：应用 · [回看 11.2](chapters/11-tuning/index.md#112)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**12.** 9 月 25 日销售额 436.5 万元，28 日 502.6 万元。某同学画柱状图时让纵轴从 430 开始，28 日的柱子看起来是 25 日的 10 倍多。这张图的问题是（　　）。

- A. 柱状图的纵轴没有从 0 开始，柱子的长度不再和数值成比例
- B. 应该改用饼图
- C. 数据算错了
- D. 没有问题，这样差别看得更清楚

</div>

??? tip "答案与解析"
    **答案：A**

    502.6 只比 436.5 多 15%。柱状图靠柱子的长度表达数值，纵轴必须从 0 开始；折线图才可以不从 0 开始，但要写明起点。

    考查：误导性图表 · 认知层次：评价 · [回看 12.1.2](chapters/12-visualization-project/index.md#1212)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**13.** 下列表中，属于事实表的是（　　）。

- A. 用户表（用户编号、性别、年龄、省份）
- B. 商品表（商品编号、名称、品类、价格）
- C. 省份表（省份、所属大区）
- D. 订单表（订单编号、用户、商品、数量、金额、下单时间）

</div>

??? tip "答案与解析"
    **答案：D**

    订单表记录发生过的业务事件，每下一单多一行，带有可以累加的金额和数量，是事实表；其他三张是描述“谁、什么、在哪”的维度表。

    考查：事实表与维度表 · 认知层次：理解 · [回看 5.2.2](chapters/05-hive/index.md#522)
    { .quiz-meta }

<div class="quiz" data-answer="B" markdown>

**14.** 执行 `rdd2 = sc.textFile("access.log").map(lambda l: l.split())` 后，Spark（　　）。

- A. 立刻读完整个文件，把结果存在 rdd2 中
- B. 只记下要做的操作，还没有读文件
- C. 读了文件的第一行
- D. 报错，因为没有行动

</div>

??? tip "答案与解析"
    **答案：B**

    `textFile` 和 `map` 都是转换，惰性执行，要等行动（如 `count()`）出现时才真正读文件。实验 6-1 中定义三个转换只用了 0.6 秒。

    考查：惰性执行 · 认知层次：理解 · [回看 6.2.3](chapters/06-spark-rdd/index.md#623)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**15.** 下列 DataFrame 操作中，属于行动的是（　　）。

- A. `df.select("amount")`
- B. `df.filter(F.col("amount") > 0)`
- C. `df.withColumn("unit", F.col("amount") / F.col("quantity"))`
- D. `df.count()`

</div>

??? tip "答案与解析"
    **答案：D**

    `count` 要返回一个数，是行动；`select`、`filter`、`withColumn` 都返回新的 DataFrame，是转换。DataFrame 和 RDD 一样是惰性执行的。

    考查：DataFrame · 认知层次：理解 · [回看 7.2.3](chapters/07-data-cleaning/index.md#723)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**16.** `done.groupBy("level").pivot("category").agg(F.sum("amount"))` 得到的结果是（　　）。

- A. 每个会员等级一行，每个品类一列，格子里是销售额
- B. 每个品类一行，每个会员等级一列
- C. 每个订单一行，多出一列销售额
- D. 只有一行，是全部销售额

</div>

??? tip "答案与解析"
    **答案：A**

    `groupBy` 的列成为行，`pivot` 的列的每个取值展开成一列，`agg` 算格子里的值。

    考查：透视表 · 认知层次：理解 · [回看 8.2.3](chapters/08-spark-sql/index.md#823)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**17.** 主题 orders 有 3 个分区，“大屏组”中启动了 4 个消费者，那么（　　）。

- A. 每个消费者读 3/4 个分区
- B. 4 个消费者都读全部 3 个分区
- C. Kafka 自动把分区增加到 4 个
- D. 有 1 个消费者分不到分区，闲着

</div>

??? tip "答案与解析"
    **答案：D**

    组内一个分区只分给一个消费者，3 个分区最多让 3 个消费者同时干活，多出来的只能闲着。想用更多消费者，要先增加分区。

    考查：消费者组 · 认知层次：应用 · [回看 9.2.3](chapters/09-streaming/index.md#923)
    { .quiz-meta }

<div class="quiz" data-answer="A" markdown>

**18.** 会员等级有普通、白银、黄金、钻石 4 种，用 `StringIndexer` 编号以后还要再做独热编码，原因是（　　）。

- A. 避免模型把编号 0、1、2、3 当成有大小、有倍数关系的数
- B. 独热编码能让数据量变小
- C. 模型不接受整数，只接受 0 和 1
- D. 独热编码可以去掉重复的数据

</div>

??? tip "答案与解析"
    **答案：A**

    编号只是“代号”，钻石编 3 不代表它是白银（1）的 3 倍。独热编码把每种取值变成单独的一位，各等级之间不再有大小关系。

    考查：独热编码 · 认知层次：理解 · [回看 10.3.2](chapters/10-mllib/index.md#1032)
    { .quiz-meta }

<div class="quiz" data-answer="C" markdown>

**19.** 执行计划中出现 `ReadSchema: struct<amount:double,province:string,status:string>`，说明 Spark 对 Parquet 做了（　　）。

- A. 谓词下推
- B. 广播连接
- C. 列裁剪
- D. 数据倾斜处理

</div>

??? tip "答案与解析"
    **答案：C**

    ReadSchema 列出了实际读取的列，订单表有 9 列，这里只读了查询用到的 3 列，这就是列裁剪。谓词下推对应的是 `PushedFilters`。

    考查：列裁剪 · 认知层次：理解 · [回看 11.3](chapters/11-tuning/index.md#113-csv-parquet)
    { .quiz-meta }

<div class="quiz" data-answer="D" markdown>

**20.** 用 pyecharts 画中国地图，数据中的省份是“广东”“河南”这样的简称，程序没有报错，但地图上一片空白。最可能的原因是（　　）。

- A. 没有安装 Spark
- B. 数据太少
- C. 地图必须用柱状图代替
- D. 地图要求省份用全称，如“广东省”

</div>

??? tip "答案与解析"
    **答案：D**

    pyecharts 2.x 的中国地图用省份全称匹配区域，简称匹配不上，就没有颜色也不报错。实验 12-1 用一个字典把简称转换成全称。

    考查：pyecharts · 认知层次：应用 · [回看 12.2.2](chapters/12-visualization-project/index.md#1222)
    { .quiz-meta }

**二、多项选择题**（每题 4 分）

<div class="quiz quiz-multi" data-answer="ABC" markdown>

**1.** 下列属于检查 Hadoop 集群是否正常的方法有（　　）。

- A. 在每个容器中运行 `jps`，看守护进程是否齐全
- B. 运行 `hdfs dfsadmin -report`，看 Live datanodes 的数量
- C. 打开 8088 网页的 Nodes 页，看节点是否都是 RUNNING
- D. 运行 `docker images`，看镜像是否存在

</div>

??? tip "答案与解析"
    **答案：ABC**

    D 只能说明镜像下载了，不能说明集群在正常运行。检查要从进程、节点、网页三个层面进行。

    考查：检查集群 · 认知层次：应用 · [回看 2.5](chapters/02-environment/index.md#25)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ABD" markdown>

**2.** 用 Python 编写 Hadoop Streaming 的 reducer 时，下列说法正确的有（　　）。

- A. 从标准输入一行一行读入“键 Tab 值”
- B. 读入的数据已经按键排好序，相同的键挨在一起
- C. 每次读入一个键和它的全部值组成的列表
- D. 循环结束后要记得输出最后一个键的结果

</div>

??? tip "答案与解析"
    **答案：ABD**

    C 是 Java 版 Reduce 函数的输入方式。Streaming 的 reducer 拿到的是一行一行的数据，要自己判断“键什么时候变了”，并在循环结束后补上最后一个键。

    考查：Hadoop Streaming · 认知层次：理解 · [回看 4.3.2](chapters/04-mapreduce-yarn/index.md#432-mapperpy-reducerpy)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ACD" markdown>

**3.** 下列 RDD 操作中，需要 Shuffle 的有（　　）。

- A. `reduceByKey`
- B. `filter`
- C. `distinct`
- D. `join`

</div>

??? tip "答案与解析"
    **答案：ACD**

    这三个操作都要把相同键（或相同元素）的数据汇集到一起；`filter` 每个分区自己就能完成，不需要 Shuffle。

    考查：Shuffle · 认知层次：分析 · [回看 6.2.4](chapters/06-spark-rdd/index.md#624-jobstage-task)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ACD" markdown>

**4.** 关于 Spark 上的分布式 K-Means，下列说法正确的有（　　）。

- A. 每一轮，Driver 把 k 个中心广播给各个节点
- B. 每一轮，各节点把自己的全部数据发给 Driver，由 Driver 计算距离
- C. 各节点在本地给自己的数据找最近的中心，并按群求出人数和坐标之和
- D. Driver 把各节点的求和结果加起来，除以人数得到新中心

</div>

??? tip "答案与解析"
    **答案：ACD**

    数据不动、中心来回跑：网络上传的只是每个节点 k 组“人数和坐标之和”，而不是原始数据，这是 K-Means 能在大数据上运行的关键。B 的做法会把全部数据集中到一台机器上，正是分布式要避免的。

    考查：K-Means · 认知层次：理解 · [回看 10.4.3](chapters/10-mllib/index.md#1043-k-means)
    { .quiz-meta }

<div class="quiz quiz-multi" data-answer="ACD" markdown>

**5.** 关于 Superset，下列说法正确的有（　　）。

- A. 它是 Apache 基金会的开源 BI 平台
- B. 它自己把 10 亿行原始数据读进内存计算，所以不需要 Spark
- C. 连接数据库时用 SQLAlchemy URI，例如 `mysql+pymysql://...`
- D. 做好的图表可以拼成看板，数据更新后刷新即可看到

</div>

??? tip "答案与解析"
    **答案：ACD**

    Superset 把每个图表翻译成 SQL，交给数据库执行，它本身不负责计算大数据。所以看板背后应该是 Spark 等工具算好的结果表。

    考查：Superset · 认知层次：理解 · [回看 12.3.1](chapters/12-visualization-project/index.md#1231)
    { .quiz-meta }

**三、判断题**（每题 1 分）

<div class="quiz quiz-judge" data-answer="√" markdown>

**1.** 一个 10 MB 的文件存入块大小为 128 MB 的 HDFS，只占用 10 MB（乘以副本数）的空间。

</div>

??? tip "答案与解析"
    **答案：√**

    不足一块的部分只占实际大小。实验 2-3 中 8 MB 的订单表就只有 1 个块，长度 8383330 字节。

    考查：数据块 · 认知层次：理解 · [回看 3.2.2](chapters/03-hdfs/index.md#322-128-mb)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**2.** 某个 Map 任务所在的机器出了故障，框架会把这个任务换到别的容器重新运行，不需要整个作业从头再来。

</div>

??? tip "答案与解析"
    **答案：√**

    任务是失败重试的单位，ApplicationMaster 会重新申请容器运行失败的任务，默认最多尝试 4 次。

    考查：作业与任务 · 认知层次：理解 · [回看 4.5.3](chapters/04-mapreduce-yarn/index.md#453)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**3.** 往外部表指向的 HDFS 目录中再放一个格式相同的文件，不用改表结构，再查询时就能查到新文件中的数据。

</div>

??? tip "答案与解析"
    **答案：√**

    Hive 查询时读取表目录下的全部文件，表结构只说明“怎样解读这些文件”。

    考查：Hive 表与 HDFS 文件 · 认知层次：理解 · [回看 5.3.1](chapters/05-hive/index.md#531-hive-hdfs)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**4.** 调用 `rdd.cache()` 的那一刻，Spark 就把 rdd 的数据全部放进了内存。

</div>

??? tip "答案与解析"
    **答案：×**

    `cache()` 也是惰性的，要等第一次行动计算出 rdd 时才放进内存。

    考查：缓存 · 认知层次：理解 · [回看 6.6](chapters/06-spark-rdd/index.md#66)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**5.** `try_cast` 转换失败时返回 NULL 而不报错，所以用了它之后要再统计一次空值，看看有没有数据在转换中丢失。

</div>

??? tip "答案与解析"
    **答案：√**

    `try_cast` 不报错，但转不了的值会悄悄变成 NULL，转换前后对比空值数才能发现问题。

    考查：try_cast · 认知层次：理解 · [回看 7.3.3](chapters/07-data-cleaning/index.md#733-cast-try_cast)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="×" markdown>

**6.** 用 `F.lag("gmv", 1)` 计算环比时，第一天的环比结果是 0。

</div>

??? tip "答案与解析"
    **答案：×**

    第一天前面没有行，`lag` 得到 NULL，环比也是 NULL，而不是 0。

    考查：环比 · 认知层次：理解 · [回看 8.3](chapters/08-spark-sql/index.md#83)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**7.** 一个消费者组第一次读某个主题时，`auto_offset_reset="earliest"` 让它从最早的消息开始读；组读过以后再启动，就从组记下的偏移量接着读。

</div>

??? tip "答案与解析"
    **答案：√**

    实验 9-1 第 7 步中，大屏组从偏移量 249、254、259 接着读，而不是从 0 开始。

    考查：偏移量 · 认知层次：理解 · [回看 9.3](chapters/09-streaming/index.md#93-python)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**8.** 在 Pipeline 中把逻辑回归换成随机森林，只需要替换最后一个步骤，前面的特征处理可以不改。

</div>

??? tip "答案与解析"
    **答案：√**

    Pipeline 的每一步各管各的，换模型只要换掉 `stages` 中的最后一个。实验 10-1 第 ⑦ 步就是这样比较随机森林的。

    考查：Pipeline · 认知层次：应用 · [回看 10.4.2](chapters/10-mllib/index.md#1042)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**9.** 读 CSV 时给出表结构（schema），比使用 `inferSchema=True` 更快。

</div>

??? tip "答案与解析"
    **答案：√**

    `inferSchema=True` 要先把文件完整读一遍来猜每列的类型。实验 11-1 中数行数时，inferSchema 用了 2.1 秒，给出 schema 只用 0.5 秒。

    考查：Parquet · 认知层次：记忆 · [回看 11.3](chapters/11-tuning/index.md#113-csv-parquet)
    { .quiz-meta }

<div class="quiz quiz-judge" data-answer="√" markdown>

**10.** pyecharts 是在 Python 中调用国产开源图表库 ECharts 的工具，生成的是可以交互的网页。

</div>

??? tip "答案与解析"
    **答案：√**

    pyecharts 用 Python 代码生成包含 ECharts 图表的 HTML 文件，用浏览器打开后，鼠标移上去可以看数值，地图可以缩放。

    考查：pyecharts · 认知层次：记忆 · [回看 12.2.1](chapters/12-visualization-project/index.md#1221-echarts-pyecharts)
    { .quiz-meta }

**四、简答题**（每题 10 分）

**1.** 用“食堂做饭”打比方，说明 Driver、Executor、分区、Task 分别相当于什么。

??? tip "答案与解析"
    **参考答案：** Driver 像食堂经理，拿着菜单（程序）把做饭拆成一道道工序，分配给厨师，最后把做好的菜收上来；Executor 像各个灶台前的厨师，真正动手干活，还能把备好的料（缓存）留在手边；分区像把一大筐菜分成的若干小筐；Task 像“处理一小筐菜的一道工序”，一筐菜一个 Task，厨师越多、同时处理的小筐越多。

    **评分要点：** 四个概念各 1 分，比喻合理、说出它们之间的关系 2 分（共 6 分）。

    考查：Driver 与 Executor · 认知层次：理解 · [回看 6.2.1](chapters/06-spark-rdd/index.md#621-driver-executor)
    { .quiz-meta }

**2.** 用“快递中转站”打比方，说明 Kafka 的主题、分区、偏移量和消费者组分别相当于什么。

??? tip "答案与解析"
    **参考答案：** Kafka 像一个快递中转站。主题相当于站里按类别划分的区域，比如“生鲜区”“家电区”，同一类包裹放在一起；分区相当于一个区域里的几条货架，包裹分散放在几条货架上，几个人可以同时取；偏移量相当于包裹在货架上的编号，从 0 开始依次排列；消费者组相当于一家来取件的公司，公司里几个取件员分工，每条货架只由一个取件员负责，公司记下每条货架取到了第几号；另一家公司来取件，可以从头把所有包裹再取一遍，因为中转站并不在取件后扔掉包裹。

    **评分要点：** 四个概念各 1.5 分，比方合理即可。

    考查：Kafka · 认知层次：理解 · [回看 9.2](chapters/09-streaming/index.md#92-kafka)
    { .quiz-meta }

**3.** 法务部发现，公司有几位同事把含有会员手机号、身份证号的原始数据导出到了自己的电脑上做分析。请你从数据合规的角度提出整改建议。

??? tip "答案与解析"
    **参考答案：** ① 立即删除个人电脑上的原始数据，登记导出了哪些数据、给了谁；② 分类分级：手机号、身份证号属于个人信息，身份证号属于敏感个人信息，定为公司内部的最高级别；③ 分析用脱敏或去标识化的数据：手机号、身份证号遮盖，地址截断到城市，需要关联时用加盐哈希代替身份证号，盐由专人保管；能用汇总结果的就只给汇总结果；④ 最小权限：原始数据放在受控目录（如 HDFS 上权限 750 的目录），只有极少数人经审批才能访问；分析人员和看板用只读账号，只能看到脱敏后的结果表；⑤ 制度和培训：禁止把数据拷到个人电脑和网盘，导出要审批、留记录，对员工进行个人信息保护法、数据安全法的培训。

    **评分要点：** 删除和登记 1 分，分类分级 1 分，脱敏 2 分，最小权限 1 分，制度和培训 1 分。

    考查：数据合规 · 认知层次：评价 · [回看 12.4](chapters/12-visualization-project/index.md#124)
    { .quiz-meta }
