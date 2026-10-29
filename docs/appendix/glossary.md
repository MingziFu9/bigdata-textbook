---
title: 附录 D 术语表
description: 本书主要术语的中英文名称、简明定义和例子，按章节顺序排列
---

# 附录 D 术语表

本表按章节顺序列出本书的主要术语，每个术语给出英文名称、简明定义和例子。正文中带虚线下划线的术语，把鼠标停在上面就能看到定义，点击可以跳到本表。

通用术语（如“过拟合”“精确率”）的译名和定义以本系列教材共用的《系列公共术语表》为准，各书保持一致。

## 第 1 章 大数据概述与技术生态

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 2 章 实验环境：Docker 与 Hadoop 集群

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 3 章 HDFS 分布式文件系统

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 4 章 MapReduce 与 YARN

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-MapReduce"></span>MapReduce | MapReduce | Hadoop 的分布式计算模型：把一个大任务拆成许多 Map 任务在多台机器上并行处理，再由 Reduce 任务按键汇总结果 | 把 100 万行访问日志分给 2 个 Map 任务统计，再由 1 个 Reduce 任务汇总出每小时的 PV |
| <span id="g-分而治之"></span>分而治之 | Divide and Conquer | 把一个大问题拆成许多互不相干的小问题分别解决，再把小问题的答案合并起来的思路 | 全校期末考试分给各班老师批改，教务处只汇总各班成绩 |
| <span id="g-作业"></span>作业 | Job | 用户提交给集群的一次完整计算，一个 MapReduce 作业包含若干个 Map 任务和 Reduce 任务 | “小象优选-每小时PV与UV”就是一个作业，编号 job_1793288590533_0001 |
| <span id="g-任务"></span>任务 | Task | 作业拆分出来、交给一台机器执行的最小工作单元，分为 Map 任务和 Reduce 任务；失败的任务会自动重试 | 70 MB 的日志切成 2 个分片，启动 2 个 Map 任务 |
| <span id="g-输入分片"></span>输入分片 | Input Split | MapReduce 交给一个 Map 任务处理的那一段输入数据，默认和一个 HDFS 数据块一样大（128 MB） | 700 MB 的日志文件被切成 6 个分片 |
| <span id="g-键值对"></span>键值对 | Key-Value Pair | MapReduce 中数据的基本形式，由“键”和“值”两部分组成，同一个键的值会被送到一起汇总 | (手机, 1)、(08, u0001234) |
| <span id="g-Map-阶段"></span>Map 阶段 | Map Phase | MapReduce 的第一步：每个 Map 任务逐行读取自己的分片，把每行数据转换成若干个键值对 | 把一行日志变成键值对 (08, u0001234)，键是小时、值是用户编号 |
| <span id="g-Shuffle"></span>Shuffle | Shuffle | Map 和 Reduce 之间的数据重新分发过程：Map 输出按键分区、排序后，每个 Reduce 任务从所有 Map 任务那里拉取属于自己的数据 | 所有 Map 输出的“耳机”都被送到 Reducer 0 |
| <span id="g-Reduce-阶段"></span>Reduce 阶段 | Reduce Phase | MapReduce 的最后一步：每个 Reduce 任务把同一个键的所有值汇总成结果，写到 HDFS | (耳机, [1, 1, 1]) → (耳机, 3) |
| <span id="g-分区"></span>分区 | Partition | 决定每个键交给哪个 Reduce 任务的规则，默认用键的哈希值对 Reduce 任务个数取余，保证同一个键只进一个 Reduce | 2 个 Reducer 时“充电宝”“耳机”进分区 0，“手机”“鞋”进分区 1 |
| <span id="g-Combiner"></span>Combiner | Combiner | 在 Map 端先把相同键的值合并一次的“本地小 Reduce”，用来减少 Shuffle 传输的数据量，只适合求和、计数、求最大值这类可以分步合并的计算 | 第 2 行的两个 (耳机, 1) 在 Map 端先合并成 (耳机, 2) |
| <span id="g-Hadoop-Streaming"></span>Hadoop Streaming | Hadoop Streaming | Hadoop 自带的工具，让用 Python 等任何语言编写、从标准输入读数据、向标准输出写结果的程序充当 mapper 和 reducer | `mapred streaming -mapper "python3 pv_mapper.py" -reducer "python3 pv_reducer.py" …` |
| <span id="g-计数器"></span>计数器 | Counter | MapReduce 作业运行时自动统计的各种数量，如读入多少行、Map 输出多少条、Shuffle 传了多少字节，是判断作业是否正常的依据 | Map input records=1000000 |
| <span id="g-PV"></span>PV | Page View | 页面浏览量，用户每打开一次页面算一次，同一个人打开多次算多次 | 9 月 30 日 21 点小象优选的 PV 是 88286 |
| <span id="g-UV"></span>UV | Unique Visitor | 独立访客数，一段时间内访问过网站的不同用户个数，同一个人访问多次只算一次 | 9 月 30 日 21 点的 UV 是 43162 |
| <span id="g-TopN"></span>TopN | Top N | 按某个指标从大到小取前 N 名的统计，也叫排行榜 | 访问量最高的 10 个页面 |
| <span id="g-YARN"></span>YARN | Yet Another Resource Negotiator | Hadoop 的资源管理和作业调度系统，负责把集群的内存和 CPU 分给各个作业，MapReduce、Spark 都可以运行在它上面 | 在 8088 网页上查看集群里正在运行的作业 |
| <span id="g-ResourceManager"></span>ResourceManager | ResourceManager | YARN 的总管，整个集群只有一个，掌握所有机器的资源，决定把资源分给哪个作业 | 随书集群中运行在 master 容器上，网页端口 8088 |
| <span id="g-NodeManager"></span>NodeManager | NodeManager | YARN 在每台工作机器上的管理员，向 ResourceManager 汇报本机资源，按指令启动和监控容器 | worker1、worker2 上各运行一个，每个拿出 2048 MB 内存 |
| <span id="g-ApplicationMaster"></span>ApplicationMaster | ApplicationMaster | 每个作业专属的“项目经理”，运行在一个容器里，负责向 ResourceManager 申请资源、安排任务、跟踪进度、重试失败任务 | MapReduce 作业的 AM 叫 MRAppMaster，默认占 512 MB |
| <span id="g-容器"></span>容器 | Container | YARN 分配资源的单位，是某台机器上的一份内存和 CPU，任务就在容器里运行 | 随书集群每个 Map 任务占一个 512 MB 的容器 |
| <span id="g-数据本地化"></span>数据本地化 | Data Locality | 把计算任务安排到存有它要读的数据块的机器上运行，避免通过网络搬运数据 | 计数器 Data-local map tasks=2 表示两个 Map 任务都读的是本机数据 |

## 第 5 章 Hive 数据仓库

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 6 章 Spark 入门与 RDD 编程

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 7 章 用 Spark 做数据清洗

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 8 章 DataFrame 与 Spark SQL 数据分析

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 9 章 Kafka 与实时数据处理

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 10 章 Spark MLlib 机器学习应用

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 11 章 常见问题与调优

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|

## 第 12 章 数据可视化、数据合规与综合项目

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
