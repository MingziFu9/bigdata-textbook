---
title: 交互动画
description: 本书配套的交互动画，可以在浏览器里直接操作，帮助理解抽象概念
---

# 交互动画

这些动画都是普通网页，不需要安装任何软件，也不需要联网调用接口，在电脑和手机浏览器上都能打开。每个动画都嵌在对应章节的正文里，也可以从这里单独打开，适合课堂演示。

| 章 | 动画 | 你可以做什么 |
|---|---|---|
| 第 1 章 | [单机与集群耗时估算器](cluster-estimator/index.md) | 改变数据量和机器数，比较一台电脑和集群各要多久，找出值得用集群的数据量 |
| 第 2 章 | [集群地图](cluster-map/index.md) | 点击每个容器和端口，看集群里运行着什么，模拟加节点、停节点 |
| 第 3 章 | [HDFS 切块与副本](hdfs-blocks/index.md) | 上传文件看它被切块、放副本，停掉机器看副本怎样补齐 |
| 第 4 章 | [MapReduce 词频统计全流程](mapreduce-wordcount/index.md) | 输入几行文字，一步一步看 Map、Shuffle、Reduce 的中间数据，比较加不加 Combiner |
| 第 4 章 | [YARN 资源分配](yarn-scheduling/index.md) | 调整节点、内存和作业规模，逐秒观察容器怎样分配，找出作业卡住的原因 |
| 第 5 章 | [分区表查询](partition-query/index.md) | 比较分区表和不分区的表，看带不带分区条件时要读多少数据 |
| 第 6 章 | [Spark 作业怎样拆成 Stage 和 Task](spark-stages/index.md) | 拼一个 RDD 程序，看行动怎样触发 Job、Shuffle 怎样切出 Stage、Task 怎样分批运行 |
| 第 7 章 | [数据清洗流水线](cleaning-pipeline/index.md) | 勾选清洗步骤，看一份脏订单一步步变干净，顺序错了会丢哪些数据 |
| 第 8 章 | [窗口函数](window-functions/index.md) | 选择分组排名、累计、移动平均、环比，逐行看每一行的结果怎样算出来 |
| 第 9 章 | [Kafka 分区与消费者组](kafka-groups/index.md) | 发带键的消息看它进哪个分区，调整消费者个数看组内怎样分摊分区、积压怎样变化 |
| 第 9 章 | [时间窗口与迟到数据](late-data/index.md) | 选择水位线，逐笔看迟到的订单被计入还是丢弃，比较准确与及时的取舍 |
| 第 10 章 | [分布式 K-Means](dist-kmeans/index.md) | 一步步看中心点广播到各节点、各节点本地分配求和、Driver 汇总更新中心，直到收敛 |
| 第 11 章 | [行式与列式存储](row-column/index.md) | 同一张表按行式和列式存放，选不同的查询，看各要读出多少数据，理解列裁剪和谓词下推 |
| 第 11 章 | [数据倾斜与加盐](skew-salt/index.md) | 调整热点占比、盐的个数和 CPU 核数，看各任务的数据量和运行时间线 |
