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
