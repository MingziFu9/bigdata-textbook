---
title: 学习图谱
description: 全书核心概念及其先修关系，点一个概念就能看到学习路径
---

# 学习图谱

学习图谱把全书的核心概念和它们之间的“先修关系”画在一张图上：要理解一个概念，得先懂哪些更基础的概念；顺着箭头往回找，就是一条学习路径。

<iframe class="sim-frame" src="main.html" height="900" loading="lazy" title="学习图谱"></iframe>

[全屏打开](main.html){ target=_blank }

## 怎样使用

- **预习**：学新的一章前，点开本章的概念，看看橙色的先修概念自己是否都掌握了，没掌握的先回去复习；
- **复习**：期末复习时，从最后几章的概念出发，顺着学习路径往回梳理，就能把全书串起来；
- **查漏补缺**：做练习时遇到不懂的概念，在搜索框里输入它，看它依赖哪些更基础的概念。

<!-- graph:start -->

图谱共有 **135 个概念、175 条先修关系**，概念按章排列，颜色表示 8 个类别（计算模型、编程与统计、资源调度、大数据基础、实验环境、分布式存储、数据仓库、数据清洗与分析）。概念的定义都来自[附录 D 术语表](../appendix/glossary.md)，先修关系由编者根据各章内容整理。

## 文字版

下面按章列出每个概念的直接先修概念，便于打印或用读屏软件阅读。

??? note "第 1 章 概述与生态（16 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [大数据](../appendix/glossary.md#g-大数据) | 大数据基础 | — |
    | [5V 特征](../appendix/glossary.md#g-5V-特征) | 大数据基础 | [大数据](../appendix/glossary.md#g-大数据) |
    | [结构化数据](../appendix/glossary.md#g-结构化数据) | 大数据基础 | [5V 特征](../appendix/glossary.md#g-5V-特征) |
    | [半结构化数据](../appendix/glossary.md#g-半结构化数据) | 大数据基础 | [5V 特征](../appendix/glossary.md#g-5V-特征) |
    | [非结构化数据](../appendix/glossary.md#g-非结构化数据) | 大数据基础 | [5V 特征](../appendix/glossary.md#g-5V-特征) |
    | [纵向扩展](../appendix/glossary.md#g-纵向扩展) | 大数据基础 | [大数据](../appendix/glossary.md#g-大数据) |
    | [横向扩展](../appendix/glossary.md#g-横向扩展) | 大数据基础 | [大数据](../appendix/glossary.md#g-大数据) |
    | [集群](../appendix/glossary.md#g-集群) | 大数据基础 | [横向扩展](../appendix/glossary.md#g-横向扩展) |
    | [节点](../appendix/glossary.md#g-节点) | 大数据基础 | [集群](../appendix/glossary.md#g-集群) |
    | [移动计算](../appendix/glossary.md#g-移动计算) | 大数据基础 | [集群](../appendix/glossary.md#g-集群) |
    | [大数据处理流程](../appendix/glossary.md#g-大数据处理流程) | 大数据基础 | [大数据](../appendix/glossary.md#g-大数据) |
    | [批处理](../appendix/glossary.md#g-批处理) | 大数据基础 | [大数据处理流程](../appendix/glossary.md#g-大数据处理流程) |
    | [流处理](../appendix/glossary.md#g-流处理) | 大数据基础 | [大数据处理流程](../appendix/glossary.md#g-大数据处理流程) |
    | [Hadoop](../appendix/glossary.md#g-Hadoop) | 大数据基础 | [集群](../appendix/glossary.md#g-集群) |
    | [本地模式](../appendix/glossary.md#g-本地模式) | 大数据基础 | [Hadoop](../appendix/glossary.md#g-Hadoop) |
    | [Text-to-SQL](../appendix/glossary.md#g-Text-to-SQL) | 大数据基础 | [大数据处理流程](../appendix/glossary.md#g-大数据处理流程) |

??? note "第 2 章 Docker 与集群（13 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [Docker](../appendix/glossary.md#g-Docker) | 实验环境 | — |
    | [镜像](../appendix/glossary.md#g-镜像) | 实验环境 | [Docker](../appendix/glossary.md#g-Docker)、[镜像仓库](../appendix/glossary.md#g-镜像仓库) |
    | [Docker 容器](../appendix/glossary.md#g-Docker-容器) | 实验环境 | [镜像](../appendix/glossary.md#g-镜像) |
    | [镜像仓库](../appendix/glossary.md#g-镜像仓库) | 实验环境 | [镜像加速](../appendix/glossary.md#g-镜像加速) |
    | [镜像加速](../appendix/glossary.md#g-镜像加速) | 实验环境 | — |
    | [端口映射](../appendix/glossary.md#g-端口映射) | 实验环境 | [Docker 容器](../appendix/glossary.md#g-Docker-容器) |
    | [数据卷](../appendix/glossary.md#g-数据卷) | 实验环境 | [Docker 容器](../appendix/glossary.md#g-Docker-容器) |
    | [Docker Compose](../appendix/glossary.md#g-Docker-Compose) | 实验环境 | [Docker 容器](../appendix/glossary.md#g-Docker-容器) |
    | [主节点](../appendix/glossary.md#g-主节点) | 实验环境 | [Docker Compose](../appendix/glossary.md#g-Docker-Compose)、[节点](../appendix/glossary.md#g-节点) |
    | [工作节点](../appendix/glossary.md#g-工作节点) | 实验环境 | [Docker Compose](../appendix/glossary.md#g-Docker-Compose)、[节点](../appendix/glossary.md#g-节点) |
    | [守护进程](../appendix/glossary.md#g-守护进程) | 实验环境 | [主节点](../appendix/glossary.md#g-主节点)、[工作节点](../appendix/glossary.md#g-工作节点) |
    | [格式化 NameNode](../appendix/glossary.md#g-格式化-NameNode) | 实验环境 | [主节点](../appendix/glossary.md#g-主节点) |
    | [jps](../appendix/glossary.md#g-jps) | 实验环境 | [守护进程](../appendix/glossary.md#g-守护进程) |

??? note "第 3 章 HDFS（17 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [HDFS](../appendix/glossary.md#g-HDFS) | 分布式存储 | [Hadoop](../appendix/glossary.md#g-Hadoop) |
    | [NameNode](../appendix/glossary.md#g-NameNode) | 分布式存储 | [HDFS](../appendix/glossary.md#g-HDFS)、[主节点](../appendix/glossary.md#g-主节点)、[格式化 NameNode](../appendix/glossary.md#g-格式化-NameNode) |
    | [DataNode](../appendix/glossary.md#g-DataNode) | 分布式存储 | [HDFS](../appendix/glossary.md#g-HDFS)、[工作节点](../appendix/glossary.md#g-工作节点) |
    | [元数据](../appendix/glossary.md#g-元数据) | 分布式存储 | [NameNode](../appendix/glossary.md#g-NameNode) |
    | [数据块](../appendix/glossary.md#g-数据块) | 分布式存储 | [HDFS](../appendix/glossary.md#g-HDFS) |
    | [副本](../appendix/glossary.md#g-副本) | 分布式存储 | [数据块](../appendix/glossary.md#g-数据块) |
    | [心跳](../appendix/glossary.md#g-心跳) | 分布式存储 | [DataNode](../appendix/glossary.md#g-DataNode) |
    | [块报告](../appendix/glossary.md#g-块报告) | 分布式存储 | [DataNode](../appendix/glossary.md#g-DataNode) |
    | [副本不足](../appendix/glossary.md#g-副本不足) | 分布式存储 | [心跳](../appendix/glossary.md#g-心跳)、[副本](../appendix/glossary.md#g-副本) |
    | [安全模式](../appendix/glossary.md#g-安全模式) | 分布式存储 | [块报告](../appendix/glossary.md#g-块报告) |
    | [hdfs dfs](../appendix/glossary.md#g-hdfs-dfs) | 分布式存储 | [HDFS](../appendix/glossary.md#g-HDFS) |
    | [hdfs fsck](../appendix/glossary.md#g-hdfs-fsck) | 分布式存储 | [数据块](../appendix/glossary.md#g-数据块) |
    | [WebHDFS](../appendix/glossary.md#g-WebHDFS) | 分布式存储 | [HDFS](../appendix/glossary.md#g-HDFS) |
    | [HttpFS](../appendix/glossary.md#g-HttpFS) | 分布式存储 | [WebHDFS](../appendix/glossary.md#g-WebHDFS)、[端口映射](../appendix/glossary.md#g-端口映射) |
    | [小文件问题](../appendix/glossary.md#g-小文件问题) | 分布式存储 | [元数据](../appendix/glossary.md#g-元数据) |
    | [对象存储](../appendix/glossary.md#g-对象存储) | 分布式存储 | [HDFS](../appendix/glossary.md#g-HDFS) |
    | [存算分离](../appendix/glossary.md#g-存算分离) | 分布式存储 | [对象存储](../appendix/glossary.md#g-对象存储)、[移动计算](../appendix/glossary.md#g-移动计算) |

??? note "第 4 章 MapReduce 与 YARN（22 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [MapReduce](../appendix/glossary.md#g-MapReduce) | 计算模型 | [分而治之](../appendix/glossary.md#g-分而治之)、[Hadoop](../appendix/glossary.md#g-Hadoop)、[分而治之](../appendix/glossary.md#g-分而治之) |
    | [分而治之](../appendix/glossary.md#g-分而治之) | 计算模型 | — |
    | [作业](../appendix/glossary.md#g-作业) | 计算模型 | [MapReduce](../appendix/glossary.md#g-MapReduce) |
    | [任务](../appendix/glossary.md#g-任务) | 计算模型 | [作业](../appendix/glossary.md#g-作业) |
    | [输入分片](../appendix/glossary.md#g-输入分片) | 计算模型 | [数据块](../appendix/glossary.md#g-数据块) |
    | [键值对](../appendix/glossary.md#g-键值对) | 计算模型 | — |
    | [Map 阶段](../appendix/glossary.md#g-Map-阶段) | 计算模型 | [输入分片](../appendix/glossary.md#g-输入分片)、[键值对](../appendix/glossary.md#g-键值对) |
    | [Shuffle](../appendix/glossary.md#g-Shuffle) | 计算模型 | [分区](../appendix/glossary.md#g-分区)、[Combiner](../appendix/glossary.md#g-Combiner) |
    | [Reduce 阶段](../appendix/glossary.md#g-Reduce-阶段) | 计算模型 | [Shuffle](../appendix/glossary.md#g-Shuffle) |
    | [分区](../appendix/glossary.md#g-分区) | 计算模型 | [Map 阶段](../appendix/glossary.md#g-Map-阶段) |
    | [Combiner](../appendix/glossary.md#g-Combiner) | 计算模型 | [Map 阶段](../appendix/glossary.md#g-Map-阶段) |
    | [Hadoop Streaming](../appendix/glossary.md#g-Hadoop-Streaming) | 编程与统计 | [Reduce 阶段](../appendix/glossary.md#g-Reduce-阶段)、[Map 阶段](../appendix/glossary.md#g-Map-阶段) |
    | [计数器](../appendix/glossary.md#g-计数器) | 编程与统计 | [任务](../appendix/glossary.md#g-任务)、[Shuffle](../appendix/glossary.md#g-Shuffle) |
    | [PV](../appendix/glossary.md#g-PV) | 编程与统计 | [Hadoop Streaming](../appendix/glossary.md#g-Hadoop-Streaming) |
    | [UV](../appendix/glossary.md#g-UV) | 编程与统计 | [Hadoop Streaming](../appendix/glossary.md#g-Hadoop-Streaming)、[PV](../appendix/glossary.md#g-PV) |
    | [TopN](../appendix/glossary.md#g-TopN) | 编程与统计 | [Hadoop Streaming](../appendix/glossary.md#g-Hadoop-Streaming)、[Reduce 阶段](../appendix/glossary.md#g-Reduce-阶段) |
    | [YARN](../appendix/glossary.md#g-YARN) | 资源调度 | [Hadoop](../appendix/glossary.md#g-Hadoop)、[MapReduce](../appendix/glossary.md#g-MapReduce) |
    | [ResourceManager](../appendix/glossary.md#g-ResourceManager) | 资源调度 | [YARN](../appendix/glossary.md#g-YARN) |
    | [NodeManager](../appendix/glossary.md#g-NodeManager) | 资源调度 | [YARN](../appendix/glossary.md#g-YARN) |
    | [ApplicationMaster](../appendix/glossary.md#g-ApplicationMaster) | 资源调度 | [ResourceManager](../appendix/glossary.md#g-ResourceManager)、[作业](../appendix/glossary.md#g-作业)、[容器](../appendix/glossary.md#g-容器) |
    | [容器](../appendix/glossary.md#g-容器) | 资源调度 | [Docker 容器](../appendix/glossary.md#g-Docker-容器)、[任务](../appendix/glossary.md#g-任务)、[NodeManager](../appendix/glossary.md#g-NodeManager) |
    | [数据本地化](../appendix/glossary.md#g-数据本地化) | 资源调度 | [移动计算](../appendix/glossary.md#g-移动计算)、[输入分片](../appendix/glossary.md#g-输入分片)、[ResourceManager](../appendix/glossary.md#g-ResourceManager) |

??? note "第 5 章 Hive（21 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [Hive](../appendix/glossary.md#g-Hive) | 数据仓库 | [Hadoop](../appendix/glossary.md#g-Hadoop)、[MapReduce](../appendix/glossary.md#g-MapReduce) |
    | [HiveQL](../appendix/glossary.md#g-HiveQL) | 数据仓库 | [Hive](../appendix/glossary.md#g-Hive) |
    | [数据仓库](../appendix/glossary.md#g-数据仓库) | 数据仓库 | [Hive](../appendix/glossary.md#g-Hive) |
    | [ODS](../appendix/glossary.md#g-ODS) | 数据仓库 | [数据仓库](../appendix/glossary.md#g-数据仓库) |
    | [DWD](../appendix/glossary.md#g-DWD) | 数据仓库 | [ODS](../appendix/glossary.md#g-ODS) |
    | [DWS](../appendix/glossary.md#g-DWS) | 数据仓库 | [DWD](../appendix/glossary.md#g-DWD) |
    | [ADS](../appendix/glossary.md#g-ADS) | 数据仓库 | [DWS](../appendix/glossary.md#g-DWS) |
    | [事实表](../appendix/glossary.md#g-事实表) | 数据仓库 | [数据仓库](../appendix/glossary.md#g-数据仓库) |
    | [维度表](../appendix/glossary.md#g-维度表) | 数据仓库 | [数据仓库](../appendix/glossary.md#g-数据仓库) |
    | [Metastore](../appendix/glossary.md#g-Metastore) | 数据仓库 | [元数据](../appendix/glossary.md#g-元数据)、[Hive](../appendix/glossary.md#g-Hive) |
    | [HiveServer2](../appendix/glossary.md#g-HiveServer2) | 数据仓库 | [Hive](../appendix/glossary.md#g-Hive) |
    | [Beeline](../appendix/glossary.md#g-Beeline) | 数据仓库 | [HiveServer2](../appendix/glossary.md#g-HiveServer2) |
    | [Tez](../appendix/glossary.md#g-Tez) | 计算模型 | [YARN](../appendix/glossary.md#g-YARN)、[Hive](../appendix/glossary.md#g-Hive) |
    | [内部表](../appendix/glossary.md#g-内部表) | 数据仓库 | [Hive](../appendix/glossary.md#g-Hive) |
    | [外部表](../appendix/glossary.md#g-外部表) | 数据仓库 | [Hive](../appendix/glossary.md#g-Hive)、[HDFS](../appendix/glossary.md#g-HDFS) |
    | [分区表](../appendix/glossary.md#g-分区表) | 数据仓库 | [Hive](../appendix/glossary.md#g-Hive) |
    | [分区裁剪](../appendix/glossary.md#g-分区裁剪) | 数据仓库 | [分区表](../appendix/glossary.md#g-分区表) |
    | [动态分区](../appendix/glossary.md#g-动态分区) | 数据仓库 | [分区表](../appendix/glossary.md#g-分区表) |
    | [ORC](../appendix/glossary.md#g-ORC) | 分布式存储 | [Hive](../appendix/glossary.md#g-Hive) |
    | [窗口函数](../appendix/glossary.md#g-窗口函数) | 编程与统计 | [HiveQL](../appendix/glossary.md#g-HiveQL) |
    | [DataX](../appendix/glossary.md#g-DataX) | 数据仓库 | [ODS](../appendix/glossary.md#g-ODS) |

??? note "第 6 章 Spark 与 RDD（16 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [Spark](../appendix/glossary.md#g-Spark) | 计算模型 | [MapReduce](../appendix/glossary.md#g-MapReduce) |
    | [Driver](../appendix/glossary.md#g-Driver) | 计算模型 | [Spark](../appendix/glossary.md#g-Spark) |
    | [Executor](../appendix/glossary.md#g-Executor) | 计算模型 | [Spark](../appendix/glossary.md#g-Spark)、[容器](../appendix/glossary.md#g-容器) |
    | [RDD](../appendix/glossary.md#g-RDD) | 计算模型 | [Spark](../appendix/glossary.md#g-Spark)、[分区](../appendix/glossary.md#g-分区) |
    | [转换](../appendix/glossary.md#g-转换) | 计算模型 | [RDD](../appendix/glossary.md#g-RDD) |
    | [行动](../appendix/glossary.md#g-行动) | 计算模型 | [RDD](../appendix/glossary.md#g-RDD) |
    | [惰性执行](../appendix/glossary.md#g-惰性执行) | 计算模型 | [转换](../appendix/glossary.md#g-转换)、[行动](../appendix/glossary.md#g-行动) |
    | [Job](../appendix/glossary.md#g-Job) | 计算模型 | [行动](../appendix/glossary.md#g-行动) |
    | [Stage](../appendix/glossary.md#g-Stage) | 计算模型 | [Job](../appendix/glossary.md#g-Job)、[Shuffle](../appendix/glossary.md#g-Shuffle) |
    | [Task](../appendix/glossary.md#g-Task) | 计算模型 | [Stage](../appendix/glossary.md#g-Stage) |
    | [Spark UI](../appendix/glossary.md#g-Spark-UI) | 计算模型 | [Driver](../appendix/glossary.md#g-Driver)、[Job](../appendix/glossary.md#g-Job) |
    | [pyspark](../appendix/glossary.md#g-pyspark) | 实验环境 | [Spark](../appendix/glossary.md#g-Spark) |
    | [spark-submit](../appendix/glossary.md#g-spark-submit) | 实验环境 | [Spark](../appendix/glossary.md#g-Spark)、[YARN](../appendix/glossary.md#g-YARN) |
    | [键值对 RDD](../appendix/glossary.md#g-键值对-RDD) | 编程与统计 | [键值对](../appendix/glossary.md#g-键值对)、[RDD](../appendix/glossary.md#g-RDD) |
    | [缓存](../appendix/glossary.md#g-缓存) | 计算模型 | [RDD](../appendix/glossary.md#g-RDD)、[Executor](../appendix/glossary.md#g-Executor) |
    | [Serverless Spark](../appendix/glossary.md#g-Serverless-Spark) | 资源调度 | [Spark](../appendix/glossary.md#g-Spark)、[存算分离](../appendix/glossary.md#g-存算分离) |

??? note "第 7 章 数据清洗（16 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [数据清洗](../appendix/glossary.md#g-数据清洗) | 数据清洗与分析 | [脏数据](../appendix/glossary.md#g-脏数据)、[数据质量](../appendix/glossary.md#g-数据质量) |
    | [脏数据](../appendix/glossary.md#g-脏数据) | 数据清洗与分析 | [数据质量](../appendix/glossary.md#g-数据质量) |
    | [数据质量](../appendix/glossary.md#g-数据质量) | 数据清洗与分析 | — |
    | [缺失值](../appendix/glossary.md#g-缺失值) | 数据清洗与分析 | [脏数据](../appendix/glossary.md#g-脏数据) |
    | [重复值](../appendix/glossary.md#g-重复值) | 数据清洗与分析 | [脏数据](../appendix/glossary.md#g-脏数据) |
    | [异常值](../appendix/glossary.md#g-异常值) | 数据清洗与分析 | [脏数据](../appendix/glossary.md#g-脏数据) |
    | [DataFrame](../appendix/glossary.md#g-DataFrame) | 编程与统计 | [SparkSession](../appendix/glossary.md#g-SparkSession)、[RDD](../appendix/glossary.md#g-RDD)、[结构化数据](../appendix/glossary.md#g-结构化数据) |
    | [SparkSession](../appendix/glossary.md#g-SparkSession) | 编程与统计 | [Spark](../appendix/glossary.md#g-Spark) |
    | [Schema](../appendix/glossary.md#g-Schema) | 编程与统计 | [DataFrame](../appendix/glossary.md#g-DataFrame) |
    | [ANSI 模式](../appendix/glossary.md#g-ANSI-模式) | 编程与统计 | [Schema](../appendix/glossary.md#g-Schema) |
    | [try_cast](../appendix/glossary.md#g-try_cast) | 编程与统计 | [ANSI 模式](../appendix/glossary.md#g-ANSI-模式) |
    | [正则表达式](../appendix/glossary.md#g-正则表达式) | 编程与统计 | [数据清洗](../appendix/glossary.md#g-数据清洗) |
    | [左连接](../appendix/glossary.md#g-左连接) | 编程与统计 | [DataFrame](../appendix/glossary.md#g-DataFrame) |
    | [宽表](../appendix/glossary.md#g-宽表) | 数据清洗与分析 | [事实表](../appendix/glossary.md#g-事实表)、[维度表](../appendix/glossary.md#g-维度表)、[左连接](../appendix/glossary.md#g-左连接) |
    | [Parquet](../appendix/glossary.md#g-Parquet) | 分布式存储 | [列式存储](../appendix/glossary.md#g-列式存储)、[分区表](../appendix/glossary.md#g-分区表) |
    | [列式存储](../appendix/glossary.md#g-列式存储) | 分布式存储 | [ORC](../appendix/glossary.md#g-ORC) |

??? note "第 8 章 Spark SQL（14 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [Spark SQL](../appendix/glossary.md#g-Spark-SQL) | 编程与统计 | [DataFrame](../appendix/glossary.md#g-DataFrame)、[HiveQL](../appendix/glossary.md#g-HiveQL) |
    | [临时视图](../appendix/glossary.md#g-临时视图) | 编程与统计 | [Spark SQL](../appendix/glossary.md#g-Spark-SQL) |
    | [透视表](../appendix/glossary.md#g-透视表) | 编程与统计 | [Spark SQL](../appendix/glossary.md#g-Spark-SQL) |
    | [移动平均](../appendix/glossary.md#g-移动平均) | 编程与统计 | [窗口函数](../appendix/glossary.md#g-窗口函数) |
    | [环比](../appendix/glossary.md#g-环比) | 编程与统计 | [窗口函数](../appendix/glossary.md#g-窗口函数) |
    | [同比](../appendix/glossary.md#g-同比) | 编程与统计 | [环比](../appendix/glossary.md#g-环比) |
    | [GMV](../appendix/glossary.md#g-GMV) | 数据清洗与分析 | [宽表](../appendix/glossary.md#g-宽表) |
    | [客单价](../appendix/glossary.md#g-客单价) | 数据清洗与分析 | [GMV](../appendix/glossary.md#g-GMV) |
    | [复购率](../appendix/glossary.md#g-复购率) | 数据清洗与分析 | [宽表](../appendix/glossary.md#g-宽表) |
    | [转化率](../appendix/glossary.md#g-转化率) | 数据清洗与分析 | [UV](../appendix/glossary.md#g-UV) |
    | [留存率](../appendix/glossary.md#g-留存率) | 数据清洗与分析 | [复购率](../appendix/glossary.md#g-复购率) |
    | [RFM 模型](../appendix/glossary.md#g-RFM-模型) | 数据清洗与分析 | [复购率](../appendix/glossary.md#g-复购率)、[GMV](../appendix/glossary.md#g-GMV) |
    | [用户分层](../appendix/glossary.md#g-用户分层) | 数据清洗与分析 | [RFM 模型](../appendix/glossary.md#g-RFM-模型) |
    | [JDBC](../appendix/glossary.md#g-JDBC) | 编程与统计 | [Spark SQL](../appendix/glossary.md#g-Spark-SQL) |

<!-- graph:end -->
