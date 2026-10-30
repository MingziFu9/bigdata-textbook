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

图谱共有 **68 个概念、88 条先修关系**，概念按章排列，颜色表示 6 个类别（计算模型、编程与统计、资源调度、大数据基础、实验环境、分布式存储）。概念的定义都来自[附录 D 术语表](../appendix/glossary.md)，先修关系由编者根据各章内容整理。

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

<!-- graph:end -->
