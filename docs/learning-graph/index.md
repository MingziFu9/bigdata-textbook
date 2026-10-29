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

图谱共有 **22 个概念、29 条先修关系**，概念按章排列，颜色表示 3 个类别（计算模型、编程与统计、资源调度）。概念的定义都来自[附录 D 术语表](../appendix/glossary.md)，先修关系由编者根据各章内容整理。

## 文字版

下面按章列出每个概念的直接先修概念，便于打印或用读屏软件阅读。

??? note "第 4 章 MapReduce 与 YARN（22 个概念）"

    | 概念 | 类别 | 直接先修 |
    |---|---|---|
    | [MapReduce](../appendix/glossary.md#g-MapReduce) | 计算模型 | [分而治之](../appendix/glossary.md#g-分而治之) |
    | [分而治之](../appendix/glossary.md#g-分而治之) | 计算模型 | — |
    | [作业](../appendix/glossary.md#g-作业) | 计算模型 | [MapReduce](../appendix/glossary.md#g-MapReduce) |
    | [任务](../appendix/glossary.md#g-任务) | 计算模型 | [作业](../appendix/glossary.md#g-作业) |
    | [输入分片](../appendix/glossary.md#g-输入分片) | 计算模型 | — |
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
    | [YARN](../appendix/glossary.md#g-YARN) | 资源调度 | [MapReduce](../appendix/glossary.md#g-MapReduce) |
    | [ResourceManager](../appendix/glossary.md#g-ResourceManager) | 资源调度 | [YARN](../appendix/glossary.md#g-YARN) |
    | [NodeManager](../appendix/glossary.md#g-NodeManager) | 资源调度 | [YARN](../appendix/glossary.md#g-YARN) |
    | [ApplicationMaster](../appendix/glossary.md#g-ApplicationMaster) | 资源调度 | [ResourceManager](../appendix/glossary.md#g-ResourceManager)、[作业](../appendix/glossary.md#g-作业)、[容器](../appendix/glossary.md#g-容器) |
    | [容器](../appendix/glossary.md#g-容器) | 资源调度 | [任务](../appendix/glossary.md#g-任务)、[NodeManager](../appendix/glossary.md#g-NodeManager) |
    | [数据本地化](../appendix/glossary.md#g-数据本地化) | 资源调度 | [输入分片](../appendix/glossary.md#g-输入分片)、[ResourceManager](../appendix/glossary.md#g-ResourceManager) |

<!-- graph:end -->
