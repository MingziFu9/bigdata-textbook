---
title: 常见问题
description: 按类别整理的常见问题，每个答案都链接回教材正文
---

# 常见问题

这里按类别整理了学习本课程时最常遇到的问题。点开问题查看简短的回答，想深入了解，就顺着链接回到正文对应的小节。

| 类别 | 问题数 |
|---|---|
| [概念理解](#faq-1) | 2 |
| [实验环境与排错](#faq-2) | 3 |

## 概念理解 { #faq-1 }

??? question "现在都用 Spark 了，为什么还要学 MapReduce？"

    手写 MapReduce 程序的场合确实少了，但 Hive、Spark 执行分组统计时同样要经过 Map、Shuffle、Reduce 这几步。看懂了 MapReduce，才能读懂 Spark 作业为什么慢、数据倾斜是怎么回事。见 [4.6 MapReduce 的局限与今天的选择](../chapters/04-mapreduce-yarn/index.md#46-mapreduce)。

??? question "数据只有几十 MB，为什么集群跑得比笔记本还慢？"

    集群作业要申请资源、启动 ApplicationMaster 和任务，光准备就要十几秒。实验 4-2 中 100 万行日志在集群上要 25 秒左右，本机管道不到 10 秒。数据大到单机处理不了时，集群的优势才显现出来。见 [4.6](../chapters/04-mapreduce-yarn/index.md#46-mapreduce)。

## 实验环境与排错 { #faq-2 }

??? question "作业失败了，屏幕上只有 subprocess failed with code 2，怎么找原因？"

    退出码只说明 Python 程序非正常退出，真正的原因在任务的 stderr 日志里：`yarn logs -applicationId <作业编号> -log_files stderr`。code 2 加上 “can't open file” 多半是漏了 `-files`。见 [4.5.5 作业失败或卡住了怎么办](../chapters/04-mapreduce-yarn/index.md#455)。

??? question "作业一直停在 ACCEPTED 不动？"

    先看 8088 网页上是否有别的作业在运行（小集群只够同时运行一个作业），再看 Nodes 页面节点是否 RUNNING、是否因磁盘快满而 UNHEALTHY。见 [4.5.5](../chapters/04-mapreduce-yarn/index.md#455) 和[附录 C](../appendix/c-environment.md)。

??? question "8088 网页上点 History 打不开？"

    链接里的 `master` 是集群内部的主机名，把它改成 `localhost`，或在 hosts 文件中加一行 `127.0.0.1 master`。见[附录 C](../appendix/c-environment.md)。
