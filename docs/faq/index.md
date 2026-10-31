---
title: 常见问题
description: 按类别整理的常见问题，每个答案都链接回教材正文
---

# 常见问题

这里按类别整理了学习本课程时最常遇到的问题。点开问题查看简短的回答，想深入了解，就顺着链接回到正文对应的小节。

| 类别 | 问题数 |
|---|---|
| [概念理解](#faq-1) | 4 |
| [实验环境与排错](#faq-2) | 6 |

## 概念理解 { #faq-1 }

??? question "现在都用 Spark 了，为什么还要学 MapReduce？"

    手写 MapReduce 程序的场合确实少了，但 Hive、Spark 执行分组统计时同样要经过 Map、Shuffle、Reduce 这几步。看懂了 MapReduce，才能读懂 Spark 作业为什么慢、数据倾斜是怎么回事。见 [4.6 MapReduce 的局限与今天的选择](../chapters/04-mapreduce-yarn/index.md#46-mapreduce)。

??? question "数据只有几十 MB，为什么集群跑得比笔记本还慢？"

    集群作业要申请资源、启动 ApplicationMaster 和任务，光准备就要十几秒。实验 4-2 中 100 万行日志在集群上要 25 秒左右，本机管道不到 10 秒。数据大到单机处理不了时，集群的优势才显现出来。见 [4.6](../chapters/04-mapreduce-yarn/index.md#46-mapreduce)。

??? question "我的数据该用 pandas 还是 Spark？"

    看数据量。一台电脑内存装得下（几 GB 以内）时，pandas 更快、更简单；数据大到读不进内存或持续增长时，再用 Spark 和集群。实验 1-1 测得 pandas 读入订单表后占用的内存约是文件的 5.6 倍，可以据此估算自己电脑的上限。见 [1.2.4 用动画比一比](../chapters/01-overview/index.md#124)。

??? question "副本数设成多少合适？"

    生产环境默认 3，能同时承受 2 台机器损坏；随书集群只有 2 个工作节点，设为 2。副本数越大越安全，但占用空间成倍增加，而且不能超过 DataNode 的台数。见 [3.2.3 副本](../chapters/03-hdfs/index.md#323)。

??? question "Hive 和 Spark SQL 都能写 SQL，该用哪个？"

    两者可以配合使用：Hive 负责管理数据仓库的表结构（Metastore），适合每天定时运行、对速度要求不高的批量 SQL；Spark SQL 能直接读写 Hive 的表，速度更快，还能和 Python 代码、数据清洗、机器学习写在一个程序里。本书实验 8-3 就是用 Spark 读 Hive 的 ODS 表、把结果写回 Hive 的 ADS 层。见 [5.6.2 什么时候不该用 Hive](../chapters/05-hive/index.md#562-hive) 和 [8.5.1 读写 Hive 表](../chapters/08-spark-sql/index.md#851-hive)。

??? question "内部表和外部表怎么选？"

    数据已经在 HDFS 上、还有别的程序在用，建外部表，只登记表结构；数据完全由 Hive 管理（如中间结果表），用内部表。注意 Hive 4 中不带事务的 `CREATE TABLE` 也会被当成外部表，但删表时仍会删除数据。见 [5.4.2 内部表和外部表](../chapters/05-hive/index.md#542)。

??? question "RDD 和 DataFrame 用哪个？"

    处理表格数据优先用 DataFrame：代码短，Spark 知道每列的名字和类型，能自动优化。RDD 适合格式很不规整、需要逐行写复杂逻辑的数据。两者的 Job、Stage、Task 概念相同。见 [6.7 前沿视窗](../chapters/06-spark-rdd/index.md#67) 和 [7.2.1 DataFrame](../chapters/07-data-cleaning/index.md#721-dataframe)。

??? question "清洗数据时，为什么不直接把有问题的行删掉？"

    删掉的往往是真实发生的订单，统计结果会偏小。实验 7-1 中直接 `dropna()` 会删掉 1300 个真实订单。能补的（按单价 × 数量补金额、按用户表补省份）尽量补，确实错了又补不回来的才删，并在清洗规则说明里写清楚。见 [7.3.2 缺失值](../chapters/07-data-cleaning/index.md#732)。

??? question "客单价和笔单价有什么区别？"

    客单价是销售额 ÷ 下单用户数，笔单价是销售额 ÷ 订单数。一个用户可能下好几单，所以客单价通常比笔单价高：9 月小象优选的客单价 2911.70 元，笔单价 1580.06 元。写报告时要注明用的是哪种口径。见 [8.4.1](../chapters/08-spark-sql/index.md#841)。

## 实验环境与排错 { #faq-2 }

??? question "docker pull 很慢或者一直超时怎么办？"

    国内访问 Docker Hub 常常不稳定。可以配置学校或云服务商提供的镜像加速地址，或请老师用 `docker save` 导出镜像文件，大家用 `docker load -i` 导入。见 [2.2.3 安装 Docker 与国内镜像加速](../chapters/02-environment/index.md#223-docker)。

??? question "改了 hdfs-site.xml，为什么没有生效？"

    守护进程只在启动时读一次配置，改完要 `docker compose restart`；有的编辑器保存时会替换文件，容器里看到的仍是旧文件，重启后也能恢复。用 `hdfs getconf -confKey dfs.replication` 确认。另外，副本数的修改只影响新文件，已有文件要用 `hdfs dfs -setrep`。见 [2.4.2 改了配置怎样生效](../chapters/02-environment/index.md#242)。

??? question "在本机用 Python 访问 9870 端口的 WebHDFS 读文件，为什么跳转到 worker1:9864 打不开？"

    NameNode 会让客户端直接到存数据的 DataNode 上读，`worker1` 是集群内部的主机名，本机访问不到。改用 14000 端口的 HttpFS，它代为转发全部数据。见 [3.4.1 两种办法](../chapters/03-hdfs/index.md#341)。



??? question "作业失败了，屏幕上只有 subprocess failed with code 2，怎么找原因？"

    退出码只说明 Python 程序非正常退出，真正的原因在任务的 stderr 日志里：`yarn logs -applicationId <作业编号> -log_files stderr`。code 2 加上 “can't open file” 多半是漏了 `-files`。见 [4.5.5 作业失败或卡住了怎么办](../chapters/04-mapreduce-yarn/index.md#455)。

??? question "作业一直停在 ACCEPTED 不动？"

    先看 8088 网页上是否有别的作业在运行（小集群只够同时运行一个作业），再看 Nodes 页面节点是否 RUNNING、是否因磁盘快满而 UNHEALTHY。见 [4.5.5](../chapters/04-mapreduce-yarn/index.md#455) 和[附录 C](../appendix/c-environment.md)。

??? question "8088 网页上点 History 打不开？"

    链接里的 `master` 是集群内部的主机名，把它改成 `localhost`，或在 hosts 文件中加一行 `127.0.0.1 master`。见[附录 C](../appendix/c-environment.md)。

??? question "在 beeline 中运行 SQL 文件，为什么有的语句没有执行？"

    进入 beeline 后用 `source /labs/ch05/文件.sql;` 运行文件。分号后面同一行如果写了注释，下一条语句会被一起吞掉，所以注释要单独写一行。见[附录 C](../appendix/c-environment.md)。

??? question "Spark 报 CAST_INVALID_INPUT 是什么意思？"

    Spark 4 默认开启 ANSI 模式，把 `¥59.80` 这样的值转成数字时直接报错。先用 `regexp_replace` 去掉多余的字符，再用 `try_cast` 转换，转换失败的会变成 NULL；转换后要再统计一次空值。见 [7.3.3 类型转换](../chapters/07-data-cleaning/index.md#733-cast-try_cast)。

??? question "用 Spark 读 Hive 表，为什么行数比 Hive 里多 1？"

    第 5 章的 ODS 表指向带表头的 CSV 文件，用 `skip.header.line.count` 跳过表头。Hive 认这个设置，Spark 不认，表头被当成了一行数据。读的时候加一句 `filter(F.col("order_id") != "order_id")` 去掉它。见 [8.5.1](../chapters/08-spark-sql/index.md#851-hive)。

??? question "localhost:4040 打不开？"

    Spark UI 只在程序运行期间存在，程序结束就关了。可以在 pyspark 中操作，退出前一直能访问；或者在程序末尾暂停一会儿，如实验 6-1。见[实验 6-1](../chapters/06-spark-rdd/index.md#6-1-pyspark-spark-ui)。
