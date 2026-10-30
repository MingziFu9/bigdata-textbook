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
| <span id="g-大数据"></span>大数据 | Big Data | 规模大、产生快、种类多，用传统单机软件难以在可接受时间内存储和处理的数据集合，也指处理这类数据的技术 | 小象优选每天 100 万行访问日志、一年 25 GB，加上订单、评价、图片，需要用集群存储和计算 |
| <span id="g-5V-特征"></span>5V 特征 | 5V | 描述大数据的五个方面：量大（Volume）、快（Velocity）、多样（Variety）、价值密度低（Value）、真实性（Veracity） | 大促时每秒上千次点击体现“快”；约 5% 的刷评体现“真实性”问题 |
| <span id="g-结构化数据"></span>结构化数据 | Structured Data | 有固定的行和列、每列含义和类型事先规定好的数据，可以直接放进数据库表 | 订单表、用户表、商品表 |
| <span id="g-半结构化数据"></span>半结构化数据 | Semi-structured Data | 有一定格式但不像表格那样整齐、不同记录字段可以不同的数据 | 访问日志、JSON 埋点数据 |
| <span id="g-非结构化数据"></span>非结构化数据 | Unstructured Data | 没有预先定义格式的数据，要借助人工智能技术才能变成可统计的信息 | 商品评价文字、商品图片、直播视频 |
| <span id="g-纵向扩展"></span>纵向扩展 | Scale Up | 通过给一台机器加内存、换更快的硬盘和 CPU 来提高处理能力 | 把服务器内存从 64 GB 升级到 512 GB |
| <span id="g-横向扩展"></span>横向扩展 | Scale Out | 通过增加机器数量、组成集群来提高处理能力 | 把 1 TB 数据分给 100 台普通服务器一起处理 |
| <span id="g-集群"></span>集群 | Cluster | 通过网络连接、由软件统一协调、一起完成存储和计算任务的一组计算机 | 随书 Docker 集群由 1 个主节点、2 个工作节点组成 |
| <span id="g-节点"></span>节点 | Node | 集群中的一台机器（在随书集群中是一个 Docker 容器），分为主节点和工作节点 | master 是主节点，worker1、worker2 是工作节点 |
| <span id="g-移动计算"></span>移动计算 | Moving Computation | 把计算程序发送到存放数据的机器上就地运行，而不是把数据集中搬到一处再算的做法 | 程序只有几 KB，发到存数据的 100 台机器上，比把 1 TB 数据传到一台机器上快得多 |
| <span id="g-大数据处理流程"></span>大数据处理流程 | Big Data Pipeline | 数据从产生到产生价值经过的五个步骤：采集、存储、计算、分析与挖掘、可视化 | 日志采集到 HDFS，用 Spark 清洗汇总，存进 Hive，最后在看板上展示 |
| <span id="g-批处理"></span>批处理 | Batch Processing | 把一段时间内积累的数据一次性处理完的方式，结果有一定延迟 | 每天凌晨统计前一天各省的销售额 |
| <span id="g-流处理"></span>流处理 | Stream Processing | 数据一产生就在几秒内处理的方式，也叫实时处理 | 大促期间每隔几秒更新一次的成交额大屏 |
| <span id="g-Hadoop"></span>Hadoop | Apache Hadoop | 开源的分布式存储和计算框架，核心包括 HDFS、MapReduce 和 YARN，围绕它形成了庞大的大数据生态 | 本书用 Docker 运行 Hadoop 3.4.3 搭建小集群 |
| <span id="g-本地模式"></span>本地模式 | Local Mode | Spark 在一台电脑上运行、用多个 CPU 核模拟集群的方式，不需要安装 Hadoop | `.master("local[*]")` 用本机所有 CPU 核运行实验 1-2 |
| <span id="g-Text-to-SQL"></span>Text-to-SQL | Text-to-SQL | 用大语言模型等技术把自然语言的问题自动翻译成 SQL 查询语句 | 输入“9 月广东销售额前 10 的商品”，模型生成对应的 SELECT 语句 |

## 第 2 章 实验环境：Docker 与 Hadoop 集群

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-Docker"></span>Docker | Docker | 把软件和它需要的运行环境打包在一起、在任何装有 Docker 的电脑上以相同方式运行的容器技术 | 用 Docker 在笔记本上一条命令启动 Hadoop 集群 |
| <span id="g-镜像"></span>镜像 | Image | 打包好的软件和运行环境，只读，用来启动容器，名字后面的标签表示版本 | `apache/hadoop:3.4.3`、`nginx:1.29` |
| <span id="g-Docker-容器"></span>Docker 容器 | Container | 用镜像启动的运行实例，有自己的主机名、IP 地址和文件，彼此隔离；与 YARN 的“容器”不是一回事 | 随书集群的 master、worker1、worker2 是用同一个镜像启动的三个容器 |
| <span id="g-镜像仓库"></span>镜像仓库 | Registry | 存放和分发镜像的服务，最大的公共仓库是 Docker Hub | `docker pull` 从 Docker Hub 下载 `apache/hadoop` 镜像 |
| <span id="g-镜像加速"></span>镜像加速 | Registry Mirror | 让 Docker 改从国内的镜像站下载镜像的设置，解决访问 Docker Hub 慢的问题 | 在 Docker 配置中加入 `registry-mirrors` 地址 |
| <span id="g-端口映射"></span>端口映射 | Port Mapping | 把宿主机的端口转发到容器的端口，让外部能访问容器里的服务，写作“宿主机端口:容器端口” | `9870:9870` 让浏览器打开 localhost:9870 就能看到 NameNode 网页 |
| <span id="g-数据卷"></span>数据卷 | Volume | 由 Docker 管理、在删除容器后仍然保留的存储空间，也可以把宿主机目录挂载进容器 | HDFS 数据存在 `master-data`、`worker1-data` 等数据卷里 |
| <span id="g-Docker-Compose"></span>Docker Compose | Docker Compose | 把多个容器的设置写在一个 YAML 文件里，用一条命令统一启动、停止的工具 | `docker compose up -d` 启动随书集群的三个容器 |
| <span id="g-主节点"></span>主节点 | Master Node | 集群中负责管理和协调的节点，运行 NameNode、ResourceManager 等管理进程 | 随书集群的 master 容器 |
| <span id="g-工作节点"></span>工作节点 | Worker Node | 集群中负责存储数据和执行计算任务的节点，运行 DataNode 和 NodeManager | worker1、worker2，实验 2-3 再加上 worker3 |
| <span id="g-守护进程"></span>守护进程 | Daemon | 在后台持续运行、对外提供服务的程序 | NameNode、DataNode、ResourceManager、NodeManager 都是守护进程 |
| <span id="g-格式化-NameNode"></span>格式化 NameNode | Format NameNode | 第一次使用 HDFS 前初始化 NameNode 元数据目录的操作，相当于给新硬盘建立文件系统，只做一次 | `start-master.sh` 第一次启动时执行 `hdfs namenode -format` |
| <span id="g-jps"></span>jps | jps | Java 自带的命令，列出正在运行的 Java 进程，用来检查 Hadoop 守护进程是否启动 | `docker compose exec worker1 jps` 应列出 DataNode 和 NodeManager |

## 第 3 章 HDFS 分布式文件系统

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-HDFS"></span>HDFS | Hadoop Distributed File System | Hadoop 分布式文件系统，把集群中许多台机器的硬盘组织成一个大文件系统，文件切块、多副本存放 | 小象优选的访问日志存放在 HDFS 的 `/xiaoxiang/logs` 目录 |
| <span id="g-NameNode"></span>NameNode | NameNode | HDFS 的管理节点，保存元数据（目录树、文件由哪些块组成、每块在哪），不存文件内容 | 随书集群的 NameNode 运行在 master 容器，网页端口 9870 |
| <span id="g-DataNode"></span>DataNode | DataNode | HDFS 的数据节点，在本机硬盘上存放数据块，定期向 NameNode 发送心跳和块报告 | worker1、worker2 上各运行一个 DataNode |
| <span id="g-元数据"></span>元数据 | Metadata | 描述数据的数据，在 HDFS 中指目录结构、文件属性、文件与块的对应关系和块的位置 | “access.log 有 6 块，块 0 在 worker1 和 worker2 上” |
| <span id="g-数据块"></span>数据块 | Block | HDFS 存储文件的基本单位，默认 128 MB，文件按顺序切块，最后一块按实际大小存放 | 701.8 MB 的大日志切成 6 块，最后一块约 61.8 MB |
| <span id="g-副本"></span>副本 | Replica | 同一个数据块在不同机器上保存的完全相同的几份拷贝，份数由副本数决定 | 副本数为 2 时，每块在两台 DataNode 上各存一份 |
| <span id="g-心跳"></span>心跳 | Heartbeat | DataNode 每隔几秒向 NameNode 发送的“我还活着”的信号，长时间收不到就判定节点死亡 | 随书集群停掉 worker2 约 87 秒后被判定死亡 |
| <span id="g-块报告"></span>块报告 | Block Report | DataNode 定期向 NameNode 报告本机存有哪些数据块的清单 | NameNode 启动时根据块报告重建每个块的位置信息 |
| <span id="g-副本不足"></span>副本不足 | Under-replicated | 某个块可用的副本数少于目标副本数的状态，NameNode 会安排复制补齐 | 停掉一台 DataNode 后，它上面的块暂时都副本不足 |
| <span id="g-安全模式"></span>安全模式 | Safe Mode | NameNode 启动时等待块报告的只读状态，确认绝大多数块可用后自动退出 | 刚启动集群就上传文件，提示 `Name node is in safe mode` |
| <span id="g-hdfs-dfs"></span>hdfs dfs | HDFS Shell | 操作 HDFS 文件的命令行工具，用法与 Linux 命令相似 | `hdfs dfs -put access.log /xiaoxiang/logs/` |
| <span id="g-hdfs-fsck"></span>hdfs fsck | fsck | 检查 HDFS 文件健康状况的工具，可以列出文件的每个块和副本所在的位置 | `hdfs fsck /a.log -files -blocks -locations` |
| <span id="g-WebHDFS"></span>WebHDFS | WebHDFS | HDFS 提供的 HTTP 接口，用网址加 `op=` 参数完成列目录、读写文件等操作 | `/webhdfs/v1/xiaoxiang/raw?op=LISTSTATUS` |
| <span id="g-HttpFS"></span>HttpFS | HttpFS | 与 WebHDFS 接口相同的 HTTP 网关，由它代为转发全部数据，让集群外的程序只需访问一个地址 | 本机 Python 通过 localhost:14000 读写随书集群的 HDFS |
| <span id="g-小文件问题"></span>小文件问题 | Small Files Problem | 大量远小于块大小的文件占用过多 NameNode 内存、产生过多计算任务，导致集群变慢的问题 | 100 万个 1 KB 文件约占 NameNode 300 MB 内存 |
| <span id="g-对象存储"></span>对象存储 | Object Storage | 把每个文件当作一个对象、通过网址存取、容量几乎无限、按用量付费的云存储服务 | 阿里云 OSS、华为云 OBS、腾讯云 COS |
| <span id="g-存算分离"></span>存算分离 | Storage-Compute Separation | 数据统一存放在对象存储等共享存储中，计算集群按需开关、与存储各自扩展的架构 | 数据放在 OSS，需要分析时才启动 Spark 集群 |

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
