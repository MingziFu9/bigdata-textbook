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
| <span id="g-Hive"></span>Hive | Apache Hive | 建立在 Hadoop 上的数据仓库工具，把 HDFS 上的文件看成表，用类似 SQL 的语言查询，在背后把 SQL 翻译成分布式计算任务 | 用一句 `GROUP BY` 统计 HDFS 上 10 万行订单的各品类销售额 |
| <span id="g-HiveQL"></span>HiveQL | Hive Query Language | Hive 的查询语言，写法和标准 SQL 几乎一样，另有分区、存储格式等 Hive 特有的语法 | `SELECT status, COUNT(*) FROM ods_orders GROUP BY status;` |
| <span id="g-数据仓库"></span>数据仓库 | Data Warehouse | 汇集多个业务系统的历史数据、专门用于统计分析和决策支持的数据存储，以批量读取和汇总为主，很少修改 | 小象优选每天把业务库的订单同步到 Hive 中做报表 |
| <span id="g-ODS"></span>ODS | Operational Data Store | 数据仓库的操作数据层，原样存放从业务系统同步来的数据 | `ods_orders`：订单表原始 CSV 上建的外部表 |
| <span id="g-DWD"></span>DWD | Data Warehouse Detail | 数据仓库的明细数据层，存放清洗、统一格式并补充常用字段后的明细数据 | `dwd_order_detail`：只留已完成订单，补上品类和会员等级 |
| <span id="g-DWS"></span>DWS | Data Warehouse Summary | 数据仓库的汇总数据层，存放按常用维度汇总好、供多张报表共用的数据 | `dws_province_day`：每天每省一行的订单数和销售额 |
| <span id="g-ADS"></span>ADS | Application Data Service | 数据仓库的应用数据层，存放直接给报表、大屏或接口使用的结果 | `ads_province_daily_report`：每日各省销售日报 |
| <span id="g-事实表"></span>事实表 | Fact Table | 记录业务事件的表，一行是一次事件，带有金额、数量等可累加的数字，行数增长快 | 订单表 |
| <span id="g-维度表"></span>维度表 | Dimension Table | 描述业务事件的角度（谁、什么、在哪）的表，行数相对少、变化慢 | 用户表、商品表 |
| <span id="g-Metastore"></span>Metastore | Hive Metastore | Hive 的元数据服务，保存库、表、列、类型、分区和数据所在目录等表结构信息 | 随书环境的 metastore 容器，端口 9083，用 Derby 存元数据 |
| <span id="g-HiveServer2"></span>HiveServer2 | HiveServer2 | Hive 的服务端，接收客户端发来的 SQL，向 Metastore 查询表结构，再交给计算引擎执行 | 随书环境的 hiveserver2 容器，连接端口 10000，网页端口 10002 |
| <span id="g-Beeline"></span>Beeline | Beeline | Hive 自带的命令行客户端，通过 JDBC 连接 HiveServer2 执行 HiveQL | `beeline -u jdbc:hive2://localhost:10000 -n root` |
| <span id="g-Tez"></span>Tez | Apache Tez | Hive 默认使用的计算引擎，把一条 SQL 编排成由多个 Map、Reducer 步骤组成的计算图执行，比 MapReduce 少写中间结果 | 查询进度表中的 `Map 1`、`Reducer 2` |
| <span id="g-内部表"></span>内部表 | Managed Table | 由 Hive 管理数据的表，数据默认放在 warehouse 目录，删表时数据一起删除 | `products_managed`，删表后目录被删除 |
| <span id="g-外部表"></span>外部表 | External Table | 用 `LOCATION` 指向已有 HDFS 目录的表，删表时只删表结构，数据文件保留 | `ods_products` 指向 `/xiaoxiang/ods/products` |
| <span id="g-分区表"></span>分区表 | Partitioned Table | 按某个字段的值把数据存放在表目录下不同子目录中的表，每个值一个分区 | `orders_part` 按 `dt` 分成 30 个分区 |
| <span id="g-分区裁剪"></span>分区裁剪 | Partition Pruning | 查询条件中有分区字段时，只读取符合条件的分区目录、跳过其余分区 | `WHERE dt='2026-09-15'` 只读 1 个分区约 10 万行 |
| <span id="g-动态分区"></span>动态分区 | Dynamic Partition | 写入分区表时，按每行数据中分区字段的值自动把它放进对应分区 | `INSERT OVERWRITE TABLE orders_part PARTITION (dt) SELECT ..., dt FROM ...` |
| <span id="g-ORC"></span>ORC | Optimized Row Columnar | 一种列式存储文件格式，按列压缩存放，查询时只读需要的列，Hive 加工表常用 | `STORED AS ORC`；10 万行订单的 DWD 表约 1.1 MB |
| <span id="g-窗口函数"></span>窗口函数 | Window Function | 在保留每一行的同时，对同一组的若干行做计算的函数，写成 `函数() OVER (PARTITION BY ... ORDER BY ...)` | `RANK()` 排名、`SUM() OVER` 累计、`LAG()` 环比 |
| <span id="g-DataX"></span>DataX | DataX | 阿里巴巴开源的离线数据同步工具，用读插件和写插件在 MySQL、HDFS、Hive 等数据源之间搬运数据 | 每天凌晨把业务库前一天的订单同步到 ODS 层 |

## 第 6 章 Spark 入门与 RDD 编程

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-Spark"></span>Spark | Apache Spark | 通用的分布式计算引擎，把多步计算组织成一个流程、中间结果尽量放在内存中，并提供 SQL、流处理、机器学习等组件 | 用约 27 行 Python 完成第 4 章两个 MapReduce 作业的统计 |
| <span id="g-Driver"></span>Driver | Driver | Spark 程序的驱动进程，运行主程序，把计算拆成任务分派给 Executor 并收集结果 | `print` 和 `collect()` 的结果出现在 Driver 一侧 |
| <span id="g-Executor"></span>Executor | Executor | 在工作节点上为某个 Spark 程序启动的进程，运行任务、缓存数据，可以接连运行很多个任务 | 实验 6-3 在 YARN 上启动了 2 个 Executor，各 1 个核 |
| <span id="g-RDD"></span>RDD | Resilient Distributed Dataset | 弹性分布式数据集，Spark 最基础的数据抽象：分成多个分区、分布在各 Executor 上、只读，丢失的分区可按由来重算 | `sc.textFile("access.log")` 得到每行一个元素的 RDD |
| <span id="g-转换"></span>转换 | Transformation | 由一个 RDD 生成新 RDD 的操作，惰性执行，只记录要怎样计算 | `map`、`filter`、`reduceByKey` |
| <span id="g-行动"></span>行动 | Action | 要求返回具体结果或写出文件的操作，会触发前面所有转换的实际计算 | `count`、`collect`、`take`、`saveAsTextFile` |
| <span id="g-惰性执行"></span>惰性执行 | Lazy Evaluation | 转换不马上执行，等遇到行动时才把整个计算流程一起执行 | 定义三个转换用时 0.6 秒，`count()` 时才读文件 |
| <span id="g-Job"></span>Job | Job | 由一个行动触发的一次完整计算 | 实验 6-1 的程序有 4 个行动，Spark UI 上有 4 个 Job |
| <span id="g-Stage"></span>Stage | Stage | Job 在 Shuffle 处切开后得到的计算阶段，同一 Stage 内的操作在每个分区上连续执行 | 含一次 `reduceByKey` 的 Job 有 2 个 Stage |
| <span id="g-Task"></span>Task | Task | Stage 在一个分区上的执行单元，一个分区一个 Task，由 Executor 的 CPU 核运行 | 70 MB 的本地日志分 3 个分区，每个 Stage 3 个 Task |
| <span id="g-Spark-UI"></span>Spark UI | Spark Web UI | Spark 程序运行时提供的网页，显示 Job、Stage、Task、缓存和 Executor 的情况，默认端口 4040 | 浏览器打开 `localhost:4040` 查看 DAG 图 |
| <span id="g-pyspark"></span>pyspark | PySpark Shell | Spark 的 Python 交互环境，启动时已创建好 `spark` 和 `sc` 两个对象 | 在 spark 容器中输入 `pyspark` |
| <span id="g-spark-submit"></span>spark-submit | spark-submit | 提交 Spark 程序运行的命令，用 `--master` 指定本地模式或集群 | `spark-submit --master yarn lab6_3_yarn_pages.py` |
| <span id="g-键值对-RDD"></span>键值对 RDD | Pair RDD | 元素是 `(键, 值)` 二元组的 RDD，可以使用 reduceByKey、join 等按键操作 | `(页面, 1)` 组成的 RDD |
| <span id="g-缓存"></span>缓存 | Cache / Persist | 把计算出的 RDD 留在 Executor 内存中，供后续多次使用，避免重复读取和计算 | `fields.cache()` 后第二次 `count` 快了 4～6 倍 |
| <span id="g-Serverless-Spark"></span>Serverless Spark | Serverless Spark | 云上无需管理集群的 Spark 服务，按作业临时启动资源、用完释放、按量计费 | 阿里云 EMR Serverless Spark |

## 第 7 章 用 Spark 做数据清洗

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-数据清洗"></span>数据清洗 | Data Cleaning | 发现并改正数据中的重复、缺失、格式不一、类型错误和异常值，使数据可以直接用于分析 | 实验 7-1 把 102000 行脏订单清洗成 99700 行 |
| <span id="g-脏数据"></span>脏数据 | Dirty Data | 含有重复、缺失、格式不一、错误或异常值的数据 | 金额写成“¥233.8”、省份写成“内蒙古自治区” |
| <span id="g-数据质量"></span>数据质量 | Data Quality | 数据满足使用要求的程度，国家标准 GB/T 36344 从规范性、完整性、准确性、一致性、时效性、可访问性六个方面评价 | 省份为空影响完整性，同一省份两种写法影响一致性 |
| <span id="g-缺失值"></span>缺失值 | Missing Value | 应该有值却为空的字段，Spark 中显示为 NULL | 脏订单中 400 行金额为空 |
| <span id="g-重复值"></span>重复值 | Duplicate | 同一条记录出现了多次，会让统计结果偏大 | 系统重复上报的 2000 行订单 |
| <span id="g-异常值"></span>异常值 | Outlier | 不符合业务规则或明显偏离正常范围的值 | 数量为 0、金额为负、下单时间在 2025 年 |
| <span id="g-DataFrame"></span>DataFrame | DataFrame | 带列名和类型的分布式数据表，Spark SQL 的核心数据结构，Spark 能据此自动优化计算 | `spark.read.csv(...)` 返回一个 DataFrame |
| <span id="g-SparkSession"></span>SparkSession | SparkSession | 使用 DataFrame 和 SQL 的入口对象，负责读数据、建表和执行 SQL | pyspark 中已建好的 `spark` 变量 |
| <span id="g-Schema"></span>Schema | Schema | DataFrame 的结构，即每一列的列名、类型和能否为空 | `printSchema()` 显示 `amount: double` |
| <span id="g-ANSI-模式"></span>ANSI 模式 | ANSI Mode | Spark 4 默认开启的 SQL 规则：类型转换失败、除以 0 等直接报错而不是悄悄返回空值 | `cast('¥59.80' as decimal)` 报 CAST_INVALID_INPUT |
| <span id="g-try_cast"></span>try_cast | try_cast | 与 cast 相同的类型转换，但转换失败时返回 NULL 而不报错 | `F.col("amount").try_cast("decimal(10,2)")` |
| <span id="g-正则表达式"></span>正则表达式 | Regular Expression | 描述文本模式的小语言，用来查找、提取或替换符合某种模式的字符串 | <code>(省&#124;市&#124;自治区)$</code> 匹配结尾的“省”“市”“自治区” |
| <span id="g-左连接"></span>左连接 | Left Join | 保留左表的全部行，右表能匹配上的补充列、匹配不上的填 NULL | 订单表左连接用户表补省份 |
| <span id="g-宽表"></span>宽表 | Wide Table | 把事实表和多张维度表合并在一起、列很多的表，便于直接分析 | 订单 + 用户 + 商品合成的 16 列宽表 |
| <span id="g-Parquet"></span>Parquet | Apache Parquet | 开源的列式存储文件格式，自带表结构、压缩率高，是 Spark 默认的文件格式 | 70.2 MB 的日志存成 Parquet 后为 19.1 MB |
| <span id="g-列式存储"></span>列式存储 | Columnar Storage | 把同一列的数据连续存放，查询时只读用到的列，同列数据相似也更容易压缩 | Parquet、ORC 都是列式存储 |

## 第 8 章 DataFrame 与 Spark SQL 数据分析

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-Spark-SQL"></span>Spark SQL | Spark SQL | Spark 处理结构化数据的模块，可以用 DataFrame 函数或 SQL 语句完成同样的计算，两者共用一个执行引擎 | `spark.sql("SELECT ...")` 返回一个 DataFrame |
| <span id="g-临时视图"></span>临时视图 | Temporary View | 把 DataFrame 注册成一个表名，供 SQL 语句查询，程序结束后消失 | `df.createOrReplaceTempView("orders")` |
| <span id="g-透视表"></span>透视表 | Pivot Table | 把某一列的不同取值展开成多列的汇总表，行、列各是一个维度 | 行是会员等级、列是品类、格子里是销售额 |
| <span id="g-移动平均"></span>移动平均 | Moving Average | 当前行和前面若干行的平均值，用来平滑每天的波动，看清趋势 | 每天的 7 日移动平均销售额 |
| <span id="g-环比"></span>环比 | Period-over-Period Growth | 本期与上一期相比的变化率：（本期 − 上期）÷ 上期 | 9 月 2 日销售额比 9 月 1 日增长 12.3% |
| <span id="g-同比"></span>同比 | Year-over-Year Growth | 本期与去年同期相比的变化率 | 2026 年 9 月与 2025 年 9 月比较 |
| <span id="g-GMV"></span>GMV | Gross Merchandise Volume | 成交总额，一段时间内成交订单的金额之和；本书只算已完成的订单 | 9 月 GMV 为 1.42 亿元 |
| <span id="g-客单价"></span>客单价 | Average Revenue per Buyer | 销售额 ÷ 下单用户数，平均每个用户花了多少钱 | 9 月客单价 2911.70 元 |
| <span id="g-复购率"></span>复购率 | Repurchase Rate | 一段时间内购买 2 次及以上的用户占下单用户的比例 | 9 月复购率 37.7% |
| <span id="g-转化率"></span>转化率 | Conversion Rate | 完成目标行为（如下单）的用户占访问用户的比例 | 9 月 30 日访问到下单的转化率 3.11% |
| <span id="g-留存率"></span>留存率 | Retention Rate | 某一时期的用户中，在之后某个时期仍然活跃的比例 | 第一周下单的用户第二周又下单的占 25.5% |
| <span id="g-RFM-模型"></span>RFM 模型 | RFM Model | 用最近一次购买距今天数（R）、购买次数（F）、购买金额（M）衡量用户价值并分层的方法 | 把 48644 个用户分成 8 类 |
| <span id="g-用户分层"></span>用户分层 | Customer Segmentation | 按价值或行为把用户分成若干类，对不同的类采取不同的运营办法 | 重要价值用户 6911 人，贡献 50.9% 的销售额 |
| <span id="g-JDBC"></span>JDBC | Java Database Connectivity | Java 程序访问关系数据库的标准接口，Spark 通过它读写 MySQL 等数据库 | `df.write.jdbc("jdbc:mysql://mysql:3306/xiaoxiang", ...)` |

## 第 9 章 Kafka 与实时数据处理

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-消息队列"></span>消息队列 | Message Queue | 在发送数据的系统和使用数据的系统之间暂存消息的中间件，让双方不必同时在线、各按自己的速度工作 | 下单系统把订单写进 Kafka，大屏和风控系统各自去取 |
| <span id="g-Kafka"></span>Kafka | Apache Kafka | 开源的分布式消息系统（事件流平台），把消息按主题和分区顺序存放在磁盘上，供多个系统读取 | 随书 Kafka 4.4.0 容器，主题 orders 有 3 个分区 |
| <span id="g-Broker"></span>Broker | Broker | 运行 Kafka 服务、存放分区数据的服务器节点 | 随书环境只有一个 Broker，容器名 kafka |
| <span id="g-主题"></span>主题 | Topic | Kafka 中存放同一类消息的逻辑分类，相当于一个文件夹 | 订单消息都发到主题 orders |
| <span id="g-生产者"></span>生产者 | Producer | 向 Kafka 主题发送消息的程序 | 实验 9-1 的 `order_producer.py` |
| <span id="g-消费者组"></span>消费者组 | Consumer Group | 用同一个 group.id 一起读一个主题的一组消费者，每个分区只分给组里的一个消费者，不同的组各读各的 | 大屏组 screen 和风控组 risk 各自读到全部订单 |
| <span id="g-偏移量"></span>偏移量 | Offset | 消息在分区中的序号，从 0 开始；消费者组靠它记住每个分区读到了哪里 | 分区 0 偏移量 259 的消息 |
| <span id="g-消费积压"></span>消费积压 | Consumer Lag | 分区中最新消息的偏移量与消费者组已读到的偏移量之差，即还没处理的消息数 | `kafka-consumer-groups.sh --describe` 输出的 LAG 列 |
| <span id="g-Structured-Streaming"></span>Structured Streaming | Structured Streaming | Spark 的流处理组件，把源源不断的数据看成一张不断变长的表，用 DataFrame 和 SQL 的写法做增量计算 | `spark.readStream.format("kafka")` |
| <span id="g-无界表"></span>无界表 | Unbounded Table | Structured Streaming 对数据流的看法：新到的每条数据都是追加到表末尾的一行，表没有尽头 | 订单流就是一张一直在变长的订单表 |
| <span id="g-事件时间"></span>事件时间 | Event Time | 事件实际发生的时间，通常写在数据里；与数据到达、被处理的时间不同 | 订单消息中的 `event_time`（下单时间） |
| <span id="g-滚动窗口"></span>滚动窗口 | Tumbling Window | 长度固定、首尾相接、互不重叠的时间窗口，每条数据只属于一个窗口 | `window("event_time", "1 minute")` 每分钟一个窗口 |
| <span id="g-滑动窗口"></span>滑动窗口 | Sliding Window | 长度固定、按较短步长向前滑动、相互重叠的时间窗口，一条数据可能属于多个窗口 | `window("event_time", "5 minutes", "1 minute")` |
| <span id="g-水位线"></span>水位线 | Watermark | 流处理中“最多等迟到数据多久”的界线，等于见过的最大事件时间减去设定的等待时间；早于它结束的窗口不再更新 | `withWatermark("event_time", "2 minutes")` |
| <span id="g-输出模式"></span>输出模式 | Output Mode | 流查询每次把结果表的哪些行写出去：append 只写最终结果，update 写有变化的行，complete 写全部结果 | 实验 9-2 用 `outputMode("update")` |
| <span id="g-检查点"></span>检查点 | Checkpoint | 流查询定期保存的读取进度和中间状态，程序重启后从这里接着算，不丢不重 | `option("checkpointLocation", "/xiaoxiang/checkpoint/lab9_2")` |
| <span id="g-Flink"></span>Flink | Apache Flink | 开源的分布式流处理引擎，逐条处理数据、延迟低，国内实时计算的主流选择 | 阿里云“实时计算 Flink 版” |
| <span id="g-实时数仓"></span>实时数仓 | Real-time Data Warehouse | 数据从产生到可以查询只需秒级或分钟级的数据仓库，由流处理引擎持续加工各层数据 | Flink + Paimon 搭建的流式湖仓 |

## 第 10 章 Spark MLlib 机器学习应用

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-机器学习"></span>机器学习 | Machine Learning | 让计算机从历史数据中自动找出规律（模型），再用这个规律对新数据做预测或分组的方法 | 用 9 月的订单学出“什么样的用户下周会再买” |
| <span id="g-特征"></span>特征 | Feature | 描述一个样本的输入变量，模型根据它们做判断 | 用户的购买次数 F、最近一次购买距今天数 R、会员等级 |
| <span id="g-标签"></span>标签 | Label | 监督学习中要预测的结果，训练数据里已经知道答案 | 实验 10-1 的 label：下周复购记 1，没复购记 0 |
| <span id="g-训练集"></span>训练集 | Training Set | 用来让模型学习规律的那部分样本 | `randomSplit([0.8, 0.2])` 中的 80%，32564 个用户 |
| <span id="g-测试集"></span>测试集 | Test Set | 训练时不让模型看到、专门用来检验模型效果的样本 | 实验 10-1 中的 8331 个用户 |
| <span id="g-过拟合"></span>过拟合 | Overfitting | 模型把训练数据中的偶然细节也背了下来，在训练集上很好、在新数据上变差 | 一棵很深的决策树把训练集每个人都分对，测试集 AUC 反而下降 |
| <span id="g-特征向量"></span>特征向量 | Feature Vector | 把一个样本的全部特征按固定顺序排成的一列数，MLlib 的模型只接受这种向量列 | `VectorAssembler` 输出的 features 列 |
| <span id="g-Pipeline"></span>Pipeline | Pipeline | MLlib 中把特征处理和模型按顺序串起来的流水线，整体训练（fit）、整体使用（transform）、整体保存 | 编号 → 独热 → 组装 → 标准化 → 逻辑回归 5 个步骤 |
| <span id="g-独热编码"></span>独热编码 | One-Hot Encoding | 把有 n 种取值的类别列变成 n 个（MLlib 默认 n−1 个）只取 0 或 1 的列，避免把类别编号当成大小 | 会员等级 4 种，编码成 3 位的向量 |
| <span id="g-标准化"></span>标准化 | Standardization | 把各个特征缩放到相近的范围（一般是除以标准差，可再减去平均值），让单位不同的特征可以放在一起比较 | `StandardScaler`：金额以元计、次数以次计，缩放后量级一致 |
| <span id="g-逻辑回归"></span>逻辑回归 | Logistic Regression | 把特征加权求和后转换成 0～1 之间的概率，用来做“是 / 否”二分类的模型 | 输出每个用户复购的概率 |
| <span id="g-随机森林"></span>随机森林 | Random Forest | 训练很多棵各不相同的决策树，让它们投票决定结果的模型，能给出特征重要性 | 实验 10-1 中 50 棵树，AUC 0.638 |
| <span id="g-混淆矩阵"></span>混淆矩阵 | Confusion Matrix | 按“真实是什么、预测成什么”交叉统计样本数的表，用来看模型错在哪里 | 1741 个真复购的用户，模型只预测对 203 个 |
| <span id="g-AUC"></span>AUC | Area Under the ROC Curve | 衡量模型把正例排在负例前面的能力，0.5 相当于随便猜，1 是完全排对 | 实验 10-1 逻辑回归 AUC 0.631 |
| <span id="g-交叉验证"></span>交叉验证 | Cross-Validation | 把训练集分成 k 份，轮流用其中一份检验、其余训练，取平均成绩来挑选参数 | 3 折 × 3 个 regParam，共训练 9 次 |
| <span id="g-K-Means"></span>K-Means | K-Means | 把样本分成 k 群的聚类方法：反复把每个点分给最近的中心、再把中心移到本群平均位置，直到中心不再移动 | 在 RFM 特征上把 48644 个用户分成 4 群 |
| <span id="g-轮廓系数"></span>轮廓系数 | Silhouette Coefficient | 衡量聚类效果的指标，−1～1，越接近 1 说明同群的点越近、不同群的点越远 | `ClusteringEvaluator` 计算，k = 4 时 0.450 |
| <span id="g-批量打分"></span>批量打分 | Batch Scoring | 加载已经训练好的模型，对一大批新数据一次性算出预测结果 | 实验 10-3 给 48644 个老用户打复购概率，用时约 6 秒 |

## 第 11 章 常见问题与调优

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
| <span id="g-行式存储"></span>行式存储 | Row-oriented Storage | 把一行的各列数据连续存放，适合按行写入和按主键查整行，分析时即使只要一列也得读出整行 | CSV、MySQL 的默认存储方式 |
| <span id="g-行组"></span>行组 | Row Group | Parquet 文件中按行切分的数据块，块内按列存放，并记录每列的最小值、最大值等统计信息 | 实验 11-1 中 300 万行写成 4 个文件，每个文件按行组存放 |
| <span id="g-列裁剪"></span>列裁剪 | Column Pruning | 查询只读取用到的列，其余列不从磁盘读出，列式存储才能做到 | 按省统计销售额只读 province、status、amount 3 列 |
| <span id="g-谓词下推"></span>谓词下推 | Predicate Pushdown | 把过滤条件交给数据源在读取时执行，借助统计信息跳过不可能满足条件的数据 | 执行计划中的 `PushedFilters: [EqualTo(status,已完成)]` |
| <span id="g-广播连接"></span>广播连接 | Broadcast Hash Join | 把小表复制到每个执行器，大表在本地直接与它关联，大表不需要 Shuffle | 2000 行的商品表关联 300 万行订单，0.58 秒 |
| <span id="g-排序合并连接"></span>排序合并连接 | Sort Merge Join | 两张表都按关联键 Shuffle 并排序，再把键相同的行合并，适合两张大表关联 | 关闭广播后订单关联商品，Shuffle 写出 14.0 MB，1.31 秒 |
| <span id="g-数据倾斜"></span>数据倾斜 | Data Skew | 某些键的数据特别多，Shuffle 后集中到少数任务，使这几个任务特别慢、拖慢整个阶段 | 秒杀日 s001 占 57% 的订单，一个任务处理 170 万行 |
| <span id="g-加盐"></span>加盐 | Salting | 给热点键加上随机后缀（盐），把它的数据拆到多个任务；关联时另一张表的对应行复制相同的份数 | 盐取 0～19，最大的任务从 170 万行降到 17.7 万行 |
| <span id="g-自适应查询执行"></span>自适应查询执行 | Adaptive Query Execution (AQE) | Spark 在运行中根据每个阶段的实际数据量调整后续执行计划，如合并小分区、拆分倾斜分区、改用广播连接 | Spark 3.2 起默认打开，实验 11-2 中把 200 个任务合并成 3 个 |
| <span id="g-内存溢出"></span>内存溢出 | Out of Memory (OOM) | 程序需要的内存超过了分配给它的上限而报错退出 | 把 300 万行 `collect()` 到只有 512 MB 的 Driver，报 `OutOfMemoryError: Java heap space` |
| <span id="g-并行度"></span>并行度 | Parallelism | 一个阶段同时被拆成多少个任务，决定能用上多少个 CPU 核、每个任务处理多少数据 | `spark.sql.shuffle.partitions` 默认 200 |
| <span id="g-湖仓一体"></span>湖仓一体 | Lakehouse | 在数据湖的开放文件格式上加上事务、版本等表管理能力，让一份数据同时支持报表分析和机器学习 | Iceberg、Paimon、Delta Lake 等表格式 |

## 第 12 章 数据可视化、数据合规与综合项目

| 术语 | 英文 | 定义 | 例子 |
|---|---|---|---|
