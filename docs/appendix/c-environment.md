---
title: 附录 C 实验环境与数据说明
description: 实验所需的软硬件、随书 Docker 集群的启动与网页端口、模拟数据的生成方法，以及常见环境问题的解决办法
---

# 附录 C 实验环境与数据说明

## C.1 软硬件要求

本书的实验分两种走法，每个实验开头都写明用哪一种：

| 走法 | 适用 | 说明 |
|---|---|---|
| 本机 Python | 实验 1-1、4-1 等只用 Python 的实验 | Python 3.12（numpy 2.5 需要 3.12 及以上），任何电脑都可以 |
| 本机 PySpark | 实验 1-2 等本地模式的 Spark 实验 | 另需 Java 17 或 21；Windows 建议在 WSL 中运行 |
| 本机 Docker 小集群 | 大多数实验 | 建议内存 8 GB 以上、空闲磁盘 20 GB 以上；Windows 和 macOS 安装 Docker Desktop，Linux 安装 Docker Engine 和 Compose 插件 |

电脑配置不够时，可以在云服务器（4 核 16 GB 的按量付费实例即可）或学校实训平台上运行同样的命令。编者的编写环境：4 核 CPU、16 GB 内存的 Linux 主机，Docker 29.9，Docker Compose 5.6，Hadoop 3.4.3 官方镜像。

## C.2 安装 Python 依赖

```bash
cd labs
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

## C.3 随书 Hadoop 小集群

集群的配置文件在 `labs/cluster/`：1 个主节点 master（运行 NameNode、ResourceManager、作业历史服务器和 HttpFS）和 2 个工作节点 worker1、worker2（各运行 DataNode 和 NodeManager）。整个 `labs/` 目录挂载在每个容器的 `/labs` 下，本机改了代码，容器里马上就能用。

```bash
cd labs/cluster
docker compose up -d          # 启动（第一次会下载 Hadoop 镜像，解压后约 2.2 GB）
docker compose ps             # 查看三个容器是否都在运行
docker compose exec master bash   # 进入主节点的命令行
docker compose down           # 停止（HDFS 中的数据保留）
docker compose down -v        # 停止并删除 HDFS 中的全部数据，回到初始状态
```

| 网页 | 地址 | 用途 |
|---|---|---|
| HDFS（NameNode） | <http://localhost:9870> | 查看 DataNode、文件和数据块 |
| YARN（ResourceManager） | <http://localhost:8088> | 查看节点、作业列表和进度 |
| 作业历史 | <http://localhost:19888> | 查看已结束作业的计数器、任务和日志 |
| HttpFS | `http://localhost:14000/webhdfs/v1/…` | 在本机用 Python 读写 HDFS（第 3 章） |

集群的资源设置：每个 NodeManager 给 YARN 分配 2048 MB 内存、2 个虚拟 CPU；MapReduce 的 ApplicationMaster、Map 和 Reduce 任务各用 512 MB 的容器；HDFS 块大小 128 MB、副本数 2；DataNode 停止约 1.5 分钟后被判定死亡（默认 10 分 30 秒，随书配置缩短了检查间隔，便于实验 3-2 观察）。

**加一个工作节点。**实验 2-3 用 `docker-compose.worker3.yml` 加入 worker3：`docker compose -f docker-compose.yml -f docker-compose.worker3.yml up -d`。加入后，每条 `docker compose` 命令都要带上这两个 `-f`。

**改了配置怎样生效。**改完 `conf/` 下的文件后执行 `docker compose restart`，再用 `docker compose exec master hdfs getconf -confKey 配置项` 确认。配置文件是单个文件挂载进容器的，有的编辑器保存时会替换文件，容器里看到的仍是旧文件，重启后即可恢复。

**让网页上的 `master` 链接能打开。**网页里的很多链接写的是集群内部的主机名（如 `http://master:19888/…`）。除了手动把 `master` 改成 `localhost`，也可以在电脑的 hosts 文件中加一行：

```text
127.0.0.1 master
```

hosts 文件在 Windows 上是 `C:\Windows\System32\drivers\etc\hosts`（用管理员身份打开记事本编辑），在 macOS 和 Linux 上是 `/etc/hosts`（用 `sudo` 编辑）。

**第 5 章起加上 Hive、Spark、MySQL、Kafka、Superset。**这几部分各有一个配置文件，需要哪个就叠加哪个 `-f`，例如实验 8-3 同时要用到全部：

```bash
docker compose -f docker-compose.yml -f docker-compose.hive.yml -f docker-compose.spark.yml -f docker-compose.mysql.yml up -d
```

| 配置文件 | 增加的容器 | 端口 | 说明 |
|---|---|---|---|
| `docker-compose.hive.yml` | metastore、hiveserver2（`apache/hive:4.2.1`） | 9083、10000、10002 | 元数据存在 Derby 数据库（数据卷 hive-meta）；Tez 在 hiveserver2 容器内以本地模式运行，读写集群的 HDFS，所以 Hive 的查询不出现在 8088 网页上；`hive-conf/` 让 Hive 写 HDFS 时用 2 个副本 |
| `docker-compose.spark.yml` | spark（`apache/spark:4.1.4-scala2.13-java21-python3-ubuntu`） | 4040 | 常驻的 Spark 命令行容器，默认本地模式；`spark-conf/` 中设定内存、时区、YARN 用的 Java 21（数据卷 spark-jdk 共享给 worker1、worker2）和 Hive Metastore 的连接 |
| `docker-compose.mysql.yml` | mysql（`mysql:8.4`） | 3306 | 第 8 章存放报表结果，数据卷 mysql-data |
| `docker-compose.kafka.yml` | kafka（`apache/kafka:4.4.0`） | 9094 | 第 9 章，KRaft 模式单节点；本机连 9094，容器之间连 `kafka:9092` |
| `docker-compose.superset.yml` | superset（`xiaoxiang/superset:6.1.0`，在官方镜像上加装 PyMySQL） | 8089 | 第 12 章数据看板，需要同时启动 MySQL；数据卷 superset-home |

几点说明：

1. 只开需要的容器。全部开启时空闲内存占用约 7 GB，Docker 至少要分配 8 GB；只用 Spark 本地模式时，`docker-compose.yml` 加 `docker-compose.spark.yml` 就够了。
2. 第 5 章起不需要实验 2-3 加入的 worker3；提交 Spark 作业到 YARN 时更不要启动它，worker3 没有挂载 Java 21。
3. Spark 读写 Hive 表要用 Hive 4 的客户端程序库，它们来自 hiveserver2 容器（数据卷 hive-lib），所以要先启动过一次 Hive；`spark-defaults.conf` 中的 `spark.sql.hive.metastore.version` 设为 4.1.0。用 Spark 读 Hive 的 CSV 文本表时，表头会被当成一行数据，见 8.5.1 节。
4. 第 8 章写 MySQL 要用 JDBC 驱动 `mysql-connector-j-8.4.0.jar`，放在 `labs/jars/`，下载方法见实验 8-3。
5. 第 9 章 Spark 读 Kafka 要用 4 个 jar 包，清单在 `labs/jars/kafka-jars.txt`，下载方法见实验 9-2；spark 容器的环境变量 `KAFKA_JARS` 已经列好了它们的路径。
6. 第 10 章起 spark 服务改为用 `labs/cluster/spark/Dockerfile` 构建（在官方镜像上加装 NumPy，MLlib 需要它），第一次 `up -d` 时会自动构建，默认使用清华 PyPI 镜像。
7. 第 12 章同时运行集群、spark、MySQL、Superset 时，Docker 至少分配 7 GB 内存；不用的 hive、kafka 容器可以先停掉。

## C.4 模拟数据

全书的实验数据是虚构电商平台“小象优选”的模拟数据，由 `labs/data/make_data.py` 按固定随机种子生成，同样的参数每次生成的内容完全相同，所以你的结果可以和书上逐行对照。各文件的字段和规模见 `labs/data/README.md`。

```bash
cd labs
python data/make_data.py                                   # 默认规模，约 10 秒
python data/make_data.py --only logs --logs 10000000 --out data/xiaoxiang_big   # 1000 万行日志，约 80 秒
```

第 7 章另有脚本 `labs/ch07/make_dirty.py`，从上面的数据“弄脏”出一份带重复、缺失、格式不一和异常值的订单与日志（随机种子 7），供清洗实验使用；清洗结果存放在 `labs/data/xiaoxiang_clean/`。

第 12 章的脚本 `labs/ch12/make_members.py` 生成 10 万条会员信息（姓名、手机号、身份证号、地址、邮箱，随机种子 12），供脱敏实验使用：手机号以 `100` 开头、身份证号以 `99` 开头，都不是真实存在的号码。

数据中的用户编号、IP 地址、订单、会员信息都是随机生成的，不对应任何真实的人和机构。

## C.5 常见环境问题

| 现象 | 原因 | 解决办法 |
|---|---|---|
| 8088 网页的 Nodes 页面显示节点 UNHEALTHY，作业一直 ACCEPTED | NodeManager 默认在磁盘使用率超过 90% 时把节点标为不健康，不再分配任务 | 清理电脑磁盘；随书配置已把阈值放宽到 98.5%（`yarn-site.xml` 中的 `yarn.nodemanager.disk-health-checker.max-disk-utilization-per-disk-percentage`） |
| 同时提交两个作业，第二个一直 ACCEPTED | 容量调度器默认只允许 ApplicationMaster 占集群资源的 10%，小集群只够一个作业 | 等前一个作业结束；原理见 4.5.5 节 |
| 网页上的时间比北京时间少 8 小时 | 9870、8088 的部分页面按浏览器所在电脑的时区显示时间，电脑（如云主机、虚拟机）时区是世界协调时（UTC） | 加 8 小时即可；命令行输出的时间是北京时间 |
| `docker pull` 很慢或超时 | 访问 Docker Hub 的网络不通 | 配置镜像加速，或由老师用 `docker save` / `docker load -i` 离线分发镜像，见 2.2.3 节 |
| 容器反复重启，状态 `Exited (137)` | 内存不够 | 关闭其他程序；Docker Desktop 中把内存调到 6 GB 以上 |
| 改了配置文件，`getconf` 仍显示旧值 | 没有重启，或编辑器替换了文件 | `docker compose restart` |
| `docker compose up` 报端口被占用 | 本机已有程序占用了 9870、8088 等端口 | 关闭占用端口的程序，或修改 `docker-compose.yml` 中 `ports` 冒号左边的端口号 |
| beeline 执行 `-f 文件` 没有反应，或多条语句只执行了一条 | 随书镜像中的 beeline 不支持这种用法；分号后面同一行的注释会让下一条语句被吞掉 | 进入 beeline 后用 `source /labs/...sql;`；注释单独写一行 |
| Spark 报 `[CAST_INVALID_INPUT]` | Spark 4 默认开启 ANSI 模式，转换失败直接报错 | 脏数据用 `try_cast`、`try_to_timestamp`，见 7.3.3 节 |
| Spark 读 Hive 表报 `Invalid method name: 'get_table'` | Spark 自带的 Hive 客户端版本太旧，连不上 Hive 4 的 Metastore | 使用随书 `spark-conf/`（Metastore 版本 4.1.0），并先启动过一次 Hive |
| `ClassNotFoundException: com.mysql.cj.jdbc.Driver` | 没有 JDBC 驱动 | 下载到 `labs/jars/`，运行时加 `--jars`，见实验 8-3 |
| 本机 Python 连 Kafka 报 `NoBrokersAvailable` | kafka 容器没启动或还没就绪；或者连了容器内部地址 | 等十几秒再试；本机程序连 `localhost:9094`，见实验 9-1 |
| Spark 报 `Failed to find data source: kafka` | 没有带上读 Kafka 的 jar 包 | 下载 4 个 jar 到 `labs/jars/`，运行时加 `--jars $KAFKA_JARS`，见实验 9-2 |
| 流处理改了代码后报 `Some data may have been lost` | 检查点里的偏移量和 Kafka 对不上 | 删掉 HDFS 上对应的检查点目录再运行，见 9.5.3 节 |
| 构建 spark 镜像时 pip 下载很慢或失败 | 访问 PyPI 的网络不通 | Dockerfile 默认用清华镜像；可用 `--build-arg PIP_INDEX_URL=其他镜像地址` 指定 |
| Superset 连接 MySQL 失败 | 主机名写成了 localhost，或没有建只读账号 | 主机名写 `mysql`；先运行 `labs/ch12/lab12_2_bi_user.sql`，见实验 12-2 |
| 打开 8089 看到的是 YARN 或打不开 | 端口映射被改过，或 Superset 还在初始化 | 检查 `docker-compose.superset.yml` 的 `ports`；用 `logs -f superset` 等它就绪 |
