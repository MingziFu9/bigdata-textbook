---
title: 附录 C 实验环境与数据说明
description: 实验所需的软硬件、随书 Docker 集群的启动与网页端口、模拟数据的生成方法，以及常见环境问题的解决办法
---

# 附录 C 实验环境与数据说明

!!! note "编写中"
    本附录随各章同步完善。目前收录了第 1～4 章用到的环境、集群和数据说明，第 5 章起的 Hive、Spark、Kafka 部分写到对应章节时补充。

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

## C.4 模拟数据

全书的实验数据是虚构电商平台“小象优选”的模拟数据，由 `labs/data/make_data.py` 按固定随机种子生成，同样的参数每次生成的内容完全相同，所以你的结果可以和书上逐行对照。各文件的字段和规模见 `labs/data/README.md`。

```bash
cd labs
python data/make_data.py                                   # 默认规模，约 10 秒
python data/make_data.py --only logs --logs 10000000 --out data/xiaoxiang_big   # 1000 万行日志，约 80 秒
```

数据中的用户编号、IP 地址、订单都是随机生成的，不对应任何真实的人和机构。

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
