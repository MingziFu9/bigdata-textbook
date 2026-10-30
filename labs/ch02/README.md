# 第 2 章 实验环境：Docker 与 Hadoop 集群：实验代码与数据

本章的实验文件都在 `../cluster/` 目录：

| 文件 | 用途 |
|---|---|
| `docker-compose.yml` | 1 个主节点 + 2 个工作节点的 Hadoop 3.4.3 集群（实验 2-2） |
| `docker-compose.worker3.yml` | 实验 2-3：再加一个工作节点 worker3，启动时与上一个文件一起用 `-f` 指定 |
| `start-master.sh`、`start-worker.sh` | 容器启动时运行的脚本：第一次格式化 NameNode，启动各守护进程 |
| `check_cluster.sh` | 实验 2-2：一键检查容器、进程、DataNode、NodeManager 和网页；加了 worker3 后运行 `bash check_cluster.sh worker3` |
| `conf/*.xml`、`conf/workers` | Hadoop 配置文件，实验 2-3 修改 `hdfs-site.xml` 中的副本数 |

实验 2-1 只用到 Docker 官方的 `hello-world` 和 `nginx:1.29` 镜像。
