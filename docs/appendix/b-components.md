---
title: 附录 B 大数据组件速查
description: 本书用到的 HDFS、YARN、MapReduce、Hive、Spark、Kafka 等组件的常用命令速查
---

# 附录 B 大数据组件速查

按组件分节，每节列出书中实验真正用到的命令。在随书集群上，这些命令都在 master 容器里运行（`docker compose exec master bash`）。

!!! note "编写中"
    目前收录了第 2～8 章用到的 Docker、HDFS、YARN、MapReduce、Hive、Spark 和 MySQL 命令；Kafka 部分随第 9 章补充。

## B.1 Docker 与随书集群

在 `labs/cluster` 目录下运行。加了 worker3 以后，每条 `docker compose` 命令都要写成 `docker compose -f docker-compose.yml -f docker-compose.worker3.yml ...`。

| 命令 | 作用 |
|---|---|
| `docker compose up -d` | 启动集群（第一次会下载镜像、格式化 NameNode） |
| `docker compose ps` | 查看各容器的状态 |
| `docker compose logs master` | 查看某个容器的日志，排错用 |
| `docker compose exec master bash` | 进入 master 容器的命令行 |
| `docker compose restart` | 重启全部容器（改了配置文件后执行） |
| `docker compose stop` / `start` | 暂停 / 继续，容器和数据都保留 |
| `docker compose down` | 删除容器，保留数据卷（HDFS 文件还在） |
| `docker compose down -v` | 删除容器和数据卷，HDFS 清空 |
| `bash check_cluster.sh` | 一键检查进程、节点和网页（加了 worker3 后写 `bash check_cluster.sh worker3`） |

单个容器的命令：`docker pull`、`docker images`、`docker run -d -p 宿主机端口:容器端口 --name 名字 镜像`、`docker ps -a`、`docker logs`、`docker exec -it 名字 bash`、`docker stop`、`docker rm`，见 [2.2.4 节](../chapters/02-environment/index.md#224)。

## B.2 HDFS

| 命令 | 作用 |
|---|---|
| `hdfs dfs -mkdir -p /目录` | 新建目录（`-p` 连上级目录一起建） |
| `hdfs dfs -put -f 本地文件 /目录/` | 上传文件，`-f` 覆盖同名文件 |
| `hdfs dfs -get /文件 本地路径` | 下载文件 |
| `hdfs dfs -ls -h /目录` | 列出目录内容，`-R` 递归 |
| `hdfs dfs -cat /目录/part-*` | 显示文件内容，可用通配符 |
| `hdfs dfs -head /文件` / `-tail /文件` | 显示开头或结尾 1 KB |
| `hdfs dfs -cp` / `-mv` | HDFS 内复制 / 移动 |
| `hdfs dfs -rm -r /目录` | 删除目录（MapReduce 重跑前删除输出目录） |
| `hdfs dfs -du -h /目录` | 各目录大小（第一列）和含副本的占用空间（第二列） |
| `hdfs dfs -df -h` | HDFS 总容量和剩余空间 |
| `hdfs dfs -count -h /目录` | 目录数、文件数、总大小 |
| `hdfs dfs -setrep -w 3 /文件` | 修改已有文件的副本数并等待完成 |
| `hdfs dfs -test -e /路径` | 判断路径是否存在（返回码 0 表示存在） |
| `hdfs fsck /文件 -files -blocks -locations` | 查看文件的每个块和副本位置 |
| `hdfs dfsadmin -report` | 各 DataNode 的状态和容量，Live / Dead datanodes |
| `hdfs dfsadmin -safemode get` | 查看是否处于安全模式 |
| `hdfs getconf -confKey 配置项` | 查看当前生效的配置值，如 `dfs.replication` |

在集群外用 Python 读写 HDFS，通过 HttpFS：`http://localhost:14000/webhdfs/v1/<HDFS 路径>?op=<操作>&user.name=root`，操作有 `LISTSTATUS`、`OPEN`、`MKDIRS`、`CREATE`、`DELETE` 等，见 [3.4 节](../chapters/03-hdfs/index.md#34-python-hdfs)。

## B.3 MapReduce（Hadoop Streaming）

```bash
mapred streaming \
  -D mapreduce.job.name="作业名" -D mapreduce.job.reduces=1 \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" -combiner "python3 reducer.py" -reducer "python3 reducer.py" \
  -input /输入路径 -output /输出目录（必须不存在）
```

各参数的含义见 [4.3.4 节](../chapters/04-mapreduce-yarn/index.md#434)。

## B.4 YARN

| 命令 | 作用 |
|---|---|
| `yarn node -list` | 查看 NodeManager 是否都在运行 |
| `yarn application -list -appStates ALL` | 列出所有作业及状态 |
| `yarn application -kill <作业编号>` | 终止作业 |
| `yarn logs -applicationId <作业编号> -log_files stderr` | 查看各任务的 stderr 日志（Python 的报错在这里） |

## B.5 Hive（第 5 章）

在 `labs/cluster` 目录下运行，命令都要带上 `-f docker-compose.yml -f docker-compose.hive.yml`（下表简写为 `docker compose ...`）。

| 命令 | 作用 |
|---|---|
| `docker compose ... up -d` | 启动集群和 Hive（metastore、hiveserver2 两个容器） |
| `docker compose ... exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n root` | 进入 beeline，`!quit` 退出 |
| `source /labs/ch05/文件.sql;` | 在 beeline 中执行一个 SQL 文件（不要在分号后面同一行写注释） |
| `SHOW DATABASES;`、`USE xiaoxiang;`、`SHOW TABLES;` | 查看库和表 |
| `DESCRIBE FORMATTED 表名;` | 查看表类型（内部 / 外部）、存放位置、格式 |
| `SHOW PARTITIONS 表名;` | 查看分区表的分区 |
| `SET hive.exec.dynamic.partition.mode=nonstrict;` | 动态分区前设置 |

HiveServer2 的网页：<http://localhost:10002>，可以看到最近执行过的查询和耗时。

## B.6 Spark（第 6～8 章）

在 `labs/cluster` 目录下运行，命令都要带上 `-f docker-compose.yml -f docker-compose.spark.yml`。

| 命令 | 作用 |
|---|---|
| `docker compose ... up -d` | 启动集群和 spark 容器 |
| `docker compose ... exec spark bash` | 进入 spark 容器 |
| `pyspark` | 交互环境，已建好 `spark` 和 `sc`，`exit()` 退出 |
| `spark-submit /labs/chNN/程序.py` | 以本地模式运行程序 |
| `spark-submit --master yarn --num-executors 2 /labs/...` | 提交到 YARN 集群（第 6 章） |
| `spark-submit --jars /labs/jars/mysql-connector-j-8.4.0.jar /labs/...` | 带上 MySQL 的 JDBC 驱动（第 8 章） |

Spark UI：程序运行期间打开 <http://localhost:4040>。路径写法：`file:///labs/...` 是容器里的本地文件，`/xiaoxiang/...` 是 HDFS 上的文件。

## B.7 MySQL（第 8 章）

命令要带上 `-f docker-compose.yml -f docker-compose.mysql.yml`。用户 root，密码 xiaoxiang，数据库 xiaoxiang。

| 命令 | 作用 |
|---|---|
| `docker compose ... up -d mysql` | 启动 MySQL |
| `docker compose ... exec mysql mysql --default-character-set=utf8mb4 -uroot -pxiaoxiang xiaoxiang` | 进入 MySQL 命令行，`exit` 退出 |
| `SHOW TABLES;`、`DESCRIBE 表名;` | 查看表和表结构 |

连接地址：容器之间用 `jdbc:mysql://mysql:3306/xiaoxiang`，本机程序用 `localhost:3306`。
