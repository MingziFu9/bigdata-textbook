---
title: 附录 B 大数据组件速查
description: 本书用到的 HDFS、YARN、MapReduce、Hive、Spark、Kafka 等组件的常用命令速查
---

# 附录 B 大数据组件速查

按组件分节，每节列出书中实验真正用到的命令。在随书集群上，这些命令都在 master 容器里运行（`docker compose exec master bash`）。

!!! note "编写中"
    目前收录了第 3、4 章用到的 HDFS、YARN 和 MapReduce 命令；Hive、Spark、Kafka 部分随各章补充。

## B.1 HDFS

| 命令 | 作用 |
|---|---|
| `hdfs dfs -mkdir -p /目录` | 新建目录（`-p` 连上级目录一起建） |
| `hdfs dfs -put -f 本地文件 /目录/` | 上传文件，`-f` 覆盖同名文件 |
| `hdfs dfs -ls -h /目录` | 列出目录内容 |
| `hdfs dfs -cat /目录/part-*` | 显示文件内容，可用通配符 |
| `hdfs dfs -rm -r /目录` | 删除目录（MapReduce 重跑前删除输出目录） |
| `hdfs dfs -test -e /路径` | 判断路径是否存在（返回码 0 表示存在） |

## B.2 MapReduce（Hadoop Streaming）

```bash
mapred streaming \
  -D mapreduce.job.name="作业名" -D mapreduce.job.reduces=1 \
  -files mapper.py,reducer.py \
  -mapper "python3 mapper.py" -combiner "python3 reducer.py" -reducer "python3 reducer.py" \
  -input /输入路径 -output /输出目录（必须不存在）
```

各参数的含义见 [4.3.4 节](../chapters/04-mapreduce-yarn/index.md#434)。

## B.3 YARN

| 命令 | 作用 |
|---|---|
| `yarn node -list` | 查看 NodeManager 是否都在运行 |
| `yarn application -list -appStates ALL` | 列出所有作业及状态 |
| `yarn application -kill <作业编号>` | 终止作业 |
| `yarn logs -applicationId <作业编号> -log_files stderr` | 查看各任务的 stderr 日志（Python 的报错在这里） |
