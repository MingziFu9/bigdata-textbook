#!/usr/bin/env bash
# master 容器的启动脚本：NameNode + HttpFS + ResourceManager + 作业历史服务器
set -e
# 第一次启动时格式化 NameNode（相当于给新硬盘建文件系统，只做一次）
if [ ! -d /data/hadoop/dfs/name/current ]; then
  echo "第一次启动，格式化 NameNode ……"
  hdfs namenode -format -nonInteractive -clusterId bigdata-textbook
fi
hdfs --daemon start namenode
hdfs --daemon start httpfs          # HttpFS 网关：让集群外的 Python 程序通过 HTTP 读写 HDFS（端口 14000）
mapred --daemon start historyserver
echo "master 已启动：NameNode http://localhost:9870 ，ResourceManager http://localhost:8088"
exec yarn resourcemanager
