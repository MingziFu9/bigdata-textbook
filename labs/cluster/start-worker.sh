#!/usr/bin/env bash
# worker 容器的启动脚本：DataNode + NodeManager（存数据的机器同时负责计算，体现“移动计算”）
set -e
hdfs --daemon start datanode
exec yarn nodemanager
