#!/usr/bin/env bash
# 实验 12-3：综合项目的参考流水线，从原始数据一路跑到看板数据。
# 在本机 labs 目录运行：bash ch12/lab12_3_pipeline.sh
# 前提：已按第 12 章启动集群、spark、MySQL、Superset 容器；本机 Python 装好了 pandas、numpy
set -e
cd "$(dirname "$0")/.."                       # 进入 labs 目录
PY=${PY:-python}                              # 本机的 Python，可以用 PY=python3 bash ... 指定
C="docker compose -f cluster/docker-compose.yml -f cluster/docker-compose.spark.yml -f cluster/docker-compose.mysql.yml"
SUBMIT="$C exec -T spark spark-submit"

step() { echo; echo "========== $1 =========="; date +%T; }

step "1/7 采集：生成会员信息（虚构）"
$PY ch12/make_members.py
step "2/7 脱敏：会员信息脱敏后写入 HDFS"
$SUBMIT /labs/ch12/lab12_3_mask.py 2>/dev/null | grep -E "^[③④]"
$C exec -T master hdfs dfs -chmod 750 /xiaoxiang/secure       # 最小权限：其他用户不能访问
$C exec -T master hdfs dfs -ls /xiaoxiang | grep secure
step "3/7 清洗：订单和日志（第 7 章）"
$PY ch07/make_dirty.py > /dev/null
$SUBMIT /labs/ch07/lab7_1_clean_orders.py 2>/dev/null | tail -3
$SUBMIT /labs/ch07/lab7_2_parse_log.py 2>/dev/null | tail -2
$SUBMIT /labs/ch07/lab7_3_merge_report.py 2>/dev/null | tail -2
step "4/7 统计：RFM 用户分层（第 8 章）"
$SUBMIT /labs/ch08/lab8_2_rfm.py 2>/dev/null | tail -1
step "5/7 分群：K-Means（第 10 章）"
$SUBMIT /labs/ch10/lab10_2_kmeans.py 2>/dev/null | tail -1
step "6/7 入库：看板数据写进 MySQL"
$SUBMIT --jars /labs/jars/mysql-connector-j-8.4.0.jar /labs/ch12/lab12_2_to_mysql.py 2>/dev/null | grep -E "^[①②]"
step "7/7 完成"
echo "打开 http://localhost:8089 ，在看板右上角点“刷新”，就能看到最新的数据。"
date +%T
