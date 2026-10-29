#!/usr/bin/env bash
# 实验 4-2：用 Hadoop Streaming 分析小象优选的访问日志
# 在 master 容器里运行：docker compose exec master bash /labs/ch04/lab4_2_log_analysis.sh
set -e
cd /labs/ch04
LOG=/labs/data/xiaoxiang/access.log
STREAMING="mapred streaming"

echo "== 第 1 步：把访问日志上传到 HDFS"
hdfs dfs -mkdir -p /xiaoxiang/logs
hdfs dfs -put -f $LOG /xiaoxiang/logs/
hdfs dfs -ls -h /xiaoxiang/logs

echo "== 第 2 步：作业一，统计每小时的 PV 和 UV"
hdfs dfs -rm -r -f -skipTrash /xiaoxiang/out/pv_uv
$STREAMING \
  -D mapreduce.job.name="小象优选-每小时PV与UV" \
  -files pv_mapper.py,pv_reducer.py \
  -mapper "python3 pv_mapper.py" \
  -reducer "python3 pv_reducer.py" \
  -input /xiaoxiang/logs/access.log \
  -output /xiaoxiang/out/pv_uv 2>&1 | grep -E "Running job|completed successfully|Map input records|Reduce output records"
echo "小时  PV     UV"
hdfs dfs -cat /xiaoxiang/out/pv_uv/part-*

echo "== 第 3 步：作业二，最热门的 10 个页面（TopN，只用 1 个 Reducer）"
hdfs dfs -rm -r -f -skipTrash /xiaoxiang/out/top_pages
$STREAMING \
  -D mapreduce.job.name="小象优选-热门页面Top10" \
  -D mapreduce.job.reduces=1 \
  -files page_mapper.py,topn_reducer.py \
  -mapper "python3 page_mapper.py" \
  -reducer "python3 topn_reducer.py" \
  -input /xiaoxiang/logs/access.log \
  -output /xiaoxiang/out/top_pages 2>&1 | grep -E "Running job|completed successfully"
hdfs dfs -cat /xiaoxiang/out/top_pages/part-00000

echo "== 第 4 步：在 YARN 上查看刚才运行过的两个作业"
yarn application -list -appStates FINISHED 2>/dev/null | grep -E "Application-Id|小象优选"
