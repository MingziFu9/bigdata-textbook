#!/usr/bin/env bash
# 实验 2-2：检查随书 Hadoop 集群是否正常（在 labs/cluster 目录下运行：bash check_cluster.sh）
# 加了 worker3 以后运行：bash check_cluster.sh worker3
NODES="master worker1 worker2 $*"
COMPOSE="docker compose"
[ -n "$*" ] && COMPOSE="docker compose -f docker-compose.yml -f docker-compose.worker3.yml"

echo "== 1. 容器是否都在运行"
$COMPOSE ps --format 'table {{.Service}}\t{{.Status}}'

echo; echo "== 2. 每个容器里运行着哪些 Hadoop 进程（jps 列出 Java 进程）"
for n in $NODES; do
  echo "[$n] $($COMPOSE exec -T $n jps | grep -v Jps | awk '{print $2}' | sort | tr '\n' ' ')"
done

echo; echo "== 3. HDFS：有几个 DataNode 在线"
$COMPOSE exec -T master hdfs dfsadmin -report 2>/dev/null | grep -E "^Live datanodes|^Dead datanodes|^Name:|^DFS Remaining:" | head -20

echo; echo "== 4. YARN：有几个 NodeManager 在线"
$COMPOSE exec -T master yarn node -list -all 2>/dev/null | grep -vE "^[0-9-]+ [0-9:]+ INFO"

echo; echo "== 5. 网页能否打开（200 表示正常）"
for url in http://localhost:9870 http://localhost:8088/cluster http://localhost:19888 "http://localhost:14000/webhdfs/v1/?op=LISTSTATUS&user.name=root"; do
  echo "$(curl -s -o /dev/null -w '%{http_code}' -L "$url")  $url"
done
