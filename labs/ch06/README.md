# 第 6 章 Spark 入门与 RDD 编程：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `lab6_1_first.py` | 实验 6-1：转换与行动、惰性执行、缓存；结束前暂停 60 秒供查看 Spark UI | spark 容器：`spark-submit /labs/ch06/lab6_1_first.py` |
| `lab6_2_log_rdd.py` | 实验 6-2：用 RDD 统计每小时 PV、UV 和热门页面前 10，与第 4 章实验 4-2 对照 | spark 容器：`spark-submit /labs/ch06/lab6_2_log_rdd.py` |
| `lab6_3_yarn_pages.py` | 实验 6-3：统计 HDFS 上 1000 万行日志的热门页面，提交到 YARN | spark 容器：`spark-submit --master yarn --num-executors 2 /labs/ch06/lab6_3_yarn_pages.py` |

启动并进入 spark 容器：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.spark.yml up -d
docker compose -f docker-compose.yml -f docker-compose.spark.yml exec spark bash
```

Spark UI：程序运行期间在本机浏览器打开 `http://localhost:4040`。
