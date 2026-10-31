# 第 9 章 Kafka 与实时数据处理：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `order_producer.py` | 生产者：模拟国庆晚上 20:00 起 10 分钟的实时下单（1975 笔，固定随机种子），`--late 0.05` 加入迟到订单，`--dry-run` 只打印每分钟的标准答案 | 本机 `labs` 目录：`python ch09/order_producer.py` |
| `order_consumer.py` | 消费者：加入指定的消费者组读取订单，按 `Ctrl + C` 后打印分到的分区和各分区读到的条数 | 本机 `labs` 目录：`python ch09/order_consumer.py --group screen` |
| `lab9_2_stream.py` | 实验 9-2：Structured Streaming 读 Kafka，按事件时间每分钟统计成交额，检查点在 HDFS 的 `/xiaoxiang/checkpoint/lab9_2` | spark 容器：`spark-submit --jars $KAFKA_JARS /labs/ch09/lab9_2_stream.py` |
| `lab9_3_late.py` | 实验 9-3：用不同的水位线统计带迟到订单的主题 `orders_late`，与标准答案比较 | spark 容器：`spark-submit --jars $KAFKA_JARS /labs/ch09/lab9_3_late.py "10 seconds"`（或 `"2 minutes"`、`"5 minutes"`） |

准备工作：

- 本机安装 kafka-python：`pip install kafka-python`。
- 下载 Spark 读 Kafka 用的 4 个 jar 包（路径见 `labs/jars/kafka-jars.txt`）到 `labs/jars/`。
- 启动集群、spark 容器和 Kafka 容器：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.spark.yml -f docker-compose.kafka.yml up -d
```

本机程序连 `localhost:9094`，容器里的程序连 `kafka:9092`。
