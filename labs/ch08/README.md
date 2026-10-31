# 第 8 章 DataFrame 与 Spark SQL 数据分析：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `lab8_1_sales.py` | 实验 8-1：整体指标、按品类（DataFrame 与 SQL 两种写法）、透视表、分组排名、环比/累计/移动平均、转化率 | spark 容器：`spark-submit /labs/ch08/lab8_1_sales.py` |
| `lab8_2_rfm.py` | 实验 8-2：RFM 用户分层，结果存到 `data/xiaoxiang_clean/rfm/` | spark 容器：`spark-submit /labs/ch08/lab8_2_rfm.py` |
| `lab8_3_hive_mysql.py` | 实验 8-3：读 Hive 表，写回 Hive 并写进 MySQL | spark 容器：`spark-submit --jars /labs/jars/mysql-connector-j-8.4.0.jar /labs/ch08/lab8_3_hive_mysql.py` |
| `lab8_3_plot.py` | 实验 8-3：本机 pandas 读 MySQL、画图，图片存到 `ch08/output/` | 本机 `labs` 目录：`python ch08/lab8_3_plot.py` |
| `lab8_3_pandas_time.py` | 实验 8-3：pandas 处理 1 万～300 万行的耗时 | 本机 `labs` 目录：`python ch08/lab8_3_pandas_time.py` |
| `lab8_3_spark_time.py` | 实验 8-3：Spark 处理同样数据的耗时 | spark 容器：`spark-submit /labs/ch08/lab8_3_spark_time.py` |

准备工作：

- 实验 8-1、8-2 要先完成实验 7-1、7-2、7-3（生成 `data/xiaoxiang_clean/`）。
- 实验 8-3 要先完成实验 5-1（Hive 中的 `xiaoxiang` 库），并在本机 `labs` 目录下载 MySQL 的 JDBC 驱动：

```bash
mkdir jars
curl -o jars/mysql-connector-j-8.4.0.jar https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar
```

启动实验 8-3 需要的全部容器（Docker 至少分配 8 GB 内存）：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.hive.yml -f docker-compose.spark.yml -f docker-compose.mysql.yml up -d
```

MySQL：主机 `mysql`（容器之间）或 `localhost`（本机），端口 3306，用户 root，密码 xiaoxiang，数据库 xiaoxiang。
