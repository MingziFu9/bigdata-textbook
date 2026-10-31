# 第 5 章 Hive 数据仓库：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `lab5_1_create.sql` | 实验 5-1：建库 `xiaoxiang`，为订单、用户、商品建外部表 | Beeline 中 `source /labs/ch05/lab5_1_create.sql;` |
| `lab5_1_query.sql` | 实验 5-1：订单状态、各品类销售额、各会员等级人均消费 | 同上 |
| `lab5_1_tabletype.sql` | 实验 5-1：内部表和外部表删表时的区别（运行前先把 `products.csv` 放到 HDFS 的 `/tmp/`） | 同上 |
| `lab5_2_partition.sql` | 实验 5-2：300 万行订单的外部表、按 `dt` 分区的 `orders_part`，动态分区写入 | 同上 |
| `lab5_2_compare.sql` | 实验 5-2：比较查询一天时分区表和不分区的表 | 同上 |
| `lab5_3_report.sql` | 实验 5-3：ODS → DWD → DWS → ADS 加工每日各省销售日报 | 同上 |

启动 Hive 并连接：

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.hive.yml up -d
docker compose -f docker-compose.yml -f docker-compose.hive.yml exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -n root
```

注意：SQL 文件中的注释要单独占一行，写在分号后面会影响 `source` 对下一条语句的解析。
