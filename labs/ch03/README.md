# 第 3 章 HDFS 分布式文件系统：实验代码与数据

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `lab3_3_hdfs_python.py` | 实验 3-3：通过 HttpFS（14000 端口）读取 HDFS 上的订单表，用 pandas 统计各支付方式，结果写回 `/xiaoxiang/result/pay_type.csv` | 本机 `python ch03/lab3_3_hdfs_python.py`，需要 `requests`、`pandas` |

实验 3-1、3-2 是 `hdfs dfs`、`hdfs fsck`、`docker compose` 命令，按教材步骤在 master 容器或 `labs/cluster` 目录下逐条运行。实验 3-1 用到的 1000 万行大日志在本机生成：`python data/make_data.py --only logs --logs 10000000 --out data/xiaoxiang_big`。实验 3-2 需要实验 2-3 加入的 worker3。
