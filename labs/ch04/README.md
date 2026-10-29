# 第 4 章 MapReduce 与 YARN：实验代码与数据

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `mr_simulate.py` | 实验 4-1：用纯 Python 模拟 Map、分区、Shuffle、Reduce 每个阶段 | 本机 `python ch04/mr_simulate.py` |
| `wc_mapper.py`、`wc_reducer.py` | 词频统计的 mapper 和 reducer | 本机管道或集群 |
| `pv_mapper.py`、`pv_reducer.py` | 每小时 PV 和 UV（去重计数） | 集群，实验 4-2 |
| `page_mapper.py`、`sum_reducer.py` | 页面访问次数（计数求和），`sum_reducer.py` 也可当 Combiner | 集群，实验 4-2、4-3 |
| `topn_reducer.py` | 热门页面前 10 名（排行榜，只能用 1 个 Reduce） | 集群，实验 4-2 |
| `lab4_2_log_analysis.sh` | 实验 4-2 一键脚本 | `docker compose exec master bash /labs/ch04/lab4_2_log_analysis.sh` |
| `lab4_3_combiner.py` | 实验 4-3：比较 Combiner 和 Reduce 个数 | `docker compose exec master python3 /labs/ch04/lab4_3_combiner.py` |

数据见 `../data/README.md`。本机调试用管道：`cat 文件 | python ch04/xx_mapper.py | sort | python ch04/xx_reducer.py`。
