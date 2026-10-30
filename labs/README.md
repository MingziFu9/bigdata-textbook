# 实验代码

按章存放，`chNN/` 对应第 NN 章，文件名为 `labN_M_英文短名.py`（第 N 章实验 N-M），公共数据放在 `data/`，见 `data/README.md`。

安装本机的 Python 依赖（Python 3.12；实验 1-2 等 PySpark 实验另需 Java 17 或 21）：

```bash
pip install -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
```

先生成模拟数据：`python data/make_data.py`（约 10 秒）。

Hadoop 小集群在 `cluster/` 目录：`cd cluster && docker compose up -d` 启动（1 个主节点 + 2 个工作节点，实验 2-3 起可再加 worker3），`labs/` 整个目录挂载在容器里的 `/labs` 下。需要在集群上运行的脚本，文件开头的注释写明了运行方式，例如 `docker compose exec master bash /labs/ch04/lab4_2_log_analysis.sh`。

每个脚本的运行结果都和教材正文中的“运行结果”一致（耗时因电脑而异）。
