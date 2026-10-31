"""实验 8-3（第 3 部分）：pandas 处理不同数据量要多久。在本机 labs 目录下运行：
    python ch08/lab8_3_pandas_time.py
任务与 lab8_3_spark_time.py 完全相同：读订单 CSV，只留已完成的订单，按省份求销售额，排序。
数据来自实验 1-1 生成的 data/scale/orders_*.csv。
"""
import os
import time

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
for n in [10_000, 100_000, 1_000_000, 3_000_000]:
    path = os.path.join(HERE, "..", "data", "scale", f"orders_{n}.csv")
    if not os.path.exists(path):
        print(f"{n:>9,} 行：没有找到 {path}，先运行实验 1-1 的 make_data.py --scale")
        continue
    t0 = time.time()
    df = pd.read_csv(path)
    t1 = time.time()
    result = (df[df["status"] == "已完成"].groupby("province")["amount"].sum()
                .sort_values(ascending=False))
    t2 = time.time()
    mem = df.memory_usage(deep=True).sum() / 1048576
    print(f"{n:>9,} 行：读入 {t1 - t0:5.2f} 秒，统计 {t2 - t1:5.2f} 秒，合计 {t2 - t0:5.2f} 秒，"
          f"占内存 {mem:6.1f} MB，第一名 {result.index[0]} {result.iloc[0]:,.2f}")
