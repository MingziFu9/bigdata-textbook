"""实验 1-1：生成不同规模的小象优选订单数据，感受“数据变大”以后会发生什么。

在 labs/ 目录下运行：
    python ch01/lab1_1_data_size.py                  # 1 万、10 万、100 万行
    python ch01/lab1_1_data_size.py --max 3000000    # 再加一档 300 万行（需要约 3 GB 空闲内存）
生成的文件放在 data/scale/，不进仓库，做完实验可以删掉。
"""
import argparse
import os
import sys
import time

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data"))
from make_data import SEED, make_orders, make_products, make_users   # 复用随书数据脚本里的函数

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "scale")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max", type=int, default=1_000_000, help="最大行数")
    a = ap.parse_args()
    os.makedirs(OUT, exist_ok=True)
    users = make_users(np.random.default_rng(SEED), 100_000)
    products = make_products(np.random.default_rng(SEED + 1), 2_000)

    sizes = [n for n in (10_000, 100_000, 1_000_000, 3_000_000, 10_000_000) if n <= a.max]
    print("行数 | 文件大小(MB) | 生成(秒) | pandas 读取(秒) | 占用内存(MB) | 内存/文件")
    for n in sizes:
        path = os.path.join(OUT, f"orders_{n}.csv")
        t = time.time()
        make_orders(np.random.default_rng(SEED + 2), n, users, products).to_csv(path, index=False)
        gen = time.time() - t
        file_mb = os.path.getsize(path) / 1024 / 1024

        t = time.time()
        df = pd.read_csv(path)                                   # 读进内存
        read = time.time() - t
        mem_mb = df.memory_usage(deep=True).sum() / 1024 / 1024  # deep=True：连字符串的实际大小一起算
        print(f"{n:,} | {file_mb:.1f} | {gen:.1f} | {read:.2f} | {mem_mb:.0f} | {mem_mb / file_mb:.1f} 倍")
        del df

    # 用最大一档的结果推算：按同样的比例，1 亿行订单需要多少内存
    print(f"\n按 {sizes[-1]:,} 行的结果推算 1 亿行：文件约 {file_mb / sizes[-1] * 1e8 / 1024:.1f} GB，"
          f"读进 pandas 约需内存 {mem_mb / sizes[-1] * 1e8 / 1024:.1f} GB，读取约 {read / sizes[-1] * 1e8 / 60:.1f} 分钟")


if __name__ == "__main__":
    main()
