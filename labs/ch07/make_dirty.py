"""实验 7-1、7-2：给干净的小象优选数据“弄脏”，模拟真实业务数据中的常见问题。

在 labs/ 目录下运行（需要先运行 data/make_data.py）：
    python ch07/make_dirty.py
生成 data/xiaoxiang_dirty/ 下的 orders_dirty.csv 和 access_dirty.log，
并打印每类问题制造了多少条，清洗后可以拿来核对。随机种子固定，每次结果相同。
"""
import os

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.normpath(os.path.join(HERE, "..", "data", "xiaoxiang"))
OUT = os.path.normpath(os.path.join(HERE, "..", "data", "xiaoxiang_dirty"))
rng = np.random.default_rng(7)


def pick(n_total, k, used):
    """从还没被改过的行里随机挑 k 行，避免一行同时有好几种问题，方便核对"""
    free = np.setdiff1d(np.arange(n_total), np.fromiter(used, int, len(used)))
    idx = rng.choice(free, k, replace=False)
    used.update(idx.tolist())
    return idx


def dirty_orders():
    df = pd.read_csv(os.path.join(SRC, "orders.csv"), dtype=str)
    n, used, log = len(df), set(), {}

    # 1. 格式不统一（行数据本身是对的，只是写法不同）
    i = pick(n, 3000, used)
    t = pd.to_datetime(df.loc[i, "order_time"])
    df.loc[i[:1000], "order_time"] = [f"{x.year}/{x.month}/{x.day} {x:%H:%M:%S}" for x in t[:1000]]  # 2026/9/1 08:05:03
    df.loc[i[1000:2000], "order_time"] = t[1000:2000].dt.strftime("%Y-%m-%dT%H:%M:%S").values  # 2026-09-01T08:05:03
    df.loc[i[2000:], "order_time"] = t[2000:].dt.strftime("%Y%m%d %H%M%S").values            # 20260901 080503
    log["下单时间格式不统一"] = 3000
    i = pick(n, 1500, used)
    full = {"北京": "北京市", "上海": "上海市", "天津": "天津市", "重庆": "重庆市", "广西": "广西壮族自治区",
            "内蒙古": "内蒙古自治区", "宁夏": "宁夏回族自治区", "新疆": "新疆维吾尔自治区", "西藏": "西藏自治区"}
    df.loc[i, "province"] = df.loc[i, "province"].map(lambda p: full.get(p, p + "省"))
    log["省份写成全称（广东省、广西壮族自治区）"] = 1500
    i = pick(n, 1000, used)
    df.loc[i, "pay_type"] = " " + df.loc[i, "pay_type"] + "  "
    log["支付方式前后有空格"] = 1000
    i = pick(n, 800, used)
    df.loc[i, "amount"] = "¥" + df.loc[i, "amount"]
    log["金额带 ¥ 符号"] = 800
    i = pick(n, 500, used)
    df.loc[i, "user_id"] = df.loc[i, "user_id"].str.upper()
    log["用户编号大写 U"] = 500

    # 2. 缺失值（可以补回来）
    i = pick(n, 600, used); df.loc[i, "province"] = ""
    log["省份缺失（可按用户表补）"] = 600
    i = pick(n, 400, used); df.loc[i, "amount"] = ""
    log["金额缺失（可按商品单价×数量补）"] = 400
    i = pick(n, 300, used); df.loc[i, "pay_type"] = ""
    log["支付方式缺失（填“未知”）"] = 300

    # 3. 异常值（应该删掉）
    i = pick(n, 100, used); df.loc[i, "quantity"] = "0"
    log["数量为 0"] = 100
    i = pick(n, 100, used); df.loc[i, "amount"] = "-" + df.loc[i, "amount"]
    log["金额为负"] = 100
    i = pick(n, 100, used)
    df.loc[i, "order_time"] = df.loc[i, "order_time"].str.replace("2026-09", "2025-09", regex=False)
    log["下单时间不在 2026 年 9 月"] = 100

    # 4. 重复：同一订单被上报了两次（整行完全相同）
    dup = df.loc[rng.choice(n, 2000, replace=False)]
    log["完全重复的行"] = 2000
    df = pd.concat([df, dup]).sample(frac=1, random_state=7).reset_index(drop=True)

    os.makedirs(OUT, exist_ok=True)
    path = os.path.join(OUT, "orders_dirty.csv")
    df.to_csv(path, index=False)
    print(f"已生成 {path}：{len(df)} 行（原始 {n} 行）")
    for k, v in log.items():
        print(f"  {k}：{v} 行")


def dirty_logs():
    src = os.path.join(SRC, "access.log")
    lines = open(src, encoding="utf-8").read().splitlines()
    bad = []
    for k in range(500):                       # 截断的行：网络中断，只写了一半
        s = lines[rng.integers(len(lines))]
        bad.append(s[: rng.integers(10, len(s) - 10)])
    for k in range(300):                       # 爬虫写的乱码行
        bad.append("".join(rng.choice(list("abcdefXYZ0123456789#$%&*"), 40)))
    for k in range(200):                       # 空行
        bad.append("")
    pos = rng.choice(len(lines) + len(bad), len(bad), replace=False)
    out, j, b = [], 0, iter(bad)
    posset = set(pos.tolist())
    for k in range(len(lines) + len(bad)):
        out.append(next(b) if k in posset else lines[(j := j + 1) - 1])
    path = os.path.join(OUT, "access_dirty.log")
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(out) + "\n")
    print(f"已生成 {path}：{len(out)} 行（原始 {len(lines)} 行，加入截断 500 行、乱码 300 行、空行 200 行）")


if __name__ == "__main__":
    dirty_orders()
    dirty_logs()
