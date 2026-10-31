"""实验 12-3 用：生成“小象优选”会员注册信息（含姓名、手机号、身份证号、地址），用来练习脱敏。

在本机 labs 目录运行：python ch12/make_members.py
结果：data/xiaoxiang/members.csv（10 万行，与 users.csv 的用户编号一一对应）

注意：全部内容为教材编写的虚构数据。手机号故意以 100 开头、身份证号故意以 99 开头，
都是不存在的号段和地区码，不对应任何真实的人。
"""
import os

import numpy as np
import pandas as pd

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "xiaoxiang")
SURNAMES = list("王李张刘陈杨黄赵吴周徐孙马朱胡郭何高林罗郑梁谢宋唐许韩冯邓曹彭曾肖田董袁潘")
GIVEN = list("伟芳娜敏静丽强磊军洋勇艳杰涛明超秀霞平刚桂英华玉兰红梅琳宇浩然欣怡子轩")
STREETS = ["人民路", "解放路", "中山路", "建设路", "和平路", "胜利街", "新华路", "朝阳路", "文化路", "光明街"]


def main():
    rng = np.random.default_rng(12)
    users = pd.read_csv(os.path.join(ROOT, "users.csv"))
    n = len(users)
    names = [rng.choice(SURNAMES) + "".join(rng.choice(GIVEN, rng.choice([1, 2]))) for _ in range(n)]
    phones = [f"100{x:08d}" for x in rng.integers(0, 10**8, n)]
    birth = pd.to_datetime("2026-10-01") - pd.to_timedelta(users["age"] * 365 + rng.integers(0, 365, n), unit="D")
    ids = [f"99{rng.integers(0, 10**4):04d}{b:%Y%m%d}{rng.integers(0, 1000):03d}{rng.choice(list('0123456789X'))}"
           for b in birth]
    addr = [f"{p}{c}{rng.choice(STREETS)}{rng.integers(1, 300)}号" for p, c in zip(users["province"], users["city"])]
    df = pd.DataFrame({"user_id": users["user_id"], "name": names, "phone": phones, "id_card": ids,
                       "address": addr, "email": [f"user{i:06d}@example.com" for i in range(1, n + 1)]})
    out = os.path.join(ROOT, "members.csv")
    df.to_csv(out, index=False)
    print(f"已生成 {os.path.abspath(out)}，{n} 行")
    print(df.head(3).to_string(index=False))


if __name__ == "__main__":
    main()
