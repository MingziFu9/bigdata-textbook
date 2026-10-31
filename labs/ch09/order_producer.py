"""实验 9-1～9-3：模拟小象优选国庆晚上的实时下单，把订单发到 Kafka（在本机运行）。

用法（在 labs/ 目录下运行，先 pip install kafka-python）：
    python ch09/order_producer.py                          # 平均每秒 20 笔，模拟 10 分钟，发到主题 orders
    python ch09/order_producer.py --late 0.05 --topic orders_late    # 5% 的订单迟到 1～5 分钟（实验 9-3）
    python ch09/order_producer.py --dry-run                # 不发送，只打印每分钟的“标准答案”

模拟时钟从 2026-10-01 20:00:00 开始，--speed 6 表示现实 1 秒 = 模拟 6 秒，
所以模拟 10 分钟的订单只要 100 秒就能发完。固定随机种子，每个人发出的订单完全相同。
"""
import argparse
import json
import os
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.normpath(os.path.join(HERE, "..", "data", "xiaoxiang"))
START = datetime(2026, 10, 1, 20, 0, 0)
PAY_TYPES = ["微信支付", "支付宝", "银行卡", "花呗分期"]


def make_orders(minutes, rate, speed, late):
    """生成订单列表，按发送顺序排列；late 是迟到订单的比例。"""
    rng = np.random.default_rng(9)
    users = pd.read_csv(os.path.join(DATA, "users.csv"), usecols=["user_id", "province"])
    products = pd.read_csv(os.path.join(DATA, "products.csv"), usecols=["product_id", "category", "price"])
    gaps = rng.exponential(speed / rate, int(minutes * 60 * rate / speed * 1.2))   # 相邻两笔订单相隔的模拟秒数，平均 speed / rate
    offsets = np.cumsum(gaps) - gaps[0]
    offsets = offsets[offsets < minutes * 60]
    n = len(offsets)
    u = users.iloc[rng.integers(0, len(users), n)].reset_index(drop=True)
    p = products.iloc[rng.integers(0, len(products), n)].reset_index(drop=True)
    qty = rng.choice([1, 1, 1, 2, 2, 3], n)
    pay = rng.choice(PAY_TYPES, n, p=[0.45, 0.35, 0.1, 0.1])
    is_late = rng.random(n) < late
    delay = rng.integers(60, 301, n)            # 迟到 1～5 分钟
    orders = []
    for i in range(n):
        send_at = START + timedelta(seconds=float(offsets[i]))
        event = send_at - timedelta(seconds=int(delay[i])) if is_late[i] else send_at
        orders.append({
            "send_at": float(offsets[i]) / speed,      # 第几秒发出（现实时间），不放进消息
            "late": bool(is_late[i]),
            "order_id": f"r{i + 1:06d}",
            "user_id": u.user_id[i],
            "product_id": p.product_id[i],
            "category": p.category[i],
            "quantity": int(qty[i]),
            "amount": round(float(p.price[i]) * int(qty[i]), 2),
            "province": u.province[i],
            "pay_type": str(pay[i]),
            "event_time": event.strftime("%Y-%m-%d %H:%M:%S"),
        })
    return orders


def truth(orders):
    """按下单时间（事件时间）每分钟汇总，作为核对流计算结果的标准答案。"""
    df = pd.DataFrame(orders)
    df["minute"] = df.event_time.str[:16]
    t = df.groupby("minute").agg(orders=("order_id", "count"), gmv=("amount", "sum")).round(2)
    return t


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--server", default="localhost:9094")
    ap.add_argument("--minutes", type=int, default=10, help="模拟多少分钟的订单")
    ap.add_argument("--rate", type=float, default=20, help="现实中平均每秒发几笔")
    ap.add_argument("--speed", type=float, default=6, help="现实 1 秒相当于模拟几秒")
    ap.add_argument("--late", type=float, default=0.0, help="迟到订单的比例，如 0.05")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    orders = make_orders(a.minutes, a.rate, a.speed, a.late)
    print(f"共 {len(orders)} 笔订单，模拟 {START:%m-%d %H:%M} 起 {a.minutes} 分钟，"
          f"其中迟到 {sum(o['late'] for o in orders)} 笔")
    if a.dry_run:
        print(truth(orders).to_string())
        return

    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=a.server)
    t0 = time.time()
    for i, o in enumerate(orders):
        wait = t0 + o.pop("send_at") - time.time()
        if wait > 0:
            time.sleep(wait)                                   # 按模拟的下单节奏发送
        o.pop("late")
        value = json.dumps(o, ensure_ascii=False).encode("utf-8")      # 消息内容：JSON 文本转成字节
        producer.send(a.topic, key=o["user_id"].encode(), value=value)  # 键是用户编号：同一个用户的订单进同一个分区
        if (i + 1) % 500 == 0:
            print(f"已发送 {i + 1} 笔，最新一笔下单时间 {o['event_time'][11:]}")
    producer.flush()
    print(f"发送完毕，用时 {time.time() - t0:.1f} 秒")


if __name__ == "__main__":
    main()
