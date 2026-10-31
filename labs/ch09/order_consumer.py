"""实验 9-1：从 Kafka 读订单消息（在本机运行）。

用法（在 labs/ 目录下运行）：
    python ch09/order_consumer.py                       # 消费者组 screen，从最早的消息开始读
    python ch09/order_consumer.py --group screen        # 再开一个窗口运行同一条命令：两个消费者分摊分区
    python ch09/order_consumer.py --group risk          # 换一个消费者组：从头再读一遍全部消息
按 Ctrl + C 停止，停止前打印每个分区读了多少条。
"""
import argparse
import json
from collections import Counter

from kafka import KafkaConsumer


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--topic", default="orders")
    ap.add_argument("--server", default="localhost:9094")
    ap.add_argument("--group", default="screen")
    ap.add_argument("--show", type=int, default=5, help="每个分区打印前几条")
    a = ap.parse_args()

    consumer = KafkaConsumer(
        a.topic,
        bootstrap_servers=a.server,
        group_id=a.group,
        auto_offset_reset="earliest",          # 这个组第一次读时，从最早的消息开始
    )
    count, gmv = Counter(), 0.0
    print(f"消费者组 {a.group} 已连接，等待消息……")
    try:
        for msg in consumer:
            o = json.loads(msg.value.decode("utf-8"))      # 字节转回 JSON
            count[msg.partition] += 1
            gmv += o["amount"]
            if count[msg.partition] <= a.show:
                print(f"分区 {msg.partition} 偏移量 {msg.offset:>4}  {o['event_time'][11:]}  "
                      f"{o['user_id']}  {o['category']:<4}  {o['amount']:>9.2f} 元")
    except KeyboardInterrupt:
        pass
    finally:
        print(f"\n本消费者分到的分区：{sorted(p.partition for p in consumer.assignment())}")
        for p in sorted(count):
            print(f"  分区 {p}：{count[p]} 条")
        print(f"合计 {sum(count.values())} 条，金额 {gmv:,.2f} 元")
        consumer.close()


if __name__ == "__main__":
    main()
