#!/usr/bin/env python3
"""实验 4-2 TopN 的 reducer：边累加边维护一个只有 N 个位置的“排行榜”，最后输出前 N 名。

注意：只有让所有页面都进入同一个 Reducer（-numReduceTasks 1），得到的才是全局前 N 名。
"""
import heapq
import sys

N = 10
top = []                                  # 小顶堆，堆顶是榜上第 N 名
current_key, total = None, 0


def push(key, count):
    heapq.heappush(top, (count, key))
    if len(top) > N:
        heapq.heappop(top)                # 挤掉榜上最小的那个


for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    if key != current_key:
        if current_key is not None:
            push(current_key, total)
        current_key, total = key, 0
    total += int(value)
if current_key is not None:
    push(current_key, total)
for count, key in sorted(top, reverse=True):
    print(f"{key}\t{count}")
