#!/usr/bin/env python3
"""实验 4-2 每小时 PV、UV 的 reducer：输出“小时<TAB>PV<TAB>UV”。

PV（页面浏览量）= 这一小时的请求条数；UV（独立访客数）= 这一小时出现过的不同登录用户数，
未登录访客的用户字段是“-”，不计入 UV。
"""
import sys


def emit(hour, pv, users):
    print(f"{hour}\t{pv}\t{len(users)}")


current_hour, pv, users = None, 0, set()
for line in sys.stdin:
    hour, user = line.rstrip("\n").split("\t")
    if hour != current_hour:
        if current_hour is not None:
            emit(current_hour, pv, users)
        current_hour, pv, users = hour, 0, set()
    pv += 1
    if user != "-":
        users.add(user)                  # 同一小时的用户放进集合里自动去重
if current_hour is not None:
    emit(current_hour, pv, users)
