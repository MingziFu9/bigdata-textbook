#!/usr/bin/env python3
"""实验 4-2 每小时 PV、UV 的 mapper。

日志每行 8 个字段：时间 IP 用户 请求方法 页面 状态码 字节数 终端
例：2026-09-30T08:00:03 183.6.12.34 u0001234 GET /item/p01024 200 5320 Android
输出“小时<TAB>用户”，例如“08<TAB>u0001234”。
"""
import sys

for line in sys.stdin:
    fields = line.split()
    if len(fields) != 8:                 # 跳过格式不对的行（真实日志里总会有几行坏数据）
        continue
    hour = fields[0][11:13]              # 从“2026-09-30T08:00:03”中取出“08”
    print(f"{hour}\t{fields[2]}")
