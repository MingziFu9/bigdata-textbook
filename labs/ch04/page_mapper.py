#!/usr/bin/env python3
"""实验 4-2、4-3 热门页面的 mapper：每条成功的访问输出“页面<TAB>1”。"""
import sys

for line in sys.stdin:
    fields = line.split()
    if len(fields) == 8 and fields[5] == "200":   # 只统计成功返回的请求
        print(f"{fields[4]}\t1")
