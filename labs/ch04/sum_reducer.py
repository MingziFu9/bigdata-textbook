#!/usr/bin/env python3
"""通用的求和 reducer：把相同键的值加起来。也可以直接拿来当 Combiner 用。"""
import sys

current_key, total = None, 0
for line in sys.stdin:
    key, value = line.rstrip("\n").split("\t")
    if key != current_key:
        if current_key is not None:
            print(f"{current_key}\t{total}")
        current_key, total = key, 0
    total += int(value)
if current_key is not None:
    print(f"{current_key}\t{total}")
