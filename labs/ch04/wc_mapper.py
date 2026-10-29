#!/usr/bin/env python3
"""实验 4-1 WordCount 的 mapper：每读入一行，就把行里的每个词输出成“词<TAB>1”。

中文评论里的短语用中文逗号隔开，英文用空格隔开，这里两种都当作分隔符。
"""
import re
import sys

for line in sys.stdin:                      # Hadoop 把输入分片一行一行地送到标准输入
    for word in re.split(r"[\s，,。！!]+", line.strip()):
        if word:
            print(f"{word}\t1")             # 输出到标准输出：键<TAB>值
