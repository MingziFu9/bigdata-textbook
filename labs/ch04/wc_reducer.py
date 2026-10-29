#!/usr/bin/env python3
"""实验 4-1 WordCount 的 reducer：输入已经按词排好序，相同的词挨在一起，逐个累加。"""
import sys

current_word, current_count = None, 0
for line in sys.stdin:
    word, count = line.rstrip("\n").split("\t")
    if word == current_word:                # 还是同一个词，继续累加
        current_count += int(count)
    else:                                   # 换了一个新词：先输出上一个词的结果
        if current_word is not None:
            print(f"{current_word}\t{current_count}")
        current_word, current_count = word, int(count)
if current_word is not None:                # 别忘了最后一个词
    print(f"{current_word}\t{current_count}")
