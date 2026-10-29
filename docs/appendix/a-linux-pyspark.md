---
title: 附录 A Linux 与 PySpark 速查
description: 本书实验中常用的 Linux 命令、管道写法和 PySpark 接口速查
---

# 附录 A Linux 与 PySpark 速查

本附录汇总本书实验中最常用的命令和接口，供做实验时随时查阅。它不是完整的教程，Linux 的系统学习请参考“Linux 操作系统”课程。

!!! note "编写中"
    目前收录了第 4 章用到的 Linux 命令；PySpark 部分随第 6～10 章补充。

## A.1 查看文件

```bash
ls -lh data/xiaoxiang/        # 列出文件，-h 用 KB、MB 显示大小
head -3 access.log            # 看前 3 行
tail -n +2 orders.csv         # 从第 2 行开始显示（去掉表头）
wc -l access.log              # 数一数有多少行
```

## A.2 管道与文本处理

管道 `|` 把前一个命令的输出交给后一个命令当输入。本书用它在本机模拟 Hadoop Streaming：

```bash
cat 文件 | python3 mapper.py | sort | python3 reducer.py
```

| 命令 | 作用 | 例子 |
|---|---|---|
| `cut -d, -f5` | 按分隔符取第几列 | 取 CSV 的第 5 列 |
| `sort` | 排序（默认按整行的字符顺序） | 模拟 Shuffle 的排序 |
| `sort -t$'\t' -k2 -nr` | 以 Tab 分列，按第 2 列的数值从大到小排 | 按次数排行 |
| `uniq -c` | 统计相邻重复行的个数（先 `sort`） | `sort | uniq -c` |
| `grep 关键词` | 只保留包含关键词的行 | `grep "/cart" access.log` |
