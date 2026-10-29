---
title: MapReduce 词频统计全流程
description: 输入几行文字，一步一步看 Map、Combine、分区排序、Shuffle、Reduce 每个阶段的数据，以及计数器怎样变化（第 4 章）
---

# MapReduce 词频统计全流程

<iframe class="sim-frame" src="main.html" height="900" loading="lazy" title="MapReduce 词频统计全流程"></iframe>

[全屏打开](main.html){ target=_blank } · 对应教材：[4.2 看懂背后发生了什么](../../chapters/04-mapreduce-yarn/index.md#42-mapshufflereduce)

## 教学说明

**学习目标**

1. 能按顺序说出一个 MapReduce 作业经过的阶段：输入分片、Map、（Combine）、分区与排序、Shuffle、Reduce、输出（记忆、理解）；
2. 能手推一个小例子每个阶段的中间数据，并用动画核对（应用）；
3. 能说明 Combiner 和 Reducer 个数怎样改变 Shuffle 的数据量和输出文件个数（分析）。

**画面与操作**

- 上方文本框里每一行是一个输入分片，交给一个 Map 任务。可以直接改文字，也可以点“换一组”切换三组例子。
- 按“下一步”逐个阶段播放，阶段条标出当前在哪一步；“直接看结果”一次播完；“重新开始”回到第一步。
- 键值对的颜色表示分区：同一颜色的键值对会被送到同一个 Reducer。分区号的算法与 Hadoop 默认的 HashPartitioner 相同，所以和真实集群上的结果完全一致。
- “Reducer 个数”改成 1～4，看分区和输出文件 part-0000N 怎样变化；勾选“使用 Combiner”，看 Map 端先合并后传给 Reduce 的键值对少了多少。
- 下方计数器的名字和 Hadoop 作业结束时打印的计数器相同，方便和实验 4-2、4-3 的运行结果对照。

**数据来源**：默认例子是 4.2.2 节“算一算”的三行搜索关键词（随书 `labs/data/search_words.txt`）。2 个 Reducer 时，“充电宝、耳机”进入分区 0，“手机、鞋”进入分区 1，Map 输出 9 个键值对，加 Combiner 后 Shuffle 只传 8 个，与实验 4-1 模拟程序和真实集群的输出一致。

**课堂用法建议**

1. 先让学生在纸上手推三行关键词的 Map 输出和 Reduce 结果，再用动画逐步核对。
2. 勾选 Combiner，请学生指出是哪一个 Map 任务的输出变少了、为什么（第 2 行有两个“耳机”）。
3. 把“热门商品点击”例子的 Reducer 个数从 1 改到 4，讨论：输出文件变多了，但每个文件里的键还会重复吗？
