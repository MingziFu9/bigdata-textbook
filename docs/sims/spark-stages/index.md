---
title: Spark 作业怎样拆成 Stage 和 Task
description: 用按钮拼一个 RDD 程序，看行动怎样触发 Job、Shuffle 怎样把 Job 切成 Stage、每个 Stage 有几个 Task（第 6 章）
---

# Spark 作业怎样拆成 Stage 和 Task

<iframe class="sim-frame" src="main.html" height="720" loading="lazy" title="Spark 作业怎样拆成 Stage 和 Task"></iframe>

[全屏打开](main.html){ target=_blank } · 对应教材：[6.2 看懂背后发生了什么](../../chapters/06-spark-rdd/index.md#62)

## 教学说明

**学习目标**

1. 能区分转换和行动，解释“没有行动就不计算”的惰性执行（理解）；
2. 能根据程序中要 Shuffle 的转换，判断一个 Job 会被切成几个 Stage（分析）；
3. 能根据分区数和 CPU 核数，估计每个 Stage 的 Task 数和要分几批运行（应用）。

**画面与操作**

- 点按钮拼程序，或从“例子”中选择。蓝色是普通转换（map、filter 等），橙色是要 Shuffle 的转换（reduceByKey、distinct 等），绿色是行动（count、collect 等）。
- 按“运行”后，② 中每个粉色框是一个 Stage，小方块是 Task：灰色等待、黄色运行、绿色完成。改变分区数和 CPU 核数，看 Task 怎样分批运行。

**数据来源**：默认例子与实验 6-1 一致。编者在随书 spark 容器中运行实验 6-1，Spark UI 显示第一个 Job 有 2 个 Stage、每个 Stage 3 个 Task（70 MB 的本地日志被分成 3 个分区），见第 6 章的截图。动画中 Shuffle 后的 Task 数取与分区数相同，是 Spark 的默认行为；Spark UI 会把同一 Stage 中的多个转换合并显示。

**课堂用法建议**

1. 只拼 textFile、map、filter，不加行动，按“运行”，引出惰性执行。
2. 选“热门页面”，请学生先预测 Stage 数，再运行核对；再选“每小时 UV”，有两次 Shuffle，变成 3 个 Stage。
3. 分区数 6、核数 2，观察 Task 分 3 批运行；改成核数 4，讨论为什么第 4 章说“集群越大越快”有前提。
