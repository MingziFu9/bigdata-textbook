---
title: YARN 资源分配
description: 调整节点数、节点内存、容器大小和作业规模，逐秒观察 ResourceManager 怎样把内存分成容器交给 ApplicationMaster、Map 和 Reduce 任务（第 4 章）
---

# YARN 资源分配

<iframe class="sim-frame" src="main.html" height="820" loading="lazy" title="YARN 资源分配"></iframe>

[全屏打开](main.html){ target=_blank } · 对应教材：[4.5 YARN：集群的调度中心](../../chapters/04-mapreduce-yarn/index.md#45-yarn)

## 教学说明

**学习目标**

1. 能说出 ResourceManager、NodeManager、ApplicationMaster 和 Container 在一次作业中各做什么（记忆、理解）；
2. 能根据节点内存和容器大小，估算集群同时能运行多少个任务（应用）；
3. 能解释作业为什么会停在 ACCEPTED 状态或一直不动，并提出调整办法（分析）。

**画面与操作**

- 上方“作业列表”对应 8088 网页的 Applications 页，显示每个作业的状态（ACCEPTED、RUNNING、FINISHED）、正在运行的容器数、排队的请求数和进度。
- 中间每个方框是一台 NodeManager，横条是它的内存，每个色块是一个容器：深色是 ApplicationMaster，蓝色是作业 A 的任务，橙色是作业 B 的任务，斜纹表示 Reduce。
- 按“下一步”前进 1 秒，“自动播放”连续播放。下方“发生了什么”按时间倒序列出每次分配和释放。
- 改变节点数、每节点内存、容器大小、作业 A 的输入大小（每 128 MB 一个 Map）和 Reduce 数，或勾选“再提交作业 B”，动画会重新开始。

**数据来源**：默认参数与随书 Docker 集群一致：2 台 NodeManager，每台 2048 MB；AM、Map、Reduce 容器都是 512 MB（见 `labs/cluster/conf/yarn-site.xml` 和 `mapred-site.xml`）。700 MB 输入切成 6 个分片，与实验 4-3 的 1000 万行日志相同。AM 合计最多占集群内存 10% 的规则来自容量调度器的默认配置 `yarn.scheduler.capacity.maximum-am-resource-percent=0.1`；在随书集群上同时提交两个作业，第二个作业确实会停在 ACCEPTED，直到第一个结束。任务时长是为演示设定的，不是实测值。

**课堂用法建议**

1. 用默认参数播放，请学生数一数集群一共能放几个 512 MB 的容器（8 个），作业 A 需要几个（1 个 AM + 6 个 Map + 1 个 Reduce）。
2. 把作业 A 改成 2 GB（16 个 Map），观察 Map 分两批运行；再把节点数改成 4，比较总用时。
3. 节点数 1、容器 2 GB：AM 占满了唯一一台机器，Map 永远等不到容器。请学生说出两种解决办法。
4. 勾选作业 B，观察 B 停在 ACCEPTED：集群明明还有空闲内存，可两个 AM 加起来会超过集群内存的 10%，B 要等 A 结束才能启动。这正是实验 4-2 里同时提交两个作业时看到的现象。再把节点改成 4 台、每台 8 GB，两个作业就能同时运行了。
