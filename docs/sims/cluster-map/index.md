---
title: 集群地图
description: 点击随书 Hadoop 集群中的每个容器和端口，看它运行哪些进程、数据存在哪里、浏览器访问 localhost 时连到了谁，并模拟加节点和停节点（第 2 章）
---

# 集群地图

<iframe class="sim-frame" src="main.html" height="900" loading="lazy" title="集群地图"></iframe>

[全屏打开](main.html){ target=_blank } · 对应教材：[2.3 一条命令启动 Hadoop 集群](../../chapters/02-environment/index.md#23-hadoop)

## 教学说明

**学习目标**

1. 能说出随书集群中 master 和 worker 容器各运行哪些 Hadoop 进程（记忆）；
2. 能解释“浏览器打开 localhost:9870”时请求怎样经过端口映射到达 master 容器（理解）；
3. 能预测加入或停掉一个工作节点后，DataNode 数、YARN 资源会怎样变化（应用）。

**画面与操作**

- 最外层灰框是你的电脑，虚线框是 Docker 创建的网络，里面每张卡片是一个容器，绿色进程属于 HDFS，蓝色属于 YARN。
- 点容器卡片，下方显示该容器的进程、端口、数据卷和检查命令；点上方的 localhost 端口，显示请求的转发过程。
- “加入 worker3”对应实验 2-3；“停掉 worker2”对应实验 3-2。上方的“集群状态”会跟着变化。

**数据来源**：容器、进程、端口和数据卷全部取自随书 `labs/cluster/docker-compose.yml`、`docker-compose.worker3.yml` 和 `start-master.sh`，与编者在 Docker 29.9 上用 `jps`、`hdfs dfsadmin -report`、`yarn node -list` 检查的结果一致。

**课堂用法建议**

1. 先不看动画，请学生根据 `docker compose ps` 和 `jps` 的输出画出集群结构，再打开动画核对。
2. 点 9870 端口，请学生用自己的话讲一遍请求的路径，引出“端口映射”。提问：为什么 worker2 的 9864 端口在浏览器里打不开？
3. 加入 worker3、停掉 worker2，让学生预测 9870 网页上 Live datanodes 和 8088 网页上的总内存，再到真实集群上验证。
