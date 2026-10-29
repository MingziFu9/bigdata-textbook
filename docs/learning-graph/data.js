// 学习图谱数据：由编者整理，概念与定义取自附录 D 术语表
// 改完后运行 python tools/graph_text.py，检查数据并更新 index.md 里的文字版
var CATS = ["计算模型", "编程与统计", "资源调度"];
var CHAPTERS = ["", "概述与生态", "Docker 与集群", "HDFS", "MapReduce 与 YARN", "Hive", "Spark 与 RDD", "数据清洗", "Spark SQL", "实时处理", "MLlib", "问题与调优", "可视化与项目"];
var CHDIRS = ["", "01-overview", "02-environment", "03-hdfs", "04-mapreduce-yarn", "05-hive", "06-spark-rdd", "07-data-cleaning", "08-spark-sql", "09-streaming", "10-mllib", "11-tuning", "12-visualization-project"];
var NODES = [
  {"id": "MapReduce", "ch": 4, "cat": 0, "a": "g-MapReduce", "d": "Hadoop 的分布式计算模型：把一个大任务拆成许多 Map 任务在多台机器上并行处理，再由 Reduce 任务按键汇总结果"},
  {"id": "分而治之", "ch": 4, "cat": 0, "a": "g-分而治之", "d": "把一个大问题拆成许多互不相干的小问题分别解决，再把小问题的答案合并起来的思路"},
  {"id": "作业", "ch": 4, "cat": 0, "a": "g-作业", "d": "用户提交给集群的一次完整计算，一个 MapReduce 作业包含若干个 Map 任务和 Reduce 任务"},
  {"id": "任务", "ch": 4, "cat": 0, "a": "g-任务", "d": "作业拆分出来、交给一台机器执行的最小工作单元，分为 Map 任务和 Reduce 任务；失败的任务会自动重试"},
  {"id": "输入分片", "ch": 4, "cat": 0, "a": "g-输入分片", "d": "MapReduce 交给一个 Map 任务处理的那一段输入数据，默认和一个 HDFS 数据块一样大（128 MB）"},
  {"id": "键值对", "ch": 4, "cat": 0, "a": "g-键值对", "d": "MapReduce 中数据的基本形式，由“键”和“值”两部分组成，同一个键的值会被送到一起汇总"},
  {"id": "Map 阶段", "ch": 4, "cat": 0, "a": "g-Map-阶段", "d": "MapReduce 的第一步：每个 Map 任务逐行读取自己的分片，把每行数据转换成若干个键值对"},
  {"id": "Shuffle", "ch": 4, "cat": 0, "a": "g-Shuffle", "d": "Map 和 Reduce 之间的数据重新分发过程：Map 输出按键分区、排序后，每个 Reduce 任务从所有 Map 任务那里拉取属于自己的数据"},
  {"id": "Reduce 阶段", "ch": 4, "cat": 0, "a": "g-Reduce-阶段", "d": "MapReduce 的最后一步：每个 Reduce 任务把同一个键的所有值汇总成结果，写到 HDFS"},
  {"id": "分区", "ch": 4, "cat": 0, "a": "g-分区", "d": "决定每个键交给哪个 Reduce 任务的规则，默认用键的哈希值对 Reduce 任务个数取余，保证同一个键只进一个 Reduce"},
  {"id": "Combiner", "ch": 4, "cat": 0, "a": "g-Combiner", "d": "在 Map 端先把相同键的值合并一次的“本地小 Reduce”，用来减少 Shuffle 传输的数据量，只适合求和、计数、求最大值这类可以分步合并的计算"},
  {"id": "Hadoop Streaming", "ch": 4, "cat": 1, "a": "g-Hadoop-Streaming", "d": "Hadoop 自带的工具，让用 Python 等任何语言编写、从标准输入读数据、向标准输出写结果的程序充当 mapper 和 reducer"},
  {"id": "计数器", "ch": 4, "cat": 1, "a": "g-计数器", "d": "MapReduce 作业运行时自动统计的各种数量，如读入多少行、Map 输出多少条、Shuffle 传了多少字节，是判断作业是否正常的依据"},
  {"id": "PV", "ch": 4, "cat": 1, "a": "g-PV", "d": "页面浏览量，用户每打开一次页面算一次，同一个人打开多次算多次"},
  {"id": "UV", "ch": 4, "cat": 1, "a": "g-UV", "d": "独立访客数，一段时间内访问过网站的不同用户个数，同一个人访问多次只算一次"},
  {"id": "TopN", "ch": 4, "cat": 1, "a": "g-TopN", "d": "按某个指标从大到小取前 N 名的统计，也叫排行榜"},
  {"id": "YARN", "ch": 4, "cat": 2, "a": "g-YARN", "d": "Hadoop 的资源管理和作业调度系统，负责把集群的内存和 CPU 分给各个作业，MapReduce、Spark 都可以运行在它上面"},
  {"id": "ResourceManager", "ch": 4, "cat": 2, "a": "g-ResourceManager", "d": "YARN 的总管，整个集群只有一个，掌握所有机器的资源，决定把资源分给哪个作业"},
  {"id": "NodeManager", "ch": 4, "cat": 2, "a": "g-NodeManager", "d": "YARN 在每台工作机器上的管理员，向 ResourceManager 汇报本机资源，按指令启动和监控容器"},
  {"id": "ApplicationMaster", "ch": 4, "cat": 2, "a": "g-ApplicationMaster", "d": "每个作业专属的“项目经理”，运行在一个容器里，负责向 ResourceManager 申请资源、安排任务、跟踪进度、重试失败任务"},
  {"id": "容器", "ch": 4, "cat": 2, "a": "g-容器", "d": "YARN 分配资源的单位，是某台机器上的一份内存和 CPU，任务就在容器里运行"},
  {"id": "数据本地化", "ch": 4, "cat": 2, "a": "g-数据本地化", "d": "把计算任务安排到存有它要读的数据块的机器上运行，避免通过网络搬运数据"}
];
var EDGES = [["分而治之", "MapReduce"], ["MapReduce", "作业"], ["作业", "任务"], ["输入分片", "Map 阶段"], ["键值对", "Map 阶段"], ["Map 阶段", "分区"], ["分区", "Shuffle"], ["Shuffle", "Reduce 阶段"], ["Map 阶段", "Combiner"], ["Combiner", "Shuffle"], ["Reduce 阶段", "Hadoop Streaming"], ["Map 阶段", "Hadoop Streaming"], ["Hadoop Streaming", "PV"], ["Hadoop Streaming", "UV"], ["Hadoop Streaming", "TopN"], ["Reduce 阶段", "TopN"], ["PV", "UV"], ["任务", "计数器"], ["Shuffle", "计数器"], ["任务", "容器"], ["YARN", "ResourceManager"], ["YARN", "NodeManager"], ["NodeManager", "容器"], ["ResourceManager", "ApplicationMaster"], ["作业", "ApplicationMaster"], ["容器", "ApplicationMaster"], ["输入分片", "数据本地化"], ["ResourceManager", "数据本地化"], ["MapReduce", "YARN"]];
