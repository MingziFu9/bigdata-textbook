"""第 11 章共用：计时，以及从 Spark UI 的接口读出一次运行的读入量、Shuffle 量和任务耗时。

Spark UI 网页上看到的数字，都可以从 http://<driver>:4040/api/v1/ 下的接口取到；
这里把它们打印出来，方便填写实验记录表。运行时用 --py-files 带上本文件。
"""
import json
import time
import urllib.request


def _get(spark, path):
    sc = spark.sparkContext
    url = f"{sc.uiWebUrl}/api/v1/applications/{sc.applicationId}/{path}"
    with urllib.request.urlopen(url) as r:
        return json.load(r)


def timed(spark, label, action, repeat=3):
    """把 action 运行 repeat 次，取最短的一次（第一次常因预热偏慢），返回 (秒数, 最后一次的结果)。
    每次运行放进同一个作业组 label，之后可用 stats(spark, label) 查看它读了多少、Shuffle 了多少。"""
    best, result = None, None
    for i in range(repeat):
        spark.sparkContext.setJobGroup(f"{label}#{i}", label)
        t = time.time()
        result = action()
        best = min(best or 1e9, time.time() - t)
    spark.sparkContext.setJobGroup(f"{label}#{repeat - 1}", label)   # stats 只看最后一次
    return best, result


def stats(spark, label, repeat=3):
    """返回最后一次运行的：读入字节数、Shuffle 写出字节数、各阶段的任务数。"""
    group = f"{label}#{repeat - 1}"
    ids = {s for j in _get(spark, "jobs") if j.get("jobGroup") == group for s in j["stageIds"]}
    stages = [s for s in _get(spark, "stages") if s["stageId"] in ids and s["status"] == "COMPLETE"]
    return {"input_mb": sum(s["inputBytes"] for s in stages) / 2**20,
            "shuffle_mb": sum(s["shuffleWriteBytes"] for s in stages) / 2**20,
            "tasks": sum(s["numCompleteTasks"] for s in stages)}


def slowest_stage(spark, label, repeat=3):
    """找出最后一次运行中、读取 Shuffle 数据的阶段里耗时最长的一个，返回它的任务耗时和读入记录数的中位数、最大值。"""
    group = f"{label}#{repeat - 1}"
    ids = {s for j in _get(spark, "jobs") if j.get("jobGroup") == group for s in j["stageIds"]}
    stages = [s for s in _get(spark, "stages") if s["stageId"] in ids and s["status"] == "COMPLETE"]
    st = max([s for s in stages if s["shuffleReadRecords"] > 0], key=lambda s: s["executorRunTime"])
    q = _get(spark, f"stages/{st['stageId']}/{st['attemptId']}/taskSummary?quantiles=0.5,1.0")
    return {"stage": st["stageId"], "tasks": st["numCompleteTasks"],
            "time_median_ms": q["executorRunTime"][0], "time_max_ms": q["executorRunTime"][1],
            "rows_median": q["shuffleReadMetrics"]["readRecords"][0],
            "rows_max": q["shuffleReadMetrics"]["readRecords"][1]}
