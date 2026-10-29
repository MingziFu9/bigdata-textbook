"""实验 4-3：比较 Combiner 和 Reducer 个数对 MapReduce 作业的影响。

在 master 容器里运行（需要先生成 1000 万行日志，见实验步骤）：
    docker compose exec master python3 /labs/ch04/lab4_3_combiner.py
脚本依次提交 4 个 Hadoop Streaming 作业，从作业计数器里读出 Shuffle 的数据量和耗时，最后汇总成一张表。
"""
import re
import subprocess
import time

INPUT = "/xiaoxiang/logs_big/access.log"
CASES = [            # (名称, 是否用 Combiner, Reducer 个数)
    ("A 不用 Combiner，1 个 Reducer", False, 1),
    ("B 用 Combiner，1 个 Reducer", True, 1),
    ("C 用 Combiner，2 个 Reducer", True, 2),
    ("D 用 Combiner，4 个 Reducer", True, 4),
]
COUNTERS = ["Map output records", "Combine input records",
            "Combine output records", "Reduce input records", "Reduce shuffle bytes"]


def run(name, use_combiner, reducers, idx):
    out = f"/xiaoxiang/out/lab4_3_{idx}"
    subprocess.run(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", out], capture_output=True)
    cmd = ["mapred", "streaming",
           "-D", f"mapreduce.job.name=实验4-3-{name[0]}",
           "-D", f"mapreduce.job.reduces={reducers}",
           "-files", "/labs/ch04/page_mapper.py,/labs/ch04/sum_reducer.py",
           "-mapper", "python3 page_mapper.py",
           "-reducer", "python3 sum_reducer.py",
           "-input", INPUT, "-output", out]
    if use_combiner:
        cmd[cmd.index("-reducer"):cmd.index("-reducer")] = ["-combiner", "python3 sum_reducer.py"]
    t = time.time()
    r = subprocess.run(cmd, capture_output=True, text=True)
    log = r.stdout + r.stderr
    seconds = time.time() - t
    values = {}
    for c in COUNTERS:
        m = re.search(rf"\b{c}=(\d+)", log)
        values[c] = int(m.group(1)) if m else 0
    return seconds, values


def upload():
    """第一次运行时把 1000 万行日志上传到 HDFS（约 700 MB，需要 1 分钟左右）"""
    if subprocess.run(["hdfs", "dfs", "-test", "-e", INPUT]).returncode != 0:
        print("正在上传 /labs/data/xiaoxiang_big/access.log 到 HDFS ……", flush=True)
        subprocess.run(["hdfs", "dfs", "-mkdir", "-p", "/xiaoxiang/logs_big"], check=True)
        subprocess.run(["hdfs", "dfs", "-put", "/labs/data/xiaoxiang_big/access.log", INPUT], check=True)


def main():
    upload()
    rows = []
    for i, (name, comb, red) in enumerate(CASES):
        print(f"正在运行：{name} ……", flush=True)
        rows.append((name, *run(name, comb, red, i)))
    print()
    print("方案 | Map 输出记录 | Combine 输出记录 | Reduce 收到记录 | Shuffle 数据量(MB) | 耗时(秒)")
    for name, sec, v in rows:
        print(f"{name} | {v['Map output records']:,} | {v['Combine output records']:,} | "
              f"{v['Reduce input records']:,} | {v['Reduce shuffle bytes'] / 1024 / 1024:.1f} | {sec:.1f}")


if __name__ == "__main__":
    main()
