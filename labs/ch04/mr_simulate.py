"""实验 4-1：用几十行 Python 把 MapReduce 的每个阶段“演”一遍。

不需要 Hadoop，在任何装了 Python 的电脑上都能运行：
    python ch04/mr_simulate.py                       # 默认用 4.2 节“算一算”的三行搜索关键词，2 个 Reducer
    python ch04/mr_simulate.py --reducers 3          # 改成 3 个 Reducer，看分区怎样变化
    python ch04/mr_simulate.py --combiner            # 在 Map 端先合并一次，看传给 Reduce 的数据少了多少
    python ch04/mr_simulate.py --file data/search_words.txt --split 2   # 指定文件，每 2 行切成一个分片
"""
import argparse
from collections import defaultdict

TEXT = ["手机 耳机 充电宝", "耳机 耳机 手机", "充电宝 手机 鞋"]   # 三位用户的搜索关键词


def mapper(line):
    """Map：一行文字 → 若干个 (词, 1)"""
    return [(word, 1) for word in line.split()]


def reducer(key, values):
    """Reduce：一个词和它的所有 1 → (词, 总数)"""
    return key, sum(values)


def text_hash(key):
    """和 Hadoop 中 Text 类型的 hashCode 算法相同：h = 31 * h + 每个字节"""
    h = 1
    for b in key.encode("utf-8"):
        h = (31 * h + (b - 256 if b > 127 else b)) & 0xFFFFFFFF
    return h


def partition(key, n):
    """分区：决定一个键交给哪个 Reducer。Hadoop 默认的 HashPartitioner 用键的哈希值对 Reducer 个数取余，
    所以同一个键不管出现在哪个 Map 里，都一定会被送到同一个 Reducer。"""
    return (text_hash(key) & 0x7FFFFFFF) % n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--file")
    ap.add_argument("--split", type=int, default=1, help="每个输入分片包含几行")
    ap.add_argument("--reducers", type=int, default=2)
    ap.add_argument("--combiner", action="store_true")
    a = ap.parse_args()
    lines = open(a.file, encoding="utf-8").read().split("\n") if a.file else TEXT
    lines = [x for x in lines if x.strip()]
    splits = [lines[i:i + a.split] for i in range(0, len(lines), a.split)]

    print(f"输入共 {len(lines)} 行，切成 {len(splits)} 个分片，启动 {len(splits)} 个 Map 任务、{a.reducers} 个 Reduce 任务\n")
    # ---------- Map 阶段：每个分片一个 Map 任务，互不干扰 ----------
    map_outputs = []
    shuffled = 0
    for i, split in enumerate(splits):
        pairs = [p for line in split for p in mapper(line)]
        print(f"[Map {i}] 输入 {split}")
        print(f"         输出 {pairs}")
        if a.combiner:
            # Combiner：在 Map 端先把本分片里相同的词加起来，相当于“本地的小 Reduce”
            local = defaultdict(int)
            for k, v in pairs:
                local[k] += v
            pairs = sorted(local.items())
            print(f"         合并 {pairs}")
        # 分区 + 排序：按分区号分成几堆，每堆按键排序后写到本地磁盘
        buckets = defaultdict(list)
        for k, v in pairs:
            buckets[partition(k, a.reducers)].append((k, v))
        for r in sorted(buckets):
            buckets[r].sort()
            print(f"         → 分区 {r}：{buckets[r]}")
        shuffled += len(pairs)
        map_outputs.append(buckets)
    print(f"\nShuffle：共有 {shuffled} 个键值对从 Map 端传到 Reduce 端\n")

    # ---------- Shuffle + Reduce 阶段：每个 Reducer 去所有 Map 那里拉取属于自己的分区 ----------
    for r in range(a.reducers):
        fetched = sorted(p for out in map_outputs for p in out.get(r, []))
        groups = defaultdict(list)
        for k, v in fetched:                       # 排好序后相同的键挨在一起，按键分组
            groups[k].append(v)
        print(f"[Reduce {r}] 拉取并归并 {fetched}")
        print(f"           分组 {dict(groups)}")
        print(f"           输出 part-0000{r}：{[reducer(k, vs) for k, vs in groups.items()]}")


if __name__ == "__main__":
    main()
