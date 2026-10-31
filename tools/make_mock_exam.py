"""从各章课后练习中抽题，组成两套在线模拟卷，写入 docs/mock-exam.md。

用法：python tools/make_mock_exam.py   （可重复运行）

题型和分值按期末闭卷考试：单选 20 题 × 2 分，多选 5 题 × 4 分，判断 10 题 × 1 分，简答 3 题 × 10 分，共 100 分。
抽题规则写在 PAPERS 里：(章号, 题型, 本章第几题)。
"""
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
TYPES = {"一": "single", "二": "multi", "三": "judge", "四": "short", "五": "practice"}
NAMES = {"single": ("一、单项选择题", 2), "multi": ("二、多项选择题", 4), "judge": ("三、判断题", 1), "short": ("四、简答题", 10)}


def chapter_dirs():
    return {int(p.name[:2]): p.name for p in sorted((DOCS / "chapters").iterdir()) if p.is_dir()}


def parse(num, d):
    """返回 {题型: [题目块文本, ...]}，题目块包括题干和“答案与解析”"""
    text = (DOCS / "chapters" / d / "index.md").read_text(encoding="utf-8")
    text = text.split("## 课后练习", 1)[1]
    out, cur = {}, None
    for part in re.split(r"\n(?=\*\*[一二三四五]、)", text):
        m = re.match(r"\*\*([一二三四五])、", part)
        if not m:
            continue
        cur = TYPES[m.group(1)]
        body = part.split("\n", 1)[1]
        start = r'<div class="quiz' if cur in ("single", "multi", "judge") else r"\*\*\d+\.\*\*"
        blocks = [b for b in re.split(rf"\n(?={start})", "\n" + body) if re.match(start, b)]
        out[cur] = [fix_links(b.rstrip() + "\n", d) for b in blocks]
    return out


def fix_links(s, d):
    """章内的相对链接改成从 docs/ 根目录出发"""
    s = re.sub(r"\]\((#[^)]+)\)", lambda m: f"](chapters/{d}/index.md{m.group(1)})", s)
    s = s.replace("](../../", "](")
    s = re.sub(r"\]\((?!https?:|chapters/|appendix/|sims/|#)([^)]+)\)", lambda m: f"](chapters/{d}/{m.group(1)})", s)
    s = s.replace('src="../../', 'src="')
    return s


def renumber(block, n):
    return re.sub(r"\*\*\d+\.\*\*", f"**{n}.**", block, count=1)


# (章, 题型, 本章第几题)
PAPERS = {
    "模拟卷一": {
        "single": [(c, "single", 1) for c in range(1, 13)] + [(c, "single", 2) for c in range(1, 9)],
        "multi": [(c, "multi", 1) for c in (1, 3, 5, 7, 9)],
        "judge": [(c, "judge", 1) for c in range(1, 11)],
        "short": [(4, "short", 1), (8, "short", 1), (12, "short", 1)],
    },
    "模拟卷二": {
        "single": [(c, "single", 3) for c in range(1, 13)] + [(c, "single", 4) for c in range(5, 13)],
        "multi": [(c, "multi", 2) for c in (2, 4, 6, 10, 12)],
        "judge": [(c, "judge", 2) for c in range(3, 13)],
        "short": [(6, "short", 1), (9, "short", 1), (12, "short", 2)],
    },
}


def main():
    dirs = chapter_dirs()
    bank = {c: parse(c, d) for c, d in dirs.items()}
    lines = ["---", "title: 在线模拟卷", "description: 按期末闭卷考试的题型和分值，从各章课后练习中组成的两套在线模拟卷，可以直接作答并查看解析", "---", "",
             "# 在线模拟卷", "",
             "本课程期末为闭卷考试。下面两套模拟卷按期末考试的题型和分值，从各章课后练习中抽题组成，覆盖全书 12 章：",
             "", "| 题型 | 题数 | 每题分值 | 合计 |", "|---|---|---|---|"]
    for t, (name, score) in NAMES.items():
        k = len(PAPERS["模拟卷一"][t])
        lines.append(f"| {name[2:]} | {k} | {score} | {k * score} |")
    lines += ["", "建议限时 90 分钟，先独立做完一整套，再展开“答案与解析”核对。简答题解析中的“评分要点”沿用各章练习的原始分值，自评时按比例换算成 10 分。选择题和判断题可以直接点击作答；每道题解析下方的“回看”链接可以跳回对应章节复习。", ""]
    for paper, spec in PAPERS.items():
        lines += [f"## {paper}", ""]
        for t, (name, score) in NAMES.items():
            lines += [f"**{name}**（每题 {score} 分）", ""]
            for i, (c, typ, k) in enumerate(spec[t], 1):
                lines += [renumber(bank[c][typ][k - 1], i).rstrip(), ""]
    (DOCS / "mock-exam.md").write_text("\n".join(lines), encoding="utf-8")
    print("已生成 docs/mock-exam.md：", {p: sum(len(v) for v in s.values()) for p, s in PAPERS.items()})


if __name__ == "__main__":
    main()
