"""根据 course.yml 同步全书的公共部分。

用法：
    python tools/course.py sync              # 改完 course.yml 后运行，可重复运行
    python tools/course.py new-sim 名称 章号 "动画标题"   # 新建一个交互动画骨架

sync 会做这些事（只增不删，已写好的正文不会被覆盖）：
    1. mkdocs.yml：站点名、描述、网址、导航（nav: 之后的内容全部重新生成）
    2. 首页：书名、课程信息框、“目录与实验”表格
    3. 章节：course.yml 里有、但文件夹还不存在的章，按 tools/templates/chapter.md 生成骨架
    4. 实验：每章一个 labs/chNN/ 文件夹（没有实验学时的章不建）
    5. 术语表：每章一个“## 第 N 章”小节
    6. 学习图谱：data.js 里的章节名、文件夹列表
    7. 附录：按顺序编号为 A、B、C……（标题和“## A.1”这类小节号一起改）
    8. 交互动画目录页（按各动画 index.md 头部的 title、description 生成）
    9. 首页“先玩一玩”轮播（course.yml 的 showcase 段，见 tools/showcase.py）

动画属于第几章：index.md 头部写 chapter: N，或者 description 以“（第 N 章）”结尾。
"""
import re
import sys

from common import (CHAPTERS, DOCS, ROOT, chapter_heading, chapter_path, course, front_matter,
                    replace_between, site_url)
from showcase import sync as sync_showcase

GLOSSARY = DOCS / "appendix/glossary.md"

TEMPLATES = ROOT / "tools/templates"


def fill(template, **kw):
    for k, v in kw.items():
        template = template.replace(f"<<{k}>>", str(v))
    return template


# ---------- 章节骨架 ----------

def hours_line(ch, c):
    w = c["lab_word"]
    if ch["labs"] and ch["lab"]:
        return f"理论 {ch['theory']} 学时 · 实验 {ch['lab']} 学时（本章共 {ch['labs']} 个{w}，教师可任选）"
    if ch["labs"]:
        return f"{ch['theory']} 学时（本章共 {ch['labs']} 个{w}）"
    return f"{ch['theory']} 学时"


def render_chapter(ch, c):
    n = ch["num"]
    labs = ""
    if ch["labs"]:
        lab_tpl = (TEMPLATES / "lab.md").read_text(encoding="utf-8")
        w = c["lab_word"]
        rows = "\n".join(f"    | {w} {n}-{i} | ★ 基础 | 45 分钟 | …… |" for i in range(1, ch["labs"] + 1))
        labs = (f"## {c['lab_section']}\n\n!!! abstract \"{w}总览\"\n    | {w} | 类型 | 建议时长 | 内容 |\n    |---|---|---|---|\n{rows}\n\n"
                f"    代码见配套资源 `labs/ch{n:02d}/` 目录。\n\n")
        labs += "".join(fill(lab_tpl, num=n, i=i, nn=f"{n:02d}", word=w) for i in range(1, ch["labs"] + 1))
    cert = ""
    if c["certificates"]:
        cert = ('!!! cert "证书考点"\n    本节对应 **证书名称** 的“……”考核要求：……'
                "（详见[证书考点对照](../../certificate.md)）。\n\n")
    return fill((TEMPLATES / "chapter.md").read_text(encoding="utf-8"),
                num=n, title=ch["title"], short=ch["short"], hours_line=hours_line(ch, c),
                frontier_title=c.get("frontier_title", "前沿视窗"), labs_block=labs, cert_block=cert)


def sync_chapters(c):
    for ch in c["chapters"]:
        p = chapter_path(ch)
        if not p.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
            (p.parent / "images").mkdir(exist_ok=True)
            p.write_text(render_chapter(ch, c), encoding="utf-8")
            print(f"  新建章节骨架 {p.relative_to(ROOT)}")
        if ch["labs"] and ch["lab"]:     # 没有实验学时（如导论的在线体验）不建 labs 目录
            lab = ROOT / "labs" / f"ch{ch['num']:02d}"
            if not lab.exists():
                lab.mkdir(parents=True)
                (lab / "README.md").write_text(f"# {chapter_heading(ch)}：实验代码与数据\n\n", encoding="utf-8")
    known = {ch["dir"] for ch in c["chapters"]}
    for d in sorted(CHAPTERS.iterdir()):
        if d.is_dir() and d.name not in known:
            print(f"  注意：docs/chapters/{d.name} 不在 course.yml 里，不会出现在导航中（不需要可以删掉）")


# ---------- 交互动画 ----------

def sims():
    out = []
    for p in sorted((DOCS / "sims").glob("*/index.md")):
        fm, _ = front_matter(p.read_text(encoding="utf-8"))
        desc = fm.get("description", "")
        m = re.search(r"（第 (\d+) 章）$", desc)
        out.append({"name": p.parent.name, "title": fm.get("title", p.parent.name),
                    "chapter": int(fm.get("chapter", m.group(1) if m else 0)),
                    "summary": re.sub(r"（第 \d+ 章）$", "", desc)})
    return sorted(out, key=lambda s: (s["chapter"], s["name"]))


def sync_sims_index():
    p = DOCS / "sims/index.md"
    rows = ["| 章 | 动画 | 你可以做什么 |", "|---|---|---|"]
    rows += [f"| 第 {s['chapter']} 章 | [{s['title']}]({s['name']}/index.md) | {s['summary']} |" for s in sims()]
    s = replace_between(p.read_text(encoding="utf-8"), "<!-- sims:start -->", "<!-- sims:end -->", "\n".join(rows))
    if s:
        p.write_text(s, encoding="utf-8")


def new_sim(name, chapter, title):
    d = DOCS / "sims" / name
    if d.exists():
        raise SystemExit(f"{d} 已存在")
    d.mkdir(parents=True)
    for f in ("main.html", "index.md"):
        t = (TEMPLATES / f"sim-{f}").read_text(encoding="utf-8")
        (d / f).write_text(fill(t, title=title, chapter=chapter), encoding="utf-8")
    print(f"已新建 {d.relative_to(ROOT)}，写好后运行 python tools/course.py sync 更新导航和动画目录")


# ---------- mkdocs.yml ----------

def q(s):
    return '"' + s.replace('"', '\\"') + '"'


def sync_mkdocs(c):
    p = ROOT / "mkdocs.yml"
    s = p.read_text(encoding="utf-8")
    for key, val in (("site_name", c["title"]), ("site_description", c["description"]),
                     ("site_url", site_url(c)), ("repo_url", f"https://github.com/{c['github_user']}/{c['repo']}")):
        s = re.sub(rf"^{key}:.*$", f"{key}: {val}", s, count=1, flags=re.M)
    nav = ["nav:", "  - 首页: index.md", "  - 课程描述: course-description.md"]
    nav += [f"  - {q(chapter_heading(ch))}: chapters/{ch['dir']}/index.md" for ch in c["chapters"]]
    nav += ["  - 交互动画:", "    - sims/index.md"] + [f"    - {q(x['title'])}: sims/{x['name']}/index.md" for x in sims()]
    nav += ["  - 学习图谱: learning-graph/index.md", "  - 常见问题: faq/index.md"]
    nav += [f"  - {q(x['title'])}: {x['file']}" for x in c.get("extra_pages", [])]
    if c["certificates"] and (DOCS / "certificate.md").exists():
        nav.append("  - 证书考点对照: certificate.md")
    if c.get("appendices"):
        nav.append("  - 附录:")
        nav += [f"    - {q('附录 ' + ap['letter'] + ' ' + ap['title'])}: appendix/{ap['file']}" for ap in c["appendices"]]
    s = s[:s.index("\nnav:") + 1] + "\n".join(nav) + "\n"
    p.write_text(s, encoding="utf-8")


# ---------- 首页 ----------

def lab_titles(ch, c):
    p = chapter_path(ch)
    if not p.exists():
        return []
    return re.findall(rf"^### ({c['lab_word']} \d+-\d+ .+)$", p.read_text(encoding="utf-8"), re.M)


def sync_index(c):
    p = DOCS / "index.md"
    s = p.read_text(encoding="utf-8")
    s = re.sub(r"^# .*$", f"# {c['title']}", s, count=1, flags=re.M)
    h = c["hours"]
    total_labs = sum(ch["labs"] for ch in c["chapters"])
    info = f"{c['credits']} 学分 · {h['theory'] + h['lab']} 学时"
    info += f"（理论 {h['theory']} 学时 + 实验 {h['lab']} 学时）" if h["lab"] else ""
    info += f" · 第 {c['semester']} 学期 · {len(c['chapters'])} 章"
    info += f" · {total_labs} 个{c['lab_word']}" if total_labs else ""
    info += f" · 期末{c['exam']}"
    block = f'!!! abstract "课程信息"\n    {info}'
    if c["certificates"]:
        block += "\n\n    对应证书：" + "、".join(x["name"] for x in c["certificates"])
    s = replace_between(s, "<!-- course:start -->", "<!-- course:end -->", block) or s
    if total_labs:
        rows = [f"| 章节 | {c['lab_word']} |", "|---|---|"]
        rows += [f"| [{chapter_heading(ch)}](chapters/{ch['dir']}/index.md) | {'<br>'.join(lab_titles(ch, c)) or '—'} |" for ch in c["chapters"]]
    else:
        rows = ["| 章节 | 学时 |", "|---|---|"]
        rows += [f"| [{chapter_heading(ch)}](chapters/{ch['dir']}/index.md) | {ch['theory']} |" for ch in c["chapters"]]
    if c.get("appendices"):
        rows += ["", "附录：" + " · ".join(f"[{ap['letter']} {ap['title']}](appendix/{ap['file']})" for ap in c["appendices"])]
    s = replace_between(s, "<!-- toc:start -->", "<!-- toc:end -->", "\n".join(rows)) or s
    p.write_text(s, encoding="utf-8")


# ---------- 术语表、学习图谱、附录 ----------

def sync_glossary(c):
    p = GLOSSARY
    s = p.read_text(encoding="utf-8")
    for ch in c["chapters"]:
        head = f"## 第 {ch['num']} 章 "
        line = head + ch["title"]
        if re.search(rf"^{re.escape(head)}", s, re.M):
            s = re.sub(rf"^{re.escape(head)}.*$", line, s, count=1, flags=re.M)
        else:
            s = s.rstrip("\n") + f"\n\n{line}\n\n| 术语 | 英文 | 定义 | 例子 |\n|---|---|---|---|\n"
    p.write_text(s, encoding="utf-8")


def sync_graph(c):
    p = DOCS / "learning-graph/data.js"
    s = p.read_text(encoding="utf-8")
    chapters = ", ".join(q(x) for x in [""] + [ch["short"] for ch in c["chapters"]])
    dirs = ", ".join(q(x) for x in [""] + [ch["dir"] for ch in c["chapters"]])
    s = re.sub(r"^var CHAPTERS = .*$", f"var CHAPTERS = [{chapters}];", s, count=1, flags=re.M)
    s = re.sub(r"^var CHDIRS = .*$", f"var CHDIRS = [{dirs}];", s, count=1, flags=re.M)
    p.write_text(s, encoding="utf-8")


def sync_appendices(c):
    for ap in c.get("appendices", []):
        p = DOCS / "appendix" / ap["file"]
        if not p.exists():
            p.write_text(f"---\ntitle: 附录 {ap['letter']} {ap['title']}\ndescription: 待填写\n---\n\n"
                         f"# 附录 {ap['letter']} {ap['title']}\n\n待填写。\n", encoding="utf-8")
            print(f"  新建附录 {p.relative_to(ROOT)}")
            continue
        s = p.read_text(encoding="utf-8")
        full = f"附录 {ap['letter']} {ap['title']}"
        s = re.sub(r"^title: .*$", f"title: {full}", s, count=1, flags=re.M)
        s = re.sub(r"^# .*$", f"# {full}", s, count=1, flags=re.M)
        s = re.sub(r"^(##+ )[A-Z]\.(\d)", rf"\g<1>{ap['letter']}.\2", s, flags=re.M)
        p.write_text(s, encoding="utf-8")


def sync():
    c = course()
    print(f"同步《{c['title']}》：{len(c['chapters'])} 章")
    sync_chapters(c)
    sync_appendices(c)
    sync_glossary(c)
    sync_graph(c)
    sync_sims_index()
    sync_index(c)
    sync_showcase(c)
    sync_mkdocs(c)
    print("完成。接下来运行 mkdocs serve 预览。")


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == "sync":
        sync()
    elif sys.argv[1] == "new-sim":
        new_sim(sys.argv[2], int(sys.argv[3]), sys.argv[4])
    else:
        print(__doc__)
