"""首页“先玩一玩”轮播：根据 course.yml 的 showcase 段生成首页区块，并录制每个动画的短视频。

用法：
    python tools/showcase.py sync            # 更新首页轮播（course.py sync 也会顺带运行）
    python tools/showcase.py record [名称…]  # 录短视频和封面图，存到 docs/assets/showcase/（需要 npm ci 和 ffmpeg）

course.yml 写法（按轮播顺序，建议 5 个，第 1 个放画面最震撼、上手最快的）：

    showcase_tagline: 拖一拖、点一点，看算法怎样从数据里学出规律
    showcase:
      - sim: dbscan-density                       # docs/sims/ 下的目录名
        hook: 两道弯月缠在一起，算法能把它们分开吗？   # 一句问句或悬念（大字）
        play: 调一下半径 eps，颜色马上重新分组。       # 一句玩法
        steps:                                    # 录视频时自动做的操作，见 编写规范.md“首页轮播”
          - [slide, "#eps", 0.1]
          - [poster]
          - [click, 球形（3 群）]

动画标题取自 docs/sims/<名称>/index.md 的 title；“去读第 N 章”链接指向正文里嵌入这个动画的页面。
"""
import functools
import http.server
import json
import re
import shutil
import subprocess
import sys
import tempfile
import threading

from common import DOCS, ROOT, course, front_matter, replace_between

ASSETS = DOCS / "assets/showcase"
CSS, JS = "css/showcase.css", "javascripts/showcase.js"
START, END = "<!-- showcase:start -->", "<!-- showcase:end -->"


def esc(s):
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def sim_title(name):
    p = DOCS / "sims" / name / "index.md"
    if not p.exists():
        raise SystemExit(f"course.yml 的 showcase 里写了 {name}，但 docs/sims/{name}/ 不存在")
    fm, _ = front_matter(p.read_text(encoding="utf-8"))
    return fm.get("title", name)


def embed_page(name, c):
    """找正文里第一次嵌入这个动画的页面，返回 (网址, 章号, 章标题)。"""
    pat = f"sims/{name}/main.html"
    for ch in c["chapters"]:
        d = DOCS / "chapters" / ch["dir"]
        for p in sorted(d.glob("*.md"), key=lambda q: (q.name != "index.md", q.name)):
            if pat in p.read_text(encoding="utf-8"):
                url = f"chapters/{ch['dir']}/" + ("" if p.name == "index.md" else p.stem + "/")
                return url, ch["num"], ch["title"]
    return None, None, None


def items(c):
    out = []
    for x in c.get("showcase") or []:
        name = x["sim"]
        url, num, ctitle = embed_page(name, c)
        out.append({**x, "title": x.get("title") or sim_title(name), "url": url, "num": num, "ctitle": ctitle})
    return out


def render(c):
    rows = []
    for it in items(c):
        n = it["sim"]
        video = f"assets/showcase/{n}" if (ASSETS / f"{n}.mp4").exists() else ""
        poster = f"assets/showcase/{n}.jpg"
        if not (ASSETS / f"{n}.jpg").exists():
            print(f"  提示：还没有 {poster}，运行 python tools/showcase.py record {n} 录制")
        chap = (f'<a class="sc-chap" href="{it["url"]}" title="第 {it["num"]} 章 {esc(it["ctitle"])}">去读第{it["num"]}章 →</a>'
                if it["url"] else "")
        rows.append(
            f'<li data-sim="sims/{n}/main.html" data-video="{video}" data-title="{esc(it["title"])}" data-play="{esc(it["play"])}">'
            f'<img src="{poster}" alt="{esc(it["title"])}截图" loading="lazy">'
            f'<p class="sc-hook">{esc(it["hook"])}</p>'
            f'<p><b>{esc(it["title"])}</b>　{esc(it["play"])}</p>'
            f'<p><a href="sims/{n}/main.html">打开动画</a>　{chap}</p></li>')
    if not rows:
        return ""
    tag = c.get("showcase_tagline", "")
    return ("## 先玩一玩\n\n"
            + (f'<p class="sc-tagline">{esc(tag)}</p>\n\n' if tag else "")
            + '<div class="showcase" aria-label="精选交互动画">\n<ol class="sc-list">\n'
            + "\n".join(rows) + "\n</ol>\n</div>")


def sync_mkdocs_assets():
    p = ROOT / "mkdocs.yml"
    s = p.read_text(encoding="utf-8")
    for key, f in (("extra_css", CSS), ("extra_javascript", JS)):
        if f in s:
            continue
        m = re.search(rf"^{key}:\n((?:  - .*\n)+)", s, re.M)
        if m:   # 加在这一段的末尾
            s = s[:m.end()] + f"  - {f}\n" + s[m.end():]
        else:
            s = s.rstrip("\n") + f"\n{key}:\n  - {f}\n"
    p.write_text(s, encoding="utf-8")


def sync(c=None):
    c = c or course()
    if not c.get("showcase"):
        return
    p = DOCS / "index.md"
    s = p.read_text(encoding="utf-8")
    if START not in s:   # 第一次：放在课程信息框后面
        anchor = "<!-- course:end -->"
        if anchor in s:
            i = s.index(anchor) + len(anchor)
            s = s[:i] + f"\n\n{START}\n{END}" + s[i:]
        else:            # 没有标记时放在第一个二级标题前（通常就在课程信息框后面）
            m = re.search(r"^## ", s, re.M)
            if not m:
                raise SystemExit("首页找不到放轮播的位置，请手工写上 <!-- showcase:start --> 和 <!-- showcase:end -->")
            s = s[:m.start()] + f"{START}\n{END}\n\n" + s[m.start():]
    s = replace_between(s, START, END, render(c))
    p.write_text(s, encoding="utf-8")
    sync_mkdocs_assets()
    print(f"  首页轮播：{len(c['showcase'])} 个动画")


def record(names):
    c = course()
    todo = [x for x in c.get("showcase") or [] if not names or x["sim"] in names]
    if not todo:
        raise SystemExit("course.yml 里没有要录的 showcase 条目")
    if not shutil.which("ffmpeg"):
        raise SystemExit("需要先安装 ffmpeg")
    handler = functools.partial(http.server.SimpleHTTPRequestHandler, directory=str(DOCS))
    http.server.SimpleHTTPRequestHandler.log_message = lambda *a: None
    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    ASSETS.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        job = {"base": f"http://127.0.0.1:{srv.server_address[1]}",
               "items": [{"name": x["sim"], "url": f"sims/{x['sim']}/main.html", "steps": x.get("steps") or [],
                          **{k: x[k] for k in ("vw", "vh", "dsf", "load", "intro", "outro", "zoom") if k in x}} for x in todo]}
        jf = f"{tmp}/job.json"
        with open(jf, "w", encoding="utf-8") as f:
            json.dump(job, f, ensure_ascii=False)
        subprocess.run(["node", str(ROOT / "tools/record_showcase.mjs"), jf, str(ASSETS)], check=True)
    srv.shutdown()
    res = json.loads((ASSETS / "result.json").read_text(encoding="utf-8"))
    (ASSETS / "result.json").unlink()
    bad = [r for r in res if "error" in r]
    for r in bad:
        print(f"  ✗ {r['name']}：{r['error']}")
    print(f"录好 {len(res) - len(bad)} 个，存在 docs/assets/showcase/。")
    sync(c)
    if bad:
        sys.exit(1)


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "sync"
    if cmd == "sync":
        sync()
    elif cmd == "record":
        record(sys.argv[2:])
    else:
        print(__doc__)
