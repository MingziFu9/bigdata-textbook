"""把网页教材导出为 Word 版（之后可在 Word / WPS 里另存为 PDF）。

用法（在仓库根目录运行，需要先安装 pandoc 3.x）：

    python tools/export_docx.py              # 全书，按 mkdocs.yml 的 nav 顺序，输出 build/<书名>.docx
    python tools/export_docx.py 01-sample    # 只导出一章，输出 build/01-sample.docx
    python tools/export_docx.py --pdf        # 同时用 LibreOffice 转出 PDF（需要安装 LibreOffice）

网页特有的元素按下面的办法转换：交互动画换成动画截图、二维码和网址（扫码即可在手机上操作）；
Mermaid 图换成渲染好的图片；提示框变成带粗体标题的引用块；在线答题的选项照常列出，
答案与解析保留在题目下方；术语和站内链接只保留文字。
截图和 Mermaid 图片由 npm run figures（tools/render_figures.mjs）生成，没有生成时退回“请在网页版查看”的提示。
"""
import re
import subprocess
import sys
from pathlib import Path

import yaml

from common import course, mermaid_png, qr_png, sim_png, site_url

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
OUT = ROOT / "build"


class _Loader(yaml.SafeLoader):
    pass


_Loader.add_multi_constructor("", lambda loader, suffix, node: None)


def nav_pages():
    """按 nav 顺序列出正文页（跳过动画、学习图谱等只适合网页的页面）"""
    cfg = yaml.load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"), Loader=_Loader)
    pages = []

    def walk(items):
        for it in items:
            if isinstance(it, str):
                pages.append(it)
            elif isinstance(it, dict):
                for v in it.values():
                    walk(v) if isinstance(v, list) else pages.append(v)

    walk(cfg["nav"])
    return [p for p in pages if p == "course-description.md" or p.startswith(("chapters/", "appendix/"))]


def mermaid_figure(m):
    png = mermaid_png(m.group(1))
    return f"![]({png.as_posix()}){{WIDTH=90%}}" if png else "> 【图示】本图为交互图，请在网页版查看。"


def sim_figure(m):
    name, title = m.group(1), m.group(2)
    url = f"{site_url(course())}sims/{name}/main.html"
    png, qr = sim_png(name), qr_png(url)
    out = [f"![]({png.as_posix()}){{WIDTH=85%}}", ""] if png else []   # alt 留空：后面去链接的替换会吃掉带文字的图片
    if qr:
        out += [f"![]({qr.as_posix()}){{WIDTH=2.5cm}}", ""]
    out.append(f"> 【交互动画】{title}：扫描二维码或打开 <{url}> 动手操作。")
    return "\n".join(out)


def image_figure(m, page):
    src = (DOCS / page).parent / m.group(2)
    if src.suffix == ".svg" and src.with_suffix(".png").exists():   # Word 对 SVG 支持不好，有同名 PNG 就用 PNG
        src = src.with_suffix(".png")
    if not src.exists():
        return f"> 【图】{m.group(1)}：请在网页版查看。"
    return f"![]({src.resolve().as_posix()}){{WIDTH=80%}}\n\n*{m.group(1)}*"


def convert(md, page="index.md"):
    md = re.sub(r"\A---\n.*?\n---\n", "", md, flags=re.S)
    md = re.sub(r"!\[([^\]]*)\]\(((?!https?:)[^)\s]+)\)(\{[^}]*\})?", lambda m: image_figure(m, page), md)
    md = re.sub(r"```mermaid\n(.*?)```", mermaid_figure, md, flags=re.S)
    md = re.sub(r'<iframe[^>]*src="[^"]*sims/([\w-]+)/main\.html"[^>]*title="([^"]*)"[^>]*></iframe>', sim_figure, md)
    md = re.sub(r'<iframe[^>]*title="([^"]*)"[^>]*></iframe>', r"> 【交互动画】\1：请在网页版查看。", md)
    md = re.sub(r"^.*\{ \.sim-caption \}\n", "", md, flags=re.M)
    md = re.sub(r'<sup class="cite">\[\[(\d+)\]\]\([^)]*\)</sup>', r"^[\1]^", md)
    md = re.sub(r'<span class="ref-note">(.*?)</span>', r"\1", md)
    md = re.sub(r"<span id=\"[^\"]*\"></span>", "", md)
    md = re.sub(r"^</?div[^>]*>\s*$", "", md, flags=re.M)
    # 术语链接、站内链接只留文字
    md = re.sub(r"\[([^\]]+)\]\((?!https?:)[^)]*\)", r"\1", md)
    md = re.sub(r"\{ *[.#:a-z][^}]*\}", "", md)
    # 提示框：!!! / ??? 类型 "标题" + 缩进正文 → 引用块
    lines, out, i = md.split("\n"), [], 0
    while i < len(lines):
        m = re.match(r'^(!!!|\?\?\?\+?) *[\w-]+(?: +"([^"]*)")?\s*$', lines[i])
        if not m:
            out.append(lines[i]); i += 1; continue
        out.append(f"> **{m.group(2) or ''}**")
        out.append(">")
        i += 1
        while i < len(lines) and (lines[i].startswith("    ") or lines[i].strip() == ""):
            if lines[i].strip() == "" and i + 1 < len(lines) and not lines[i + 1].startswith("    "):
                break
            out.append(("> " + lines[i][4:]).rstrip())
            i += 1
    return "\n".join(out).replace("{WIDTH=", "{width=")   # 图片宽度用大写写入，躲过上面去掉属性的替换


def main():
    OUT.mkdir(exist_ok=True)
    args = [a for a in sys.argv[1:] if a != "--pdf"]
    if args:
        pages, name = [f"chapters/{args[0]}/index.md"], args[0]
    else:
        pages, name = nav_pages(), course()["title"]
    title = yaml.load((ROOT / "mkdocs.yml").read_text(encoding="utf-8"), Loader=_Loader)["site_name"]
    body = "\n\n".join(convert((DOCS / p).read_text(encoding="utf-8"), p) for p in pages)
    tmp = OUT / f"{name}.md"
    tmp.write_text(f"---\ntitle: {title}\n---\n\n{body}", encoding="utf-8")
    dst = OUT / f"{name}.docx"
    subprocess.run(["pandoc", str(tmp), "-f", "markdown+tex_math_single_backslash", "-o", str(dst),
                    "--toc", "--toc-depth=2", "-M", "toc-title=目录", "--resource-path", str(DOCS)], check=True)
    print(f"已导出 {dst.relative_to(ROOT)}（{len(pages)} 页）")
    if "--pdf" in sys.argv:
        subprocess.run(["soffice", "--headless", "--convert-to", "pdf", "--outdir", str(OUT), str(dst)], check=True)


if __name__ == "__main__":
    main()
