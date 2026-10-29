"""每章一份教案（Word .docx），通用格式：课程信息、教学目标、重点难点、教学方法与资源、
教学过程（课前线上 / 课中 / 课后线上）、作业与考核、证书考点、教学反思。

用法（在仓库根目录运行）：
    python tools/make_lesson_plan.py draft 3     # 从第 3 章正文生成教案底稿 lesson-plans/ch03.yml（已有则不覆盖，加 --force 覆盖）
    python tools/make_lesson_plan.py build 3     # 把底稿排版成 build/教案/第03章 章名 教案.docx
    python tools/make_lesson_plan.py build all

底稿是 YAML，大部分内容已从正文和 course.yml 自动填好（学时、目标、动画、实验、练习、证书考点），
标着“【待填】”的地方需要编者补写，build 时会列出还没填的项。字段说明见 lesson-plans/README.md。
"""
import re
import sys

import yaml
from docx import Document
from docx.enum.section import WD_ORIENT
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.shared import Cm, Pt, RGBColor

from common import (BUILD, ROOT, admonition, chapter_by_num, chapter_heading, chapter_text, course, headings,
                    lab_list, list_items, other_exercises, parse_quizzes, plain, safe_name, section, sims_in,
                    site_url)

PLANS = ROOT / "lesson-plans"
TODO = "【待填】"
BLOOM = {"记忆": "knowledge", "理解": "knowledge", "应用": "skill", "分析": "skill", "评价": "skill", "创造": "skill"}
# 目标没写认知层次时，按开头的行为动词判断
KNOW_VERBS = ("说出", "列举", "复述", "识别", "描述", "解释", "说明", "理解", "了解", "区分", "概括")


# ======================================================================
# 一、生成底稿
# ======================================================================

def split_hours(c, ch):
    total = ch["theory"] + ch["lab"]
    if "offline" in ch:                 # 章节里写了本章线下学时，就按它分
        return total - ch["offline"], ch["offline"]
    online = round(total * c["lesson_plan"].get("online_ratio", 0.5))
    return online, total - online


def draft(num, force=False):
    c = course()
    ch = chapter_by_num(num, c)
    text = chapter_text(ch)
    path = PLANS / f"ch{num:02d}.yml"
    if path.exists() and not force:
        print(f"{path.relative_to(ROOT)} 已存在，没有覆盖（要重新生成请加 --force）")
        return
    online, offline = split_hours(c, ch)

    objectives = {"knowledge": [], "skill": [], "quality": [f"{TODO}职业素养、科学态度或科技伦理方面的目标，1–2 条"]}
    for g in list_items(section(text, "学习目标")):
        g = plain(g).replace("**", "")
        level = re.search(r"（([^（）]*)）[；。]?$", g)
        if level and level.group(1).split("、")[0] in BLOOM:
            key = BLOOM[level.group(1).split("、")[0]]
        else:
            key = "knowledge" if g.startswith(KNOW_VERBS) else "skill"
        objectives[key].append(re.sub(r"（[^（）]*）$", "", g.rstrip("；。")))

    sections = [(t, b) for t, b in headings(text, 2) if re.match(r"^\d+\.\d+ ", t)]
    frontier = c["frontier_title"]
    teach = sections
    sims = sims_in(text)
    labs = lab_list(text, c["lab_word"])
    quizzes = parse_quizzes(text)
    others = other_exercises(text)

    # 课中时间：开场导入 10 分钟、小结 5 分钟、课堂练习 10 分钟，实验占实验学时，其余按各节篇幅分给讲授
    minutes = offline * 45
    lab_min = min(ch["lab"] * 45, max(minutes - 40, 0)) if labs else 0
    lecture = max(minutes - 25 - lab_min, 0)
    sizes = [len(re.findall(r"[一-鿿]", b)) for _, b in teach] or [1]
    steps_in = [{"name": "情景导入", "minutes": 10, "content": "情景导入：" + plain(section(text, "情景导入")).replace("**", "")[:60] + "……",
                 "teacher": "讲述情景，提出本章要解决的问题", "student": "思考并回答问题，说出自己的经验",
                 "intent": "联系工作和生活场景，激发学习兴趣"}]
    for (t, b), sz in zip(teach, sizes):
        sim_names = [title for _, title in sims_in(b)]
        subs = [re.sub(r"^\d+(\.\d+)+ ", "", plain(x)).split("：")[0] for x, _ in headings(b, 3)]
        steps_in.append({
            "name": "拓展" if frontier in t else "讲授" + ("与演示" if sim_names else ""),
            "minutes": max(5, round(lecture * sz / sum(sizes) / 5) * 5),
            "content": t + ("：" + "、".join(subs[:4]) + ("等" if len(subs) > 4 else "") if subs else ""),
            "teacher": "讲解要点，结合例子和“算一算”演示" + (f"；用交互动画“{'”“'.join(sim_names)}”课堂演示" if sim_names else ""),
            "student": "听讲、跟着计算" + ("、操作动画并回答提问" if sim_names else "、记录要点"),
            "intent": TODO})
    for no, name, goals in labs[:max(1, ch["lab"])] if lab_min else []:
        steps_in.append({"name": c["lab_word"], "minutes": round(lab_min / max(1, min(len(labs), ch["lab"]))),
                         "content": f"{c['lab_word']} {no} {name.lstrip('★☆ ')}", "teacher": "演示关键步骤，巡视指导，收集常见错误",
                         "student": "按步骤完成" + c["lab_word"] + "，填写记录表",
                         "intent": "；".join(g.rstrip("；。") for g in goals)[:60] or TODO})
    steps_in.append({"name": "课堂练习", "minutes": 10, "content": f"课后练习中的单选、判断题（在线作答，共 {len(quizzes)} 题，选做）",
                     "teacher": "组织学生在网页上作答，讲评错误率高的题", "student": "在线作答，查看解析",
                     "intent": "即时检验学习效果"})
    steps_in.append({"name": "小结", "minutes": 5, "content": "用本章知识导图回顾要点，预告下一章",
                     "teacher": "带领学生梳理知识导图", "student": "复述本章要点", "intent": "形成知识结构"})

    pre_min, post_min = round(online * 45 * 0.4), online * 45 - round(online * 45 * 0.4)
    steps_pre = [{"name": "自主预习", "minutes": pre_min,
                  "content": f"阅读网页教材{chapter_heading(ch)}的情景导入和第一节" + (f"，操作交互动画“{sims[0][1]}”" if sims else ""),
                  "teacher": "在学习平台发布预习任务和预习问题", "student": "阅读教材、观看课件，在讨论区提出疑问",
                  "intent": "带着问题进课堂"}] if pre_min else []
    steps_post = [{"name": "课后巩固", "minutes": post_min,
                   "content": f"完成本章课后练习（在线作答 {len(quizzes)} 题，简答 / 实操 {len(others)} 题）" + ("，提交实验报告" if labs else ""),
                   "teacher": "批改作业和实验报告，在讨论区答疑", "student": "完成练习，错题点“回看”复习对应小节",
                   "intent": "巩固知识，形成技能"}] if post_min else []

    plan = {
        "chapter": num,
        "online": online,
        "offline": offline,
        "methods": ["讲授法", "案例教学法", "演示法", "线上自主学习"] + (["实验教学法"] if labs else []),
        "objectives": objectives,
        "key_points": [re.sub(r"^\d+\.\d+ ", "", t) for t, _ in teach if frontier not in t][:4],
        "difficulties": [{"point": TODO + "学生最容易卡住的 1–2 个难点", "solution": TODO + "突破难点的办法，如动画演示、手算例子"}],
        "process": [
            {"phase": "课前（线上）", "steps": steps_pre},
            {"phase": "课中（线下面授 / 直播）", "steps": steps_in},
            {"phase": "课后（线上）", "steps": steps_post},
        ],
        "homework": [f"{k}：{q}" for k, q in others] + ([f"{c['lab_word']}报告：" + "、".join(f"{c['lab_word']} {n}" for n, _, _ in labs)] if labs else []),
        "assessment": "课堂练习正确率、课后作业和" + (c["lab_word"] + "报告" if labs else "讨论区参与") + "计入平时成绩",
        "reflection": "",
    }
    PLANS.mkdir(exist_ok=True)
    header = (f"# 第 {num} 章教案底稿，由 python tools/make_lesson_plan.py draft 生成。\n"
              "# 改好“【待填】”的地方，删减或补充其他内容后运行 build。字段说明见 lesson-plans/README.md\n")
    path.write_text(header + yaml.safe_dump(plan, allow_unicode=True, sort_keys=False, width=200), encoding="utf-8")
    print(f"已生成底稿 {path.relative_to(ROOT)}，请补写“{TODO}”处后运行 build")


# ======================================================================
# 二、排版成 Word
# ======================================================================

BODY, HEAD = "宋体", "黑体"


def set_font(run, size=10.5, bold=False, font=BODY, color=None):
    run.font.size, run.font.bold, run.font.name = Pt(size), bold, font
    rpr = run._element.get_or_add_rPr()
    fonts = rpr.find(qn("w:rFonts"))
    if fonts is None:
        fonts = OxmlElement("w:rFonts")
        rpr.append(fonts)
    for k in ("w:eastAsia", "w:ascii", "w:hAnsi"):
        fonts.set(qn(k), font)
    if color:
        run.font.color.rgb = RGBColor.from_string(color)


def para(doc_or_cell, text="", size=10.5, bold=False, font=BODY, align=None, space=4):
    p = doc_or_cell.add_paragraph()
    p.paragraph_format.space_after = Pt(space)
    p.paragraph_format.space_before = Pt(0)
    if align:
        p.alignment = align
    if text:
        for part in re.split(r"(【待填】)", str(text)):
            if part:
                set_font(p.add_run(part), size, bold, font, "C62828" if part == TODO else None)
    return p


def heading(doc, text):
    p = para(doc, text, 12, True, HEAD, space=6)
    p.paragraph_format.space_before = Pt(10)


def shade(cell, color):
    tcpr = cell._element.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:fill"), color)
    tcpr.append(shd)


def cell_text(cell, text, size=10, bold=False, fill=None):
    cell.text = ""
    p = cell.paragraphs[0]
    p.paragraph_format.space_after = Pt(0)
    for k, line in enumerate(str(text).split("\n")):
        if k:
            p = cell.add_paragraph()
            p.paragraph_format.space_after = Pt(0)
        for part in re.split(r"(【待填】)", line):
            if part:
                set_font(p.add_run(part), size, bold, HEAD if bold else BODY, "C62828" if part == TODO else None)
    if fill:
        shade(cell, fill)


def table(doc, rows, widths, header=True, size=10):
    t = doc.add_table(rows=len(rows), cols=len(widths))
    t.style = "Table Grid"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    t.autofit = False
    grid = t._tbl.tblGrid
    for j, col in enumerate(grid.findall(qn("w:gridCol"))):
        col.set(qn("w:w"), str(int(widths[j] * 567)))     # 1 厘米 = 567 缇
    for j, col in enumerate(t.columns):
        col.width = Cm(widths[j])
    for i, r in enumerate(rows):
        for j, v in enumerate(r):
            cell_text(t.cell(i, j), v, size, header and i == 0, "E8EAF6" if header and i == 0 else None)
            t.cell(i, j).width = Cm(widths[j])
    return t


def bullet_lines(doc, items, numbered=True):
    for k, x in enumerate(items, 1):
        para(doc, f"{k}. {x}" if numbered else f"· {x}")


def build(num):
    c = course()
    ch = chapter_by_num(num, c)
    src = PLANS / f"ch{num:02d}.yml"
    if not src.exists():
        print(f"没有 {src.relative_to(ROOT)}，先自动生成底稿")
        draft(num)
    plan = yaml.safe_load(src.read_text(encoding="utf-8"))
    text = chapter_text(ch)
    lp = c["lesson_plan"]

    doc = Document()
    sec = doc.sections[0]
    sec.page_width, sec.page_height = Cm(21), Cm(29.7)
    sec.left_margin = sec.right_margin = Cm(2)
    sec.top_margin = sec.bottom_margin = Cm(2)
    style = doc.styles["Normal"]
    style.font.name, style.font.size = BODY, Pt(10.5)
    style.element.rPr.rFonts.set(qn("w:eastAsia"), BODY)

    para(doc, f"《{c['title']}》教案", 18, True, HEAD, WD_ALIGN_PARAGRAPH.CENTER, 6)
    para(doc, chapter_heading(ch), 14, True, HEAD, WD_ALIGN_PARAGRAPH.CENTER, 12)

    total = ch["theory"] + ch["lab"]
    teacher = lp.get("teacher") or "　" * 6
    rows = [
        ["课程名称", c["title"], "授课章节", chapter_heading(ch)],
        ["授课对象", c["audience"], "授课教师", teacher],
        ["学时", f"共 {total} 学时（理论 {ch['theory']}，{c['lab_word']} {ch['lab']}）", "学时分配", f"线上 {plan['online']} 学时，线下 {plan['offline']} 学时"],
        ["授课时间", "　年　月　日", "授课地点", "　" * 6],
        ["教学方法", "、".join(plan.get("methods", [])), "教材网址", f"{site_url(c)}chapters/{ch['dir']}/"],
    ]
    if lp.get("school"):
        rows.insert(0, ["学校", lp["school"], "开课学期", f"第 {c['semester']} 学期"])
    t = table(doc, rows, [2.4, 6.1, 2.4, 6.1], header=False)
    for r in t.rows:
        for j in (0, 2):
            shade(r.cells[j], "E8EAF6")

    heading(doc, "一、教学目标")
    names = {"knowledge": "知识目标", "skill": "技能目标", "quality": "素养目标"}
    for key, name in names.items():
        items = plan.get("objectives", {}).get(key) or []
        if items:
            para(doc, name, 10.5, True, HEAD)
            bullet_lines(doc, items)

    heading(doc, "二、教学重点与难点")
    para(doc, "教学重点", 10.5, True, HEAD)
    bullet_lines(doc, plan.get("key_points", []))
    para(doc, "教学难点及突破方法", 10.5, True, HEAD)
    for k, d in enumerate(plan.get("difficulties", []), 1):
        para(doc, f"{k}. {d['point']}")
        para(doc, f"　 突破方法：{d.get('solution', '')}")

    heading(doc, "三、教学资源")
    res = [f"网页教材：{chapter_heading(ch)}（{site_url(c)}chapters/{ch['dir']}/）",
           f"课件：第{num:02d}章 PowerPoint 课件"]
    sims = sims_in(text)
    if sims:
        res.append("交互动画：" + "；".join(f"{t}（{site_url(c)}sims/{n}/）" for n, t in sims))
    labs = lab_list(text, c["lab_word"])
    if labs:
        res.append(f"{c['lab_word']}：" + "；".join(f"{c['lab_word']} {n} {t.lstrip('★☆ ')}" for n, t, _ in labs) + (f"（代码在 labs/ch{num:02d}/）" if (ROOT / f"labs/ch{num:02d}").is_dir() else ""))
    res += plan.get("resources", [])
    bullet_lines(doc, res)

    heading(doc, "四、教学过程")
    rows = [["教学环节", "时间", "教学内容", "教师活动", "学生活动", "设计意图"]]
    for ph in plan.get("process", []):
        if not ph.get("steps"):
            continue
        rows.append([ph["phase"], "", "", "", "", ""])
        for st in ph["steps"]:
            rows.append([st.get("name", ""), f"{st.get('minutes', '')} 分钟", st.get("content", ""),
                         st.get("teacher", ""), st.get("student", ""), st.get("intent", "")])
    t = table(doc, rows, [1.8, 1.4, 4.2, 3.4, 3.2, 3.0], size=9)
    for i, r in enumerate(rows):
        if i and not r[1]:
            merged = t.cell(i, 0).merge(t.cell(i, 5))
            cell_text(merged, r[0], 9.5, True, "F5F5F5")

    heading(doc, "五、作业与考核")
    bullet_lines(doc, plan.get("homework", []))
    if plan.get("assessment"):
        para(doc, "考核方式：" + plan["assessment"])

    certs = admonition(text, "cert")
    if certs or c["certificates"]:
        heading(doc, "六、证书考点")
        if certs:
            bullet_lines(doc, [f"{t or '证书考点'}：{plain(b)}" for t, b in certs])
        else:
            para(doc, "本章没有标注证书考点。")

    heading(doc, ("七" if certs or c["certificates"] else "六") + "、教学反思")
    para(doc, plan.get("reflection") or "（课后填写：目标达成情况、学生反馈、需要改进的地方）")
    for _ in range(4):
        para(doc, "")

    out = BUILD / "教案" / f"第{num:02d}章 {safe_name(ch['title'])} 教案.docx"
    out.parent.mkdir(parents=True, exist_ok=True)
    doc.save(out)
    todo = sum(l.count(TODO) for l in src.read_text(encoding="utf-8").split("\n") if not l.lstrip().startswith("#"))
    print(f"已生成 {out.relative_to(ROOT)}" + (f"（底稿里还有 {todo} 处“{TODO}”，教案中以红字显示）" if todo else ""))


if __name__ == "__main__":
    if len(sys.argv) < 3 or sys.argv[1] not in ("draft", "build"):
        print(__doc__)
        sys.exit(1)
    nums = [ch["num"] for ch in course()["chapters"]] if sys.argv[2] == "all" else [int(sys.argv[2])]
    for n in nums:
        draft(n, "--force" in sys.argv) if sys.argv[1] == "draft" else build(n)
