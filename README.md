# 大数据技术处理技术

人工智能技术应用专业（成人大专）系列网络教材之一，面向成人教育（开放教育）专科学生，4 学分 72 学时，12 章 36 个实验。网页版：https://mingzifu9.github.io/bigdata-textbook/

全书以虚构电商平台“小象优选”为贯穿案例，在自己电脑上用 Docker 搭建 Hadoop 小集群，只用 Python 和 SQL，学习 HDFS、MapReduce 与 YARN、Hive、Spark、Kafka、MLlib，以及常见问题排查、可视化和数据合规。

## 目录结构

| 路径 | 内容 |
|---|---|
| `course.yml` | 课程配置：书名、学时、章节、附录。改完运行 `python tools/course.py sync` |
| `编写规范.md`、`系列公共术语表.md` | 系列教材共用的编写规范和术语 |
| `docs/chapters/` | 各章正文（目前完成第 4 章样章，其余为计划内容） |
| `docs/sims/` | 交互动画 |
| `docs/learning-graph/` | 学习图谱 |
| `docs/appendix/` | 附录 A Linux 与 PySpark 速查、B 大数据组件速查、C 实验环境与数据说明、D 术语表 |
| `labs/` | 实验代码（按章分目录）、模拟数据生成脚本、`cluster/` 下的 Docker Hadoop 集群配置 |
| `slides/`、`lesson-plans/` | 每章课件、教案底稿 |
| `tools/` | 维护脚本 |

## 本地预览与维护

```bash
pip install -r requirements-docs.txt
mkdocs serve                               # 浏览器打开 http://127.0.0.1:8000
mkdocs build --strict                      # 提交前确认没有警告

python tools/glossary_links.py anchors     # 术语锚点
python tools/glossary_links.py link 04     # 第 4 章术语链接
python tools/glossary_links.py concepts 04 # 第 4 章概念清单
python tools/graph_text.py                 # 检查学习图谱
python tools/book_stats.py                 # 首页统计
```

课件、教案和 Word/PDF 版：先 `pip install -r requirements-tools.txt`、`npm install`，再 `npm run figures`，然后 `python tools/make_slides.py build 4`、`python tools/make_lesson_plan.py build 4`、`python tools/export_docx.py --pdf`；也可以在 GitHub 的 Actions 里运行“导出课件、教案和 Word/PDF”工作流。

## 实验环境

见 `labs/README.md` 和附录 C。书中运行结果均为编者在随书集群上实测；实验数据由 `labs/data/make_data.py` 按固定随机种子生成，内容为虚构。
