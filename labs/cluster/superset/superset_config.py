# 第 12 章：Superset 的配置（只供实验使用）
# 真实环境中 SECRET_KEY 必须换成一长串随机字符，并且不能写在代码里
SECRET_KEY = "xiaoxiang-superset-lab-only-please-change-me"
# 界面默认显示中文，右上角可以切换成英文
BABEL_DEFAULT_LOCALE = "zh"
LANGUAGES = {"zh": {"flag": "cn", "name": "简体中文"}, "en": {"flag": "us", "name": "English"}}

# Superset 处理 MySQL 查询结果时要用 MySQLdb 模块；我们装的是纯 Python 的 PyMySQL，让它冒充 MySQLdb
import pymysql
pymysql.install_as_MySQLdb()
