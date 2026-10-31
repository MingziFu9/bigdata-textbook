"""实验 8-3（第 2 部分）：在本机用 pandas 读取 MySQL 中的结果并画图。

先运行 lab8_3_hive_mysql.py，再在本机 labs 目录下运行：
    python ch08/lab8_3_plot.py
图片保存到 ch08/output/category_daily.png
"""
import os

import matplotlib
import pandas as pd
from sqlalchemy import create_engine

matplotlib.use("Agg")                     # 不弹窗口，直接存成图片
import matplotlib.pyplot as plt

plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "SimHei", "PingFang SC", "WenQuanYi Zen Hei", "Noto Sans CJK SC"]
plt.rcParams["axes.unicode_minus"] = False

# 本机访问 MySQL 容器：主机 localhost，端口 3306
engine = create_engine("mysql+pymysql://root:xiaoxiang@localhost:3306/xiaoxiang?charset=utf8mb4")
df = pd.read_sql("SELECT dt, category, gmv FROM category_daily", engine)
print(df.shape)
print(df.head())

# 行是日期、列是品类的透视表，单位万元
table = df.pivot_table(index="dt", columns="category", values="gmv", aggfunc="sum") / 10000
table.index = pd.to_datetime(table.index).day          # 横轴只显示几号
top = table.sum().sort_values(ascending=False)
print("9 月各品类销售额（万元）：")
print(top.round(1))

fig, axes = plt.subplots(1, 2, figsize=(12, 4.5))
top.plot.barh(ax=axes[0], color="#3f51b5").invert_yaxis()
axes[0].set_title("9 月各品类销售额（万元）")
axes[0].set_ylabel("")
table[top.index[1:5]].plot(ax=axes[1], marker="o", markersize=3)   # 手机数码太大，单独看其余四个
axes[1].set_title("每日销售额：销售额第 2～5 的品类（万元）")
axes[1].set_xlabel("9 月几日")
axes[1].legend(title="", ncol=4, loc="upper center", bbox_to_anchor=(0.5, -0.15))
fig.tight_layout()
out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
os.makedirs(out, exist_ok=True)
fig.savefig(os.path.join(out, "category_daily.png"), dpi=120)
print("已保存", os.path.join(out, "category_daily.png"))
