"""实验 12-1 第 2 步：用 pyecharts 把 5 张小表画成一个网页报告。

先在 spark 容器中运行 lab12_1_prepare.py，再在本机 labs 目录运行：
    pip install pyecharts pandas
    python ch12/lab12_1_report.py
结果：data/report/xiaoxiang_report.html，用浏览器打开（要能上网，图表库从 pyecharts 的网站加载）
"""
import argparse
import glob
import os

import pandas as pd
from pyecharts import options as opts
from pyecharts.charts import Bar, Funnel, Line, Map, Page, Pie
from pyecharts.globals import CurrentConfig

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "report")


def read(name):
    """读 Spark 写出的目录里那一个 CSV 文件"""
    return pd.read_csv(glob.glob(os.path.join(ROOT, name, "*.csv"))[0])


# 地图上的省份要用全称，数据里是简称
SPECIAL = {"北京": "北京市", "天津": "天津市", "上海": "上海市", "重庆": "重庆市", "内蒙古": "内蒙古自治区",
           "广西": "广西壮族自治区", "西藏": "西藏自治区", "宁夏": "宁夏回族自治区", "新疆": "新疆维吾尔自治区",
           "香港": "香港特别行政区", "澳门": "澳门特别行政区"}


def full_name(p):
    return SPECIAL.get(p, p if p.endswith("省") else p + "省")


def bar_category():          # 比较：柱状图，从高到低排
    d = read("category")
    return (Bar(init_opts=opts.InitOpts(width="900px", height="380px"))
            .add_xaxis(d["category"].tolist())
            .add_yaxis("销售额（万元）", d["gmv_wan"].tolist(), color="#3f51b5", label_opts=opts.LabelOpts(position="top"))
            .set_global_opts(title_opts=opts.TitleOpts(title="① 各品类销售额（万元）", subtitle="9 月已完成订单"),
                             legend_opts=opts.LegendOpts(is_show=False)))


def line_daily():            # 趋势：折线图，标出最高和最低
    d = read("daily")
    return (Line(init_opts=opts.InitOpts(width="900px", height="380px"))
            .add_xaxis([s[5:] for s in d["dt"]])
            .add_yaxis("销售额（万元）", d["gmv_wan"].tolist(), is_smooth=True, color="#e53935",
                       label_opts=opts.LabelOpts(is_show=False),
                       markpoint_opts=opts.MarkPointOpts(data=[opts.MarkPointItem(type_="max", name="最高"),
                                                               opts.MarkPointItem(type_="min", name="最低")]),
                       markline_opts=opts.MarkLineOpts(data=[opts.MarkLineItem(type_="average", name="平均")]))
            .set_global_opts(title_opts=opts.TitleOpts(title="② 每日销售额（万元）", subtitle="9 月 1～30 日，纵轴从 300 开始"),
                             tooltip_opts=opts.TooltipOpts(trigger="axis"),
                             yaxis_opts=opts.AxisOpts(min_=300),
                             legend_opts=opts.LegendOpts(is_show=False)))


def map_province():          # 地区分布：地图，颜色越深销售额越高
    d = read("province")
    pairs = [(full_name(p), v) for p, v in zip(d["province"], d["gmv_wan"])]
    return (Map(init_opts=opts.InitOpts(width="900px", height="520px"))
            .add("销售额（万元）", pairs, "china", is_map_symbol_show=False)
            .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
            .set_global_opts(title_opts=opts.TitleOpts(title="③ 各省销售额"),
                             visualmap_opts=opts.VisualMapOpts(max_=float(d["gmv_wan"].max()), is_piecewise=False,
                                                               range_color=["#e8eaf6", "#3f51b5"]),
                             legend_opts=opts.LegendOpts(is_show=False)))


def pie_pay():               # 构成：环形图（类别不多时才用饼图）
    d = read("pay_type")
    return (Pie(init_opts=opts.InitOpts(width="900px", height="380px"))
            .add("订单数", list(zip(d["pay_type"], d["count"].tolist())), radius=["40%", "65%"])
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}：{d}%"))
            .set_global_opts(title_opts=opts.TitleOpts(title="④ 支付方式构成"),
                             legend_opts=opts.LegendOpts(pos_left="right", orient="vertical")))


def funnel_conversion():     # 转化：漏斗图
    d = read("funnel")
    first = d["users"].iloc[0]
    data = [(f"{s}：{u} 人（{u / first:.1%}）", int(u)) for s, u in zip(d["step"], d["users"])]
    return (Funnel(init_opts=opts.InitOpts(width="900px", height="380px"))
            .add("用户数", data, sort_="descending", label_opts=opts.LabelOpts(position="inside"))
            .set_global_opts(title_opts=opts.TitleOpts(title="⑤ 购买转化漏斗", subtitle="9 月 30 日登录用户"),
                             legend_opts=opts.LegendOpts(is_show=False)))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--assets", help="不能上网时，指定本地 echarts 文件所在的目录（目录下有 echarts.min.js 和 maps/china.js）")
    a = ap.parse_args()
    if a.assets:
        CurrentConfig.ONLINE_HOST = a.assets.rstrip("/") + "/"
    page = Page(page_title="小象优选 9 月销售报告", layout=Page.SimplePageLayout)
    page.add(bar_category(), line_daily(), map_province(), pie_pay(), funnel_conversion())
    out = os.path.join(ROOT, "xiaoxiang_report.html")
    page.render(out)
    print("报告已生成：", os.path.abspath(out))
