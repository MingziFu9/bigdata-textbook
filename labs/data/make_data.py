"""生成贯穿全书的“小象优选”电商模拟数据（教材编写，内容为虚构）。

用法（在 labs/ 目录下运行）：
    python data/make_data.py                       # 默认规模：10 万用户、2 千商品、10 万订单、100 万行访问日志、2 万条评论
    python data/make_data.py --orders 1000000      # 订单改成 100 万行
    python data/make_data.py --logs 10000000       # 访问日志改成 1000 万行（约 0.6 GB）
    python data/make_data.py --only logs           # 只生成访问日志
    python data/make_data.py --out /tmp/xx         # 换输出目录（默认 data/xiaoxiang/）

固定随机种子 2026，同样的参数在任何电脑上生成的数据完全相同，所以书中的运行结果可以复现。
生成的文件：
    users.csv     用户表：user_id,gender,age,province,city,register_date,level
    products.csv  商品表：product_id,product_name,category,price,shop_id
    orders.csv    订单表：order_id,user_id,product_id,quantity,amount,order_time,province,pay_type,status
    access.log    网站访问日志，每行 8 个字段，用空格分隔：
                  时间 IP 用户 请求方法 页面 状态码 字节数 终端
                  2026-09-30T08:00:03 183.6.12.34 u0001234 GET /item/p01024 200 5320 Android
    reviews.csv   商品评论：review_id,product_id,user_id,rating,content
"""
import argparse
import os
import time

import numpy as np
import pandas as pd

SEED = 2026

# 31 个省级行政区及大致人口（百万，取整，仅用作抽样权重）和两个代表城市
PROVINCES = [
    ("广东", 127, "广州", "深圳"), ("山东", 101, "济南", "青岛"), ("河南", 98, "郑州", "洛阳"),
    ("江苏", 85, "南京", "苏州"), ("四川", 84, "成都", "绵阳"), ("河北", 74, "石家庄", "保定"),
    ("湖南", 66, "长沙", "株洲"), ("浙江", 66, "杭州", "宁波"), ("安徽", 61, "合肥", "芜湖"),
    ("湖北", 58, "武汉", "宜昌"), ("广西", 50, "南宁", "柳州"), ("云南", 47, "昆明", "大理"),
    ("江西", 45, "南昌", "赣州"), ("辽宁", 42, "沈阳", "大连"), ("福建", 42, "福州", "厦门"),
    ("陕西", 40, "西安", "宝鸡"), ("贵州", 39, "贵阳", "遵义"), ("山西", 34, "太原", "大同"),
    ("重庆", 32, "重庆", "万州"), ("黑龙江", 31, "哈尔滨", "大庆"), ("新疆", 26, "乌鲁木齐", "喀什"),
    ("上海", 25, "上海", "上海"), ("北京", 22, "北京", "北京"), ("甘肃", 25, "兰州", "天水"),
    ("内蒙古", 24, "呼和浩特", "包头"), ("吉林", 23, "长春", "吉林"), ("天津", 14, "天津", "天津"),
    ("海南", 10, "海口", "三亚"), ("宁夏", 7, "银川", "石嘴山"), ("青海", 6, "西宁", "海东"),
    ("西藏", 4, "拉萨", "日喀则"),
]

# 品类、价格区间（元）和商品名用词
CATEGORIES = {
    "手机数码": (299, 6999, ["蓝牙耳机", "充电宝", "智能手表", "手机壳", "数据线", "平板电脑", "智能手机"]),
    "家用电器": (99, 4999, ["电饭煲", "空气炸锅", "扫地机器人", "电风扇", "吹风机", "净水器", "微波炉"]),
    "服饰鞋包": (39, 899, ["运动鞋", "卫衣", "牛仔裤", "双肩包", "羽绒服", "T恤", "帆布鞋"]),
    "食品生鲜": (9, 299, ["坚果礼盒", "牛奶", "大米", "鸡蛋", "水果礼盒", "茶叶", "零食大礼包"]),
    "美妆个护": (19, 699, ["洗面奶", "面霜", "口红", "洗发水", "牙刷", "防晒霜", "沐浴露"]),
    "家居日用": (9, 599, ["收纳箱", "毛巾", "四件套", "保温杯", "拖把", "衣架", "垃圾袋"]),
    "母婴玩具": (29, 999, ["纸尿裤", "奶粉", "积木", "绘本", "婴儿车", "儿童水杯", "拼图"]),
    "图书文具": (5, 199, ["笔记本", "中性笔", "考试用书", "小说", "台灯", "书包", "Python 入门书"]),
}
ADJ = ["经典款", "升级版", "家庭装", "大容量", "轻薄款", "加厚款", "便携款", "国潮款", "标准版", "旗舰款"]
PAY_TYPES = ["微信支付", "支付宝", "银行卡", "花呗分期"]
DEVICES = ["Android", "iPhone", "PC", "MiniProgram"]
REVIEW_GOOD = ["质量很好", "物流很快", "包装完好", "性价比高", "和描述一致", "会回购", "客服态度好", "做工精细"]
REVIEW_BAD = ["质量一般", "物流太慢", "包装破损", "和图片有色差", "用了两天就坏了", "客服不理人", "尺码偏小"]

START = pd.Timestamp("2026-09-01")
DAYS = 30


def hour_weights():
    """一天 24 小时的访问 / 下单权重：上午 10–12 点和晚上 20–22 点是高峰。"""
    w = np.array([2, 1, 1, 1, 1, 1, 2, 4, 6, 8, 10, 10, 9, 7, 7, 7, 7, 7, 8, 9, 12, 13, 10, 5], float)
    return w / w.sum()


def make_users(rng, n):
    pw = np.array([p[1] for p in PROVINCES], float)
    pidx = rng.choice(len(PROVINCES), n, p=pw / pw.sum())
    city_pick = rng.random(n) < 0.6
    df = pd.DataFrame({
        "user_id": [f"u{i:07d}" for i in range(1, n + 1)],
        "gender": rng.choice(["男", "女"], n, p=[0.46, 0.54]),
        "age": np.clip(rng.normal(33, 9, n).round(), 18, 70).astype(int),
        "province": [PROVINCES[i][0] for i in pidx],
        "city": [PROVINCES[i][2] if c else PROVINCES[i][3] for i, c in zip(pidx, city_pick)],
        "register_date": (pd.Timestamp("2023-01-01")
                          + pd.to_timedelta(rng.integers(0, 1338, n), unit="D")).strftime("%Y-%m-%d"),
        "level": rng.choice(["普通", "白银", "黄金", "钻石"], n, p=[0.55, 0.25, 0.15, 0.05]),
    })
    return df


def make_products(rng, n):
    cats = list(CATEGORIES)
    cidx = rng.choice(len(cats), n, p=[0.16, 0.10, 0.18, 0.16, 0.12, 0.12, 0.08, 0.08])
    rows = []
    for i, c in enumerate(cidx, start=1):
        lo, hi, names = CATEGORIES[cats[c]]
        price = float(np.exp(rng.uniform(np.log(lo), np.log(hi))))
        price = round(price) - 0.1 if price > 20 else round(price, 1)
        rows.append((f"p{i:05d}", f"{rng.choice(ADJ)}{rng.choice(names)}", cats[c], price,
                     f"s{rng.integers(1, 201):03d}"))
    df = pd.DataFrame(rows, columns=["product_id", "product_name", "category", "price", "shop_id"])
    # 第 11 章“数据倾斜”实验用：店铺 s001 是平台的头部旗舰店，挂了约 8% 的商品
    hot = rng.choice(n, int(n * 0.08), replace=False)
    df.loc[hot, "shop_id"] = "s001"
    return df


def zipf_index(rng, n_items, size, a=1.0):
    """按齐普夫分布抽样：少数热门商品被买得最多。"""
    ranks = np.arange(1, n_items + 1)
    w = 1.0 / ranks ** a
    perm = np.random.default_rng(SEED + 7).permutation(n_items)  # 热门商品随机分散在编号里
    return perm[rng.choice(n_items, size, p=w / w.sum())]


def random_times(rng, size, days=DAYS, start=START):
    day = rng.integers(0, days, size)
    hour = rng.choice(24, size, p=hour_weights())
    sec = rng.integers(0, 3600, size)
    return start + pd.to_timedelta(day * 86400 + hour * 3600 + sec, unit="s")


def make_orders(rng, n, users, products):
    uidx = zipf_index(rng, len(users), n, a=0.6)          # 少数老用户下单多
    pidx = zipf_index(rng, len(products), n)
    qty = rng.choice([1, 1, 1, 1, 2, 2, 3, 5], n)
    price = products["price"].to_numpy()[pidx]
    t = random_times(rng, n).sort_values()
    df = pd.DataFrame({
        "order_id": [f"o{i:09d}" for i in range(1, n + 1)],
        "user_id": users["user_id"].to_numpy()[uidx],
        "product_id": products["product_id"].to_numpy()[pidx],
        "quantity": qty,
        "amount": np.round(price * qty, 2),
        "order_time": t.strftime("%Y-%m-%d %H:%M:%S"),
        "province": users["province"].to_numpy()[uidx],
        "pay_type": rng.choice(PAY_TYPES, n, p=[0.45, 0.38, 0.07, 0.10]),
        "status": rng.choice(["已完成", "已取消", "已退款"], n, p=[0.90, 0.07, 0.03]),
    })
    return df


def make_logs(rng, n, users, products, path, day="2026-09-30"):
    """访问日志：默认只生成 9 月 30 日一天；按小时分块生成并写出，1000 万行也不会占满内存。"""
    n_users = len(users)
    uid = users["user_id"].to_numpy()
    pid = products["product_id"].to_numpy()
    # 每个用户固定一个常用 IP 和终端
    ips = np.array([f"{a}.{b}.{c}.{d}" for a, b, c, d in rng.integers(1, 255, (n_users, 4))])
    dev = rng.choice(DEVICES, n_users, p=[0.48, 0.30, 0.12, 0.10])
    pages = np.array(["/", "/search", "/cart", "/order/submit", "/pay", "/login", "/coupon"])
    t0 = pd.Timestamp(day)
    per_hour = rng.multinomial(n, hour_weights())
    with open(path, "w", encoding="utf-8") as f:
        for hour, m in enumerate(per_hour):
            u = zipf_index(rng, n_users, m, a=0.6)
            # 60% 的请求是商品详情页，其余是首页、搜索、购物车等
            is_item = rng.random(m) < 0.6
            item = pid[zipf_index(rng, len(pid), m)]
            other = rng.choice(pages, m, p=[0.30, 0.30, 0.15, 0.08, 0.06, 0.06, 0.05])
            url = np.where(is_item, np.char.add("/item/", item.astype(str)), other)
            status = rng.choice(["200", "304", "404", "500"], m, p=[0.90, 0.06, 0.03, 0.01])
            size = rng.integers(300, 20000, m).astype(str)
            sec = np.sort(hour * 3600 + rng.integers(0, 3600, m))
            ts = (t0 + pd.to_timedelta(sec, unit="s")).strftime("%Y-%m-%dT%H:%M:%S").to_numpy()
            # 约 15% 的请求来自未登录访客，用户字段写“-”
            user_col = np.where(rng.random(m) < 0.15, "-", uid[u])
            method = np.where(np.isin(url, ["/order/submit", "/pay", "/login"]), "POST", "GET")
            lines = [" ".join(r) for r in zip(ts, ips[u], user_col, method, url, status, size, dev[u])]
            if lines:
                f.write("\n".join(lines) + "\n")
            print(f"  访问日志 {hour:02d} 时：{m:,} 行")


def make_reviews(rng, n, users, products):
    pidx = zipf_index(rng, len(products), n)
    rating = rng.choice([5, 4, 3, 2, 1], n, p=[0.55, 0.22, 0.10, 0.05, 0.08])
    content = []
    for r in rating:
        pool = REVIEW_GOOD if r >= 4 else REVIEW_BAD if r <= 2 else REVIEW_GOOD + REVIEW_BAD
        k = rng.integers(1, 4)
        content.append("，".join(rng.choice(pool, k, replace=False)))
    df = pd.DataFrame({
        "review_id": [f"r{i:07d}" for i in range(1, n + 1)],
        "product_id": products["product_id"].to_numpy()[pidx],
        "user_id": users["user_id"].to_numpy()[rng.integers(0, len(users), n)],
        "rating": rating,
        "content": content,
    })
    # 约 5% 的评论是“刷单”复制的近似重复评论，第 10 章用 MinHash 找出来
    dup = rng.choice(n, n // 20, replace=False)
    src = rng.choice(n, n // 20)
    df.loc[dup, "content"] = df.loc[src, "content"].to_numpy() + "！"
    return df


def main():
    ap = argparse.ArgumentParser(description="生成小象优选模拟数据")
    ap.add_argument("--users", type=int, default=100_000)
    ap.add_argument("--products", type=int, default=2_000)
    ap.add_argument("--orders", type=int, default=100_000)
    ap.add_argument("--logs", type=int, default=1_000_000)
    ap.add_argument("--reviews", type=int, default=20_000)
    ap.add_argument("--only", choices=["users", "products", "orders", "logs", "reviews"])
    ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "xiaoxiang"))
    a = ap.parse_args()
    os.makedirs(a.out, exist_ok=True)
    t = time.time()
    # 每张表用各自的随机数流，改一张表的行数不会影响其他表
    users = make_users(np.random.default_rng(SEED), a.users)
    products = make_products(np.random.default_rng(SEED + 1), a.products)
    jobs = {
        "users": lambda: users.to_csv(os.path.join(a.out, "users.csv"), index=False),
        "products": lambda: products.to_csv(os.path.join(a.out, "products.csv"), index=False),
        "orders": lambda: make_orders(np.random.default_rng(SEED + 2), a.orders, users, products)
        .to_csv(os.path.join(a.out, "orders.csv"), index=False),
        "logs": lambda: make_logs(np.random.default_rng(SEED + 3), a.logs, users, products,
                                  os.path.join(a.out, "access.log")),
        "reviews": lambda: make_reviews(np.random.default_rng(SEED + 4), a.reviews, users, products)
        .to_csv(os.path.join(a.out, "reviews.csv"), index=False),
    }
    for name, job in jobs.items():
        if a.only and name != a.only:
            continue
        print(f"生成 {name} ……")
        job()
    print(f"完成，用时 {time.time() - t:.1f} 秒，输出目录：{a.out}")
    for fn in sorted(os.listdir(a.out)):
        size = os.path.getsize(os.path.join(a.out, fn))
        print(f"  {fn:<14}{size / 1024 / 1024:8.1f} MB")


if __name__ == "__main__":
    main()
