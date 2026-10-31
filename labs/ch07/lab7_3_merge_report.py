"""实验 7-3：多表合并与清洗报告。

先完成实验 7-1（生成 data/xiaoxiang_clean/orders/），再在 spark 容器中运行：
    spark-submit /labs/ch07/lab7_3_merge_report.py
输出：宽表 data/xiaoxiang_clean/order_wide/，报告 ch07/output/清洗报告.md
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验7-3 多表合并与清洗报告").getOrCreate()
DATA = "file:///labs/data"

dirty = spark.read.csv(f"{DATA}/xiaoxiang_dirty/orders_dirty.csv", header=True)
clean = spark.read.parquet(f"{DATA}/xiaoxiang_clean/orders")
users = spark.read.csv(f"{DATA}/xiaoxiang/users.csv", header=True, inferSchema=True)
products = spark.read.csv(f"{DATA}/xiaoxiang/products.csv", header=True, inferSchema=True)

# ① 多表合并：订单（事实）+ 用户、商品（维度）→ 一张宽表，后面的分析直接用它
wide = (clean
        .join(users.select("user_id", "gender", "age", "level"), "user_id", "left")
        .join(products.select("product_id", "product_name", "category", "price"), "product_id", "left"))
wide.write.mode("overwrite").partitionBy("dt").parquet(f"{DATA}/xiaoxiang_clean/order_wide")
print("宽表行数", wide.count(), "，列数", len(wide.columns))
print("关联不上用户或商品的订单", wide.filter(F.col("level").isNull() | F.col("category").isNull()).count())


# ② 清洗前后对比
def nulls(df, cols):
    return df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first().asDict()


cols = ["user_id", "quantity", "amount", "order_time", "province", "pay_type"]
before, after = nulls(dirty, cols), nulls(clean, cols)
n_before, n_after = dirty.count(), clean.count()
dup = n_before - dirty.dropDuplicates().count()
prov_before = dirty.select("province").distinct().count()
prov_after = clean.select("province").distinct().count()
gmv = clean.filter(F.col("status") == "已完成").agg(F.sum("amount")).first()[0]

lines = [
    "# 小象优选订单数据清洗报告", "",
    "数据：`data/xiaoxiang_dirty/orders_dirty.csv` → `data/xiaoxiang_clean/orders/`（Parquet，按 dt 分区）", "",
    "## 一、行数变化", "",
    "| 项目 | 行数 |", "|---|---|",
    f"| 清洗前 | {n_before} |",
    f"| 完全重复的行（删除） | {dup} |",
    f"| 数量、金额或时间异常的行（删除） | {n_before - dup - n_after} |",
    f"| 清洗后 | {n_after} |", "",
    "## 二、各列缺失值", "",
    "| 列 | 清洗前 | 清洗后 | 处理办法 |", "|---|---|---|---|",
]
how = {"province": "按用户表补", "amount": "单价 × 数量", "pay_type": "填“未知”"}
for c in cols:
    lines.append(f"| {c} | {before[c]} | {after[c]} | {how.get(c, '—')} |")
lines += ["", "## 三、格式统一", "",
          f"- 省份的不同写法：清洗前 {prov_before} 种（含空值），清洗后 {prov_after} 种",
          "- 下单时间：4 种写法统一为 `yyyy-MM-dd HH:mm:ss`；金额去掉 ¥；用户编号统一小写；支付方式去掉前后空格", "",
          "## 四、清洗后概况", "",
          f"- 已完成订单销售额：{gmv:,.2f} 元",
          f"- 与用户表、商品表合并后的宽表：{wide.count()} 行、{len(wide.columns)} 列"]
report = "\n".join(lines)
os.makedirs("/labs/ch07/output", exist_ok=True)
with open("/labs/ch07/output/清洗报告.md", "w", encoding="utf-8") as f:
    f.write(report + "\n")
print(report)
spark.stop()
