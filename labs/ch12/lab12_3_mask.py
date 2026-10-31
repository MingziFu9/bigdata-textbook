"""实验 12-3 第 1 步：会员信息脱敏后再放进 HDFS。

先在本机运行 python ch12/make_members.py 生成虚构的会员信息，再在 spark 容器中运行：
    spark-submit /labs/ch12/lab12_3_mask.py
结果：HDFS 上的 /xiaoxiang/secure/members_masked（Parquet）
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("实验12-3 脱敏").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
SALT = "xiaoxiang-2026"     # 加盐哈希用的“盐”，真实项目中要保密，不能写在代码里

raw = spark.read.csv("file:///labs/data/xiaoxiang/members.csv", header=True)   # 全部按字符串读，身份证号末位可能是 X
print("① 原始数据（只在受控环境中查看）：")
raw.show(3, truncate=False)

masked = raw.select(
    "user_id",
    # 姓名：只留姓
    F.concat(F.substring("name", 1, 1), F.expr("repeat('*', char_length(name) - 1)")).alias("name"),
    # 手机号：前 3 后 4，中间 4 位换成 *
    F.concat(F.substring("phone", 1, 3), F.lit("****"), F.substring("phone", 8, 4)).alias("phone"),
    # 身份证号：前 6 位（地区）和后 4 位，出生日期等 8 位换成 *
    F.concat(F.substring("id_card", 1, 6), F.lit("********"), F.substring("id_card", 15, 4)).alias("id_card"),
    # 地址：去掉街道和门牌号
    F.regexp_replace("address", r"[^\x00-\x7F]{2}[路街]\d+号$", "****").alias("address"),
    # 邮箱：用户名只留第 1 个字符
    F.regexp_replace("email", r"^(.)[^@]*", "$1***").alias("email"),
    # 假名化：身份证号加盐后做 SHA-256，不能还原，但同一个人每次得到的值相同，可以用来关联
    F.sha2(F.concat("id_card", F.lit(SALT)), 256).alias("id_hash"),
)
print("② 脱敏以后：")
masked.show(3, truncate=False)

# ③ 检查：脱敏结果中不能再出现完整的手机号和身份证号
left_phone = masked.filter(F.col("phone").rlike(r"^\d{11}$")).count()
left_id = masked.filter(F.col("id_card").rlike(r"^\d{17}[\dX]$")).count()
same = masked.groupBy("id_hash").count().filter("count > 1").count()
print(f"③ 检查：完整手机号 {left_phone} 个，完整身份证号 {left_id} 个，id_hash 重复 {same} 个")

# ④ 只把脱敏后的数据写进 HDFS（路径不带 file://，就是 HDFS）
masked.write.mode("overwrite").parquet("/xiaoxiang/secure/members_masked")
print("④ 已写入 HDFS：/xiaoxiang/secure/members_masked，共", spark.read.parquet("/xiaoxiang/secure/members_masked").count(), "行")
spark.stop()
