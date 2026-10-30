"""实验 3-3：用 Python 读写 HDFS，把各支付方式的订单统计结果写回集群。

在 labs/ 目录下运行（集群已启动，且做过实验 3-1，订单表已上传到 /xiaoxiang/raw/）：
    python ch03/lab3_3_hdfs_python.py
程序通过 HttpFS 网关（端口 14000）访问 HDFS，只用到 requests 和 pandas 两个库。
"""
import io

import pandas as pd
import requests

BASE = "http://localhost:14000/webhdfs/v1"   # HttpFS 地址，后面直接接 HDFS 路径
USER = {"user.name": "root"}                 # 以 root 身份访问（实验集群没有开启登录认证）


def hdfs(method, path, op, data=None, **params):
    """向 HttpFS 发一个请求。op 是操作名，例如 LISTSTATUS、OPEN、CREATE。"""
    params = {"op": op, **USER, **params}
    headers = {"Content-Type": "application/octet-stream"} if data is not None else {}
    r = requests.request(method, BASE + path, params=params, data=data,
                         headers=headers, timeout=60)
    r.raise_for_status()                     # 出错（例如路径不存在）时直接报错
    return r


# 1. 列目录：相当于 hdfs dfs -ls /xiaoxiang/raw
print("① /xiaoxiang/raw 下的文件：")
for f in hdfs("GET", "/xiaoxiang/raw", "LISTSTATUS").json()["FileStatuses"]["FileStatus"]:
    print(f"   {f['pathSuffix']:<12} {f['length'] / 1024 / 1024:6.2f} MB  副本数 {f['replication']}")

# 2. 读文件：相当于 hdfs dfs -cat，把内容直接交给 pandas
text = hdfs("GET", "/xiaoxiang/raw/orders.csv", "OPEN").content
orders = pd.read_csv(io.BytesIO(text))
print(f"② 从 HDFS 读到订单 {len(orders):,} 行")

# 3. 在本机用 pandas 统计：各支付方式的已完成订单数和金额
done = orders[orders["status"] == "已完成"]
result = (done.groupby("pay_type")
          .agg(订单数=("order_id", "count"), 销售额=("amount", "sum"))
          .round(2).sort_values("销售额", ascending=False).reset_index())
print("③ 统计结果：")
print(result.to_string(index=False))

# 4. 写回 HDFS：先建目录，再上传（overwrite=true 表示已存在就覆盖）
hdfs("PUT", "/xiaoxiang/result", "MKDIRS")
csv_bytes = result.to_csv(index=False).encode("utf-8")
hdfs("PUT", "/xiaoxiang/result/pay_type.csv", "CREATE", data=csv_bytes,
     overwrite="true", replication=2)

# 5. 读回来核对一遍
back = hdfs("GET", "/xiaoxiang/result/pay_type.csv", "OPEN").text
print("④ 已写入 /xiaoxiang/result/pay_type.csv，读回的前两行：")
print("   " + "\n   ".join(back.splitlines()[:2]))
