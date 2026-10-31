# 第 12 章 数据可视化、数据合规与综合项目：实验代码

| 文件 | 用途 | 运行位置 |
|---|---|---|
| `lab12_1_prepare.py` | 实验 12-1 第 1 步：从订单宽表和访问日志算出 5 张小表（品类、每日、省份、支付方式、漏斗），写到 `data/report/` | spark 容器：`spark-submit /labs/ch12/lab12_1_prepare.py` |
| `lab12_1_report.py` | 实验 12-1 第 2 步：pyecharts 画柱状图、折线图、地图、环形图、漏斗图，生成 `data/report/xiaoxiang_report.html`；不能上网时加 `--assets file:///本地目录` | 本机：`python ch12/lab12_1_report.py` |
| `lab12_2_to_mysql.py` | 实验 12-2 第 1 步：把 9 月订单和用户分群写进 MySQL 的 `dash_orders`、`dash_user_cluster` | spark 容器：`spark-submit --jars /labs/jars/mysql-connector-j-8.4.0.jar /labs/ch12/lab12_2_to_mysql.py` |
| `lab12_2_bi_user.sql` | 实验 12-2 第 2 步：建只读账号 `bi_reader`，只能查询两张看板表 | mysql 容器：`mysql -uroot -pxiaoxiang < ../ch12/lab12_2_bi_user.sql` |
| `make_members.py` | 实验 12-3：生成 10 万条**虚构**的会员信息 `data/xiaoxiang/members.csv`（手机号 100 开头、身份证号 99 开头，不对应任何真实的人） | 本机：`python ch12/make_members.py` |
| `lab12_3_mask.py` | 实验 12-3：会员信息脱敏（遮盖、截断、加盐哈希）并检查，只把脱敏结果写入 HDFS `/xiaoxiang/secure/members_masked` | spark 容器：`spark-submit /labs/ch12/lab12_3_mask.py` |
| `lab12_3_pipeline.sh` | 实验 12-3：参考流水线，采集 → 脱敏 → 清洗 → 统计 → 分群 → 入库，一次跑完（编者实测 204 秒） | 本机 `labs` 目录：`bash ch12/lab12_3_pipeline.sh` |

## 准备工作

- 先完成第 7 章（订单宽表、访问日志）和实验 10-2（用户分群）；或者直接运行 `lab12_3_pipeline.sh`，它会依次运行这些程序。
- 本机安装：`pip install pyecharts==2.1.0 pandas`。
- MySQL 驱动 `mysql-connector-j-8.4.0.jar` 放在 `labs/jars/`（见实验 8-3）。
- 实验 12-2、12-3 要同时运行集群、spark、MySQL、Superset 容器，Docker 至少分配 7 GB 内存。

## 启动 Superset

```bash
cd labs/cluster
docker compose -f docker-compose.yml -f docker-compose.mysql.yml -f docker-compose.superset.yml up -d --build superset
```

第一次启动要构建镜像并初始化，需要几分钟。浏览器打开 <http://localhost:8089>（8088 已被 YARN 占用），用户名 `admin`，密码 `admin`。连接数据库的 SQLAlchemy URI：

```text
mysql+pymysql://bi_reader:bi_reader_2026@mysql:3306/xiaoxiang?charset=utf8mb4
```

本目录中的密码、盐和 Superset 的 SECRET_KEY 只供实验使用，真实项目中必须换掉，并且不能写在代码里。
