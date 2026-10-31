-- 实验 5-2：比较查询一天的数据时，分区表和不分区的表各读了多少数据
USE xiaoxiang;

-- 先跑一个小查询“热身”，让计算引擎启动好，后面的计时才公平
SELECT COUNT(*) FROM ods_products;

-- 不分区：要把 300 万行全部读一遍，再挑出 9 月 15 日的
SELECT COUNT(*) AS cnt, SUM(amount) AS gmv
FROM ods_orders_big
WHERE TO_DATE(order_time) = '2026-09-15';

-- 分区表：只读 dt=2026-09-15 这一个目录
SELECT COUNT(*) AS cnt, SUM(amount) AS gmv
FROM orders_part
WHERE dt = '2026-09-15';

-- 查询整个月：分区表也要读全部分区
SELECT COUNT(*) AS cnt, SUM(amount) AS gmv
FROM orders_part;
