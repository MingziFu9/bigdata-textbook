-- 实验 5-1 第 4 步：统计
USE xiaoxiang;

-- ① 各订单状态的订单数
SELECT status, COUNT(*) AS cnt
FROM ods_orders
GROUP BY status;

-- ② 各品类的已完成订单销售额（订单表关联商品表），从高到低
SELECT p.category,
       COUNT(*)          AS order_cnt,
       SUM(o.amount)     AS gmv
FROM ods_orders o
JOIN ods_products p ON o.product_id = p.product_id
WHERE o.status = '已完成'
GROUP BY p.category
ORDER BY gmv DESC;

-- ③ 各会员等级的人均消费（订单表关联用户表）
SELECT u.level,
       COUNT(DISTINCT o.user_id)                         AS buyers,
       ROUND(SUM(o.amount) / COUNT(DISTINCT o.user_id), 2) AS per_buyer
FROM ods_orders o
JOIN ods_users u ON o.user_id = u.user_id
WHERE o.status = '已完成'
GROUP BY u.level
ORDER BY per_buyer DESC;
