-- 实验 12-2：给看板建一个只读账号（最小权限原则）
-- 运行：docker compose -f cluster/docker-compose.yml -f cluster/docker-compose.mysql.yml exec -T mysql mysql -uroot -pxiaoxiang < ch12/lab12_2_bi_user.sql
CREATE USER IF NOT EXISTS 'bi_reader'@'%' IDENTIFIED BY 'bi_reader_2026';
-- 只能查询（SELECT）看板用的两张表，不能改、不能删，也看不到库里的其他表
GRANT SELECT ON xiaoxiang.dash_orders TO 'bi_reader'@'%';
GRANT SELECT ON xiaoxiang.dash_user_cluster TO 'bi_reader'@'%';
SHOW GRANTS FOR 'bi_reader'@'%';
