-- 演示二：镜像完成后的持续修订（三笔独立事务，按提交顺序应用）

-- 事务 A：新增一条 2025 年震例
BEGIN;
INSERT INTO quake (quake_no, origin_utc, longitude, latitude, magnitude)
VALUES ('CE20250714093122', '2025-07-14 09:31:22+00', 140.115, 35.902, 6.1);
COMMIT;

-- 事务 B：第一次更正
BEGIN;
UPDATE quake SET magnitude = 6.4
 WHERE quake_no = 'CE20250714093122';
COMMIT;

-- 事务 C：第二次更正（最终值；旧值 6.1/6.4 不得盖回 6.5）
BEGIN;
UPDATE quake SET magnitude = 6.5, longitude = 140.180
 WHERE quake_no = 'CE20250714093122';
COMMIT;
