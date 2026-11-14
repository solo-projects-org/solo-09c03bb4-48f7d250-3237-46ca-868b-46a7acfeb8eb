-- 演示一：初始装载进行中，源库照常修改（新增 / 更正 / 删除）
-- 用单事务提交，验证“每笔源事务整笔可见”
BEGIN;

-- 新增
INSERT INTO quake (quake_no, origin_utc, longitude, latitude, magnitude)
VALUES ('CE20240403075812', '2024-04-03 07:58:12+00', 121.670, 23.820, 7.4);

-- 更正：汶川震级与定位修订（演示用的假设性修订值）
UPDATE quake
   SET magnitude = 8.2, longitude = 103.364, latitude = 31.002
 WHERE quake_no = 'CE20080512062801';

-- 删除：目录撤下一条样例
DELETE FROM quake WHERE quake_no = 'CE20240101071010';

COMMIT;
