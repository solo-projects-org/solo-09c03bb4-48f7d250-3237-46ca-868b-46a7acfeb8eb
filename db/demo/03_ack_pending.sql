-- 演示三：保存后尚未确认复制进度时重启
-- 配合 ACK_DELAY（如 20 秒）：应用写入 SQLite 后、advance 复制槽前重启，
-- 重启后该事务会被再次 peek 到，按水位线去重，结果只有一条、不重复。
BEGIN;
INSERT INTO quake (quake_no, origin_utc, longitude, latitude, magnitude)
VALUES ('CE20260305111200', '2026-03-05 11:12:00+00', 122.060, 24.150, 5.8);
COMMIT;
