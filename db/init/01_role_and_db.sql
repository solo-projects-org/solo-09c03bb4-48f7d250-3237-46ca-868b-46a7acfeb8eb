-- 01：在维护库（postgres）执行，创建复制账号与业务库
-- docker-entrypoint 按文件名顺序、连到 POSTGRES_DB 执行本目录脚本；
-- 这里用 \connect 显式切库，保证容器内外行为一致。
\connect postgres

DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'replicator') THEN
    CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD 'replicator';
  END IF;
END$$;

SELECT 'CREATE DATABASE seisnet OWNER replicator'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'seisnet')\gexec
