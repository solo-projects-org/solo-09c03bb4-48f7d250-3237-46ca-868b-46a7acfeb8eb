# 源库：PostgreSQL 15 + wal2json 逻辑解码插件
FROM postgres:15-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-15-wal2json \
    && rm -rf /var/lib/apt/lists/*

# 逻辑复制所需参数（compose 命令行也会显式声明，双保险）
COPY init/ /docker-entrypoint-initdb.d/
COPY demo/ /demo/
