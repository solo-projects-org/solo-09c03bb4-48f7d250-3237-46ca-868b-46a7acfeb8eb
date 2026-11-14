"""运行参数，全部来自环境变量（compose 里注入），带本地默认值。"""
import os


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


def _float(name: str, default: float) -> float:
    return float(os.environ.get(name, default))


# 源库（compose 内 postgres 服务）
PG_DSN = os.environ.get(
    "PG_DSN",
    "host=127.0.0.1 port=55432 dbname=seisnet user=replicator password=replicator",
)

# SQLite 镜像库（容器内持久卷）
SQLITE_PATH = os.environ.get("SQLITE_PATH", "/data/mirror.db")

SOURCE_TABLE = os.environ.get("SOURCE_TABLE", "public.quake")
SLOT_NAME = os.environ.get("SLOT_NAME", "quake_mirror_slot")
POLL_INTERVAL = _float("POLL_INTERVAL", 0.5)          # 续采轮询秒数
SNAPSHOT_BATCH = _int("SNAPSHOT_BATCH", 500)          # 初始装载每批行数

# 演示开关：SQLite 已提交、复制槽尚未前移时额外停留的秒数。
# 停留期间重启进程，即可演示“已保存但未确认进度”的崩溃恢复。
ACK_DELAY = _float("ACK_DELAY", 0.0)
