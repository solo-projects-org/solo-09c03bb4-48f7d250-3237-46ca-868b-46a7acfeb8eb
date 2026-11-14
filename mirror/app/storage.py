"""SQLite 持久层：震例镜像、复制状态与一致性读取。

设计要点：
- 状态机 INITIALIZING / STREAMING 与镜像数据在同一库内持久化；
- 每个源事务的所有行变更连同续采水位线在一个 SQLite 事务里提交，
  保证“整笔可见”，提交成功后才允许源端确认（前移复制槽）；
- synchronous=FULL + WAL，确保提交落盘后才确认复制进度；
- 单连接配合可重入锁串行化所有访问：FastAPI 工作线程与引擎线程
  不会出现一个事务进行中、另一线程再 BEGIN 的竞争；
- 所有网页查询都在显式只读事务（同一快照）内完成。
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator, Sequence

STATE_INITIALIZING = "INITIALIZING"
STATE_STREAMING = "STREAMING"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS quake (
    quake_no   TEXT PRIMARY KEY,
    origin_utc TEXT NOT NULL,
    longitude  REAL NOT NULL,
    latitude   REAL NOT NULL,
    magnitude  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS replication_state (
    id            INTEGER PRIMARY KEY CHECK (id = 1),
    state         TEXT NOT NULL,
    slot_name     TEXT,
    snapshot_id   TEXT,
    confirmed_lsn INTEGER NOT NULL DEFAULT 0,
    last_txid     BIGINT,
    last_change   TEXT,
    applied_count INTEGER NOT NULL DEFAULT 0,
    last_error    TEXT,
    updated_at    TEXT NOT NULL
);
"""


class Store:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        # 提交必须落盘，之后才允许确认复制进度
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA foreign_keys=ON")
        with self._lock:
            self._conn.executescript(_SCHEMA)
            if self._conn.execute(
                "SELECT 1 FROM replication_state WHERE id=1"
            ).fetchone() is None:
                self._conn.execute(
                    "INSERT INTO replication_state "
                    "(id, state, confirmed_lsn, applied_count, updated_at) "
                    "VALUES (1, 'INITIALIZING', 0, 0, ?)",
                    (_now(),),
                )

    # ---------- 读取（每次取数只读同一已提交状态） ----------

    @contextmanager
    def snapshot(self) -> Iterator[sqlite3.Connection]:
        """显式只读事务：事务内所有 SELECT 看到同一个快照。

        持锁直到事务结束，保证引擎线程不会在读取中途提交写事务。
        """
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                yield self._conn
            finally:
                # SELECT 未改数据，COMMIT/ROLLBACK 都只是结束快照事务
                self._conn.execute("COMMIT")

    def get_state(self) -> dict[str, Any]:
        with self.snapshot() as conn:
            row = conn.execute(
                "SELECT * FROM replication_state WHERE id=1"
            ).fetchone()
            return dict(row)

    def query_quakes(
        self,
        keyword: str | None,
        min_mag: float | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        where, params = [], []
        if keyword:
            where.append("quake_no LIKE ?")
            params.append(f"%{keyword}%")
        if min_mag is not None:
            where.append("magnitude >= ?")
            params.append(min_mag)
        clause = (" WHERE " + " AND ".join(where)) if where else ""
        with self.snapshot() as conn:
            total = conn.execute(
                f"SELECT COUNT(*) FROM quake{clause}", params
            ).fetchone()[0]
            rows = conn.execute(
                f"SELECT quake_no, origin_utc, longitude, latitude, magnitude "
                f"FROM quake{clause} ORDER BY origin_utc, quake_no "
                f"LIMIT ? OFFSET ?",
                [*params, limit, offset],
            ).fetchall()
        return [dict(r) for r in rows], total

    def csv_rows(self) -> list[Sequence[Any]]:
        with self.snapshot() as conn:
            rows = conn.execute(
                "SELECT quake_no, origin_utc, longitude, latitude, magnitude "
                "FROM quake ORDER BY origin_utc, quake_no"
            ).fetchall()
        return [tuple(r) for r in rows]

    # ---------- 初始镜像 ----------

    def begin_initialization(self, slot_name: str, slot_lsn: int,
                             snapshot_id: str | None) -> None:
        """重做场景：先清空旧镜像，再登记 INITIALIZING（此状态下不供数）。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute("DELETE FROM quake")
                self._conn.execute(
                    "UPDATE replication_state SET state=?, slot_name=?, "
                    "snapshot_id=?, confirmed_lsn=?, last_txid=NULL, "
                    "last_change=NULL, applied_count=0, last_error=NULL, "
                    "updated_at=? WHERE id=1",
                    (STATE_INITIALIZING, slot_name, snapshot_id,
                     slot_lsn, _now()),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def insert_snapshot_batch(self, rows: list[tuple]) -> None:
        # 分批提交，保持事务短小；未完成时状态始终是 INITIALIZING
        with self._lock:
            self._conn.execute("BEGIN")
            try:
                self._conn.executemany(
                    "INSERT INTO quake (quake_no, origin_utc, longitude, "
                    "latitude, magnitude) VALUES (?,?,?,?,?)",
                    rows,
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def complete_initialization(self) -> None:
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                self._conn.execute(
                    "UPDATE replication_state SET state=?, updated_at=? "
                    "WHERE id=1",
                    (STATE_STREAMING, _now()),
                )
                self._conn.execute("COMMIT")
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    # ---------- 续采：每笔源事务整笔可见 ----------

    def apply_transaction(self, events: list[dict[str, Any]], lsn: int,
                          txid: int | None, change_summary: str) -> int:
        """把一个源事务的全部行变更与新水位线在单个事务中提交。

        返回本事务应用的行数。只有本方法成功提交后，调用方才可以
        前移源端复制槽；写入失败抛异常，水位线不前移。
        """
        applied = 0
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for ev in events:
                    kind = ev["kind"]
                    if kind == "insert":
                        data = _column_map(ev)
                        self._conn.execute(
                            "INSERT INTO quake (quake_no, origin_utc, "
                            "longitude, latitude, magnitude) "
                            "VALUES (?,?,?,?,?)",
                            _quake_values(data),
                        )
                    elif kind == "update":
                        data = _column_map(ev)
                        self._conn.execute(
                            "UPDATE quake SET origin_utc=?, longitude=?, "
                            "latitude=?, magnitude=? WHERE quake_no=?",
                            (
                                data["origin_utc"], data["longitude"],
                                data["latitude"], data["magnitude"],
                                data["quake_no"],
                            ),
                        )
                    elif kind == "delete":
                        keys = ev.get("oldkeys", {})
                        names = keys.get("keynames", [])
                        values = keys.get("keyvalues", [])
                        quake_no = values[names.index("quake_no")]
                        self._conn.execute(
                            "DELETE FROM quake WHERE quake_no=?", (quake_no,)
                        )
                    else:
                        # 只处理行变更（DML），其余一律忽略
                        continue
                    applied += 1
                self._conn.execute(
                    "UPDATE replication_state SET confirmed_lsn=?, "
                    "last_txid=?, last_change=?, "
                    "applied_count=applied_count+?, last_error=NULL, "
                    "updated_at=? WHERE id=1",
                    (lsn, txid, change_summary, applied, _now()),
                )
                self._conn.execute("COMMIT")
                return applied
            except Exception:
                self._conn.execute("ROLLBACK")
                raise

    def mark_error(self, message: str) -> None:
        with self._lock:
            try:
                self._conn.execute(
                    "UPDATE replication_state SET last_error=?, updated_at=? "
                    "WHERE id=1", (message[:1000], _now())
                )
            except sqlite3.Error:
                pass

    def close(self) -> None:
        with self._lock:
            self._conn.close()


def _column_map(ev: dict[str, Any]) -> dict[str, Any]:
    return dict(zip(ev["columnnames"], ev["columnvalues"]))


def _quake_values(data: dict[str, Any]) -> tuple:
    return (
        data["quake_no"], data["origin_utc"], data["longitude"],
        data["latitude"], data["magnitude"],
    )


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
