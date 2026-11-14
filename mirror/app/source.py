"""源库 PostgreSQL 访问层（psycopg2）。

只通过 SQL 复制函数与 wal2json 插件交互，不使用协议级复制连接：
- 在 REPEATABLE READ 只读事务内 pg_create_logical_replication_slot +
  pg_export_snapshot：槽位点与装载快照在同一事务快照中成对获得；
- pg_logical_slot_peek_changes 只“偷看”变更，不前移槽位点；
- SQLite 成功落盘后才调用 pg_replication_slot_advance 确认进度。
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterator

import psycopg2

# peek_changes 的 options 为键值交错数组：['key1','val1','key2','val2', ...]
WAL2JSON_OPTIONS = [
    "include-pk", "true",
    "include-transaction", "true",
    "add-tables", "public.quake",   # 只取目标震例表
]


@dataclass
class ChangeBatch:
    """wal2json 一条消息 = 源库一个已提交事务（或一次刷新）。"""
    lsn: int          # 该消息结束 LSN（提交记录 LSN）
    xid: int | None
    events: list[dict[str, Any]]


def connect(dsn: str):
    return psycopg2.connect(dsn, connect_timeout=10)


def slot_exists(conn, slot_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name=%s",
            (slot_name,),
        )
        return cur.fetchone() is not None


def drop_slot(conn, slot_name: str) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
    conn.commit()


def lsn_to_text(lsn: int) -> str:
    """整数 LSN -> 'X/YYYY' 文本（PG 15 不支持 bigint 直接 cast 到 pg_lsn）。"""
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:08X}"


def create_slot_with_snapshot(dsn: str, slot_name: str):
    """在单个只读事务内创建逻辑槽并导出快照。

    返回 (conn, slot_lsn:int, snapshot_id:str)。
    调用方必须在整个初始装载期间保持该连接的事务打开（服务端游标
    依赖该快照），装载结束后 rollback() 释放；槽本身已持久化，
    不会随事务结束而消失。槽建立后源库照常写入，相关 WAL 被槽保留，
    稍后从 confirmed_flush_lsn 续采即可，因此装载期间的修改不漏。
    """
    conn = connect(dsn)
    # 用 psycopg2 原生会话设置：第一条语句即以 RR + 只读开启隐式事务，
    # psycopg2 会正确跟踪事务状态，命名（服务端）游标可复用该事务快照。
    conn.set_session(
        isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
        readonly=True,
    )
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (lsn - '0/0'::pg_lsn)::bigint "
            "FROM pg_create_logical_replication_slot(%s, 'wal2json')",
            (slot_name,),
        )
        slot_lsn = cur.fetchone()[0]
        cur.execute("SELECT pg_export_snapshot()")
        snapshot_id = cur.fetchone()[0]
    return conn, slot_lsn, snapshot_id


def iter_snapshot_rows(conn, table: str, batch_size: int) -> Iterator[tuple]:
    """服务端命名游标分批读取事务快照中的整表，避免一次性拉全量。

    命名游标必须在显式事务中使用，这里复用建槽事务的快照。
    """
    with conn.cursor(name="quake_snapshot_cursor") as cur:
        cur.itersize = batch_size
        cur.execute(
            f"SELECT quake_no, origin_utc::text, longitude, latitude, "
            f"magnitude FROM {table}"
        )
        yield from cur


def peek_changes(conn, slot_name: str, max_changes: int = 64) -> list[ChangeBatch]:
    """偷看变更（不推进槽位点），解析 wal2json JSON。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (lsn - '0/0'::pg_lsn)::bigint, xid, data "
            "FROM pg_logical_slot_peek_changes(%s, NULL, %s, VARIADIC %s::text[])",
            (slot_name, max_changes, WAL2JSON_OPTIONS),
        )
        raw = cur.fetchall()
    batches: list[ChangeBatch] = []
    for lsn, xid, payload in raw:
        parsed = json.loads(payload)
        events = [
            ch for ch in parsed.get("change", [])
            if ch.get("table") == "quake" and ch.get("kind") in (
                "insert", "update", "delete",
            )
        ]
        batches.append(ChangeBatch(lsn=lsn, xid=xid, events=events))
    return batches


def advance_slot(conn, slot_name: str, lsn: int) -> int:
    """确认复制进度：把槽位点前移到已持久化的 LSN。"""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (r.end_lsn - '0/0'::pg_lsn)::bigint FROM "
            "pg_replication_slot_advance(%s, %s::pg_lsn) AS r(slot_name, end_lsn)",
            (slot_name, lsn_to_text(lsn)),
        )
        advanced = cur.fetchone()[0]
    conn.commit()
    return advanced


def slot_confirmed_lsn(conn, slot_name: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT (confirmed_flush_lsn - '0/0'::pg_lsn)::bigint "
            "FROM pg_replication_slots WHERE slot_name=%s",
            (slot_name,),
        )
        row = cur.fetchone()
    return None if row is None else row[0]


def source_count(conn, table: str) -> int | None:
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            return cur.fetchone()[0]
    except psycopg2.Error:
        conn.rollback()
        return None
