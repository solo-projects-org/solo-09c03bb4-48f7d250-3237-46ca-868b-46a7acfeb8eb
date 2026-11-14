"""镜像引擎：后台工作线程的状态机。

状态：
  INITIALIZING  初始镜像未完成，网页不供数（查询/CSV 返回 503）
  STREAMING     初始镜像完成，持续逻辑续采，网页提供同一已提交快照

不漏不重的保证：
  1. 先创建复制槽（拿到 slot LSN + 导出快照），再用该快照装载全量；
     槽会保留装载期间源库产生的所有 WAL，源库无需停写。
  2. 续采用 peek 只偷看；每笔源事务的行变更先在 SQLite 整笔提交并
     提升 confirmed_lsn 水位线，提交成功后才 advance 复制槽。
  3. 任何一步崩溃：槽位点未前移，重启后旧消息被再次 peek 到，
     按 confirmed_lsn 水位线跳过/整笔重放，结果幂等、不漏不重。
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any

import psycopg2

from . import source
from .config import (
    ACK_DELAY, PG_DSN, POLL_INTERVAL, SLOT_NAME, SNAPSHOT_BATCH, SOURCE_TABLE,
)
from .storage import STATE_INITIALIZING, STATE_STREAMING, Store


class MirrorEngine:
    def __init__(self, store: Store):
        self.store = store
        self._thread: threading.Thread | None = None
        self._start_requested = threading.Event()
        self._stop = threading.Event()
        self._worker_busy = threading.Event()  # 工作线程是否在跑装载/续采

    # ---------- 生命周期 ----------

    def start_worker(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run, name="mirror-engine", daemon=True
        )
        self._thread.start()

    def request_initialization(self) -> str:
        state = self.store.get_state()
        if state["state"] == STATE_STREAMING:
            return "already_streaming"
        if self._start_requested.is_set() or self._worker_busy.is_set():
            return "already_running"
        self._start_requested.set()
        self.start_worker()
        return "started"

    def shutdown(self) -> None:
        self._stop.set()

    # ---------- 状态 ----------

    def status(self) -> dict[str, Any]:
        st = self.store.get_state()
        result: dict[str, Any] = {
            "state": st["state"],
            "slot_name": st["slot_name"],
            "snapshot_id": st["snapshot_id"],
            "confirmed_lsn": st["confirmed_lsn"],
            "confirmed_lsn_hex": _lsn_hex(st["confirmed_lsn"]),
            "last_txid": st["last_txid"],
            "last_change": st["last_change"],
            "applied_count": st["applied_count"],
            "last_error": st["last_error"],
            "updated_at": st["updated_at"],
            "worker_running": bool(
                self._thread and self._thread.is_alive()
            ),
            "start_requested": self._start_requested.is_set(),
            "ack_delay_seconds": ACK_DELAY,
        }
        # 源端视角的槽位点与源表行数（失败不影响页面）
        try:
            conn = source.connect(PG_DSN)
            try:
                result["slot_confirmed_lsn"] = source.slot_confirmed_lsn(
                    conn, SLOT_NAME
                )
                result["slot_confirmed_lsn_hex"] = _lsn_hex(
                    result["slot_confirmed_lsn"]
                )
                result["source_rows"] = source.source_count(conn, SOURCE_TABLE)
            finally:
                conn.close()
        except psycopg2.Error as exc:
            result["slot_confirmed_lsn"] = None
            result["source_rows"] = None
            result["source_error"] = str(exc).strip()[:300]
        return result

    # ---------- 主循环 ----------

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                st = self.store.get_state()
                if st["state"] == STATE_STREAMING:
                    self._stream_forever()  # 仅在停止/致命错误时返回
                elif self._start_requested.is_set():
                    self._start_requested.clear()
                    self._worker_busy.set()
                    try:
                        self._initialize()
                    finally:
                        self._worker_busy.clear()
                else:
                    time.sleep(0.2)
            except Exception as exc:  # 引擎线程绝不静默死掉
                self.store.mark_error(f"engine: {exc!r}")
                time.sleep(2)

    # ---------- 初始镜像 ----------

    def _initialize(self) -> None:
        conn = source.connect(PG_DSN)
        snapshot_conn = None
        try:
            # 未建完重做：若旧槽还在，删除后重建，保证快照与位点成对
            if source.slot_exists(conn, SLOT_NAME):
                source.drop_slot(conn, SLOT_NAME)
            conn.close()
            conn = None

            # 同一只读事务内建槽 + 导出快照，拿到成对的起始位点与快照
            snapshot_conn, slot_lsn, snapshot_id = \
                source.create_slot_with_snapshot(PG_DSN, SLOT_NAME)

            # 登记 INITIALIZING + 清空旧镜像；此刻起网页拒绝供数
            self.store.begin_initialization(SLOT_NAME, slot_lsn, snapshot_id)

            # 复用建槽事务的快照读全量；源库此刻可照常提交修改，
            # 那些修改不在快照里，但 WAL 已被槽保留，稍后续采补齐
            batch: list[tuple] = []
            for row in source.iter_snapshot_rows(
                snapshot_conn, SOURCE_TABLE, SNAPSHOT_BATCH
            ):
                batch.append(_normalize_row(row))
                if len(batch) >= SNAPSHOT_BATCH:
                    self.store.insert_snapshot_batch(batch)
                    batch.clear()
            if batch:
                self.store.insert_snapshot_batch(batch)

            # 镜像与起始位点都已持久化，切换 STREAMING 后才开始供数
            self.store.complete_initialization()
        finally:
            if snapshot_conn is not None:
                try:
                    # 释放建槽/装载事务；槽已持久化，不受影响
                    snapshot_conn.rollback()
                    snapshot_conn.close()
                except psycopg2.Error:
                    pass
            if conn is not None:
                conn.close()

    # ---------- 持续续采 ----------

    def _stream_forever(self) -> None:
        conn = source.connect(PG_DSN)
        try:
            while not self._stop.is_set():
                try:
                    if not source.slot_exists(conn, SLOT_NAME):
                        # 槽被外部删除：无法保证续采，回到未建态并停供数，
                        # 等待人工重新“开始建立镜像”
                        self.store.mark_error(
                            "replication slot missing; re-initialization required"
                        )
                        self._reset_to_initializing()
                        return
                    batches = source.peek_changes(conn, SLOT_NAME)
                except psycopg2.Error as exc:
                    # 连接中断等：重连后用持久化水位线重新 peek，不丢不重
                    self.store.mark_error(f"peek: {exc!r}")
                    try:
                        conn.close()
                    except psycopg2.Error:
                        pass
                    time.sleep(2)
                    conn = source.connect(PG_DSN)
                    continue
                if not batches:
                    time.sleep(POLL_INTERVAL)
                    continue

                watermark = self.store.get_state()["confirmed_lsn"]
                for batch in batches:
                    if self._stop.is_set():
                        return
                    if batch.lsn <= watermark:
                        # 崩溃重放：该事务已落盘，只需补确认源端位点
                        source.advance_slot(conn, SLOT_NAME, batch.lsn)
                        continue

                    summary = _summarize(batch.events)
                    try:
                        self.store.apply_transaction(
                            batch.events, batch.lsn, batch.xid, summary
                        )
                    except Exception as exc:
                        # 写入失败：水位线不前移、槽不前移，下轮重试
                        self.store.mark_error(f"apply: {exc!r}")
                        time.sleep(2)
                        break

                    # 演示窗口：已落盘但尚未确认复制进度
                    if ACK_DELAY > 0:
                        time.sleep(ACK_DELAY)

                    # 只有落盘成功后才确认；失败抛错则下轮整体重放
                    source.advance_slot(conn, SLOT_NAME, batch.lsn)
                    watermark = batch.lsn
        finally:
            try:
                conn.close()
            except psycopg2.Error:
                pass

    def _reset_to_initializing(self) -> None:
        self.store.begin_initialization(SLOT_NAME, 0, None)
        self.store.mark_error(
            "replication slot missing; re-initialization required"
        )


def _normalize_row(row: tuple) -> tuple:
    quake_no, origin_utc, longitude, latitude, magnitude = row
    if isinstance(origin_utc, datetime):
        origin_utc = origin_utc.isoformat(sep=" ")
    return (
        str(quake_no),
        str(origin_utc),
        float(longitude),
        float(latitude),
        float(magnitude),
    )


def _summarize(events: list[dict[str, Any]]) -> str:
    counts = {"insert": 0, "update": 0, "delete": 0}
    for ev in events:
        if ev.get("kind") in counts:
            counts[ev["kind"]] += 1
    parts = [f"{k}={v}" for k, v in counts.items() if v]
    return " ".join(parts) if parts else "empty"


def _lsn_hex(lsn: int | None) -> str | None:
    if lsn is None:
        return None
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:08X}"
