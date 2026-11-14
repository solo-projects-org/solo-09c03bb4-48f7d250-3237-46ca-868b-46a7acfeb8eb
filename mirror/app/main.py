"""FastAPI 网页：采集状态、震例查询、当前目录 CSV 下载。

所有数据读取都经过 Store.snapshot()（显式只读事务），
一次请求内的全部 SELECT 只读同一个已提交状态。
"""
from __future__ import annotations

import csv
import io
import os
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

from .config import SQLITE_PATH
from .engine import MirrorEngine
from .storage import STATE_INITIALIZING, Store

app = FastAPI(title="震例目录逻辑复制镜像", version="1.0")

_store = Store(SQLITE_PATH)
_engine = MirrorEngine(_store)

_STATIC_DIR = Path(__file__).parent / "static"


@app.on_event("startup")
def _startup() -> None:
    # 已建镜像（STREAMING）：工作线程自动续采；
    # 未建完（INITIALIZING 且已有槽）：自动重新开始，重做整个初始镜像；
    # 全新库：等待点击“开始建立镜像”。
    state = _store.get_state()
    if state["state"] == "STREAMING":
        _engine.start_worker()
    elif state["state"] == STATE_INITIALIZING and state["slot_name"]:
        _engine.request_initialization()


@app.on_event("shutdown")
def _shutdown() -> None:
    _engine.shutdown()


@app.get("/")
def index() -> FileResponse:
    return FileResponse(_STATIC_DIR / "index.html")


@app.post("/api/mirror/start")
def start_mirror() -> JSONResponse:
    action = _engine.request_initialization()
    return JSONResponse({"action": action, "status": _engine.status()})


@app.get("/api/status")
def status() -> dict:
    return _engine.status()


def _ensure_serving() -> None:
    # 未建完不供数
    if _store.get_state()["state"] == STATE_INITIALIZING:
        raise HTTPException(
            status_code=503,
            detail="初始镜像尚未完成，暂不供数；请在状态页查看装载进度",
        )


@app.get("/api/quakes")
def list_quakes(
    keyword: str | None = Query(None, description="震例号模糊匹配"),
    min_mag: float | None = Query(None, ge=0, le=12),
    limit: int = Query(50, ge=1, le=100000),
    offset: int = Query(0, ge=0),
) -> dict:
    _ensure_serving()
    rows, total = _store.query_quakes(keyword, min_mag, limit, offset)
    return {"total": total, "limit": limit, "offset": offset, "items": rows}


@app.get("/api/quakes.csv")
def download_csv() -> StreamingResponse:
    _ensure_serving()
    rows = _store.csv_rows()  # 同一快照内取出，导出过程不再读库

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(
        ["quake_no", "origin_utc", "longitude", "latitude", "magnitude"]
    )
    writer.writerows(rows)
    buf.seek(0)
    filename = f"quakes_{os.getpid()}.csv"
    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
        },
    )
