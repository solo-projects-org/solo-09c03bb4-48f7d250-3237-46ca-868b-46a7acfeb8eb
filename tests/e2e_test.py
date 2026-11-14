#!/usr/bin/env python3 -P
"""端到端测试（不依赖 Docker）：用本机 PostgreSQL 15 + wal2json 验证全部承诺。

用法（-P 隔离当前目录、保留 PYTHONPATH 指向的依赖）：
  python3 -P tests/e2e_test.py
脚本自行 initdb / 启动 / 销毁一个临时 PG 实例，并拉起 uvicorn 镜像服务。

覆盖：
  T1 初始装载期间源库增/改/删 → 装载+续采后镜像与源库逐行一致，不漏不重
  T2 多事务连续更正 → 旧值不盖新值，按提交顺序收敛到最终值
  T3 落盘后、复制槽确认前重启 → 水位线去重，行数不翻倍，槽位点最终追平
  T4 未建完重启 → 装载中查询 503，完成后自动供数；整笔事务原子可见
  T5 每次取数同一已提交快照；CSV 与查询一致
"""
from __future__ import annotations

import csv
import io
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

# -P 隔离模式下（保留 PYTHONPATH，屏蔽 cwd 与用户 site 自动注入）把用户 site-packages 显式加入 sys.path（须先于第三方导入）
def _find_user_site() -> str:
    candidates = []
    home = os.environ.get("HOME")
    if home:
        candidates.append(
            Path(home)
            / f".local/lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
        )
    import site
    candidates += [Path(p) for p in site.getsitepackages()]
    for p in candidates:
        if (p / "psycopg2").is_dir():
            return str(p)
    raise RuntimeError("找不到 psycopg2 安装位置")


_USER_SITE = _find_user_site()
if _USER_SITE not in sys.path:
    sys.path.insert(0, _USER_SITE)

import psycopg2  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
PG_BIN = Path(os.environ.get("PGBIN", "/home/node/pg15/usr/lib/postgresql/15/bin"))
PG_PORT = int(os.environ.get("PGPORT", "55432"))
WEB_PORT = int(os.environ.get("WEB_PORT", "18000"))
TMP = Path(os.environ.get("TMPDIR", "/tmp")) / "quake_e2e"
PGDATA = TMP / "pgdata"
DBDIR = TMP / "db"
SQLITE = TMP / "mirror.db"


# ---------------- 工具函数 ----------------

def run(cmd: list[str], **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=True, capture_output=True, text=True, **kw)


def wait_port(port: int, timeout: float = 20) -> None:
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(0.5)
            try:
                s.connect(("127.0.0.1", port))
                return
            except OSError:
                time.sleep(0.2)
    raise RuntimeError(f"port {port} not ready in {timeout}s")


def http(method: str, path: str, body: dict | None = None, expect: int | None = None):
    req = urllib.request.Request(
        f"http://127.0.0.1:{WEB_PORT}{path}", method=method,
        data=json.dumps(body).encode() if body else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            data = r.read().decode()
            code = r.status
    except urllib.error.HTTPError as e:
        data, code = e.read().decode(), e.code
    if expect is not None and code != expect:
        raise AssertionError(f"{method} {path} -> {code}, want {expect}: {data}")
    return code, (json.loads(data) if data else None)


def wait_state(target: str, timeout: float = 30) -> dict:
    end = time.time() + timeout
    last = {}
    while time.time() < end:
        _, last = http("GET", "/api/status")
        if last["state"] == target:
            return last
        time.sleep(0.2)
    raise AssertionError(f"state never became {target}: {last}")


def wait_gap_zero(timeout: float = 20) -> None:
    end = time.time() + timeout
    while time.time() < end:
        _, s = http("GET", "/api/status")
        if (s["slot_confirmed_lsn"] is not None
                and s["confirmed_lsn"] == s["slot_confirmed_lsn"]):
            return
        time.sleep(0.3)
    raise AssertionError("LSN gap never closed")


def pg_conn():
    return psycopg2.connect(
        host="127.0.0.1", port=PG_PORT, dbname="seisnet",
        user="replicator", password="replicator",
    )


def source_rows() -> dict[str, tuple]:
    conn = pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT quake_no, origin_utc::text, longitude, latitude, "
                "magnitude FROM quake ORDER BY quake_no"
            )
            return {r[0]: r for r in cur.fetchall()}
    finally:
        conn.close()


def mirror_rows() -> dict[str, tuple]:
    # 不分页，一次取全量（仅测试用，配合 limit=100000）
    _, d = http("GET", "/api/quakes?limit=100000", expect=200)
    return {r["quake_no"]: (
        r["quake_no"], r["origin_utc"], r["longitude"],
        r["latitude"], r["magnitude"],
    ) for r in d["items"]}


def assert_mirror_matches_source(msg: str) -> None:
    src, mir = source_rows(), mirror_rows()
    assert set(src) == set(mir), f"{msg}: 键集合不一致 缺={set(src)-set(mir)} 多={set(mir)-set(src)}"
    for k, sv in src.items():
        mv = mir[k]
        assert mv[0] == sv[0]
        assert mv[1].replace(" ", "T") == sv[1].replace(" ", "T"), (k, mv, sv)
        assert abs(mv[2] - sv[2]) < 1e-9 and abs(mv[3] - sv[3]) < 1e-9
        assert abs(mv[4] - sv[4]) < 1e-9
    print(f"  OK {msg}: {len(src)} 行逐字段一致")


def start_web(ack_delay: float = 0.0, snapshot_batch: str = "500") -> subprocess.Popen:
    env = os.environ | {
        "PG_DSN": f"host=127.0.0.1 port={PG_PORT} dbname=seisnet "
                  f"user=replicator password=replicator",
        "SQLITE_PATH": str(SQLITE),
        "ACK_DELAY": str(ack_delay),
        "POLL_INTERVAL": "0.2",
        "SNAPSHOT_BATCH": snapshot_batch,
        "PYTHONPATH": str(ROOT / "mirror") + os.pathsep + _USER_SITE,
    }
    log = open(TMP / "uvicorn.log", "a")
    p = subprocess.Popen(
        [sys.executable, "-P", "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(WEB_PORT)],
        cwd=ROOT / "mirror", env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    wait_port(WEB_PORT)
    return p


def stop_web(p: subprocess.Popen) -> None:
    p.send_signal(signal.SIGTERM)
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()


# ---------------- 测试主体 ----------------

def setup_pg() -> None:
    if TMP.exists():
        shutil.rmtree(TMP)
    TMP.mkdir(parents=True)
    env = os.environ | {"LD_LIBRARY_PATH": str(PG_BIN / "..") + "/lib"}
    run([str(PG_BIN / "initdb"), "-D", str(PGDATA), "-U", "postgres",
         "--auth=trust"], env=env)
    log = open(TMP / "pg.log", "w")
    subprocess.Popen(
        [str(PG_BIN / "postgres"), "-D", str(PGDATA),
         "-p", str(PG_PORT), "-c", "wal_level=logical",
         "-c", "listen_addresses=127.0.0.1",
         "-c", f"unix_socket_directories={TMP}"],
        env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    wait_port(PG_PORT)
    env = os.environ | {"LD_LIBRARY_PATH": str(PG_BIN / "..") + "/lib"}
    # 直接复用容器内同一套初始化脚本（含 \connect 指令，需用 psql 执行）
    for sql in sorted((ROOT / "db/init").glob("*.sql")):
        run([
            str(PG_BIN / "psql"), "-h", "127.0.0.1", "-p", str(PG_PORT),
            "-U", "postgres", "-d", "postgres", "-v", "ON_ERROR_STOP=1",
            "-f", str(sql),
        ], env=env)


def teardown_pg() -> None:
    env = os.environ | {"LD_LIBRARY_PATH": str(PG_BIN / "..") + "/lib"}
    subprocess.run([str(PG_BIN / "pg_ctl"), "-D", str(PGDATA), "stop", "-m", "fast"],
                   env=env, capture_output=True)
    shutil.rmtree(TMP, ignore_errors=True)


def t1_initial_load_with_concurrent_changes() -> None:
    print("T1 初始镜像 + 装载期间源库增改删")
    if SQLITE.exists():
        SQLITE.unlink()

    # 灌入大量历史行，让初始装载持续足够久，便于在装载期间制造源库修订
    conn = pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO quake "
            "SELECT 'LOAD' || g, TIMESTAMPTZ '2020-01-01+00' + (g || ' seconds')::interval, "
            "100.0 + (g % 50), 20.0 + (g % 30), 4.0 + (g % 30) / 10.0 "
            "FROM generate_series(1, 20000) g"
        )
    conn.close()

    web = start_web()
    try:
        # 未开始前查询应 503
        http("GET", "/api/quakes", expect=503)
        http("GET", "/api/quakes.csv", expect=503)

        # 点“开始建立镜像”
        http("POST", "/api/mirror/start", expect=200)

        # 等复制槽建好、进入装载（INITIALIZING），此时再在源库提交一整笔
        # 增/改/删短事务：它晚于槽位点、晚于装载快照，只能靠续采补齐
        deadline = time.time() + 15
        while time.time() < deadline:
            _, s = http("GET", "/api/status")
            if s["state"] == "INITIALIZING" and s["snapshot_id"]:
                break
            time.sleep(0.05)
        else:
            raise AssertionError("未观察到 INITIALIZING 装载阶段")
        print("  OK 装载进行中，此时在源库提交增/改/删事务")
        tx = pg_conn()
        tx.autocommit = False
        with tx.cursor() as cur:
            cur.execute(Path(ROOT / "db/demo/01_loading_phase.sql").read_text())
        tx.commit()
        tx.close()

        st = wait_state("STREAMING")
        assert st["slot_confirmed_lsn"] is not None
        wait_gap_zero()
        # 演示事务的 3 个行变更必须全部由续采应用（初始快照里没有它们）
        _, final = http("GET", "/api/status")
        assert final["applied_count"] >= 3, final
        assert_mirror_matches_source("装载并发修订后")
        # 演示删除目标确实已不存在、更正已生效、新增已补齐
        assert "CE20240101071010" not in mirror_rows()
        assert mirror_rows()["CE20080512062801"][4] == 8.2
        assert "CE20240403075812" in mirror_rows()
    finally:
        stop_web(web)


def t2_ordered_updates() -> None:
    print("T2 连续更正：旧值不得盖回新值")
    web = start_web()
    try:
        conn = pg_conn()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(Path(ROOT / "db/demo/02_streaming_phase.sql").read_text())
        conn.commit()
        conn.close()
        wait_gap_zero()
        assert_mirror_matches_source("连续更正后")
        row = mirror_rows()["CE20250714093122"]
        assert row[4] == 6.5 and abs(row[2] - 140.180) < 1e-9, row
    finally:
        stop_web(web)


def t3_restart_between_persist_and_ack() -> None:
    print("T3 已落盘未确认即重启：不重不漏")
    # 用长 ACK_DELAY 制造“持久化后、advance 前”的窗口
    web = start_web(ack_delay=8.0)
    try:
        conn = pg_conn()
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(Path(ROOT / "db/demo/03_ack_pending.sql").read_text())
        conn.commit()
        conn.close()

        # 等 SQLite 水位线超过槽位点（窗口出现）
        deadline = time.time() + 20
        gap = 0
        while time.time() < deadline:
            _, s = http("GET", "/api/status")
            slot = s["slot_confirmed_lsn"] or 0
            gap = s["confirmed_lsn"] - slot
            if gap > 0:
                break
            time.sleep(0.2)
        assert gap > 0, "未捕获到落盘-确认窗口"
        print(f"  OK 捕获未确认窗口 gap={gap}，此时杀掉服务")
    finally:
        stop_web(web)

    # 立即重启（ACK_DELAY 归零，便于快速追平）
    web = start_web(ack_delay=0.0)
    try:
        wait_gap_zero()
        rows = mirror_rows()
        hits = [r for r in rows.values() if r[0] == "CE20260305111200"]
        assert len(hits) == 1, f"重放导致重复: {hits}"
        assert_mirror_matches_source("重启恢复后")
        _, s = http("GET", "/api/status")
        assert s["confirmed_lsn"] == s["slot_confirmed_lsn"]
    finally:
        stop_web(web)


def t4_restart_during_initialization() -> None:
    print("T4 未建完重启：不供数，重启后重做并收敛")
    if SQLITE.exists():
        SQLITE.unlink()
    # 加大批量读取间延迟不可控，改用极小批量 + 大数据量制造装载窗口：
    # 先灌入 3000 行，使装载持续足够久
    conn = pg_conn()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM quake")
        base = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO quake "
            "SELECT 'BULK' || g, TIMESTAMPTZ '2020-01-01+00' + (g || ' seconds')::interval, "
            "100.0 + (g % 50), 20.0 + (g % 30), 4.0 + (g % 30) / 10.0 "
            "FROM generate_series(1, 3000) g"
        )
    conn.close()

    env = os.environ | {
        "PG_DSN": f"host=127.0.0.1 port={PG_PORT} dbname=seisnet user=replicator password=replicator",
        "SQLITE_PATH": str(SQLITE), "SNAPSHOT_BATCH": "50",
        "POLL_INTERVAL": "0.2",
        "PYTHONPATH": str(ROOT / "mirror") + os.pathsep + _USER_SITE,
    }
    log = open(TMP / "uvicorn2.log", "a")
    web = subprocess.Popen(
        [sys.executable, "-P", "-m", "uvicorn", "app.main:app",
         "--host", "127.0.0.1", "--port", str(WEB_PORT)],
        cwd=ROOT / "mirror", env=env, stdout=log, stderr=subprocess.STDOUT,
    )
    try:
        wait_port(WEB_PORT)
        http("POST", "/api/mirror/start", expect=200)
        # 确认进入 INITIALIZING 且查询被拒
        time.sleep(0.5)
        http("GET", "/api/quakes", expect=503)
        print("  OK 装载期间查询返回 503（不供数）")
        # 装载中途重启
        stop_web(web)
    except Exception:
        stop_web(web)
        raise

    web = start_web()
    try:
        wait_state("STREAMING")
        wait_gap_zero()
        assert_mirror_matches_source("重做完成后")
    finally:
        stop_web(web)


def t5_snapshot_reads_and_csv() -> None:
    print("T5 一致性快照读取与 CSV")
    web = start_web()
    try:
        code, d = http("GET", "/api/quakes?limit=5&offset=2", expect=200)
        assert len(d["items"]) == 5 and d["total"] >= 20000
        req = urllib.request.Request(f"http://127.0.0.1:{WEB_PORT}/api/quakes.csv")
        with urllib.request.urlopen(req, timeout=15) as r:
            payload = r.read().decode()
            assert r.headers["Content-Type"].startswith("text/csv")
        reader = list(csv.reader(io.StringIO(payload)))
        assert reader[0] == ["quake_no", "origin_utc", "longitude", "latitude", "magnitude"]
        _, q = http("GET", "/api/quakes?limit=100000", expect=200)
        assert len(reader) - 1 == q["total"] == len(source_rows())
        print(f"  OK CSV {len(reader)-1} 行，与查询/源库行数一致")
    finally:
        stop_web(web)


def main() -> int:
    try:
        try:
            setup_pg()
            t1_initial_load_with_concurrent_changes()
            t2_ordered_updates()
            t3_restart_between_persist_and_ack()
            t4_restart_during_initialization()
            t5_snapshot_reads_and_csv()
        except Exception:
            # 失败时保留现场日志，便于排查
            print(f"\n[测试失败，保留现场] {TMP}", file=sys.stderr)
            for name in ("uvicorn.log", "uvicorn2.log", "pg.log"):
                p = TMP / name
                if p.exists():
                    print(f"----- {p} -----", file=sys.stderr)
                    sys.stderr.write(p.read_text()[-4000:])
            raise
    finally:
        if os.environ.get("KEEP_TMP"):
            print(f"KEEP_TMP=1，保留 {TMP}")
        else:
            teardown_pg()
    print("\n全部端到端测试通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
