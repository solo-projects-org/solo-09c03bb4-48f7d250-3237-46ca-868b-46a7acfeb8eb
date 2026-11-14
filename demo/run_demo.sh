#!/usr/bin/env bash
# 端到端演示编排（面向已启动的 compose 环境）：
#   ./demo/run_demo.sh loading    初始装载期间提交增/改/删
#   ./demo/run_demo.sh streaming  镜像完成后的连续修订
#   ./demo/run_demo.sh ack        保存后未确认复制进度时重启（需 ACK_DELAY>0）
set -euo pipefail

STEP="${1:-}"
PSQL=(docker compose exec -T postgres psql -U postgres -d seisnet -v ON_ERROR_STOP=1)
HTTP="${HTTP:-http://localhost:8000}"

case "$STEP" in
  loading)
    echo "[1/3] 初始装载期间在源库提交一整笔增/改/删事务 ..."
    "${PSQL[@]}" -f /demo/01_loading_phase.sql
    echo "等待镜像追平 ..."
    for _ in $(seq 1 40); do
      gap=$(curl -fsS "$HTTP/api/status" | python3 -c \
        'import json,sys; s=json.load(sys.stdin); print((s["confirmed_lsn"])-(s["slot_confirmed_lsn"] or 0))')
      [[ "$gap" == "0" ]] && break
      sleep 0.5
    done
    echo "镜像中的汶川震级（应为 8.2，演示修订值）："
    curl -fsS "$HTTP/api/quakes?keyword=CE2008" | python3 -m json.tool
    ;;
  streaming)
    echo "[2/3] 连续三笔事务：新增 + 两次更正 ..."
    "${PSQL[@]}" -f /demo/02_streaming_phase.sql
    sleep 3
    echo "最终震级应为 6.5（旧值不得盖回新值）："
    curl -fsS "$HTTP/api/quakes?keyword=CE2025" | python3 -m json.tool
    ;;
  ack)
    echo "[3/3] 落盘后、确认复制槽前重启 mirror ..."
    "${PSQL[@]}" -f /demo/03_ack_pending.sql
    echo "等待 SQLite 水位线先于槽位点前移 ..."
    for _ in $(seq 1 40); do
      gap=$(curl -fsS "$HTTP/api/status" | python3 -c \
        'import json,sys; s=json.load(sys.stdin); print((s["confirmed_lsn"])-(s["slot_confirmed_lsn"] or 0))')
      [[ "$gap" -gt 0 ]] && { echo "捕获到未确认窗口 gap=$gap，立即重启"; break; }
      sleep 0.5
    done
    docker compose restart mirror >/dev/null
    echo "等待恢复 ..."
    for _ in $(seq 1 40); do
      sleep 1
      gap=$(curl -fsS "$HTTP/api/status" | python3 -c \
        'import json,sys; s=json.load(sys.stdin); print((s["confirmed_lsn"])-(s["slot_confirmed_lsn"] or 0))' 2>/dev/null || echo x)
      [[ "$gap" == "0" ]] && break
    done
    echo "CE20260305111200 必须恰好一条："
    curl -fsS "$HTTP/api/quakes?keyword=CE2026" | python3 -m json.tool
    ;;
  *)
    grep '^#' "$0" | sed 's/^# \{0,1\}//'
    exit 1
    ;;
esac
