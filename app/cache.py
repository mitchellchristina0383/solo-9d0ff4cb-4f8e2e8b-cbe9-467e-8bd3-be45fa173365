"""路由查询缓存实例（cache）。

两个实例共享同一个 SQLite 数据库（docker volume / 本地共享目录），通过
SQLite 的行级事务协调"跨实例并发未命中合并（singleflight）"。

关键保证：
1. 修订单调：每次查询先向配置源确认当前修订；新修订提交后发起的查询
   不可能返回旧修订的节点。
2. 同修订并发未命中合并：同一 (service, revision) 同一时刻只允许一个实例
   回源，其余实例在库内等待结果（snapshot 或 failure 记录）。
3. 租约接管：回源持有者周期性续租；持有者超时或进程退出后，其他实例
   可在租约过期后接管回源。
4. 慢回源跨修订不写脏数据：回源返回后再次核对"该修订仍是当前修订"，
   过期修订只标记为 stale，绝不作为有效缓存。
5. 二次核验失败不等同于"修订未变"：核验时源失联时，刚拉取的节点只落为
   stale，绝不标记 valid；本次请求与等待同次回源的请求都不得复用它，
   只能用此前已确认（valid）的快照降级服务，没有则明确不可用。
6. 回源失败明确 503：失败记录带冷却期，冷却期内快速失败；旧快照
   仍可通过 /snapshots 与故障响应中的 last_good 排障。
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
import time
import uuid
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_PATH = os.environ.get("CACHE_DB", "/data/cache.db")
SOURCE_BASE = os.environ.get("SOURCE_BASE", "http://source:8080")
INSTANCE_ID = os.environ.get("INSTANCE_ID", f"cache-{uuid.uuid4().hex[:8]}")
HTTP_TIMEOUT = float(os.environ.get("FETCH_TIMEOUT", "6.0"))
LEASE_TTL = float(os.environ.get("LEASE_TTL", "2.0"))
HEARTBEAT_INTERVAL = float(os.environ.get("HEARTBEAT_INTERVAL", "0.7"))
FAILURE_COOLDOWN = float(os.environ.get("FAILURE_COOLDOWN", "3.0"))
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "0.15"))


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = _connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS snapshots (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            nodes       TEXT NOT NULL,
            status      TEXT NOT NULL,            -- valid | stale
            source_published_at REAL,
            cached_at   REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inflight (
            service     TEXT NOT NULL,
            revision    INTEGER NOT NULL,
            leader      TEXT NOT NULL,
            lease_expires_at REAL NOT NULL,
            started_at  REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS failures (
            service    TEXT NOT NULL,
            revision   INTEGER NOT NULL,
            error      TEXT NOT NULL,
            kind       TEXT NOT NULL DEFAULT 'fetch',  -- fetch | verify
            cooldown_until REAL NOT NULL,
            updated_at REAL NOT NULL,
            PRIMARY KEY (service, revision)
        )
        """
    )
    # 兼容旧库：早期版本没有 kind 列，默认按回源失败处理。
    columns = {row["name"] for row in conn.execute("PRAGMA table_info(failures)").fetchall()}
    if "kind" not in columns:
        try:
            conn.execute(
                "ALTER TABLE failures ADD COLUMN kind TEXT NOT NULL DEFAULT 'fetch'"
            )
        except sqlite3.OperationalError as exc:  # 两个实例同时迁移
            if "duplicate column name" not in str(exc).lower():
                raise
    conn.close()


# ---------------------------------------------------------------------------
# 配置源客户端
# ---------------------------------------------------------------------------
class SourceUnavailable(Exception):
    pass


async def _source_current(service: str, verify: bool = False) -> int:
    """查询当前修订。

    verify=True 表示回源后的二次核验；配置源可对该阶段单独注入失联故障
    （X-Cache-Phase: verify），以便模拟"拉取成功后核验时源不可达"。
    """
    headers = {"X-Cache-Phase": "verify"} if verify else None
    try:
        async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
            r = await c.get(f"/services/{service}/current", headers=headers)
            if r.status_code == 404:
                raise HTTPException(status_code=404, detail=f"service '{service}' not found")
            r.raise_for_status()
            return int(r.json()["revision"])
    except HTTPException:
        raise
    except Exception as exc:  # 连不上 / 超时 / 5xx
        raise SourceUnavailable(f"current revision lookup failed: {exc}") from exc


async def _source_fetch(service: str, revision: int) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=SOURCE_BASE, timeout=HTTP_TIMEOUT) as c:
        r = await c.get(f"/services/{service}/revisions/{revision}")
        if r.status_code >= 400:
            raise SourceUnavailable(f"origin returned HTTP {r.status_code}: {r.text[:200]}")
        return r.json()


# ---------------------------------------------------------------------------
# DB 辅助
# ---------------------------------------------------------------------------
def _get_snapshot(conn: sqlite3.Connection, service: str, revision: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND revision=?", (service, revision)
    ).fetchone()


def _last_good(conn: sqlite3.Connection, service: str) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT * FROM snapshots WHERE service=? AND status='valid' "
        "ORDER BY revision DESC LIMIT 1",
        (service,),
    ).fetchone()
    if row is None:
        return None
    return {
        "service": row["service"],
        "revision": row["revision"],
        "nodes": json.loads(row["nodes"]),
        "cached_at": row["cached_at"],
    }


def _degraded_or_unavailable(
    service: str,
    reason: str,
    kind: str,
    last_good: dict[str, Any] | None,
) -> dict[str, Any]:
    """回源结果无法确认时：仅用"此前已确认"的快照降级服务，否则明确不可用。

    刚拉取但未能二次核验的节点不在此列（它只会以 stale 落库供排障）。
    """
    if last_good is None:
        raise HTTPException(
            status_code=503,
            detail={
                "error": "origin unavailable and no confirmed local snapshot",
                "service": service,
                "reason": reason,
                "failure_kind": kind,
                "last_good": None,
            },
        )
    return {
        "service": service,
        "revision": last_good["revision"],
        "nodes": last_good["nodes"],
        "served_by": INSTANCE_ID,
        "from_cache": True,
        "degraded": True,
        "degraded_reason": reason,
        "cached_at": last_good["cached_at"],
        "snapshot_status": "valid",
    }


def _try_acquire_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    """尝试成为 (service, revision) 的回源 leader；已有未过期租约则失败。"""
    now = time.time()
    conn.execute("BEGIN IMMEDIATE")
    try:
        row = conn.execute(
            "SELECT leader, lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        if row is not None and row["lease_expires_at"] > now:
            conn.commit()
            return False
        # 无记录，或租约已过期 -> 接管
        conn.execute(
            "INSERT INTO inflight(service, revision, leader, lease_expires_at, started_at) "
            "VALUES(?,?,?,?,?) "
            "ON CONFLICT(service, revision) DO UPDATE SET "
            "leader=excluded.leader, lease_expires_at=excluded.lease_expires_at, "
            "started_at=excluded.started_at",
            (service, revision, INSTANCE_ID, now + LEASE_TTL, now),
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        raise


def _renew_lease(conn: sqlite3.Connection, service: str, revision: int) -> bool:
    now = time.time()
    row = conn.execute(
        "SELECT leader FROM inflight WHERE service=? AND revision=?", (service, revision)
    ).fetchone()
    if row is None or row["leader"] != INSTANCE_ID:
        return False
    conn.execute(
        "UPDATE inflight SET lease_expires_at=? WHERE service=? AND revision=? AND leader=?",
        (now + LEASE_TTL, service, revision, INSTANCE_ID),
    )
    return True


def _release_lease(conn: sqlite3.Connection, service: str, revision: int) -> None:
    conn.execute(
        "DELETE FROM inflight WHERE service=? AND revision=? AND leader=?",
        (service, revision, INSTANCE_ID),
    )


def _record_failure(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    error: str,
    kind: str = "fetch",
    cooldown: float | None = None,
) -> None:
    now = time.time()
    conn.execute(
        "INSERT INTO failures(service, revision, error, kind, cooldown_until, updated_at) "
        "VALUES(?,?,?,?,?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET error=excluded.error, "
        "kind=excluded.kind, cooldown_until=excluded.cooldown_until, "
        "updated_at=excluded.updated_at",
        (
            service,
            revision,
            error[:500],
            kind,
            now + (FAILURE_COOLDOWN if cooldown is None else cooldown),
            now,
        ),
    )


def _clear_failure(conn: sqlite3.Connection, service: str, revision: int) -> None:
    conn.execute(
        "DELETE FROM failures WHERE service=? AND revision=?", (service, revision)
    )


def _save_snapshot(
    conn: sqlite3.Connection,
    service: str,
    revision: int,
    nodes: list[str],
    source_published_at: float | None,
    still_current: bool,
) -> str:
    """写入快照；仅当修订仍为当前修订时标记 valid，否则标记 stale。

    未通过核验的写入绝不能把已确认（valid）的同修订快照降级：
    同一修订的节点是不可变的，既有的 valid 记录保留原样。
    """
    now = time.time()
    if still_current:
        conn.execute(
            "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
            "VALUES(?,?,?,'valid',?,?) "
            "ON CONFLICT(service, revision) DO UPDATE SET nodes=excluded.nodes, "
            "status='valid', source_published_at=excluded.source_published_at, "
            "cached_at=excluded.cached_at",
            (service, revision, json.dumps(nodes), source_published_at, now),
        )
        return "valid"
    conn.execute(
        "INSERT INTO snapshots(service, revision, nodes, status, source_published_at, cached_at) "
        "VALUES(?,?,?,'stale',?,?) "
        "ON CONFLICT(service, revision) DO UPDATE SET "
        "nodes=CASE WHEN snapshots.status='valid' THEN snapshots.nodes ELSE excluded.nodes END, "
        "source_published_at=CASE WHEN snapshots.status='valid' "
        "THEN snapshots.source_published_at ELSE excluded.source_published_at END, "
        "cached_at=CASE WHEN snapshots.status='valid' "
        "THEN snapshots.cached_at ELSE excluded.cached_at END",
        (service, revision, json.dumps(nodes), source_published_at, now),
    )
    row = _get_snapshot(conn, service, revision)
    return row["status"] if row is not None else "stale"


def _finish_failed_attempt(
    service: str, revision: int, error: str, kind: str = "fetch"
) -> None:
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _record_failure(conn, service, revision, error, kind=kind)
        _release_lease(conn, service, revision)
        conn.commit()
    finally:
        conn.close()


def _finish_with_snapshot(
    service: str,
    revision: int,
    nodes: list[str],
    source_published_at: float | None,
    still_current: bool,
    failure: tuple[str, str, float] | None = None,
) -> str:
    """原子提交：快照 + （可选）失败标记 + 释放租约。

    快照与租约在同一事务里落库，等待者不会看到"无租约又无结论"的中间态。
    failure = (reason, kind, cooldown_seconds)。
    """
    conn = _connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        status = _save_snapshot(
            conn, service, revision, nodes, source_published_at, still_current
        )
        if failure is None:
            _clear_failure(conn, service, revision)
        else:
            reason, kind, cooldown = failure
            _record_failure(conn, service, revision, reason, kind=kind, cooldown=cooldown)
        _release_lease(conn, service, revision)
        conn.commit()
        return status
    finally:
        conn.close()


def _read_last_good(service: str) -> dict[str, Any] | None:
    conn = _connect()
    try:
        return _last_good(conn, service)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 核心：singleflight 查询
# ---------------------------------------------------------------------------
async def _query(service: str) -> dict[str, Any]:
    # 第一步：确认当前修订。配置源整体不可达时降级：
    # 仅用此前已确认的快照降级服务（标记 degraded），没有则明确 503。
    try:
        current = await _source_current(service)
    except SourceUnavailable as exc:
        good = _read_last_good(service)
        return _degraded_or_unavailable(
            service, str(exc), "current", good
        )

    conn = _connect()
    try:
        snap = _get_snapshot(conn, service, current)
        if snap is not None and snap["status"] == "valid":
            return {
                "service": service,
                "revision": current,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "cached_at": snap["cached_at"],
            }

        # 失败冷却期内快速明确失败（旧快照仍可单独查询）。
        # 若仍有 leader 持有效租约（其回源可能仍在途），不提前失败，转入等待/接管逻辑。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, current)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, current),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        # 核验失败（kind=verify）不进入冷却快失败：它不该阻碍配置源恢复后的
        # 重新回源；新请求直接去抢租约重新拉取。
        if (
            fail is not None
            and fail["kind"] != "verify"
            and fail["cooldown_until"] > time.time()
            and not lease_alive
        ):
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable (failure cooldown)",
                    "reason": fail["error"],
                    "retry_after": round(fail["cooldown_until"] - time.time(), 2),
                    "last_good": _last_good(conn, service),
                },
            )

        is_leader = _try_acquire_lease(conn, service, current)

        if is_leader:
            return await _run_as_leader(service, current)
        return await _wait_as_follower(conn, service, current)
    finally:
        conn.close()


async def _run_as_leader(service: str, revision: int) -> dict[str, Any]:
    # 心跳覆盖"回源 + 二次核验 + 提交"全过程：核验期间租约同样不得过期。
    heartbeat_task = asyncio.create_task(_heartbeat(service, revision))
    try:
        try:
            # 硬时限：回源不能无限占用回源资格；超时后停心跳、放租约，
            # 其他实例可在租约过期后接管（即使本进程仍然存活）。
            payload = await asyncio.wait_for(
                _source_fetch(service, revision), timeout=HTTP_TIMEOUT
            )
        except asyncio.TimeoutError:
            reason = f"origin fetch timeout after {HTTP_TIMEOUT}s"
            _finish_failed_attempt(service, revision, reason, "fetch")
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable",
                    "reason": reason,
                    "service": service,
                    "requested_revision": revision,
                    "last_good": _read_last_good(service),
                },
            )
        except SourceUnavailable as exc:
            _finish_failed_attempt(service, revision, str(exc), "fetch")
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "origin unavailable",
                    "reason": str(exc),
                    "service": service,
                    "requested_revision": revision,
                    "last_good": _read_last_good(service),
                },
            )
        except Exception as exc:
            _finish_failed_attempt(service, revision, f"unexpected: {exc!r}", "fetch")
            raise HTTPException(
                status_code=503,
                detail={"error": "origin unavailable", "reason": repr(exc),
                        "service": service, "requested_revision": revision,
                        "last_good": _read_last_good(service)},
            )

        # 回源成功：二次核对"该修订仍是当前修订"，防止慢回源把旧响应写成有效缓存。
        # 核验本身失败（源失联）绝不能等价于"修订未变"：未核验的结果不可确认，
        # 只能落为 stale，请求退回到此前已确认的快照（降级）或明确不可用。
        latest: int | None = None
        verify_error: str | None = None
        try:
            latest = await asyncio.wait_for(
                _source_current(service, verify=True), timeout=HTTP_TIMEOUT
            )
        except asyncio.TimeoutError:
            verify_error = (
                f"post-fetch revision verification timed out after {HTTP_TIMEOUT}s"
            )
        except SourceUnavailable as exc:
            verify_error = f"post-fetch revision verification failed: {exc}"
        except HTTPException as exc:
            # 源端已不认识该服务/修订：同样无法确认本次结果
            verify_error = f"post-fetch revision verification failed: {exc.detail}"

        if verify_error is not None:
            # 刚拉取的节点只落 stale 供排障，绝不作为现役。
            # failure kind=verify 让同次回源的等待者立刻拿到"未确认"结论；
            # 它不参与任何冷却快失败，新查询会立即抢占租约重新回源，
            # 因此不会拖住配置源恢复后的重新回源。
            _finish_with_snapshot(
                service,
                revision,
                payload["nodes"],
                payload.get("published_at"),
                still_current=False,
                failure=(verify_error, "verify", 0.0),
            )
            return _degraded_or_unavailable(
                service, verify_error, "verify", _read_last_good(service)
            )

        still_current = latest == revision
        status = _finish_with_snapshot(
            service,
            revision,
            payload["nodes"],
            payload.get("published_at"),
            still_current,
        )

        if not still_current:
            # 旧修订已过期：不把它当有效结果返回，让请求走新修订
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "revision advanced during slow origin fetch; retry for latest",
                    "fetched_revision": revision,
                    "current_revision": latest,
                },
            )

        return {
            "service": service,
            "revision": revision,
            "nodes": payload["nodes"],
            "served_by": INSTANCE_ID,
            "from_cache": False,
            "cached_at": time.time(),
            "snapshot_status": status,
        }
    finally:
        heartbeat_task.cancel()
        with contextlib.suppress(BaseException):
            await heartbeat_task


async def _heartbeat(service: str, revision: int) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            conn = _connect()
            try:
                if not _renew_lease(conn, service, revision):
                    return  # 已被接管
            finally:
                conn.close()
        except Exception:
            pass


async def _wait_as_follower(
    conn: sqlite3.Connection, service: str, revision: int
) -> dict[str, Any]:
    # 覆盖：leader 硬超时 + 租约 TTL + 接管者自己的回源时间，再加余量
    deadline = time.time() + HTTP_TIMEOUT * 3 + LEASE_TTL * 2
    while time.time() < deadline:
        await asyncio.sleep(POLL_INTERVAL)

        snap = _get_snapshot(conn, service, revision)
        if snap is not None and snap["status"] == "valid":
            return {
                "service": service,
                "revision": revision,
                "nodes": json.loads(snap["nodes"]),
                "served_by": INSTANCE_ID,
                "from_cache": True,
                "coalesced": True,
                "cached_at": snap["cached_at"],
            }

        # leader 失败：共享失败记录生效。
        # 但若仍有 leader 持有有效租约（例如其回源挂起、只是客户端先超时），
        # 不能直接失败，应等待租约过期后接管。
        fail = conn.execute(
            "SELECT * FROM failures WHERE service=? AND revision=?", (service, revision)
        ).fetchone()
        inflight_row = conn.execute(
            "SELECT lease_expires_at FROM inflight WHERE service=? AND revision=?",
            (service, revision),
        ).fetchone()
        lease_alive = inflight_row is not None and inflight_row["lease_expires_at"] > time.time()
        if fail is not None and not lease_alive:
            now = time.time()
            if fail["kind"] == "verify":
                # 同一次回源的二次核验未过：等待者不得复用 leader 拉取的节点，
                # 只能用此前已确认的快照降级，没有则明确不可用。
                # 该结论不进入冷却（见 _query），不会阻碍恢复后的新回源。
                return _degraded_or_unavailable(
                    service, fail["error"], "verify", _last_good(conn, service)
                )
            if fail["cooldown_until"] > now:
                raise HTTPException(
                    status_code=503,
                    detail={
                        "error": "origin unavailable (coalesced leader failure)",
                        "reason": fail["error"],
                        "retry_after": round(fail["cooldown_until"] - now, 2),
                        "last_good": _last_good(conn, service),
                    },
                )
            # 冷却已过：清掉旧失败记录，尝试接管重新回源
            _clear_failure(conn, service, revision)
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(service, revision)

        # 租约过期（leader 超时/退出）：接管
        if inflight_row is None:
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(service, revision)
        elif inflight_row["lease_expires_at"] <= time.time():
            if _try_acquire_lease(conn, service, revision):
                return await _run_as_leader(service, revision)

    raise HTTPException(
        status_code=503,
        detail={"error": "timed out waiting for in-flight origin fetch",
                "last_good": _last_good(conn, service)},
    )


# ---------------------------------------------------------------------------
# FastAPI
# ---------------------------------------------------------------------------
app = FastAPI(
    title=f"路由查询缓存 API（{INSTANCE_ID}）",
    description=(
        "两个缓存实例共享 SQLite 状态，提供跨实例并发合并、租约接管与"
        "修订一致性保证。/docs 可直接操作查询、快照排障与状态检查。"
    ),
    version="1.0.0",
)


@app.on_event("startup")
async def _startup() -> None:
    init_db()


class QueryResult(BaseModel):
    service: str
    revision: int
    nodes: list[str]
    served_by: str
    from_cache: bool
    coalesced: bool | None = None
    degraded: bool | None = None
    degraded_reason: str | None = None
    cached_at: float | None = None
    snapshot_status: str | None = None


@app.get("/routes/{service}", response_model=QueryResult, summary="查询服务当前路由（含修订）")
async def query_route(service: str) -> dict[str, Any]:
    return await _query(service)


@app.get("/snapshots/{service}", summary="列出该服务全部缓存快照（含 stale，供排障）")
async def list_snapshots(service: str) -> dict[str, Any]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT service, revision, nodes, status, source_published_at, cached_at "
            "FROM snapshots WHERE service=? ORDER BY revision",
            (service,),
        ).fetchall()
        return {
            "service": service,
            "snapshots": [
                {
                    "revision": r["revision"],
                    "nodes": json.loads(r["nodes"]),
                    "status": r["status"],
                    "source_published_at": r["source_published_at"],
                    "cached_at": r["cached_at"],
                }
                for r in rows
            ],
        }
    finally:
        conn.close()


@app.get("/snapshots/{service}/revisions/{revision}", summary="查看指定修订的历史快照（排障）")
async def get_snapshot(service: str, revision: int) -> dict[str, Any]:
    conn = _connect()
    try:
        r = _get_snapshot(conn, service, revision)
        if r is None:
            raise HTTPException(status_code=404, detail="snapshot not found")
        return {
            "service": service,
            "revision": revision,
            "nodes": json.loads(r["nodes"]),
            "status": r["status"],
            "source_published_at": r["source_published_at"],
            "cached_at": r["cached_at"],
        }
    finally:
        conn.close()


@app.get("/debug/state", summary="共享数据库内部状态（inflight/failures/快照）")
async def debug_state() -> dict[str, Any]:
    conn = _connect()
    try:
        inflight = [
            dict(r)
            for r in conn.execute(
                "SELECT service, revision, leader, lease_expires_at, started_at FROM inflight"
            ).fetchall()
        ]
        failures = [dict(r) for r in conn.execute("SELECT * FROM failures").fetchall()]
        snaps = [
            {
                "service": r["service"],
                "revision": r["revision"],
                "status": r["status"],
                "cached_at": r["cached_at"],
            }
            for r in conn.execute(
                "SELECT service, revision, status, cached_at FROM snapshots "
                "ORDER BY service, revision"
            ).fetchall()
        ]
        return {
            "instance_id": INSTANCE_ID,
            "now": time.time(),
            "inflight": inflight,
            "failures": failures,
            "snapshots": snaps,
        }
    finally:
        conn.close()


@app.post("/debug/reset", summary="清空共享缓存状态（测试用）")
async def debug_reset() -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("DELETE FROM snapshots")
        conn.execute("DELETE FROM inflight")
        conn.execute("DELETE FROM failures")
    finally:
        conn.close()
    return {"reset": True, "by": INSTANCE_ID}


@app.get("/health", summary="健康检查")
async def health() -> dict[str, str]:
    return {"status": "ok", "instance_id": INSTANCE_ID}
