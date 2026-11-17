"""二次核验失联（post-fetch verify 阶段）回归自测。

事故复现（双实例共享 SQLite）：
  配置源发布修订 2 后，缓存回源拿到节点，但二次核验时与配置源失联。
  修复要求：
    - 刚拉取的节点不得确认成现役（只能 stale 落库供排障）；
    - 等待同一次回源的查询也不得复用它；
    - 此前已确认的快照仍可按原语义降级使用，没有则明确不可用；
    - 源恢复后两台实例都取得修订 2，未核实结果不阻碍新回源；
    - 历史快照仍可排障；并发未命中仍合并；持有者超时/退出仍可被接手。
"""
from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
PY = str(ROOT / ".venv" / "bin" / "python")
DATA = ROOT / "data"
LOG = DATA / "logs"
SRC, C1, C2 = "http://127.0.0.1:8080", "http://127.0.0.1:8081", "http://127.0.0.1:8082"
SERVICE = "billing"

results: list[tuple[bool, str, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    results.append((ok, name, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")


class Stack:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def start(self, what: str, port: int, env: dict[str, str]) -> subprocess.Popen:
        log = open(LOG / f"verify_{what}.log", "ab")
        p = subprocess.Popen(
            [PY, "-m", "uvicorn", f"app.{what}:app", "--host", "127.0.0.1", "--port", str(port)],
            cwd=str(ROOT),
            env={**os.environ, **env},
            stdout=log,
            stderr=subprocess.STDOUT,
            preexec_fn=os.setsid,
        )
        self.procs.append(p)
        return p

    def stop_all(self) -> None:
        for p in self.procs:
            try:
                os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            except ProcessLookupError:
                pass
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                try:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                except ProcessLookupError:
                    pass


async def wait_ready(client: httpx.AsyncClient, urls: list[str], timeout: float = 20) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        ok = True
        for u in urls:
            try:
                if (await client.get(u)).status_code != 200:
                    ok = False
            except Exception:
                ok = False
        if ok:
            return
        await asyncio.sleep(0.3)
    raise RuntimeError("services did not become ready")


async def fault(client: httpx.AsyncClient, **kw) -> None:
    r = await client.post(f"{SRC}/admin/fault", json=kw)
    assert r.status_code == 200, r.text


async def fetch_total(client: httpx.AsyncClient) -> int:
    return (await client.get(f"{SRC}/admin/metrics")).json()["fetch_total"]


async def publish(client: httpx.AsyncClient, service: str, nodes: list[str]) -> int:
    r = await client.post(f"{SRC}/admin/publish", json={"service": service, "nodes": nodes})
    assert r.status_code == 200, r.text
    return r.json()["revision"]


async def main() -> int:
    DATA.mkdir(exist_ok=True)
    LOG.mkdir(parents=True, exist_ok=True)
    for name in ("verify_source.db", "verify_cache.db"):
        for suffix in ("", "-wal", "-shm"):
            Path(str(DATA / name) + suffix).unlink(missing_ok=True)

    stack = Stack()
    env = {"SOURCE_DB": str(DATA / "verify_source.db"),
           "CACHE_DB": str(DATA / "verify_cache.db")}
    stack.start("source", 8080, env)
    c1 = stack.start("cache", 8081, {**env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache1"})
    stack.start("cache", 8082, {**env, "SOURCE_BASE": SRC, "INSTANCE_ID": "cache2"})

    try:
        async with httpx.AsyncClient(timeout=45) as client:
            await wait_ready(client, [f"{SRC}/docs", f"{C1}/health", f"{C2}/health"])
            await client.post(f"{C1}/debug/reset")
            await fault(client, mode="ok", delay=0, code=500,
                        current_mode="ok", current_delay=0, current_code=503,
                        current_scope="verify")

            # 准备：billing 修订 1 被两台实例确认（valid）
            assert await publish(client, SERVICE, ["10.0.0.1:80"]) == 1
            a = await client.get(f"{C1}/routes/{SERVICE}")
            b = await client.get(f"{C2}/routes/{SERVICE}")
            record("准备：billing rev1 已被两实例确认",
                   a.status_code == 200 and a.json()["revision"] == 1
                   and b.status_code == 200 and b.json()["revision"] == 1,
                   f"c1_rev={a.json().get('revision')} c2_rev={b.json().get('revision')}")

            # ---- 事故：发布 rev2，回源成功后二次核验失联 ------------------
            assert await publish(client, SERVICE, ["10.0.0.2:80"]) == 2
            await fault(client, current_mode="error", current_code=503, current_scope="verify")

            before = await fetch_total(client)
            r = await client.get(f"{C1}/routes/{SERVICE}")
            body = r.json()
            fetched = await fetch_total(client) - before
            snap2 = (await client.get(f"{C1}/snapshots/{SERVICE}/revisions/2")).json()
            record(
                "W1 核验失联：rev2 不得确认为现役，退回已确认 rev1（degraded）",
                r.status_code == 200 and body.get("revision") == 1
                and body.get("nodes") == ["10.0.0.1:80"]
                and body.get("degraded") is True and body.get("from_cache") is True,
                f"status={r.status_code} rev={body.get('revision')} "
                f"nodes={body.get('nodes')} degraded={body.get('degraded')}",
            )
            record("W2 未核验的 rev2 只落为 stale，绝不写 valid",
                   snap2.get("status") == "stale"
                   and snap2.get("nodes") == ["10.0.0.2:80"],
                   f"snapshot_status={snap2.get('status')} nodes={snap2.get('nodes')}")
            record("W3 确已回源（失联发生在核验阶段，而非没拉到节点）",
                   fetched == 1, f"origin_fetches={fetched}")

            # ---- 同次回源的等待者也不得复用未核验节点 --------------------
            await fault(client, mode="delay", delay=1.0,
                        current_mode="error", current_code=503, current_scope="verify")
            before = await fetch_total(client)
            rs = await asyncio.gather(*[
                client.get(f"{C1 if i == 0 else C2}/routes/{SERVICE}") for i in range(3)
            ])
            fetched = await fetch_total(client) - before
            record(
                "W4 并发等待同一次回源者同样不复用未核验节点（仍只回源 1 次）",
                all(x.status_code == 200 for x in rs)
                and all(x.json().get("revision") == 1 for x in rs)
                and all(x.json().get("degraded") is True for x in rs)
                and all("10.0.0.2:80" not in x.json().get("nodes", []) for x in rs)
                and fetched == 1,
                f"statuses={[x.status_code for x in rs]} "
                f"revs={[x.json().get('revision') for x in rs]} fetches={fetched}",
            )
            again = await client.get(f"{C1}/routes/{SERVICE}")
            record("W5 核验持续失联期间稳定返回已确认 rev1",
                   again.status_code == 200 and again.json().get("revision") == 1
                   and again.json().get("nodes") == ["10.0.0.1:80"],
                   f"rev={again.json().get('revision')} nodes={again.json().get('nodes')}")

            # ---- 源恢复（不追加发布）：两实例都取得修订 2 -----------------
            await fault(client, mode="ok", delay=0,
                        current_mode="ok", current_scope="verify")
            t0 = time.time()
            rr1 = await client.get(f"{C1}/routes/{SERVICE}")
            rr2 = await client.get(f"{C2}/routes/{SERVICE}")
            elapsed = time.time() - t0
            record(
                "W6 源恢复后两实例均取得修订 2，未核实结果不阻碍新回源",
                rr1.status_code == 200 and rr2.status_code == 200
                and rr1.json()["revision"] == 2 and rr1.json()["nodes"] == ["10.0.0.2:80"]
                and rr2.json()["revision"] == 2 and rr2.json()["nodes"] == ["10.0.0.2:80"]
                and not rr1.json().get("degraded") and not rr2.json().get("degraded"),
                f"c1_rev={rr1.json().get('revision')} c2_rev={rr2.json().get('revision')} "
                f"elapsed={elapsed:.1f}s",
            )
            snap2_after = (await client.get(f"{C1}/snapshots/{SERVICE}/revisions/2")).json()
            record("W7 恢复后 rev2 快照转 valid",
                   snap2_after.get("status") == "valid",
                   f"snapshot_status={snap2_after.get('status')}")

            # ---- 无任何已确认快照：核验失联时明确不可用 -------------------
            assert await publish(client, "fresh", ["10.9.9.9:80"]) == 1
            await fault(client, current_mode="error", current_code=503, current_scope="verify")
            rf = await client.get(f"{C2}/routes/fresh")
            detail = rf.json().get("detail", {}) if rf.status_code == 503 else {}
            record(
                "W8 无已确认快照时核验失联明确 503，不返回未核验节点",
                rf.status_code == 503 and detail.get("failure_kind") == "verify"
                and detail.get("last_good") is None,
                f"status={rf.status_code} failure_kind={detail.get('failure_kind')} "
                f"last_good={detail.get('last_good')}",
            )
            await fault(client, current_mode="ok")

            # ---- 事故原始顺序：拉取 rev1 -> 发布 rev2 -> 核验失联 ---------
            # 旧实现会把 rev1 当作"当前修订"写成 valid 并普通 200 返回；
            # 修复后：rev1 只能 stale，且因无更早已确认快照而明确 503。
            assert await publish(client, "orders", ["10.2.0.1:80"]) == 1
            await fault(client, mode="delay", delay=1.0,
                        current_mode="error", current_code=503, current_scope="verify")
            task_old = asyncio.create_task(client.get(f"{C1}/routes/orders"))
            await asyncio.sleep(0.4)  # rev1 回源在途
            assert await publish(client, "orders", ["10.2.0.2:80"]) == 2
            r_old = await task_old
            detail_old = r_old.json().get("detail", {}) if r_old.status_code == 503 else {}
            snaps_old = (await client.get(f"{C1}/snapshots/orders")).json()["snapshots"]
            record(
                "W9 拉取 rev1 后发布 rev2 且核验失联：不以 200 返回 rev1，也不写 valid",
                r_old.status_code == 503
                and detail_old.get("failure_kind") == "verify"
                and [(s["revision"], s["status"], s["nodes"]) for s in snaps_old]
                == [(1, "stale", ["10.2.0.1:80"])],
                f"status={r_old.status_code} "
                f"snapshots={[(s['revision'], s['status']) for s in snaps_old]}",
            )
            await fault(client, mode="ok", delay=0,
                        current_mode="ok", current_scope="verify")
            o1 = await client.get(f"{C1}/routes/orders")
            o2 = await client.get(f"{C2}/routes/orders")
            record(
                "W10 恢复后两实例均取得 rev2，rev1 的 stale 不阻碍回源",
                o1.status_code == 200 and o1.json()["revision"] == 2
                and o1.json()["nodes"] == ["10.2.0.2:80"]
                and o2.status_code == 200 and o2.json()["revision"] == 2
                and o2.json()["nodes"] == ["10.2.0.2:80"],
                f"c1_rev={o1.json().get('revision')} c2_rev={o2.json().get('revision')}",
            )

            # ---- 历史快照（含 stale）在恢复后仍可排障 ---------------------
            assert await publish(client, "inventory", ["10.1.0.1:80"]) == 1   # rev1 确认
            await client.get(f"{C1}/routes/inventory")
            assert await publish(client, "inventory", ["10.1.0.2:80"]) == 2   # rev2 核验失联
            await fault(client, current_mode="error", current_code=503, current_scope="verify")
            inv2 = await client.get(f"{C1}/routes/inventory")
            assert await publish(client, "inventory", ["10.1.0.3:80"]) == 3   # 再推进到 rev3
            inv3 = await client.get(f"{C1}/routes/inventory")                 # rev3 也核验失联
            await fault(client, current_mode="ok")
            inv_ok = await client.get(f"{C1}/routes/inventory")
            inv_snaps = (await client.get(f"{C1}/snapshots/inventory")).json()["snapshots"]
            by_rev = {s["revision"]: s for s in inv_snaps}
            stale2 = (await client.get(f"{C1}/snapshots/inventory/revisions/2")).json()
            record(
                "W11 stale 历史快照在恢复后仍可排障，当前修订正常转 valid",
                inv2.status_code == 200 and inv2.json().get("revision") == 1
                and inv2.json().get("degraded") is True
                and inv3.status_code == 200 and inv3.json().get("revision") == 1
                and inv_ok.status_code == 200 and inv_ok.json().get("revision") == 3
                and by_rev.get(1, {}).get("status") == "valid"
                and by_rev.get(2, {}).get("status") == "stale"
                and by_rev.get(3, {}).get("status") == "valid"
                and stale2.get("nodes") == ["10.1.0.2:80"],
                f"snapshots={[(s['revision'], s['status']) for s in inv_snaps]}",
            )

            # ---- 源整体失联：已确认快照按原语义降级 -----------------------
            assert await publish(client, SERVICE, ["10.0.0.3:80"]) == 3
            await fault(client, current_mode="error", current_code=503, current_scope="all")
            rd = await client.get(f"{C1}/routes/{SERVICE}")
            record(
                "W12 源整体失联时已确认快照降级可用（degraded）",
                rd.status_code == 200 and rd.json().get("revision") == 2
                and rd.json().get("degraded") is True,
                f"status={rd.status_code} rev={rd.json().get('revision')} "
                f"degraded={rd.json().get('degraded')}",
            )
            await fault(client, current_mode="ok")

            # ---- 并发未命中仍合并 -----------------------------------------
            await fault(client, mode="delay", delay=1.0)
            before = await fetch_total(client)
            rs = await asyncio.gather(*[
                client.get(f"{C1 if i % 2 else C2}/routes/{SERVICE}") for i in range(8)
            ])
            fetched = await fetch_total(client) - before
            await fault(client, mode="ok")
            record(
                "W13 并发未命中仍合并为 1 次回源，取得修订 3",
                all(x.status_code == 200 for x in rs)
                and {x.json()["revision"] for x in rs} == {3}
                and fetched == 1
                and len([x for x in rs if x.json().get("coalesced")]) == 7,
                f"fetches={fetched} coalesced="
                f"{len([x for x in rs if x.json().get('coalesced')])}",
            )

            # ---- 持有者被 kill：租约仍可被接手 ----------------------------
            assert await publish(client, SERVICE, ["10.0.0.4:80"]) == 4
            await fault(client, mode="delay", delay=2.5)

            async def fire(url: str) -> httpx.Response | Exception:
                try:
                    return await client.get(f"{url}/routes/{SERVICE}")
                except Exception as exc:  # 被 kill 的实例会让在途请求失败
                    return exc

            doomed = asyncio.create_task(fire(C1))
            await asyncio.sleep(0.6)
            inflight = (await client.get(f"{C2}/debug/state")).json()["inflight"]
            leader = inflight[0]["leader"] if inflight else None
            os.killpg(os.getpgid(c1.pid), signal.SIGKILL)
            await asyncio.sleep(0.2)
            await doomed
            takeover = await client.get(f"{C2}/routes/{SERVICE}")
            record(
                "W14 持有者退出后租约仍可被接手，取得新修订",
                leader == "cache1" and takeover.status_code == 200
                and takeover.json()["revision"] == 4
                and takeover.json()["nodes"] == ["10.0.0.4:80"]
                and takeover.json().get("from_cache") is False,
                f"dead_leader={leader} rev={takeover.json().get('revision')} "
                f"from_cache={takeover.json().get('from_cache')}",
            )
            await fault(client, mode="ok")
    finally:
        stack.stop_all()

    failed = [r for r in results if not r[0]]
    print("\n================ 二次核验回归汇总 ================")
    for ok, name, detail in results:
        print(f"  [{'PASS' if ok else 'FAIL'}] {name} {detail}")
    print(f"\n通过 {len(results) - len(failed)}/{len(results)}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
