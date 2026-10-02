import asyncio
import os
import uuid
from pathlib import Path

from aiohttp import web, ClientSession, ClientWSTimeout, WSMsgType

ROOT = Path(__file__).resolve().parent / "public"
API_URL = "wss://developer.mig33.id/developer/ws"
SUBPROTOCOL = "mig33.developer.ws.v1"
PORT = int(os.environ.get("PORT", "3000"))

# Registry of upstream sockets currently used by the browser's WS1-WS10 slots.
ACTIVE_UPSTREAM = {}
SOCKET_LOCKS = {}
KICK_RATE_LOCKS = {}
KICK_RATE_STATE = {}
SUICIDE_KEYS = set()
BROWSER_CLIENTS = set()

# Active KICK ALL jobs grouped by room. Each job tracks targets that have
# already been confirmed kicked by an upstream room event.
ACTIVE_KICK_JOBS = {}

def _norm_room_key(room):
    return str(room or "").strip().casefold()

def _event_payload_dict(payload):
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    return data if isinstance(data, dict) else payload

def _kick_event_info(payload):
    data = _event_payload_dict(payload)
    typ = str(payload.get("type", data.get("type", data.get("event_type", "")))).strip().lower()
    room = str(payload.get("room", data.get("room", ""))).strip()
    target = str(payload.get("target_username", data.get("target_username", data.get("username", data.get("target", ""))))).strip()
    status = str(payload.get("status_message", data.get("status_message", data.get("message", "")))).strip().lower()
    return typ, room, target, status

def _is_confirmed_kicked_event(payload):
    typ, room, target, status = _kick_event_info(payload)
    if not target:
        return False
    exact = {
        "room.participant.kicked", "room.member.kicked", "room.user.kicked",
        "participant.kicked", "member.kicked", "user.kicked",
    }
    if typ in exact:
        return True
    if "vote" in typ or typ in {"room.kick.result", "room.command.result"}:
        return False
    return any(x in status for x in (
        "has been kicked", "was kicked", "have been kicked", "kicked from the room"
    ))

async def mark_confirmed_kick(payload):
    if not _is_confirmed_kicked_event(payload):
        return None
    typ, room, target, status = _kick_event_info(payload)
    room_key = _norm_room_key(room)
    if not room_key or not target:
        return None
    target_key = target.casefold()
    jobs = ACTIVE_KICK_JOBS.get(room_key, [])
    newly_marked = False
    for state in list(jobs):
        if not isinstance(state, dict):
            continue
        lock = state.get("lock")
        if lock is None:
            lock = asyncio.Lock()
            state["lock"] = lock
        async with lock:
            # Only targets belonging to this job's immutable original pool
            # can affect its replacement chain.
            events = state.get("events", {})
            if target_key not in events:
                continue
            kicked_targets = state.setdefault("kicked", set())
            if target_key not in kicked_targets:
                kicked_targets.add(target_key)
                events[target_key].set()
                newly_marked = True
                callback = state.get("on_kicked")
                if callback is not None:
                    task = asyncio.create_task(callback(target))
                    tasks = state.setdefault("replacement_tasks", set())
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
    if newly_marked:
        print(f"[KICK EVENT] confirmed kicked room={room!r} target={target!r} type={typ}", flush=True)
    return {"room": room, "target": target, "type": typ}


# Server-authoritative kick limit: 200 kicks/second per upstream WebSocket.
# Defined directly in app.py so every kick path uses the same limit.
KICK_MAX_PER_SECOND = 200
KICK_LIMIT_LABEL = f"{KICK_MAX_PER_SECOND}/socket/second"

DOWNLOAD_DIRS = [
    Path.home() / "storage" / "downloads",
    Path.home() / "storage" / "shared" / "Download",
    Path("/sdcard/Download"),
    Path.home() / "Downloads",
    Path(__file__).resolve().parent / "configs",
]



def safe_config_filename(name):
    name = str(name or "").strip()
    if name.lower().endswith(".json"):
        name = name[:-5]
    # Keep the filename simple and safe for Android Downloads.
    name = "".join(ch if ch.isalnum() or ch in "-_ ." else "_" for ch in name).strip(" .")
    if not name:
        raise ValueError("Nama file kosong")
    return name + ".json"

def find_download_dir():
    for d in DOWNLOAD_DIRS:
        try:
            if d.exists() and d.is_dir():
                return d
        except Exception:
            pass
    # If no platform-specific folder exists, use a local project folder.
    # This makes the same build work in GitHub Codespaces/Linux as well as Termux.
    d = Path(__file__).resolve().parent / "configs"
    try:
        d.mkdir(parents=True, exist_ok=True)
        return d
    except Exception:
        return None

def find_config_file(filename):
    for d in DOWNLOAD_DIRS:
        f = d / filename
        try:
            if f.is_file():
                return f
        except Exception:
            pass
    return None

async def save_config_file(request):
    try:
        data = await request.json()
        filename = safe_config_filename(data.get("name"))
        cfg = data.get("config")
        if not isinstance(cfg, dict):
            raise ValueError("Config tidak valid")
        d = find_download_dir()
        if d is None:
            return web.json_response({"ok": False, "error": "Folder konfigurasi tidak tersedia."}, status=500)
        path = d / filename
        import json
        path.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
        return web.json_response({"ok": True, "filename": filename, "path": str(path)})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)

async def load_config_file(request):
    try:
        filename = safe_config_filename(request.query.get("name", ""))
        path = find_config_file(filename)
        if path is None:
            return web.json_response({"ok": False, "error": f"File {filename} tidak ditemukan di Download."}, status=404)
        import json
        cfg = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(cfg, dict):
            raise ValueError("Isi file bukan config JSON")
        return web.json_response({"ok": True, "filename": filename, "config": cfg})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)


def register_socket(username, ws):
    if not username:
        return
    ACTIVE_UPSTREAM[username.lower()] = ws
    SOCKET_LOCKS.setdefault(id(ws), asyncio.Lock())
    KICK_RATE_LOCKS.setdefault(id(ws), asyncio.Lock())
    KICK_RATE_STATE.setdefault(id(ws), {"window_start": 0.0, "count": 0})

def unregister_socket(username, ws):
    if username and ACTIVE_UPSTREAM.get(username.lower()) is ws:
        ACTIVE_UPSTREAM.pop(username.lower(), None)
    SOCKET_LOCKS.pop(id(ws), None)
    KICK_RATE_LOCKS.pop(id(ws), None)
    KICK_RATE_STATE.pop(id(ws), None)

async def send_upstream(ws, payload):
    lock = SOCKET_LOCKS.setdefault(id(ws), asyncio.Lock())
    async with lock:
        if ws.closed:
            return False
        await ws.send_json(payload)
        return True

async def send_kick(ws, room, target):
    """Send one room.kick while enforcing the configured per-socket rate.

    The limiter is per upstream WebSocket and shared by all callers, so
    concurrent KICK ALL requests and SUICIDE cannot bypass the limit.
    """
    wsid = id(ws)
    rate_lock = KICK_RATE_LOCKS.setdefault(wsid, asyncio.Lock())
    state = KICK_RATE_STATE.setdefault(wsid, {"window_start": 0.0, "count": 0})

    while True:
        async with rate_lock:
            if ws.closed:
                return False
            now = asyncio.get_running_loop().time()
            if state["window_start"] <= 0 or now - state["window_start"] >= 1.0:
                state["window_start"] = now
                state["count"] = 0
            if state["count"] < KICK_MAX_PER_SECOND:
                state["count"] += 1
                break
            wait_for = max(0.001, 1.0 - (now - state["window_start"]))
        await asyncio.sleep(wait_for)

    return await send_upstream(ws, {
        "type": "room.kick",
        "room": room,
        "target_username": target,
    })

async def sandbox_kick(request):
    """Local-only kick workload simulator. Never opens or writes an upstream WebSocket."""
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "JSON tidak valid"}, status=400)
    targets = []
    seen_targets = set()
    for raw in data.get("targets", []):
        target = str(raw).strip()
        key = target.casefold()
        if target and key not in seen_targets:
            seen_targets.add(key)
            targets.append(target)
        if len(targets) >= 10:
            break
    ws_usernames = {str(name).strip().casefold() for name in ACTIVE_UPSTREAM if str(name).strip()}
    targets = [t for t in targets if t.casefold() not in ws_usernames][:10]
    try:
        loops = max(1, min(100, int(data.get("loop", 1))))
        sockets_count = max(1, min(10, int(data.get("sockets", 10))))
        delay_target = max(0, int(data.get("delayTarget", 0))) / 1000
        delay_batch = max(0, int(data.get("delayBatch", 0))) / 1000
    except Exception:
        return web.json_response({"ok": False, "error": "Parameter sandbox tidak valid"}, status=400)
    if not targets:
        return web.json_response({"ok": False, "error": "TARGET wajib diisi untuk sandbox"}, status=400)

    job_id = "sandbox-" + uuid.uuid4().hex[:12]
    total_jobs = loops * len(targets) * sockets_count
    socket_reports = {
        f"SANDBOX-{i+1}": {
            "totalJobs": loops * len(targets),
            "dispatchedJobs": 0, "failedJobs": 0,
            "lastTarget": "", "lastLoop": 0
        }
        for i in range(sockets_count)
    }

    await publish_kick_progress({
        "jobId": job_id, "phase": "started", "sandbox": True,
        "totalJobs": total_jobs, "dispatchedJobs": 0, "failedJobs": 0,
        "websockets": sockets_count, "targets": len(targets), "loop": loops,
        "burst": 0, "combo": "sandbox",
        "socketReports": [{"websocket": n, **v} for n, v in socket_reports.items()]
    })

    dispatched = 0
    started = asyncio.get_running_loop().time()
    for loop_no in range(1, loops + 1):
        for target in targets:
            for socket_name, stats in socket_reports.items():
                # Deliberately no upstream/network call: this is a local simulation only.
                await asyncio.sleep(0)
                stats["dispatchedJobs"] += 1
                stats["lastTarget"] = target
                stats["lastLoop"] = loop_no
                dispatched += 1
                await publish_kick_progress({
                    "jobId": job_id, "phase": "progress", "sandbox": True,
                    "totalJobs": total_jobs, "dispatchedJobs": dispatched,
                    "failedJobs": 0, "websockets": sockets_count,
                    "target": target, "loop": loop_no, "websocket": socket_name,
                    "socketStats": dict(stats)
                })
                if delay_target:
                    await asyncio.sleep(delay_target)
        if loop_no < loops and delay_batch:
            await asyncio.sleep(delay_batch)

    elapsed = max(0.000001, asyncio.get_running_loop().time() - started)
    await publish_kick_progress({
        "jobId": job_id, "phase": "done", "sandbox": True,
        "totalJobs": total_jobs, "dispatchedJobs": dispatched, "failedJobs": 0,
        "websockets": sockets_count, "elapsedMs": round(elapsed * 1000, 3),
        "socketReports": [{"websocket": n, **v} for n, v in socket_reports.items()]
    })
    return web.json_response({
        "ok": True, "sandbox": True, "jobId": job_id, "totalJobs": total_jobs,
        "websockets": sockets_count, "elapsedMs": round(elapsed * 1000, 3)
    })


async def publish_kick_progress(payload):
    """Broadcast real backend kick progress to every connected browser socket."""
    dead = []
    message = {"type": "kick.progress", **payload}
    for browser in list(BROWSER_CLIENTS):
        if browser.closed:
            dead.append(browser)
            continue
        try:
            await browser.send_json(message)
        except Exception:
            dead.append(browser)
    for browser in dead:
        BROWSER_CLIENTS.discard(browser)


async def kick_loop(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "JSON tidak valid"}, status=400)
    room = str(data.get("room", "")).strip()
    targets = [str(x).strip() for x in data.get("targets", []) if str(x).strip()][:10]
    try:
        loops = max(1, int(data.get("loop", 1)))
        burst = min(10, max(1, int(data.get("burst", 1))))
        combo = str(data.get("combo", "off")).strip().lower()
        if combo not in {"off", "combo1", "combo2"}:
            combo = "off"
        if combo == "combo1":
            burst = 0
        elif combo == "combo2":
            burst = 0
        delay_target = max(0, int(data.get("delayTarget", 0))) / 1000
        delay_batch = max(0, int(data.get("delayBatch", 0))) / 1000
    except Exception:
        return web.json_response({"ok": False, "error": "Parameter kick tidak valid"}, status=400)
    if not room or not targets:
        return web.json_response({"ok": False, "error": "Room dan TARGET wajib diisi"}, status=400)
    socket_entries = []
    seen_ws = set()
    for username, ws in ACTIVE_UPSTREAM.items():
        if id(ws) not in seen_ws:
            seen_ws.add(id(ws))
            socket_entries.append((username, ws))
    sockets = [ws for _, ws in socket_entries]
    if not sockets:
        return web.json_response({"ok": False, "error": "Tidak ada WebSocket yang aktif"}, status=409)

    job_id = uuid.uuid4().hex[:12]
    combo_bursts = {"combo1": (2, 3), "combo2": (3, 4)}
    if combo in combo_bursts:
        b1, b2 = combo_bursts[combo]
        combo_waves = max((len(targets) + b1 - 1) // b1, (len(targets) + b2 - 1) // b2)
        total_per_socket = sum(min(b1, max(0, len(targets) - i*b1)) for i in range((len(targets)+b1-1)//b1)) + sum(min(b2, max(0, len(targets) - i*b2)) for i in range((len(targets)+b2-1)//b2))
        total_jobs = loops * total_per_socket * len(sockets)
        per_socket_total = loops * total_per_socket
    else:
        combo_waves = 0
        total_jobs = loops * len(targets) * len(sockets)
        per_socket_total = loops * len(targets)
    socket_stats = {
        ws_name: {"totalJobs": per_socket_total, "dispatchedJobs": 0, "failedJobs": 0, "lastTarget": "", "lastLoop": 0}
        for ws_name, _ in socket_entries
    }

    def socket_reports():
        return [
            {"websocket": ws_name, **stats}
            for ws_name, stats in socket_stats.items()
        ]

    progress = {"jobId": job_id, "phase": "started", "totalJobs": total_jobs,
                "dispatchedJobs": 0, "failedJobs": 0, "websockets": len(sockets),
                "targets": len(targets), "loop": loops, "burst": burst, "combo": combo,
                "kickLimit": KICK_LIMIT_LABEL, "socketReports": socket_reports()}
    await publish_kick_progress(progress)

    total = 0
    failed = 0
    progress_lock = asyncio.Lock()

    room_key = _norm_room_key(room)
    # Immutable replacement source: exactly the original target pool (max 10).
    # BRUTE/COMBO still decide the normal dispatch pattern; replacement only
    # reacts to a confirmed kicked event and never creates a new target.
    replacement_state = {
        "targets": list(targets),
        "kicked": set(),
        "claimed": set(),
        "events": {t.casefold(): asyncio.Event() for t in targets},
        "replacement_tasks": set(),
        "replacement_dispatched": 0,
        "lock": asyncio.Lock(),
    }

    async def dispatch_replacement(kicked_target):
        async with replacement_state["lock"]:
            kicked_key = str(kicked_target).strip().casefold()
            candidates = [
                t for t in replacement_state["targets"]
                if t.casefold() not in replacement_state["kicked"]
                and t.casefold() != kicked_key
                and t.casefold() not in replacement_state["claimed"]
            ]
            if not candidates:
                return
            replacement = candidates[0]
            replacement_state["claimed"].add(replacement.casefold())
            replacement_state["replacement_dispatched"] += len(socket_entries)

        if delay_target:
            await asyncio.sleep(delay_target)

        # Replacement uses the same active WS set as the selected BRUTE/COMBO
        # job. It does not introduce another brute/combo pattern.
        jobs = [
            run_one(ws, ws_name, replacement, 0, is_replacement=True)
            for ws_name, ws in socket_entries
        ]
        await asyncio.gather(*jobs, return_exceptions=True)

    replacement_state["on_kicked"] = dispatch_replacement
    ACTIVE_KICK_JOBS.setdefault(room_key, []).append(replacement_state)

    async def run_one(ws, ws_name, target, loop_no, is_replacement=False):
        nonlocal total, failed
        current_target = target

        while current_target:
            target_key = str(current_target).strip().casefold()

            # Never send another kick to a target already confirmed as kicked.
            state = replacement_state
            async with state["lock"]:
                if target_key in state["kicked"]:
                    return

            try:
                result = await send_kick(ws, room, current_target)
            except Exception:
                result = False

            async with progress_lock:
                if result is True:
                    total += 1
                    socket_stats[ws_name]["dispatchedJobs"] += 1
                else:
                    failed += 1
                    socket_stats[ws_name]["failedJobs"] += 1
                socket_stats[ws_name]["lastTarget"] = current_target
                socket_stats[ws_name]["lastLoop"] = loop_no + 1
                current_total, current_failed = total, failed
                current_socket = dict(socket_stats[ws_name])

            await publish_kick_progress({
                "jobId": job_id, "phase": "progress",
                "totalJobs": total_jobs, "dispatchedJobs": current_total,
                "failedJobs": current_failed,
                "websockets": len(sockets),
                "loop": loop_no + 1, "loops": loops,
                "target": current_target, "websocket": ws_name,
                "replacement": bool(is_replacement),
                "kickedTargets": len(state["kicked"]),
                "socketStats": {"websocket": ws_name, **current_socket},
            })

            # Replacement is event-driven from mark_confirmed_kick(). Do not
            # guess that a target was kicked merely because send_kick succeeded.
            return

    for loop_no in range(loops):
        if combo in combo_bursts:
            b1, b2 = combo_bursts[combo]
            batches1 = [targets[i:i+b1] for i in range(0, len(targets), b1)]
            batches2 = [targets[i:i+b2] for i in range(0, len(targets), b2)]
            # Dua pola BRUTE berjalan bersamaan pada setiap wave, lalu wave berikutnya
            # bergantian. COMBO1 = BRUTE2 + BRUTE3, COMBO2 = BRUTE3 + BRUTE4.
            for wave in range(max(len(batches1), len(batches2))):
                jobs = []
                if wave < len(batches1):
                    jobs.extend(run_one(ws, ws_name, target, loop_no)
                                for ws_name, ws in socket_entries for target in batches1[wave])
                if wave < len(batches2):
                    jobs.extend(run_one(ws, ws_name, target, loop_no)
                                for ws_name, ws in socket_entries for target in batches2[wave])
                if jobs:
                    await asyncio.gather(*jobs, return_exceptions=True)
                if wave + 1 < max(len(batches1), len(batches2)) and delay_target:
                    await asyncio.sleep(delay_target)
        else:
            for start in range(0, len(targets), burst):
                batch = targets[start:start + burst]
                jobs = [run_one(ws, ws_name, target, loop_no)
                        for ws_name, ws in socket_entries for target in batch]
                await asyncio.gather(*jobs, return_exceptions=True)
                if start + burst < len(targets) and delay_target:
                    await asyncio.sleep(delay_target)
        if loop_no + 1 < loops and delay_batch:
            await asyncio.sleep(delay_batch)

    # Wait for replacement chains already triggered by confirmed events. A
    # replacement may itself be kicked and therefore schedule another one.
    while replacement_state.get("replacement_tasks"):
        pending = list(replacement_state["replacement_tasks"])
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    active_jobs = ACTIVE_KICK_JOBS.get(room_key, [])
    if replacement_state in active_jobs:
        active_jobs.remove(replacement_state)
    if not active_jobs:
        ACTIVE_KICK_JOBS.pop(room_key, None)

    reports = socket_reports()
    await publish_kick_progress({
        "jobId": job_id, "phase": "done", "totalJobs": total_jobs,
        "dispatchedJobs": total, "failedJobs": failed,
        "websockets": len(sockets), "targets": len(targets),
        "loop": loops, "burst": burst, "combo": combo,
        "replacement": True, "kickedTargets": len(replacement_state["kicked"]),
        "replacementDispatched": replacement_state["replacement_dispatched"],
        "kickLimit": KICK_LIMIT_LABEL,
        "socketReports": reports
    })
    return web.json_response({"ok": True, "jobId": job_id, "totalJobs": total_jobs,
                              "dispatchedJobs": total, "failedJobs": failed,
                              "websockets": len(sockets), "targets": len(targets),
                              "loop": loops, "burst": burst, "combo": combo,
                              "replacement": True, "kickedTargets": len(replacement_state["kicked"]),
                              "replacementDispatched": replacement_state["replacement_dispatched"],
                              "kickLimit": KICK_LIMIT_LABEL, "socketReports": reports})

async def suicide(request):
    try:
        data = await request.json()
    except Exception:
        return web.json_response({"ok": False, "error": "JSON tidak valid"}, status=400)
    room = str(data.get("room", "")).strip()
    target = str(data.get("target_username", "")).strip()
    key = str(data.get("key", "")).strip()
    if not room or not target:
        return web.json_response({"ok": False, "error": "Room dan target wajib diisi"}, status=400)
    if key and key in SUICIDE_KEYS:
        return web.json_response({"ok": True, "duplicate": True, "websockets": 0})
    if key:
        SUICIDE_KEYS.add(key)
        if len(SUICIDE_KEYS) > 200:
            SUICIDE_KEYS.clear()
    sockets = list(dict.fromkeys(ACTIVE_UPSTREAM.values()))
    jobs = [send_kick(ws, room, target) for ws in sockets]
    results = await asyncio.gather(*jobs, return_exceptions=True)
    sent = sum(1 for x in results if x is True)
    return web.json_response({"ok": True, "websockets": sent, "target": target, "kickLimit": KICK_LIMIT_LABEL})

async def index(request):
    return web.FileResponse(ROOT / "index.html")

async def proxy(request):
    browser = web.WebSocketResponse(heartbeat=20)
    await browser.prepare(request)
    BROWSER_CLIENTS.add(browser)
    upstream = None
    session = None
    relay_task = None
    try:
        session = ClientSession()
        upstream_username = None
        # No receive timeout: the API is a long-lived WebSocket and has its own JSON ping rule.
        ws_timeout = ClientWSTimeout(ws_close=10, ws_receive=None)
        last_error = None
        for attempt in range(1, 3):
            try:
                upstream = await session.ws_connect(
                    API_URL,
                    protocols=(SUBPROTOCOL,),
                    heartbeat=20,
                    autoping=True,
                    timeout=ws_timeout,
                )
                print(f"UPSTREAM CONNECTED attempt={attempt} protocol={upstream.protocol!r} status={getattr(upstream, "_response", None).status if getattr(upstream, "_response", None) else "?"}", flush=True)
                break
            except Exception as e:
                last_error = e
                print(f"UPSTREAM CONNECT FAILED attempt={attempt}: {type(e).__name__}: {e!r}", flush=True)
                if attempt < 2:
                    await asyncio.sleep(0.5)
        if upstream is None:
            detail = f"{type(last_error).__name__}: {last_error}" if last_error else "unknown error"
            payload = {"type":"proxy.error","data":{"stage":"upstream_connect","message":detail[:500]}}
            if not browser.closed:
                await browser.send_json(payload)
                await browser.close(code=1011, message=b"upstream connection failed")
            return browser

        async def upstream_to_browser():
            async for msg in upstream:
                if msg.type == WSMsgType.TEXT:
                    try:
                        payload = __import__("json").loads(msg.data)
                        confirmed = await mark_confirmed_kick(payload)
                        if confirmed:
                            await browser.send_json({
                                "type": "kick.target.confirmed",
                                "room": confirmed["room"],
                                "target_username": confirmed["target"],
                                "source_type": confirmed["type"],
                            })
                    except Exception:
                        pass
                    await browser.send_str(msg.data)
                elif msg.type == WSMsgType.BINARY:
                    await browser.send_bytes(msg.data)
                elif msg.type == WSMsgType.PING:
                    await upstream.pong()
                elif msg.type == WSMsgType.PONG:
                    continue
                elif msg.type == WSMsgType.CLOSE:
                    print(f"UPSTREAM CLOSE code={upstream.close_code}", flush=True)
                    break
                elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                    print(f"UPSTREAM END type={msg.type} exception={upstream.exception()!r}", flush=True)
                    break

        relay_task = asyncio.create_task(upstream_to_browser())

        async for msg in browser:
            if msg.type == WSMsgType.TEXT:
                if upstream.closed:
                    await browser.send_json({"type":"proxy.error","data":{"stage":"upstream_send","message":"upstream socket is closed"}})
                    break
                try:
                    payload = __import__("json").loads(msg.data)
                    if payload.get("type") == "developer.login":
                        upstream_username = str(payload.get("username", "")).strip() or None
                        register_socket(upstream_username, upstream)
                except Exception:
                    pass
                await upstream.send_str(msg.data)
            elif msg.type == WSMsgType.BINARY:
                if upstream.closed:
                    break
                await upstream.send_bytes(msg.data)
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                break

    except Exception as e:
        print(f"WebSocket proxy error: {type(e).__name__}: {e!r}", flush=True)
        if not browser.closed:
            try:
                await browser.send_json({"type":"proxy.error","data":{"stage":"proxy","message":f"{type(e).__name__}: {e}"[:500]}})
            except Exception:
                pass
            await browser.close(code=1011, message=b"proxy error")
    finally:
        BROWSER_CLIENTS.discard(browser)
        if relay_task is not None:
            relay_task.cancel()
            await asyncio.gather(relay_task, return_exceptions=True)
        if upstream is not None:
            unregister_socket(upstream_username, upstream)
            if not upstream.closed:
                await upstream.close()
        if session is not None:
            await session.close()
    return browser

async def health(request):
    return web.json_response({
        "ok": True,
        "service": "migsock",
        "port": PORT,
        "upstream": API_URL,
        "ws_path": "/ws",
    })

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/health", health)
app.router.add_get("/ws", proxy)
app.router.add_post("/api/kick-loop", kick_loop)
app.router.add_post("/api/sandbox-kick", sandbox_kick)
app.router.add_post("/api/suicide", suicide)
app.router.add_static("/static/", ROOT)

if __name__ == "__main__":
    print(f"migsock listening on 0.0.0.0:{PORT}", flush=True)
    web.run_app(app, host="0.0.0.0", port=PORT)
