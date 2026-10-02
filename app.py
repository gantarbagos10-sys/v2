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
    """Record a real kick confirmation and wake every WS worker assigned to it."""
    if not _is_confirmed_kicked_event(payload):
        return None
    typ, room, target, status = _kick_event_info(payload)
    room_key = _norm_room_key(room)
    target_key = target.casefold() if target else ""
    if not target_key:
        return None

    # Some upstream events may omit room. Only use the room when there is
    # exactly one active kick job; otherwise do not guess the room.
    jobs = ACTIVE_KICK_JOBS.get(room_key, set()) if room_key else set()
    if not jobs and not room_key:
        active_rooms = [k for k, v in ACTIVE_KICK_JOBS.items() if v]
        if len(active_rooms) == 1:
            room_key = active_rooms[0]
            jobs = ACTIVE_KICK_JOBS.get(room_key, set())

    newly_marked = False
    for state in list(jobs):
        async with state["lock"]:
            if target_key not in state["kicked"]:
                state["kicked"].add(target_key)
                newly_marked = True
            event = state["confirm_events"].get(target_key)
            if event:
                event.set()

    if newly_marked:
        print(f"[KICK EVENT] confirmed kicked room={room!r} target={target!r} type={typ}", flush=True)
    return {"room": room or room_key, "target": target, "type": typ}


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


async def _replace_kicked_target(room, kicked_target):
    """Return the next live target for active workers in this room.

    This is direct target replacement, not a queued dispatch. Workers ask for
    a replacement only after the current target is confirmed kicked.
    """
    room_key = _norm_room_key(room)
    kicked_key = str(kicked_target or "").strip().casefold()
    jobs = ACTIVE_KICK_JOBS.get(room_key, set())
    for state in list(jobs):
        if state.get("lock") is None:
            state["lock"] = asyncio.Lock()
        async with state["lock"]:
            state["kicked"].add(kicked_key)
            while state["cursor"] < len(state["targets"]):
                candidate = str(state["targets"][state["cursor"]]).strip()
                state["cursor"] += 1
                if not candidate:
                    continue
                ckey = candidate.casefold()
                if ckey in state["kicked"] or ckey in state["claimed"]:
                    continue
                state["claimed"].add(ckey)
                state["target_for_key"][ckey] = candidate
                return candidate
    return None


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
    targets = [str(x).strip() for x in data.get("targets", []) if str(x).strip()][:10]
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
    # KICKALL now assigns one TARGET to each active WS. When a target is
    # confirmed kicked, that WS directly claims another TARGET from the same
    # list. Therefore the real maximum dispatch count is the number of TARGETs,
    # not TARGETs x WS.
    combo_waves = 0
    total_jobs = len(targets)
    per_socket_total = max(1, (len(targets) + len(sockets) - 1) // len(sockets))
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
                "targets": len(targets), "replacementTargets": len(targets), "loop": loops, "burst": burst, "combo": combo,
                "kickLimit": KICK_LIMIT_LABEL, "socketReports": socket_reports()}
    await publish_kick_progress(progress)

    total = 0
    failed = 0
    progress_lock = asyncio.Lock()

    room_key = _norm_room_key(room)
    replacement_state = {
        "targets": list(targets),
        "cursor": 0,
        "kicked": set(),
        "claimed": set(),
        "target_for_key": {},
        "confirm_events": {},
        "lock": asyncio.Lock(),
    }
    ACTIVE_KICK_JOBS.setdefault(room_key, set()).add(replacement_state)

    async def claim_replacement(state):
        """Claim the next live target directly from the TARGET list; no queue and never USER."""
        async with state["lock"]:
            while state["cursor"] < len(state["targets"]):
                candidate = str(state["targets"][state["cursor"]]).strip()
                state["cursor"] += 1
                if not candidate:
                    continue
                ckey = candidate.casefold()
                if ckey in state["kicked"] or ckey in state["claimed"]:
                    continue
                state["claimed"].add(ckey)
                state["target_for_key"][ckey] = candidate
                state["confirm_events"].setdefault(ckey, asyncio.Event())
                return candidate
        return None

    async def wait_for_confirmation(state, target_key):
        """Wait for actual room confirmation, so replacement happens only after kick."""
        async with state["lock"]:
            if target_key in state["kicked"]:
                return True
            event = state["confirm_events"].setdefault(target_key, asyncio.Event())
        # A missing confirmation must not leave KICKALL hanging forever.
        try:
            await asyncio.wait_for(event.wait(), timeout=10.0)
            return True
        except asyncio.TimeoutError:
            return False

    async def run_one(ws, ws_name, target, loop_no):
        nonlocal total, failed
        current_target = target
        state = replacement_state

        while current_target:
            target_key = str(current_target).strip().casefold()
            if not target_key:
                return

            # Register the confirmation waiter BEFORE sending. This prevents
            # a fast upstream confirmation from being missed.
            async with state["lock"]:
                if target_key in state["kicked"]:
                    already_kicked = True
                else:
                    already_kicked = False
                    state["confirm_events"].setdefault(target_key, asyncio.Event())
            if already_kicked:
                current_target = await claim_replacement(state)
                if current_target:
                    await publish_kick_progress({
                        "jobId": job_id, "phase": "replacement",
                        "room": room, "replacedTarget": target_key,
                        "target": current_target, "websocket": ws_name,
                        "reason": "already_confirmed"
                    })
                continue

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
                "failedJobs": current_failed, "websockets": len(sockets),
                "loop": loop_no + 1, "loops": loops,
                "target": current_target, "websocket": ws_name,
                "socketStats": {"websocket": ws_name, **current_socket},
            })

            if result is not True:
                return

            # IMPORTANT: do not replace merely because room.kick was sent.
            # Wait for the actual kicked event. Once confirmed, this WS gets
            # a different target from the same TARGET list, without a queue.
            confirmed = await wait_for_confirmation(state, target_key)
            if not confirmed:
                return

            previous_target = current_target
            current_target = await claim_replacement(state)
            if current_target:
                await publish_kick_progress({
                    "jobId": job_id, "phase": "replacement",
                    "room": room, "replacedTarget": previous_target,
                    "target": current_target, "websocket": ws_name,
                    "reason": "kick_confirmed"
                })


    # One active target per WS. When that target is confirmed kicked, that same
    # WS immediately claims the next live target from TARGET. This prevents a WS
    # from continuing to send kicks at a username already confirmed out of room.
    # `burst`/`combo` are intentionally not used to assign multiple targets to
    # the same WS here; replacement correctness takes priority.
    for loop_no in range(loops):
        jobs = []
        for ws_name, ws in socket_entries:
            target = await claim_replacement(replacement_state)
            if target:
                jobs.append(run_one(ws, ws_name, target, loop_no))
        if jobs:
            await asyncio.gather(*jobs, return_exceptions=True)
        if loop_no + 1 < loops and delay_batch:
            await asyncio.sleep(delay_batch)
        # Once every TARGET entry has been claimed/kicked, further loops cannot
        # produce a valid replacement and should stop instead of retrying dead targets.
        async with replacement_state["lock"]:
            remaining = any(
                str(t).strip() and str(t).strip().casefold() not in replacement_state["kicked"]
                for t in replacement_state["targets"]
            )
        if not remaining:
            break

    ACTIVE_KICK_JOBS.get(room_key, set()).discard(replacement_state)
    if not ACTIVE_KICK_JOBS.get(room_key):
        ACTIVE_KICK_JOBS.pop(room_key, None)

    reports = socket_reports()
    await publish_kick_progress({
        "jobId": job_id, "phase": "done", "totalJobs": total_jobs,
        "dispatchedJobs": total, "failedJobs": failed,
        "websockets": len(sockets), "targets": len(targets),
        "loop": loops, "burst": burst, "combo": combo, "kickLimit": KICK_LIMIT_LABEL,
        "socketReports": reports
    })
    return web.json_response({"ok": True, "jobId": job_id, "totalJobs": total_jobs,
                              "dispatchedJobs": total, "failedJobs": failed,
                              "websockets": len(sockets), "targets": len(targets),
                              "loop": loops, "burst": burst, "combo": combo,
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
