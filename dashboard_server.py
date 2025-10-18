
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, JSONResponse
import uvicorn
import asyncio
import sqlite3
import json
from datetime import datetime, timedelta
import os
import time
import aiosqlite

DB_PATH = "api_monitor.db"
app = FastAPI()
html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>API Monitor Dashboard</title>
</head>
<body>
  <h2>API Monitor Dashboard</h2>
  <div id="content"></div>
  <script>
    let ws = new WebSocket("ws://"+location.host+"/ws");
    ws.onmessage = function(evt){ let d=JSON.parse(evt.data); document.getElementById("content").innerText = JSON.stringify(d,null,2); };
    ws.onopen = function(){ console.log("ws open"); };
  </script>
</body>
</html>
"""

async def fetch_latest_metrics():
    if not os.path.exists(DB_PATH):
        return {"endpoints": []}
    db = await aiosqlite.connect(DB_PATH)
    cur = await db.execute("SELECT endpoint, ts_utc, status, dns, connect, tls, request_send, server_processing FROM samples WHERE ts_utc >= ? ORDER BY ts_utc DESC", ((datetime.utcnow()-timedelta(minutes=60)).isoformat(),))
    rows = await cur.fetchall()
    await db.close()
    out = {}
    for r in rows:
        ep = r[0]
        entry = {
            "ts": r[1],
            "status": r[2],
            "dns": r[3],
            "connect": r[4],
            "tls": r[5],
            "request_send": r[6],
            "server_processing": r[7]
        }
        out.setdefault(ep, []).append(entry)
    return {"endpoints": out}

@app.get("/", response_class=HTMLResponse)
async def index():
    return HTMLResponse(html)

@app.get("/api/endpoints")
async def endpoints():
    if not os.path.exists(DB_PATH):
        return JSONResponse({"endpoints": []})
    db = await aiosqlite.connect(DB_PATH)
    cur = await db.execute("SELECT DISTINCT endpoint FROM samples")
    rows = await cur.fetchall()
    await db.close()
    return JSONResponse({"endpoints": [r[0] for r in rows]})

@app.get("/api/history")
async def history(endpoint: str, minutes: int = 60):
    if not os.path.exists(DB_PATH):
        return JSONResponse({"history": []})
    db = await aiosqlite.connect(DB_PATH)
    cutoff = (datetime.utcnow()-timedelta(minutes=minutes)).isoformat()
    cur = await db.execute("SELECT ts_utc, status, dns, connect, tls, request_send, server_processing FROM samples WHERE endpoint=? AND ts_utc>=? ORDER BY ts_utc ASC", (endpoint, cutoff))
    rows = await cur.fetchall()
    await db.close()
    out = [{"ts": r[0], "status": r[1], "dns": r[2], "connect": r[3], "tls": r[4], "request_send": r[5], "server_processing": r[6]} for r in rows]
    return JSONResponse({"history": out})

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            metrics = await fetch_latest_metrics()
            await websocket.send_text(json.dumps(metrics))
            await asyncio.sleep(5)
    except Exception:
        await websocket.close()

if __name__ == "__main__":
    uvicorn.run("dashboard_server:app", host="0.0.0.0", port=8080, reload=False)