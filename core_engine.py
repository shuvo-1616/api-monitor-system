
import argparse
import asyncio
import aiohttp
import aiosqlite
import json
import os
import time
import statistics
import smtplib
import ssl
from email.message import EmailMessage
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional
from jsonschema import validate as json_validate, ValidationError

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except Exception:
    PROPHET_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    SKLEARN_AVAILABLE = True
except Exception:
    SKLEARN_AVAILABLE = False

DEFAULT_DB = "api_monitor.db"
DEFAULT_AGENT_REGISTRY = "agents.json"
USER_AGENT = "AdvancedAPIMonitor/1.0"

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def ensure_dir_for_file(path):
    d = os.path.dirname(os.path.abspath(path))
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_utc TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    region TEXT,
    status INTEGER,
    dns REAL,
    connect REAL,
    tls REAL,
    request_send REAL,
    server_processing REAL,
    content_size INTEGER,
    payload_valid INTEGER,
    raw_body TEXT
);
"""

class Storage:
    def __init__(self, db_path=DEFAULT_DB):
        ensure_dir_for_file(db_path)
        self.db_path = db_path
        self._conn = None

    async def init(self):
        self._conn = await aiosqlite.connect(self.db_path)
        await self._conn.execute(CREATE_TABLE_SQL)
        await self._conn.commit()

    async def insert_sample(self, rec: Dict[str, Any]):
        q = """INSERT INTO samples
        (ts_utc, endpoint, region, status, dns, connect, tls, request_send, server_processing, content_size, payload_valid, raw_body)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        await self._conn.execute(q, (
            rec.get("ts_utc"),
            rec.get("endpoint"),
            rec.get("region"),
            rec.get("status"),
            rec.get("dns"),
            rec.get("connect"),
            rec.get("tls"),
            rec.get("request_send"),
            rec.get("server_processing"),
            rec.get("content_size"),
            1 if rec.get("payload_valid") else 0,
            rec.get("raw_body"),
        ))
        await self._conn.commit()

    async def query_recent(self, endpoint: str, minutes: int = 60):
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=minutes)
        q = "SELECT ts_utc, status, dns, connect, tls, request_send, server_processing, content_size, payload_valid FROM samples WHERE endpoint=? AND ts_utc>=? ORDER BY ts_utc ASC"
        cur = await self._conn.execute(q, (endpoint, cutoff.isoformat()))
        rows = await cur.fetchall()
        return rows

    async def query_all_endpoints(self):
        q = "SELECT DISTINCT endpoint FROM samples"
        cur = await self._conn.execute(q)
        rows = await cur.fetchall()
        return [r[0] for r in rows]

    async def close(self):
        if self._conn:
            await self._conn.close()

class TimingTrace:
    def __init__(self):
        self.data = {}

    def make_trace(self):
        trace = aiohttp.TraceConfig()
        trace.on_dns_resolvehost_start.append(self.on_dns_start)
        trace.on_dns_resolvehost_end.append(self.on_dns_end)
        trace.on_connection_create_start.append(self.on_connect_start)
        trace.on_connection_create_end.append(self.on_connect_end)
        trace.on_request_chunk_sent.append(self.on_request_chunk_sent)
        trace.on_response_chunk_received.append(self.on_response_chunk_received)
        trace.on_connection_queued_start.append(self.on_queue_start)
        trace.on_connection_queued_end.append(self.on_queue_end)
        return trace

    async def on_dns_start(self, session, context, params):
        key = id(context)
        self.data.setdefault(key, {})["dns_start"] = time.perf_counter()

    async def on_dns_end(self, session, context, params):
        key = id(context)
        start = self.data.get(key, {}).pop("dns_start", None)
        if start:
            self.data.setdefault(key, {})["dns"] = time.perf_counter() - start

    async def on_connect_start(self, session, context, params):
        key = id(context)
        self.data.setdefault(key, {})["connect_start"] = time.perf_counter()

    async def on_connect_end(self, session, context, params):
        key = id(context)
        start = self.data.get(key, {}).pop("connect_start", None)
        if start:
            self.data.setdefault(key, {})["connect"] = time.perf_counter() - start

    async def on_request_chunk_sent(self, session, context, params):
        key = id(context)
        self.data.setdefault(key, {})["request_send"] = time.perf_counter()

    async def on_response_chunk_received(self, session, context, params):
        key = id(context)
        self.data.setdefault(key, {})["last_resp_chunk"] = time.perf_counter()

    async def on_queue_start(self, session, context, params):
        key = id(context)
        self.data.setdefault(key, {})["queue_start"] = time.perf_counter()

    async def on_queue_end(self, session, context, params):
        key = id(context)
        start = self.data.get(key, {}).pop("queue_start", None)
        if start:
            self.data.setdefault(key, {})["queue"] = time.perf_counter() - start

    def pop(self, context):
        return self.data.pop(id(context), {})

async def perform_check(session: Optional[aiohttp.ClientSession], endpoint: str, region: Optional[str],
                        schema: Optional[dict], sla_codes: Optional[List[int]], timeout: float):
    ttrace = TimingTrace()
    trace = ttrace.make_trace()
    headers = {"User-Agent": USER_AGENT, "Accept": "application/json"}
    try:
        async with aiohttp.ClientSession(trace_configs=[trace], headers=headers) as s:
            start = time.perf_counter()
            async with s.get(endpoint, timeout=timeout) as resp:
                end = time.perf_counter()
                body = await resp.text()
                timing = ttrace.pop(s)
                dns = timing.get("dns", 0.0)
                connect = timing.get("connect", 0.0)
                request_send = timing.get("request_send", 0.0)
                total = end - start
                server_processing = total - (dns + connect + (request_send - start if request_send else 0.0))
                payload_valid = True
                if schema and body:
                    try:
                        obj = json.loads(body)
                        json_validate(instance=obj, schema=schema)
                        payload_valid = True
                    except (json.JSONDecodeError, ValidationError):
                        payload_valid = False
                status = resp.status
                sla_violation = False
                if sla_codes and status not in sla_codes:
                    sla_violation = True
                return {
                    "ts_utc": now_iso(),
                    "endpoint": endpoint,
                    "region": region,
                    "status": status,
                    "dns": dns,
                    "connect": connect,
                    "tls": timing.get("tls", 0.0),
                    "request_send": max(0.0, request_send - start) if request_send else 0.0,
                    "server_processing": max(0.0, server_processing),
                    "total": total,
                    "content_size": len(body) if body else 0,
                    "payload_valid": payload_valid,
                    "sla_violation": sla_violation,
                    "raw_body": body[:2000]
                }
    except Exception as e:
        return {
            "ts_utc": now_iso(),
            "endpoint": endpoint,
            "region": region,
            "status": None,
            "dns": 0.0,
            "connect": 0.0,
            "tls": 0.0,
            "request_send": 0.0,
            "server_processing": 0.0,
            "total": None,
            "content_size": 0,
            "payload_valid": False,
            "sla_violation": True,
            "error": str(e),
            "raw_body": ""
        }

def send_slack_alert(webhook_url: str, text: str):
    import requests
    payload = {"text": text}
    try:
        r = requests.post(webhook_url, json=payload, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

def send_email_alert(smtp_cfg: Dict[str, Any], subject: str, body: str):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = smtp_cfg.get("from_addr")
    msg["To"] = ", ".join(smtp_cfg.get("to_addrs", []))
    msg.set_content(body)
    context = ssl.create_default_context()
    try:
        with smtplib.SMTP(smtp_cfg.get("host"), smtp_cfg.get("port", 587), timeout=10) as server:
            server.starttls(context=context)
            if smtp_cfg.get("username"):
                server.login(smtp_cfg.get("username"), smtp_cfg.get("password"))
            server.send_message(msg)
        return True
    except Exception:
        return False

def send_telegram_alert(bot_token: str, chat_id: str, text: str):
    import requests
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    try:
        r = requests.post(url, json=payload, timeout=5)
        return r.status_code == 200
    except Exception:
        return False

def send_sms_via_twilio(twilio_cfg: Dict[str, Any], body: str):
    try:
        from requests.auth import HTTPBasicAuth
        import requests
    except Exception:
        return False
    account = twilio_cfg.get("account_sid")
    token = twilio_cfg.get("auth_token")
    from_phone = twilio_cfg.get("from_phone")
    to_phone = twilio_cfg.get("to_phone")
    url = f"https://api.twilio.com/2010-04-01/Accounts/{account}/Messages.json"
    data = {"From": from_phone, "To": to_phone, "Body": body}
    try:
        r = requests.post(url, data=data, auth=HTTPBasicAuth(account, token), timeout=5)
        return r.status_code in (200,201)
    except Exception:
        return False

async def forecast_with_prophet(storage: Storage, endpoint: str, minutes_ahead=10):
    if not PROPHET_AVAILABLE:
        raise RuntimeError("Prophet not installed")
    rows = await storage.query_recent(endpoint, minutes=24*60)
    if not rows:
        return None
    import pandas as pd
    data = []
    for r in rows:
        try:
            ts = pd.to_datetime(r[0])
            total = (r[5] or 0.0) + (r[2] or 0.0) + (r[3] or 0.0) + (r[4] or 0.0) + (r[6] or 0.0)
        except Exception:
            continue
        data.append({"ds": ts, "y": total})
    if not data:
        return None
    df = pd.DataFrame(data)
    m = Prophet(daily_seasonality=True, weekly_seasonality=True)
    m.fit(df)
    future = m.make_future_dataframe(periods=minutes_ahead, freq='min')
    forecast = m.predict(future)
    pred = forecast.iloc[-minutes_ahead:][["ds", "yhat", "yhat_upper", "yhat_lower"]]
    max_pred = pred["yhat"].max()
    max_upper = pred["yhat_upper"].max()
    return {"yhat": max_pred, "yhat_upper": max_upper}

def simple_moving_forecast(storage: Storage, endpoint: str, window_minutes=60, ahead=10):
    async def _inner():
        rows = await storage.query_recent(endpoint, minutes=window_minutes)
        totals = []
        for r in rows:
            try:
                dns = r[2] or 0.0
                conn = r[3] or 0.0
                tls = r[4] or 0.0
                req = r[5] or 0.0
                srv = r[6] or 0.0
                totals.append(dns + conn + tls + req + srv)
            except Exception:
                continue
        if not totals:
            return None
        mean = statistics.mean(totals)
        stdev = statistics.stdev(totals) if len(totals) > 1 else 0.0
        return {"mean": mean, "upper": mean + 2 * stdev, "stdev": stdev}
    return _inner()

def detect_point_anomaly(recent_values: List[float], current_value: float, z_thresh=3.0):
    if not recent_values:
        return False
    mean = statistics.mean(recent_values)
    stdev = statistics.stdev(recent_values) if len(recent_values) > 1 else 0.0
    if stdev == 0:
        return abs(current_value - mean) > 0
    z = abs((current_value - mean) / stdev)
    return z > z_thresh

def compute_totals_from_row(r):
    dns = r[2] or 0.0
    conn = r[3] or 0.0
    tls = r[4] or 0.0
    req = r[5] or 0.0
    srv = r[6] or 0.0
    return dns + conn + tls + req + srv

class AgentRegistry:
    def __init__(self, path=DEFAULT_AGENT_REGISTRY):
        self.path = path
        ensure_dir_for_file(path)
        if not os.path.exists(self.path):
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump({}, f)

    def load(self):
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def save(self, data):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def register(self, region, info):
        data = self.load()
        key = region
        record = {"region": region, "info": info, "last_seen": now_iso()}
        data[key] = record
        self.save(data)

    def list_agents(self):
        return self.load()

class AutoHealer:
    def __init__(self, heal_cfg: Dict[str, Any]):
        self.heal_cfg = heal_cfg or {}

    async def attempt_heal(self, endpoint, reason):
        action = self.heal_cfg.get("action")
        if not action:
            return False
        try:
            if action.get("type") == "shell":
                cmd = action.get("cmd")
                os.system(cmd)
                return True
        except Exception:
            return False
    return False

async def controller_loop(cfg: Dict[str, Any]):
    storage = Storage(cfg.get("db", DEFAULT_DB))
    await storage.init()
    endpoints = cfg.get("endpoints", [])
    alert_cfg = cfg.get("alert", {})
    timeout = cfg.get("request_timeout", 10)
    poll_interval = cfg.get("poll_interval_seconds", 30)
    registry = AgentRegistry(cfg.get("agent_registry", DEFAULT_AGENT_REGISTRY))
    healer = AutoHealer(cfg.get("auto_heal", {}))
    semaphore = asyncio.Semaphore(cfg.get("concurrency", 8))

    async def check_one(ep):
        async with semaphore:
            schema = ep.get("schema")
            sla_codes = ep.get("sla_codes")
            res = await perform_check(None, ep["url"], ep.get("region"), schema, sla_codes, timeout)
            await storage.insert_sample({
                "ts_utc": res.get("ts_utc"),
                "endpoint": ep["url"],
                "region": ep.get("region"),
                "status": res.get("status"),
                "dns": res.get("dns"),
                "connect": res.get("connect"),
                "tls": res.get("tls"),
                "request_send": res.get("request_send"),
                "server_processing": res.get("server_processing"),
                "content_size": res.get("content_size"),
                "payload_valid": res.get("payload_valid"),
                "raw_body": res.get("raw_body")
            })
            alerts = []
            if res.get("sla_violation"):
                alerts.append(f"SLA violation for {ep['url']} status={res.get('status')} error={res.get('error','')}")
            if not res.get("payload_valid"):
                alerts.append(f"Payload schema invalid for {ep['url']}")
            fore = await simple_moving_forecast(storage, ep["url"], window_minutes=cfg.get("predict_window_minutes", 60))
            current_total = res.get("total") or 0.0
            if fore:
                upper = fore["upper"]
                if current_total > 0 and (current_total * 1000) > (cfg.get("alert_threshold_upper_ms", 500)):
                    alerts.append(f"High latency: {ep['url']} current_ms={(current_total*1000):.1f} upper_pred_ms={(upper*1000):.1f}")
                else:
                    rows = await storage.query_recent(ep["url"], minutes=cfg.get("alert_recent_minutes", 30))
                    recent = [compute_totals_from_row(r) for r in rows]
                    if detect_point_anomaly(recent, current_total, z_thresh=cfg.get("z_thresh", 3.0)):
                        alerts.append(f"Anomaly detected (z-score) for {ep['url']} current_ms={(current_total*1000):.1f}")
            if alerts:
                text = f"[{now_iso()}] Alerts for {ep['url']}:\n" + "\n".join(alerts)
                if alert_cfg.get("slack_webhook"):
                    send_slack_alert(alert_cfg.get("slack_webhook"), text)
                if alert_cfg.get("smtp"):
                    send_email_alert(alert_cfg.get("smtp"), f"API Alert: {ep['url']}", text)
                if alert_cfg.get("telegram"):
                    t = alert_cfg.get("telegram")
                    send_telegram_alert(t.get("bot_token"), t.get("chat_id"), text)
                if alert_cfg.get("twilio"):
                    send_sms_via_twilio(alert_cfg.get("twilio"), text)
                if cfg.get("auto_heal", {}).get("enabled"):
                    await healer.attempt_heal(ep["url"], alerts)
            return res

    try:
        while True:
            tasks = []
            for ep in endpoints:
                tasks.append(check_one(ep))
            await asyncio.gather(*tasks)
            await asyncio.sleep(poll_interval)
    except asyncio.CancelledError:
        pass
    finally:
        await storage.close()

async def agent_loop(cfg: Dict[str, Any]):
    timeout = cfg.get("request_timeout", 10)
    controller_url = cfg.get("controller_url")
    region = cfg.get("region", "unknown")
    registry = AgentRegistry(cfg.get("agent_registry", DEFAULT_AGENT_REGISTRY))
    registry.register(region, {"controller": controller_url})
    async with aiohttp.ClientSession() as session:
        while True:
            for ep in cfg.get("endpoints", []):
                res = await perform_check(session, ep["url"], region, ep.get("schema"), ep.get("sla_codes"), timeout)
                if controller_url:
                    try:
                        async with session.post(controller_url, json=res, timeout=5) as r:
                            pass
                    except Exception:
                        pass
                if cfg.get("db"):
                    storage = Storage(cfg["db"])
                    await storage.init()
                    await storage.insert_sample({
                        "ts_utc": res.get("ts_utc"),
                        "endpoint": ep["url"],
                        "region": region,
                        "status": res.get("status"),
                        "dns": res.get("dns"),
                        "connect": res.get("connect"),
                        "tls": res.get("tls"),
                        "request_send": res.get("request_send"),
                        "server_processing": res.get("server_processing"),
                        "content_size": res.get("content_size"),
                        "payload_valid": res.get("payload_valid"),
                        "raw_body": res.get("raw_body")
                    })
                    await storage.close()
                await asyncio.sleep(ep.get("interval_seconds", cfg.get("poll_interval_seconds", 30)))
            await asyncio.sleep(0)

def load_config(path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description="Advanced API Response Monitor")
    parser.add_argument("--mode", choices=["controller", "agent"], required=True)
    parser.add_argument("--config", required=True, help="Path to JSON config")
    args = parser.parse_args()
    cfg = load_config(args.config)
    loop = asyncio.get_event_loop()
    if args.mode == "controller":
        loop.run_until_complete(controller_loop(cfg))
    else:
        loop.run_until_complete(agent_loop(cfg))

if __name__ == "__main__":
    main()