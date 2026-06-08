from __future__ import annotations

import json
import hashlib
import math
import os
import re
import smtplib
import sqlite3
import statistics
import threading
import time
import urllib.parse
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from email.message import EmailMessage
from pathlib import Path
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

import requests


AK = None
AK_ATTEMPTED = False
DATA_CACHE = {}
CACHE_TTL_SECONDS = 6 * 60 * 60
QUOTE_CACHE_TTL_SECONDS = 30
APP_DIR = Path(__file__).resolve().parent
WATCHLIST_FILE = APP_DIR / "watchlist-store.json"
DB_FILE = Path(os.environ.get("INDEX_WATCH_DB", APP_DIR / "index-watch.db"))
DB_LOCK = threading.Lock()
REFRESH_LOCK = threading.Lock()
REFRESH_RUNNING = False
BEIJING_TZ = timezone(timedelta(hours=8))


INDEXES = [
    {"code": "NDX", "name": "纳斯达克100", "displayCode": "NDX.GI", "secid": "100.NDX", "csindex": None},
    {"code": "000300", "name": "沪深300", "displayCode": "000300.CSI", "secid": "1.000300", "csindex": "000300"},
    {"code": "000016", "name": "上证50", "displayCode": "000016.SH", "secid": "1.000016", "csindex": "000016"},
    {"code": "000905", "name": "中证500", "displayCode": "000905.CSI", "secid": "1.000905", "csindex": "000905"},
    {"code": "000852", "name": "中证1000", "displayCode": "000852.CSI", "secid": "1.000852", "csindex": "000852"},
    {"code": "000100", "name": "中证100", "displayCode": "000100.CSI", "secid": "1.000100", "csindex": "000100"},
    {"code": "000906", "name": "中证800", "displayCode": "000906.CSI", "secid": "1.000906", "csindex": "000906"},
    {"code": "000010", "name": "上证180", "displayCode": "000010.SH", "secid": "1.000010", "csindex": "000010"},
    {"code": "000009", "name": "上证380", "displayCode": "000009.SH", "secid": "1.000009", "csindex": "000009"},
    {"code": "000922", "name": "中证红利", "displayCode": "000922.CSI", "secid": "1.000922", "csindex": "000922"},
    {"code": "399330", "name": "深证100", "displayCode": "399330.SZ", "secid": "0.399330", "csindex": "399330"},
    {"code": "399324", "name": "深证红利", "displayCode": "399324.SZ", "secid": "0.399324", "csindex": "399324"},
    {"code": "399997", "name": "中证白酒", "displayCode": "399997.SZ", "secid": "0.399997", "csindex": None},
    {"code": "399006", "name": "创业板指", "displayCode": "399006.SZ", "secid": "0.399006", "csindex": None},
]

ETF_RUN_ROUTES = {
    "000300": ["SSE", "CSI"],
    "000922": ["CSI", "SSE"],
    "399997": ["SZSE"],
    "399006": ["SZSE"],
}

LEGULEGU_SYMBOLS = {
    "000016": "上证50",
    "000300": "沪深300",
    "000009": "上证380",
    "399673": "创业板50",
    "000905": "中证500",
    "000010": "上证180",
    "399324": "深证红利",
    "399330": "深证100",
    "000852": "中证1000",
    "000015": "上证红利",
    "000100": "中证100",
    "000906": "中证800",
}

METRIC_KEYS = {"pe", "pb", "dy", "ps"}
FUNDDB_METRIC_MAP = {"pe": "pe", "pb": "pb", "dy": "xilv"}
FUNDDB_METRIC_LABELS = {"pe": "市盈率", "pb": "市净率", "dy": "股息率"}

FUNDDB_EXTRA_INDEXES = [
    {"guCode": "980092.CNI", "rawCode": "980092", "displayCode": "980092.CNI", "name": "\u81ea\u7531\u73b0\u91d1\u6d41", "category": "funddb-extra"},
]
ALERT_LEVEL_LABELS = {
    "gt_opportunity": "\u5927\u4e8e\u673a\u4f1a\u503c",
    "lt_opportunity": "\u5c0f\u4e8e\u673a\u4f1a\u503c",
    "gt_median": "\u5927\u4e8e\u4e2d\u4f4d\u6570",
    "lt_median": "\u5c0f\u4e8e\u4e2d\u4f4d\u6570",
    "gt_danger": "\u5927\u4e8e\u5371\u9669\u503c",
    "lt_danger": "\u5c0f\u4e8e\u5371\u9669\u503c",
    "opportunity": "\u5c0f\u4e8e\u673a\u4f1a\u503c",
    "median": "\u5927\u4e8e\u4e2d\u4f4d\u6570",
    "danger": "\u5927\u4e8e\u5371\u9669\u503c",
}
ALERT_TARGETS = {
    "gt_opportunity": ("opportunity", ">"),
    "lt_opportunity": ("opportunity", "<"),
    "gt_median": ("median", ">"),
    "lt_median": ("median", "<"),
    "gt_danger": ("danger", ">"),
    "lt_danger": ("danger", "<"),
    "opportunity": ("opportunity", "<"),
    "median": ("median", ">"),
    "danger": ("danger", ">"),
}


def wants_metric(focus_metric: str | None, metric_key: str) -> bool:
    return focus_metric not in METRIC_KEYS or focus_metric == metric_key


def get_akshare():
    global AK, AK_ATTEMPTED
    if AK_ATTEMPTED:
        return AK
    AK_ATTEMPTED = True
    try:
        import akshare as ak
        AK = ak
    except Exception:
        AK = None
    return AK


def cache_get(key: tuple, ttl_seconds: int = CACHE_TTL_SECONDS):
    item = DATA_CACHE.get(key)
    if not item:
        return None
    saved_at, value = item
    if time.time() - saved_at > ttl_seconds:
        DATA_CACHE.pop(key, None)
        return None
    return deepcopy(value)


def cache_set(key: tuple, value):
    DATA_CACHE[key] = (time.time(), deepcopy(value))
    return value


def db_connect():
    conn = sqlite3.connect(DB_FILE, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db():
    with DB_LOCK:
        with db_connect() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                CREATE TABLE IF NOT EXISTS watch_items (
                    watch_key TEXT PRIMARY KEY,
                    raw_code TEXT NOT NULL,
                    code TEXT NOT NULL,
                    name TEXT NOT NULL,
                    focus TEXT,
                    range_value TEXT,
                    sort_order INTEGER DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS watch_snapshots (
                    watch_key TEXT PRIMARY KEY,
                    payload TEXT NOT NULL,
                    updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS app_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at TEXT
                );
                CREATE TABLE IF NOT EXISTS alert_events (
                    event_key TEXT PRIMARY KEY,
                    rule_id TEXT NOT NULL,
                    condition TEXT NOT NULL,
                    as_of TEXT,
                    sent_at TEXT NOT NULL
                );
            """)


def db_get_metadata(key: str):
    init_db()
    with DB_LOCK:
        with db_connect() as conn:
            row = conn.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            return row["value"] if row else None


def db_set_metadata(conn, key: str, value):
    conn.execute(
        "INSERT INTO metadata(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, "" if value is None else str(value)),
    )


def migrate_json_watchlist_if_needed():
    if db_get_metadata("json_migrated") == "1" or not WATCHLIST_FILE.exists():
        return
    try:
        data = json.loads(WATCHLIST_FILE.read_text(encoding="utf-8"))
    except Exception:
        data = {"items": [], "snapshots": {}, "lastRefresh": None}
    save_watchlist_store(data, mark_migrated=True)


def load_watchlist_store() -> dict:
    init_db()
    if db_get_metadata("json_migrated") != "1" and WATCHLIST_FILE.exists():
        migrate_json_watchlist_if_needed()
    with DB_LOCK:
        with db_connect() as conn:
            item_rows = conn.execute(
                "SELECT raw_code, code, name, focus, range_value FROM watch_items ORDER BY sort_order, rowid"
            ).fetchall()
            snapshot_rows = conn.execute("SELECT watch_key, payload FROM watch_snapshots").fetchall()
            meta = conn.execute("SELECT value FROM metadata WHERE key='lastRefresh'").fetchone()
    items = [{
        "rawCode": row["raw_code"],
        "code": row["code"],
        "name": row["name"],
        "focus": row["focus"] or "",
        "range": row["range_value"] or "10",
    } for row in item_rows]
    snapshots = {}
    for row in snapshot_rows:
        try:
            snapshots[row["watch_key"]] = json.loads(row["payload"])
        except Exception:
            pass
    return {"items": items, "snapshots": snapshots, "lastRefresh": meta["value"] if meta else None}


def save_watchlist_store(store: dict, mark_migrated: bool = False) -> dict:
    init_db()
    items = store.get("items") or []
    snapshots = store.get("snapshots") or {}
    with DB_LOCK:
        with db_connect() as conn:
            conn.execute("DELETE FROM watch_items")
            for idx, row in enumerate(items):
                key = watch_key(row.get("rawCode") or row.get("code"), row.get("focus"))
                conn.execute(
                    "INSERT INTO watch_items(watch_key, raw_code, code, name, focus, range_value, sort_order) VALUES(?, ?, ?, ?, ?, ?, ?)",
                    (key, row.get("rawCode") or "", row.get("code") or "", row.get("name") or "", row.get("focus") or "", str(row.get("range") or "10"), idx),
                )
            conn.execute("DELETE FROM watch_snapshots")
            now = datetime.now(BEIJING_TZ).isoformat(timespec="seconds")
            for key, payload in snapshots.items():
                conn.execute(
                    "INSERT INTO watch_snapshots(watch_key, payload, updated_at) VALUES(?, ?, ?)",
                    (key, json.dumps(payload, ensure_ascii=False), payload.get("cachedAt") or now if isinstance(payload, dict) else now),
                )
            db_set_metadata(conn, "lastRefresh", store.get("lastRefresh"))
            if mark_migrated:
                db_set_metadata(conn, "json_migrated", "1")
    return store


def load_app_settings() -> dict:
    init_db()
    with DB_LOCK:
        with db_connect() as conn:
            rows = conn.execute("SELECT key, value FROM app_settings").fetchall()
    defaults = {"thresholds": {}, "axisRanges": {}, "rules": []}
    for row in rows:
        try:
            defaults[row["key"]] = json.loads(row["value"])
        except Exception:
            pass
    return defaults


def save_app_settings(settings: dict) -> dict:
    init_db()
    allowed = {"thresholds", "axisRanges", "rules"}
    now = datetime.now(BEIJING_TZ).isoformat(timespec="seconds")
    with DB_LOCK:
        with db_connect() as conn:
            for key in allowed:
                if key in settings:
                    conn.execute(
                        "INSERT INTO app_settings(key, value, updated_at) VALUES(?, ?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                        (key, json.dumps(settings[key], ensure_ascii=False), now),
                    )
    return load_app_settings()


def refresh_settings_cache_timestamp():
    init_db()
    with DB_LOCK:
        with db_connect() as conn:
            db_set_metadata(conn, "settingsLastRefresh", datetime.now(BEIJING_TZ).isoformat(timespec="seconds"))


def email_alert_config() -> dict:
    return {
        "host": os.environ.get("ALERT_EMAIL_HOST", "").strip(),
        "port": int(os.environ.get("ALERT_EMAIL_PORT", "465") or "465"),
        "user": os.environ.get("ALERT_EMAIL_USER", "").strip(),
        "password": os.environ.get("ALERT_EMAIL_PASSWORD", "").strip(),
        "sender": os.environ.get("ALERT_EMAIL_FROM", os.environ.get("ALERT_EMAIL_USER", "")).strip(),
        "to": os.environ.get("ALERT_EMAIL_TO", "694301103@qq.com").strip(),
        "tls": os.environ.get("ALERT_EMAIL_TLS", "true").lower() not in {"0", "false", "no"},
    }


def email_alert_ready() -> bool:
    cfg = email_alert_config()
    return bool(cfg["host"] and cfg["sender"] and cfg["to"])


def send_email_alert(subject: str, body: str) -> dict:
    cfg = email_alert_config()
    if not email_alert_ready():
        return {"sent": False, "error": "Email alert is not configured"}
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = cfg["sender"]
    msg["To"] = cfg["to"]
    msg.set_content(body)
    if cfg["tls"]:
        with smtplib.SMTP_SSL(cfg["host"], cfg["port"], timeout=20) as smtp:
            if cfg["user"] and cfg["password"]:
                smtp.login(cfg["user"], cfg["password"])
            smtp.send_message(msg)
    else:
        with smtplib.SMTP(cfg["host"], cfg["port"], timeout=20) as smtp:
            smtp.starttls()
            if cfg["user"] and cfg["password"]:
                smtp.login(cfg["user"], cfg["password"])
            smtp.send_message(msg)
    return {"sent": True}


def alert_event_exists(event_key: str) -> bool:
    init_db()
    with DB_LOCK:
        with db_connect() as conn:
            return conn.execute("SELECT 1 FROM alert_events WHERE event_key=?", (event_key,)).fetchone() is not None


def record_alert_event(event_key: str, rule_id: str, condition: str, as_of: str | None):
    init_db()
    with DB_LOCK:
        with db_connect() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO alert_events(event_key, rule_id, condition, as_of, sent_at) VALUES(?, ?, ?, ?, ?)",
                (event_key, str(rule_id), condition, as_of or "", datetime.now(BEIJING_TZ).isoformat(timespec="seconds")),
            )


def threshold_key_for(code: str, metric_key: str) -> str:
    return f"{code}:{metric_key}"


def metric_with_custom_thresholds(snapshot: dict, metric_key: str, settings: dict) -> dict:
    metric = deepcopy((snapshot.get("metrics") or {}).get(metric_key) or {})
    thresholds = settings.get("thresholds") or {}
    custom = (
        thresholds.get(threshold_key_for(snapshot.get("code") or "", metric_key))
        or thresholds.get(threshold_key_for(snapshot.get("rawCode") or "", metric_key))
    )
    if isinstance(custom, dict):
        for key in ("danger", "median", "opportunity"):
            value = clean_number(custom.get(key))
            if value is not None:
                metric[key] = value
    return metric


def alert_condition(metric: dict, level: str) -> tuple[bool, str, float | None, float | None]:
    target_key, op = ALERT_TARGETS.get(level, (None, None))
    current = clean_number(metric.get("current"))
    target = clean_number(metric.get(target_key)) if target_key else None
    if current is None or target is None:
        return False, ALERT_LEVEL_LABELS.get(level, level), current, target
    matched = current > target if op == ">" else current < target
    return matched, ALERT_LEVEL_LABELS.get(level, level), current, target


def evaluate_alerts_for_snapshot(snapshot: dict, settings: dict | None = None, force: bool = False) -> list[dict]:
    settings = settings or load_app_settings()
    rules = settings.get("rules") or []
    results = []
    code_keys = {snapshot.get("code"), snapshot.get("rawCode"), normalize_index_code(snapshot.get("rawCode") or snapshot.get("code") or "")}
    for rule in rules:
        if rule.get("code") not in code_keys:
            continue
        metric_key = rule.get("metric")
        if metric_key not in METRIC_KEYS:
            continue
        metric = metric_with_custom_thresholds(snapshot, metric_key, settings)
        matched, label, current, target = alert_condition(metric, rule.get("level") or "")
        result = {
            "ruleId": str(rule.get("id") or ""),
            "name": snapshot.get("name") or snapshot.get("code"),
            "code": snapshot.get("code"),
            "metric": metric.get("label") or metric_key,
            "condition": label,
            "current": current,
            "target": target,
            "matched": matched,
            "sent": False,
        }
        if matched:
            as_of = metric.get("asOf") or (snapshot.get("cachedAt") or "")[:10] or datetime.now(BEIJING_TZ).date().isoformat()
            event_key = f"{result['ruleId']}:{metric_key}:{rule.get('level')}:{as_of}"
            if force or not alert_event_exists(event_key):
                subject = f"[Index Watch] {result['name']} {result['metric']} {label}"
                body = (
                    f"{result['name']} ({result['code']})\n"
                    f"{result['metric']}: {current}\n"
                    f"{label}: {target}\n"
                    f"Date: {as_of}\n"
                    f"Cached at: {snapshot.get('cachedAt') or ''}\n"
                )
                sent = send_email_alert(subject, body)
                result.update(sent)
                if sent.get("sent"):
                    record_alert_event(event_key, result["ruleId"], rule.get("level") or "", as_of)
            else:
                result["skipped"] = "already_sent"
        results.append(result)
    return results


def watch_key(raw_code: str, focus_metric: str | None) -> str:
    focus = focus_metric if focus_metric in METRIC_KEYS else ""
    return f"{normalize_index_code(raw_code)}:{focus}"


def sanitize_watch_items(items: list[dict]) -> list[dict]:
    cleaned = []
    seen = set()
    for row in items:
        raw_code = normalize_index_code(row.get("rawCode") or row.get("code") or "")
        if not raw_code:
            continue
        focus = row.get("focus") if row.get("focus") in METRIC_KEYS else (row.get("metric") if row.get("metric") in METRIC_KEYS else "")
        key = watch_key(raw_code, focus)
        if key in seen:
            continue
        seen.add(key)
        item = resolve_index(raw_code)
        cleaned.append({
            "rawCode": item["code"],
            "code": item["displayCode"],
            "name": item["name"],
            "focus": focus,
            "range": str(row.get("range") or "10"),
        })
    return cleaned


def refresh_watch_item(row: dict, quotes: dict | None = None) -> dict | None:
    raw_code = row.get("rawCode") or row.get("code")
    focus = row.get("focus") if row.get("focus") in METRIC_KEYS else None
    range_value = str(row.get("range") or "10")
    years = None if range_value == "all" else int(range_value or 10)
    item = resolve_index(raw_code)
    if quotes is None:
        try:
            quotes = eastmoney_quotes()
        except Exception:
            quotes = {}
    quote = quotes.get(item["code"]) or {}
    payload = build_index_payload(item, years, quote, focus)
    payload["cachedAt"] = datetime.now(BEIJING_TZ).isoformat(timespec="seconds")
    payload["focus"] = focus or ""
    return payload


def refresh_watchlist_cache() -> dict:
    global REFRESH_RUNNING
    with REFRESH_LOCK:
        if REFRESH_RUNNING:
            return load_watchlist_store()
        REFRESH_RUNNING = True
    try:
        store = load_watchlist_store()
        items = sanitize_watch_items(store.get("items") or [])
        snapshots = dict(store.get("snapshots") or {})
        try:
            quotes = eastmoney_quotes()
        except Exception:
            quotes = {}
        for row in items:
            try:
                snapshot = refresh_watch_item(row, quotes)
                snapshots[watch_key(row["rawCode"], row.get("focus"))] = snapshot
                try:
                    evaluate_alerts_for_snapshot(snapshot)
                except Exception as alert_exc:
                    snapshot["alertNote"] = f"Alert check failed: {type(alert_exc).__name__}: {alert_exc}"
            except Exception as exc:
                old = (store.get("snapshots") or {}).get(watch_key(row["rawCode"], row.get("focus"))) or {}
                old["sourceNote"] = f"Scheduled refresh failed: {type(exc).__name__}: {exc}"
                snapshots[watch_key(row["rawCode"], row.get("focus"))] = old
            save_watchlist_store({
                "items": items,
                "snapshots": snapshots,
                "lastRefresh": datetime.now(BEIJING_TZ).isoformat(timespec="seconds"),
            })
        refresh_settings_cache_timestamp()
        store = {
            "items": items,
            "snapshots": snapshots,
            "lastRefresh": datetime.now(BEIJING_TZ).isoformat(timespec="seconds"),
        }
        return save_watchlist_store(store)
    finally:
        with REFRESH_LOCK:
            REFRESH_RUNNING = False


def refresh_watchlist_cache_async():
    threading.Thread(target=refresh_watchlist_cache, daemon=True).start()


def expected_refresh_date(now: datetime | None = None) -> date:
    now = now or datetime.now(BEIJING_TZ)
    expected = now.date()
    if now.hour < 20:
        expected = expected - timedelta(days=1)
    return expected


def parse_refresh_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value).astimezone(BEIJING_TZ).date()
    except Exception:
        try:
            return datetime.fromisoformat(value).date()
        except Exception:
            return None


def refresh_watchlist_if_due(async_run: bool = True) -> bool:
    store = load_watchlist_store()
    if not store.get("items"):
        return False
    last_day = parse_refresh_date(store.get("lastRefresh"))
    if last_day is not None and last_day >= expected_refresh_date():
        return False
    if async_run:
        refresh_watchlist_cache_async()
    else:
        refresh_watchlist_cache()
    return True


def watchlist_scheduler():
    while True:
        now = datetime.now(BEIJING_TZ)
        target = now.replace(hour=20, minute=0, second=0, microsecond=0)
        if target <= now:
            target += timedelta(days=1)
        time.sleep(max(60, (target - now).total_seconds()))
        refresh_watchlist_cache()


def start_watchlist_scheduler():
    refresh_watchlist_if_due(async_run=True)
    threading.Thread(target=watchlist_scheduler, daemon=True).start()


def normalize_index_code(raw_code: str) -> str:
    code = str(raw_code or "").strip()
    lower = code.lower()
    if lower.startswith(("sh", "sz", "bj")) and len(code) >= 8:
        return code[2:]
    return code.split(".")[0]


def display_code_for(raw_code: str) -> str:
    code = str(raw_code or "").strip()
    lower = code.lower()
    if lower.startswith("sh"):
        return f"{code[2:]}.SH"
    if lower.startswith("sz"):
        return f"{code[2:]}.SZ"
    if lower.startswith("bj"):
        return f"{code[2:]}.BJ"
    if code.startswith("399"):
        return f"{code}.SZ"
    if code.startswith(("000", "H", "h")):
        return f"{code.upper()}.CSI" if code.upper().startswith("H") else f"{code}.CSI"
    return code


def secid_for(raw_code: str) -> str | None:
    code = str(raw_code or "").strip().lower()
    pure = normalize_index_code(code)
    if code.startswith("sz") or pure.startswith("399"):
        return f"0.{pure}"
    if code.startswith("sh") or pure.startswith("000"):
        return f"1.{pure}"
    return None


def fetch_json(url: str, timeout: int = 12) -> dict:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*", "Accept-Encoding": "gzip, deflate", "Connection": "close"}
    last_error = None
    for _ in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            if resp.headers.get("Content-Encoding", "").lower() == "br":
                import brotli
                try:
                    return json.loads(brotli.decompress(resp.content).decode(resp.encoding or "utf-8", "replace"))
                except brotli.error:
                    return json.loads(resp.text)
            return resp.json()
        except Exception as exc:
            last_error = exc
            time.sleep(0.35)
    raise last_error


def fetch_text(url: str, timeout: int = 18) -> str:
    headers = {"User-Agent": "Mozilla/5.0", "Accept": "text/html,text/plain,*/*", "Accept-Encoding": "gzip, deflate", "Connection": "close"}
    last_error = None
    for _ in range(3):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            if resp.headers.get("Content-Encoding", "").lower() == "br":
                import brotli
                try:
                    return brotli.decompress(resp.content).decode(resp.encoding or "utf-8", "replace")
                except brotli.error:
                    return resp.text
            return resp.text
        except Exception as exc:
            last_error = exc
            time.sleep(0.35)
    raise last_error


def funddb_signed_payload(data: dict) -> dict:
    payload = dict(data)
    payload.update({"type": "pc", "version": "2.2.7", "authtoken": "", "act_time": int(time.time() * 1000)})
    raw = "".join(str(payload[key]) for key in sorted(payload) if payload.get(key) or payload.get(key) == 0)
    digest = hashlib.md5((raw + "EWf45rlv#kfsr@k#gfksgkr").encode("utf-8")).hexdigest()
    d, f, V, U, S, R, L = digest[2:4], digest[5:6], digest[24:26], digest[12:14], digest[25:26], digest[16:19], digest[16:17]
    c, h, m, v, N, y = digest[29:31], digest[26:27], digest[6:8], digest[1:2], digest[21:23], digest[0:2]
    K, D, dollar, x, A, C = digest[21:23], digest[14:16], digest[29:32], digest[30:31], digest[9:11], digest[27:29]
    T, B, k, j, I = digest[17:19], digest[18:19], digest[6:8], digest[11:14], digest[26:27]
    F, E, H, O, w, P, z, q = digest[17:21], digest[23:25], digest[31:32], digest[25:27], digest[8:9], digest[11:12], digest[2:5], digest[9:11]
    payload.update({
        "tirgkjfs": y, "abiokytke": N, "u54rg5d": d, "kf54ge7": H, "tiklsktr4": v,
        "lksytkjh": F, "sbnoywr": E, "bgd7h8tyu54": k, "y654b5fs3tr": P, "bioduytlw": f,
        "bd4uy742": I, "h67456y": R, "bvytikwqjk": m, "ngd4uy551": T, "bgiuytkw": A,
        "nd354uy4752": x, "ghtoiutkmlg": j, "bd24y6421f": V, "tbvdiuytk": L, "ibvytiqjek": D,
        "jnhf8u5231": q, "fjlkatj": z, "hy5641d321t": O, "iogojti": S, "ngd4yut78": U,
        "nkjhrew": h, "yt447e13f": w, "n3bf4uj7y7": B, "nbf4uj7y432": K, "yi854tew": c,
        "h13ey474": dollar, "quikgdky": C,
    })
    return payload


def funddb_post(path: str, data: dict) -> dict:
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://funddb.cn/site/index", "Accept": "application/json"}
    resp = requests.post(f"https://api.jiucaishuo.com{path}", json=funddb_signed_payload(data), headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def clean_number(value) -> float | None:
    if value in (None, "", "-", "N/A"):
        return None
    try:
        return float(str(value).replace("$", "").replace(",", "").replace("%", "").strip())
    except ValueError:
        return None


def normalize_date(value) -> str:
    text = str(value or "")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return text


def first_match(text: str, pattern: str) -> str | None:
    match = re.search(pattern, text, re.I)
    return match.group(1) if match else None


def empty_metric(label: str) -> dict:
    return {
        "label": label,
        "current": None,
        "percentile": None,
        "danger": None,
        "median": None,
        "opportunity": None,
        "max": None,
        "min": None,
        "stdevUp": None,
        "stdevDown": None,
        "z": None,
        "source": "No public data source connected",
        "frequency": None,
        "asOf": None,
    }


def default_metrics() -> dict:
    return {
        "pe": empty_metric("市盈率TTM"),
        "pb": empty_metric("市净率LF"),
        "dy": empty_metric("股息率"),
        "ps": empty_metric("市销率TTM"),
    }


def current_metric(label: str, current, source: str, as_of: str | None = None, frequency: str | None = None) -> dict:
    metric = empty_metric(label)
    metric.update({"current": clean_number(current), "source": source, "asOf": as_of, "frequency": frequency})
    return metric


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return math.nan
    k = (len(values) - 1) * pct / 100
    lower = math.floor(k)
    upper = math.ceil(k)
    if lower == upper:
        return values[int(k)]
    return values[lower] * (upper - k) + values[upper] * (k - lower)


def metric_from_series(label: str, series: list[dict], source: str = "CSIndex", frequency: str = "日频", high_is_good: bool = False) -> dict:
    values = [row["value"] for row in series if row.get("value") is not None]
    if not values:
        return empty_metric(label)
    current = values[-1]
    sorted_values = sorted(values)
    pct = sum(1 for value in sorted_values if value <= current) / len(sorted_values) * 100
    avg = statistics.fmean(values)
    stdev = statistics.pstdev(values) if len(values) > 1 else 0
    danger_value = percentile(sorted_values, 20 if high_is_good else 80)
    opportunity_value = percentile(sorted_values, 80 if high_is_good else 20)
    return {
        "label": label,
        "current": round(current, 2),
        "percentile": round(pct, 2),
        "danger": round(danger_value, 2),
        "median": round(percentile(sorted_values, 50), 2),
        "opportunity": round(opportunity_value, 2),
        "max": round(max(values), 2),
        "min": round(min(values), 2),
        "stdevUp": round(avg + stdev, 2),
        "stdevDown": round(avg - stdev, 2),
        "z": round((current - avg) / stdev, 2) if stdev else 0,
        "source": source,
        "frequency": frequency,
        "asOf": series[-1].get("date"),
    }


def fill_metric_stats_from_series(metric: dict, series: list[dict]) -> dict:
    values = [row["value"] for row in series if row.get("value") is not None]
    current = metric.get("current")
    if not values or current is None:
        return metric
    sorted_values = sorted(values)
    avg = statistics.fmean(values)
    stdev = statistics.pstdev(values) if len(values) > 1 else 0
    metric.update({
        "percentile": round(sum(1 for value in sorted_values if value <= current) / len(sorted_values) * 100, 2),
        "danger": round(percentile(sorted_values, 80), 2),
        "median": round(percentile(sorted_values, 50), 2),
        "opportunity": round(percentile(sorted_values, 20), 2),
        "max": round(max(values), 2),
        "min": round(min(values), 2),
        "stdevUp": round(avg + stdev, 2),
        "stdevDown": round(avg - stdev, 2),
        "z": round((current - avg) / stdev, 2) if stdev else 0,
    })
    return metric


def metric_with_stats(label: str, values: dict, source: str, as_of: str | None = None, frequency: str | None = None) -> dict:
    metric = empty_metric(label)
    metric.update({
        "current": clean_number(values.get("current")),
        "percentile": clean_number(values.get("percentile")),
        "danger": clean_number(values.get("danger")),
        "median": clean_number(values.get("median")),
        "opportunity": clean_number(values.get("opportunity")),
        "max": clean_number(values.get("max")),
        "min": clean_number(values.get("min")),
        "source": source,
        "frequency": frequency,
        "asOf": as_of,
    })
    vals = [metric[k] for k in ("current", "danger", "median", "opportunity", "max", "min") if metric[k] is not None]
    if vals:
        avg = statistics.fmean(vals)
        stdev = statistics.pstdev(vals) if len(vals) > 1 else 0
        metric["stdevUp"] = round(avg + stdev, 2)
        metric["stdevDown"] = round(avg - stdev, 2)
        metric["z"] = round((metric["current"] - avg) / stdev, 2) if stdev and metric["current"] is not None else 0
    return metric


def eastmoney_quotes() -> dict[str, dict]:
    cache_key = ("eastmoney_quotes", date.today().isoformat())
    cached = cache_get(cache_key, QUOTE_CACHE_TTL_SECONDS)
    if cached is not None:
        return cached
    secids = ",".join(item["secid"] for item in INDEXES if item.get("secid"))
    params = urllib.parse.urlencode({"fltt": "2", "fields": "f12,f14,f2,f3,f4", "secids": secids})
    data = fetch_json(f"https://push2.eastmoney.com/api/qt/ulist.np/get?{params}").get("data", {}).get("diff", [])
    return cache_set(cache_key, {
        str(row.get("f12")): {
            "price": clean_number(row.get("f2")),
            "change": clean_number(row.get("f3")),
            "changeAmount": clean_number(row.get("f4")),
            "quoteName": row.get("f14"),
        }
        for row in data
    })


def eastmoney_kline(secid: str | None, years: int | None) -> list[dict]:
    if not secid:
        return []
    cache_key = ("eastmoney_kline", secid, years)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    end = date.today()
    start = date(end.year - years, end.month, end.day) if years else date(1990, 1, 1)
    params = urllib.parse.urlencode({
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
        "klt": "101",
        "fqt": "0",
        "beg": start.strftime("%Y%m%d"),
        "end": end.strftime("%Y%m%d"),
    })
    data = fetch_json(f"http://push2his.eastmoney.com/api/qt/stock/kline/get?{params}").get("data") or {}
    rows = []
    for item in data.get("klines") or []:
        parts = item.split(",")
        if len(parts) >= 3:
            rows.append({"date": parts[0], "close": clean_number(parts[2])})
    return cache_set(cache_key, rows)


def filter_years(rows: list[dict], years: int | None) -> list[dict]:
    if not years:
        return rows
    end = date.today()
    try:
        start = date(end.year - years, end.month, end.day)
    except ValueError:
        start = date(end.year - years, end.month, 28)
    return [row for row in rows if (row.get("date") or "") >= start.isoformat()]


def csindex_pe_history(index_code: str | None, years: int | None) -> list[dict]:
    if not index_code:
        return []
    end = date.today()
    start = date(end.year - years, end.month, end.day) if years else date(1990, 1, 1)
    params = urllib.parse.urlencode({
        "indexCode": index_code,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
        "pageNum": "1",
        "pageSize": "5000",
    })
    data = fetch_json(f"https://www.csindex.com.cn/csindex-home/perf/index-perf?{params}").get("data") or []
    rows = []
    for row in data:
        value = clean_number(row.get("peg"))
        if value is not None:
            rows.append({"date": normalize_date(row.get("tradeDate")), "value": value, "close": clean_number(row.get("close"))})
    return rows


def nasdaq100_pe_history(years: int | None) -> list[dict]:
    cache_key = ("nasdaq100_pe_history", years)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    data = fetch_json("https://historyofmarket.com/api/ndx/forward-pe.json", timeout=10)
    rows = []
    for row in data.get("trailing") or []:
        value = clean_number(row.get("value"))
        row_date = normalize_date(row.get("date"))
        if value is not None:
            rows.append({"date": row_date, "value": value})
    return cache_set(cache_key, filter_years(rows, years))


def etf_run_metrics(raw_code: str) -> dict:
    cache_key = ("etf_run_metrics", raw_code)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    routes = ETF_RUN_ROUTES.get(raw_code, ["SSE", "CSI", "SZSE"])
    html = ""
    used_url = ""
    for route in routes:
        url = f"https://www.etf.run/index/{route}/{raw_code}"
        try:
            html = fetch_text(url)
            if "最新市盈率" in html or "最新市净率" in html:
                used_url = url
                break
        except Exception:
            continue
    if not html:
        return cache_set(cache_key, {})
    text = re.sub(r"\s+", "", html)
    as_of = first_match(text, r"更新至(\d{4}/\d{2}/\d{2})") or first_match(text, r"更新时间：(\d{4}-\d{2}-\d{2})")
    pe_values = {
        "current": first_match(text, r"最新市盈率</span><span[^>]*>([\d.]+)</span>"),
        "max": first_match(text, r"最高市盈率</span><span[^>]*>([\d.]+)</span>"),
        "min": first_match(text, r"最低市盈率</span><span[^>]*>([\d.]+)</span>"),
        "percentile": first_match(text, r"当前分位<!-- -->([\d.]+)%"),
        "opportunity": first_match(text, r"20%分位<!-- -->([\d.]+)"),
        "median": first_match(text, r"50%分位<!-- -->([\d.]+)"),
        "danger": first_match(text, r"80%分位<!-- -->([\d.]+)"),
    }
    pb_text = ("最新市净率" + text.split("最新市净率", 1)[1]) if "最新市净率" in text else text
    pb_values = {
        "current": first_match(pb_text, r"最新市净率</span><span[^>]*>([\d.]+)</span>"),
        "max": first_match(pb_text, r"最高市净率</span><span[^>]*>([\d.]+)</span>"),
        "min": first_match(pb_text, r"最低市净率</span><span[^>]*>([\d.]+)</span>"),
        "percentile": first_match(pb_text, r"当前分位<!-- -->([\d.]+)%"),
        "opportunity": first_match(pb_text, r"20%分位<!-- -->([\d.]+)"),
        "median": first_match(pb_text, r"50%分位<!-- -->([\d.]+)"),
        "danger": first_match(pb_text, r"80%分位<!-- -->([\d.]+)"),
    }
    return cache_set(cache_key, {
        "pe": metric_with_stats("市盈率TTM", pe_values, used_url or "ETF.run", as_of, "日频/网页统计"),
        "pb": metric_with_stats("市净率LF", pb_values, used_url or "ETF.run", as_of, "日频/网页统计"),
        "_daily": parse_etf_run_daily(html),
    })


def parse_etf_run_daily(html: str) -> dict:
    text = html.replace('\\"', '"')
    marker = '"compressedIndexDaily":'
    start = text.find(marker)
    if start < 0:
        return {"pe": [], "pb": [], "price": []}
    obj_start = text.find("{", start)
    if obj_start < 0:
        return {"pe": [], "pb": [], "price": []}
    depth = 0
    in_string = False
    escape = False
    obj_end = None
    for i in range(obj_start, len(text)):
        ch = text[i]
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"':
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                obj_end = i + 1
                break
    if obj_end is None:
        return {"pe": [], "pb": [], "price": []}
    try:
        payload = json.loads(text[obj_start:obj_end])
    except Exception:
        return {"pe": [], "pb": [], "price": []}
    fields = payload.get("fieldNames") or []
    values = payload.get("values") or []
    index = {name: i for i, name in enumerate(fields)}
    pe_rows, pb_rows, price_rows = [], [], []
    for row in values:
        if not row or "date" not in index:
            continue
        day = row[index["date"]]
        close = row[index["close"]] if "close" in index and len(row) > index["close"] else None
        pe = row[index["equalWeightedPeTtm"]] if "equalWeightedPeTtm" in index and len(row) > index["equalWeightedPeTtm"] else None
        pb = row[index["equalWeightedPbTtm"]] if "equalWeightedPbTtm" in index and len(row) > index["equalWeightedPbTtm"] else None
        if clean_number(pe) is not None:
            pe_rows.append({"date": day, "value": clean_number(pe), "close": clean_number(close)})
        if clean_number(pb) is not None:
            pb_rows.append({"date": day, "value": clean_number(pb), "close": clean_number(close)})
        if clean_number(close) is not None:
            price_rows.append({"date": day, "close": clean_number(close)})
    return {"pe": pe_rows, "pb": pb_rows, "price": price_rows}


def nasdaq100_metrics() -> dict:
    text = fetch_text("https://r.jina.ai/http://r.jina.ai/http://https://vcpscanner.com/market-valuation/nasdaq-100", timeout=30)
    as_of = first_match(text, r"as of ([0-9]{4}-[0-9]{2}-[0-9]{2})")
    return {
        "pe": current_metric("市盈率TTM", first_match(text, r"Nasdaq 100 P/E ratio is ([\d.]+)"), "VCP Scanner via Jina Reader", as_of, "当前值"),
        "pb": current_metric("市净率LF", first_match(text, r"P/B\s*([\d.]+)"), "VCP Scanner via Jina Reader", as_of, "当前值"),
        "dy": current_metric("股息率", first_match(text, r"Dividend Yield\s*([\d.]+)%"), "VCP Scanner via Jina Reader", as_of, "当前值"),
        "ps": current_metric("市销率TTM", first_match(text, r"P/S\s*([\d.]+)"), "VCP Scanner via Jina Reader", as_of, "当前值"),
    }


def akshare_search_indices(keyword: str) -> list[dict]:
    ak = get_akshare()
    if ak is None:
        return []
    needle = keyword.lower()
    rows = []
    try:
        df = ak.stock_zh_index_spot_sina()
    except Exception:
        return []
    for _, row in df.iterrows():
        raw = str(row.get("代码") or row.get("code") or "")
        name = str(row.get("名称") or row.get("name") or "")
        if not raw or not name:
            continue
        if needle not in raw.lower() and needle not in name.lower():
            continue
        pure = normalize_index_code(raw)
        rows.append(basic_index_payload(
            name,
            pure,
            display_code_for(raw),
            clean_number(row.get("最新价")),
            clean_number(row.get("涨跌幅")),
        ))
    return rows


def akshare_csindex_value(index_code: str | None, years: int | None) -> dict:
    cache_key = ("akshare_csindex_value", index_code, years)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    ak = get_akshare()
    if ak is None or not index_code:
        return {"metrics": {}, "series": {"pe": [], "dy": []}}
    try:
        df = ak.stock_zh_index_value_csindex(symbol=str(index_code).upper())
    except Exception:
        return cache_set(cache_key, {"metrics": {}, "series": {"pe": [], "dy": []}})
    if df is None or getattr(df, "empty", True):
        return cache_set(cache_key, {"metrics": {}, "series": {"pe": [], "dy": []}})
    pe_rows, dy_rows = [], []
    name = None
    for _, row in df.iterrows():
        day = normalize_date(row.get("日期"))
        name = name or row.get("指数中文简称") or row.get("指数中文全称")
        pe = clean_number(row.get("市盈率2") if row.get("市盈率2") is not None else row.get("市盈率1"))
        dy = clean_number(row.get("股息率2") if row.get("股息率2") is not None else row.get("股息率1"))
        if pe is not None:
            pe_rows.append({"date": day, "value": pe})
        if dy is not None:
            dy_rows.append({"date": day, "value": dy})
    pe_rows = sorted(filter_years(pe_rows, years), key=lambda x: x["date"])
    dy_rows = sorted(filter_years(dy_rows, years), key=lambda x: x["date"])
    metrics = {}
    if pe_rows:
        metrics["pe"] = metric_from_series("市盈率TTM", pe_rows)
        metrics["pe"]["source"] = "AKShare stock_zh_index_value_csindex"
    if dy_rows:
        metrics["dy"] = metric_from_series("股息率", dy_rows, high_is_good=True)
        metrics["dy"]["source"] = "AKShare stock_zh_index_value_csindex"
    return cache_set(cache_key, {"name": name, "metrics": metrics, "series": {"pe": pe_rows, "dy": dy_rows}})


def akshare_legulegu_daily(raw_code: str, years: int | None) -> dict:
    cache_key = ("akshare_legulegu_daily", raw_code, years)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    ak = get_akshare()
    symbol = LEGULEGU_SYMBOLS.get(normalize_index_code(raw_code))
    if ak is None or not symbol:
        return {"metrics": {}, "series": {"pe": [], "pb": []}}

    metrics = {}
    pe_rows, pb_rows = [], []

    try:
        pe_df = ak.stock_index_pe_lg(symbol=symbol)
        if pe_df is not None and not getattr(pe_df, "empty", True):
            for _, row in pe_df.iterrows():
                day = normalize_date(row.get("日期"))
                value = clean_number(row.get("滚动市盈率"))
                if day and value is not None:
                    pe_rows.append({"date": day, "value": value})
    except Exception:
        pe_rows = []

    try:
        pb_df = ak.stock_index_pb_lg(symbol=symbol)
        if pb_df is not None and not getattr(pb_df, "empty", True):
            for _, row in pb_df.iterrows():
                day = normalize_date(row.get("日期"))
                value = clean_number(row.get("市净率"))
                if day and value is not None:
                    pb_rows.append({"date": day, "value": value})
    except Exception:
        pb_rows = []

    pe_rows = sorted(filter_years(pe_rows, years), key=lambda x: x["date"])
    pb_rows = sorted(filter_years(pb_rows, years), key=lambda x: x["date"])
    if pe_rows:
        metrics["pe"] = metric_from_series("市盈率TTM", pe_rows, "AKShare stock_index_pe_lg / 乐咕乐股", "日频")
    if pb_rows:
        metrics["pb"] = metric_from_series("市净率LF", pb_rows, "AKShare stock_index_pb_lg / 乐咕乐股", "日频")
    return cache_set(cache_key, {"metrics": metrics, "series": {"pe": pe_rows, "pb": pb_rows}})


def funddb_index_records() -> list[dict]:
    cache_key = ("funddb_index_records",)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    data = funddb_post("/v2/guzhi/showcategory", {"category_id": ""})
    rows = data.get("data", {}).get("right_list", []) if data.get("code") == 0 else []
    records = []
    records.extend(deepcopy(FUNDDB_EXTRA_INDEXES))
    for row in rows:
        gu_code = str(row.get("gu_code") or "")
        name = str(row.get("gu_name") or "")
        if not gu_code:
            continue
        pure = normalize_index_code(gu_code)
        records.append({
            "guCode": gu_code,
            "rawCode": pure,
            "displayCode": gu_code,
            "name": name or gu_code,
            "category": str(row.get("category") or row.get("category_name") or ""),
        })
    return cache_set(cache_key, records)


def funddb_index_map() -> dict:
    cache_key = ("funddb_index_map",)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    result = {}
    for record in funddb_index_records():
        for key in (record.get("rawCode"), record.get("displayCode"), record.get("guCode"), record.get("name")):
            if key:
                result[str(key)] = record
                result[normalize_index_code(str(key))] = record
    return cache_set(cache_key, result)


def funddb_record_for(raw_code: str) -> dict | None:
    mapping = funddb_index_map()
    for key in (raw_code, normalize_index_code(raw_code), display_code_for(normalize_index_code(raw_code))):
        if key in mapping:
            return mapping[key]
    return None


def funddb_code_for(item: dict) -> str | None:
    mapping = funddb_index_map()
    for key in (item.get("displayCode"), item.get("code"), item.get("name"), normalize_index_code(item.get("code"))):
        if key in mapping:
            return mapping[key].get("guCode")
    return None


def funddb_search_indices(keyword: str) -> list[dict]:
    needle = keyword.lower().strip()
    if not needle:
        return []
    aliases = {
        "ndx": ["ndx", "nasdaq", "\u7eb3\u65af\u8fbe\u514b", "\u7eb3\u6307"],
        "nasdaq": ["ndx", "nasdaq", "\u7eb3\u65af\u8fbe\u514b", "\u7eb3\u6307"],
    }
    needles = {needle}
    for key, values in aliases.items():
        if key in needle or any(value in needle for value in values):
            needles.update(values)
    results = []
    for record in funddb_index_records():
        haystacks = [
            str(record.get("name") or "").lower(),
            str(record.get("rawCode") or "").lower(),
            str(record.get("displayCode") or "").lower(),
            str(record.get("guCode") or "").lower(),
        ]
        if any(n in h for n in needles for h in haystacks):
            results.append(basic_index_payload(record["name"], record["rawCode"], record["displayCode"], None, None))
    return results


def funddb_metric_history(item: dict, metric_key: str, years: int | None) -> dict:
    if metric_key not in FUNDDB_METRIC_MAP:
        return {"metric": empty_metric(default_metrics()[metric_key]["label"]), "series": [], "price": []}
    cache_key = ("funddb_metric_history", item.get("code"), metric_key, years)
    cached = cache_get(cache_key)
    if cached is not None:
        return cached
    gu_code = funddb_code_for(item)
    if not gu_code:
        return {"metric": empty_metric(default_metrics()[metric_key]["label"]), "series": [], "price": []}
    year = -1 if years is None else years
    data = funddb_post("/v2/guzhi/newtubiaolinedata", {
        "gu_code": gu_code,
        "pe_category": FUNDDB_METRIC_MAP[metric_key],
        "year": year,
        "ver": "new",
    })
    tubiao = data.get("data", {}).get("tubiao", {}) if data.get("code") == 0 else {}
    metric_name = FUNDDB_METRIC_LABELS[metric_key]
    metric_rows, price_rows = [], []
    for series in tubiao.get("series") or []:
        name = str(series.get("name") or "")
        rows = series.get("data") or []
        if metric_name in name:
            for ts, value in rows:
                value = clean_number(value)
                if value is not None:
                    day = datetime.fromtimestamp(int(ts) / 1000, BEIJING_TZ).date().isoformat()
                    metric_rows.append({"date": day, "value": value})
        elif "收盘价" in name:
            for ts, value in rows:
                value = clean_number(value)
                if value is not None:
                    day = datetime.fromtimestamp(int(ts) / 1000, BEIJING_TZ).date().isoformat()
                    price_rows.append({"date": day, "close": value})
    metric_rows = sorted(filter_years(metric_rows, years), key=lambda x: x["date"])
    price_rows = sorted(filter_years(price_rows, years), key=lambda x: x["date"])
    label = default_metrics()[metric_key]["label"]
    metric = metric_from_series(label, metric_rows, f"funddb/韭圈儿 {gu_code}", "日频", high_is_good=(metric_key == "dy"))
    return cache_set(cache_key, {"metric": metric, "series": metric_rows, "price": price_rows})


def eastmoney_search_indices(keyword: str) -> list[dict]:
    groups = ["b:MK0010", "m:1+t:1", "m:0+t:5", "m:2"]
    needle = keyword.lower()
    results = []
    for fs in groups:
        params = urllib.parse.urlencode({
            "pn": "1",
            "pz": "500",
            "po": "1",
            "np": "1",
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": "2",
            "invt": "2",
            "fid": "f12",
            "fs": fs,
            "fields": "f2,f3,f4,f12,f13,f14",
        })
        try:
            rows = fetch_json(f"https://48.push2.eastmoney.com/api/qt/clist/get?{params}").get("data", {}).get("diff", [])
        except Exception:
            rows = []
        for row in rows:
            code = str(row.get("f12") or "")
            name = str(row.get("f14") or "")
            if needle not in code.lower() and needle not in name.lower():
                continue
            market = int(row.get("f13") or 1)
            results.append(basic_index_payload(name, code, f"{code}.{'SZ' if market == 0 else 'CSI'}", clean_number(row.get("f2")), clean_number(row.get("f3"))))
    return results


def nfin_search_indices(keyword: str) -> list[dict]:
    try:
        rows = fetch_json(f"https://api.nfin.dev/v1/search?{urllib.parse.urlencode({'query': keyword})}").get("data", {}).get("data", [])
    except Exception:
        rows = []
    results = []
    for row in rows:
        if str(row.get("asset", "")).upper() != "INDEX":
            continue
        symbol = row.get("symbol") or ""
        name = "纳斯达克100" if symbol == "NDX" else (row.get("name") or symbol)
        display = "NDX.GI" if symbol == "NDX" else symbol
        results.append(basic_index_payload(name, symbol, display, None, None))
    return results


def basic_index_payload(name: str, raw_code: str, display_code: str, price, change) -> dict:
    return {
        "name": name,
        "code": display_code,
        "rawCode": raw_code,
        "price": price,
        "change": change,
        "metrics": default_metrics(),
        "series": {"price": [], "pe": [], "pb": [], "dy": [], "ps": []},
        "sourceNote": "Search result",
    }


def resolve_index(raw_code: str) -> dict:
    raw_code = normalize_index_code(raw_code)
    funddb_record = funddb_record_for(raw_code)
    if funddb_record:
        return {
            "code": funddb_record["rawCode"],
            "name": funddb_record["name"],
            "displayCode": funddb_record["displayCode"],
            "secid": secid_for(funddb_record["rawCode"]),
            "csindex": funddb_record["rawCode"] if funddb_record["rawCode"].startswith(("000", "H", "h")) else None,
        }
    if raw_code == "NDX" or raw_code == "NDX.GI":
        return INDEXES[0]
    found = next((row for row in INDEXES if raw_code in (row["code"], row["displayCode"])), None)
    if found:
        return found
    return {
        "code": raw_code,
        "name": raw_code,
        "displayCode": display_code_for(raw_code),
        "secid": secid_for(raw_code),
        "csindex": raw_code if raw_code.startswith(("000", "H", "h")) else None,
    }


def longer_series(primary: list[dict], fallback: list[dict]) -> list[dict]:
    return primary if len(primary or []) >= len(fallback or []) else fallback


def build_index_payload(item: dict, years: int | None, quote: dict | None, focus_metric: str | None = None) -> dict:
    focus_metric = focus_metric if focus_metric in METRIC_KEYS else None
    errors = []
    try:
        price_series = eastmoney_kline(item.get("secid"), years)
    except Exception as exc:
        price_series = []
        errors.append(f"Eastmoney history unavailable: {type(exc).__name__}")
    pe_series = []
    if wants_metric(focus_metric, "pe"):
        try:
            pe_series = csindex_pe_history(item.get("csindex"), years)
        except Exception as exc:
            errors.append(f"CSIndex PE unavailable: {type(exc).__name__}")

    metrics = default_metrics()
    etf_daily = {"pe": [], "pb": [], "price": []}
    pb_series = []
    dy_series = []
    if item["code"] == "NDX":
        funddb_price_series = []
        for key in ("pe", "pb", "dy"):
            if wants_metric(focus_metric, key):
                try:
                    value = funddb_metric_history(item, key, years)
                    if value.get("series"):
                        if key == "pe":
                            pe_series = value["series"]
                        elif key == "pb":
                            pb_series = value["series"]
                        elif key == "dy":
                            dy_series = value["series"]
                        metrics[key] = value["metric"]
                        if value.get("price") and len(value["price"]) > len(funddb_price_series):
                            funddb_price_series = value["price"]
                except Exception as exc:
                    errors.append(f"funddb {key} unavailable: {type(exc).__name__}")
        need_ndx_current = (
            focus_metric is None
            or (wants_metric(focus_metric, "pe") and not pe_series)
            or (wants_metric(focus_metric, "pb") and not pb_series)
            or (wants_metric(focus_metric, "dy") and not dy_series)
            or wants_metric(focus_metric, "ps")
        )
        try:
            if need_ndx_current:
                ndx_metrics = nasdaq100_metrics()
                if not pe_series:
                    metrics["pe"] = ndx_metrics.get("pe", metrics["pe"])
                if not pb_series:
                    metrics["pb"] = ndx_metrics.get("pb", metrics["pb"])
                if not dy_series:
                    metrics["dy"] = ndx_metrics.get("dy", metrics["dy"])
                metrics["ps"] = ndx_metrics.get("ps", metrics["ps"])
        except Exception as exc:
            errors.append(f"Nasdaq valuation unavailable: {type(exc).__name__}")
        if wants_metric(focus_metric, "pe") and not pe_series:
            try:
                pe_series = nasdaq100_pe_history(years)
                metrics["pe"] = fill_metric_stats_from_series(metrics["pe"], pe_series)
                if pe_series:
                    metrics["pe"]["source"] = "History of Market trailing PE daily sample"
                    metrics["pe"]["frequency"] = "日频样本"
                    metrics["pe"]["asOf"] = pe_series[-1].get("date")
            except Exception as exc:
                errors.append(f"Nasdaq PE history unavailable: {type(exc).__name__}")
        if not price_series and funddb_price_series:
            price_series = funddb_price_series
    else:
        if wants_metric(focus_metric, "pe"):
            metrics["pe"] = metric_from_series("市盈率TTM", pe_series)
        funddb_price_series = []
        for key in ("pe", "pb", "dy"):
            if wants_metric(focus_metric, key):
                try:
                    value = funddb_metric_history(item, key, years)
                    if value.get("series"):
                        if key == "pe":
                            pe_series = value["series"]
                        elif key == "pb":
                            pb_series = value["series"]
                        elif key == "dy":
                            dy_series = value["series"]
                        metrics[key] = value["metric"]
                        if value.get("price") and len(value["price"]) > len(funddb_price_series):
                            funddb_price_series = value["price"]
                except Exception as exc:
                    errors.append(f"funddb {key} unavailable: {type(exc).__name__}")
        if wants_metric(focus_metric, "pe") or wants_metric(focus_metric, "pb"):
            lg_value = akshare_legulegu_daily(item["code"], years)
            if wants_metric(focus_metric, "pe") and not pe_series and lg_value.get("series", {}).get("pe"):
                pe_series = longer_series(lg_value["series"]["pe"], pe_series)
                metrics["pe"] = lg_value["metrics"]["pe"]
            if wants_metric(focus_metric, "pb") and not pb_series and lg_value.get("series", {}).get("pb"):
                pb_series = lg_value["series"]["pb"]
                metrics["pb"] = lg_value["metrics"]["pb"]
        if (wants_metric(focus_metric, "pe") and not pe_series) or (wants_metric(focus_metric, "dy") and not dy_series):
            ak_value = akshare_csindex_value(item.get("csindex") or item["code"], years)
            if wants_metric(focus_metric, "pe") and not pe_series and ak_value.get("series", {}).get("pe") and len(ak_value["series"]["pe"]) > len(pe_series):
                pe_series = ak_value["series"]["pe"]
                metrics["pe"] = ak_value["metrics"]["pe"]
            if wants_metric(focus_metric, "dy") and not dy_series and ak_value.get("series", {}).get("dy"):
                dy_series = ak_value["series"]["dy"]
                metrics["dy"] = ak_value["metrics"]["dy"]
        if wants_metric(focus_metric, "pe") or wants_metric(focus_metric, "pb"):
            try:
                etf_metrics = etf_run_metrics(item["code"])
                etf_daily = etf_metrics.pop("_daily", etf_daily)
                for key, value in etf_metrics.items():
                    if wants_metric(focus_metric, key) and (key not in metrics or metrics[key].get("current") is None):
                        metrics[key] = value
            except Exception as exc:
                errors.append(f"ETF.run valuation unavailable: {type(exc).__name__}")

        if not price_series and funddb_price_series:
            price_series = funddb_price_series

    if wants_metric(focus_metric, "pe") and not pe_series and etf_daily.get("pe") and len(filter_years(etf_daily["pe"], years)) > len(pe_series):
        pe_series = filter_years(etf_daily["pe"], years)
        metrics["pe"] = metric_from_series("市盈率TTM", pe_series, "ETF.run compressedIndexDaily", "日频")
    if wants_metric(focus_metric, "pb") and not pb_series and etf_daily.get("pb") and len(filter_years(etf_daily["pb"], years)) > len(pb_series):
        pb_series = filter_years(etf_daily["pb"], years)
        metrics["pb"] = metric_from_series("市净率LF", pb_series, "ETF.run compressedIndexDaily", "日频")
    if wants_metric(focus_metric, "pe"):
        metrics["pe"] = fill_metric_stats_from_series(metrics["pe"], pe_series)
    if wants_metric(focus_metric, "pb"):
        metrics["pb"] = fill_metric_stats_from_series(metrics["pb"], pb_series)
    if wants_metric(focus_metric, "dy"):
        metrics["dy"] = fill_metric_stats_from_series(metrics["dy"], dy_series)
    if not price_series and etf_daily.get("price"):
        price_series = filter_years(etf_daily["price"], years)

    latest_price = quote.get("price") if quote else None
    if latest_price is None and price_series:
        latest_price = price_series[-1]["close"]

    return {
        "name": item["name"],
        "code": item["displayCode"],
        "rawCode": item["code"],
        "price": latest_price,
        "change": quote.get("change") if quote else None,
        "metrics": metrics,
        "series": {"pe": pe_series, "price": price_series, "pb": pb_series, "dy": dy_series, "ps": []},
        "sourceNote": "; ".join(errors) if errors else "Quote/history: Eastmoney/AKShare; valuation: AKShare, CSIndex, ETF.run, VCP where available.",
    }


class Handler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Cache-Control", "no-store")
        super().end_headers()

    def do_GET(self):
        try:
            refresh_watchlist_if_due(async_run=True)
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/":
                self.path = "/index-valuation-watch.html"
                return super().do_GET()
            if parsed.path == "/api/search":
                return self.write_json(self.handle_search(urllib.parse.parse_qs(parsed.query)))
            if parsed.path == "/api/indices":
                return self.write_json(self.handle_indices())
            if parsed.path == "/api/index":
                return self.write_json(self.handle_index(urllib.parse.parse_qs(parsed.query)))
            if parsed.path == "/api/watchlist":
                return self.write_json(self.handle_watchlist())
            if parsed.path == "/api/settings":
                return self.write_json(self.handle_settings())
            super().do_GET()
        except Exception as exc:
            return self.write_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def do_POST(self):
        try:
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == "/api/watchlist":
                return self.write_json(self.handle_watchlist_post())
            if parsed.path == "/api/settings":
                return self.write_json(self.handle_settings_post())
            if parsed.path == "/api/test-alerts":
                return self.write_json(self.handle_test_alerts())
            return self.write_json({"ok": False, "error": "Unknown POST endpoint"})
        except Exception as exc:
            return self.write_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def handle_search(self, query: dict) -> dict:
        keyword = (query.get("q") or [""])[0].strip()
        rows = []
        if keyword:
            lower_keyword = keyword.lower()
            try:
                rows.extend(funddb_search_indices(keyword))
            except Exception:
                pass
            for item in INDEXES:
                if lower_keyword in item["name"].lower() or lower_keyword in item["code"].lower() or lower_keyword in item["displayCode"].lower():
                    rows.append(basic_index_payload(item["name"], item["code"], item["displayCode"], None, None))
            if any(term in keyword.lower() for term in ["ndx", "nasdaq", "纳斯达克", "纳指"]):
                rows.append(basic_index_payload("纳斯达克100", "NDX", "NDX.GI", None, None))
            if not rows:
                rows.extend(akshare_search_indices(keyword))
            if not rows:
                rows.extend(nfin_search_indices(keyword))
                rows.extend(eastmoney_search_indices(keyword))
        seen = set()
        unique = []
        for row in rows:
            key = row["rawCode"]
            if key not in seen:
                seen.add(key)
                unique.append(row)
        return {"ok": True, "data": unique[:30]}

    def handle_indices(self) -> dict:
        try:
            quotes = eastmoney_quotes()
        except Exception:
            quotes = {}
        rows = []
        for item in INDEXES:
            q = quotes.get(item["code"]) or {}
            rows.append(basic_index_payload(item["name"], item["code"], item["displayCode"], q.get("price"), q.get("change")))
        return {"ok": True, "data": rows}

    def handle_index(self, query: dict) -> dict:
        raw_code = (query.get("code") or ["000300"])[0]
        range_value = (query.get("range") or ["10"])[0]
        focus_metric = (query.get("focus") or [None])[0]
        years = None if range_value == "all" else int(range_value or 10)
        item = resolve_index(raw_code)
        try:
            quotes = eastmoney_quotes()
        except Exception:
            quotes = {}
        quote = quotes.get(item["code"]) or {}
        return {"ok": True, "data": build_index_payload(item, years, quote, focus_metric)}

    def handle_watchlist(self) -> dict:
        store = load_watchlist_store()
        items = sanitize_watch_items(store.get("items") or [])
        snapshots = store.get("snapshots") or {}
        rows = []
        for row in items:
            snapshot = snapshots.get(watch_key(row["rawCode"], row.get("focus")))
            if snapshot:
                rows.append(snapshot)
            else:
                base = basic_index_payload(row["name"], row["rawCode"], row["code"], None, None)
                base["focus"] = row.get("focus") or ""
                rows.append(base)
        return {"ok": True, "data": {"items": items, "snapshots": rows, "lastRefresh": store.get("lastRefresh")}}

    def handle_settings(self) -> dict:
        return {"ok": True, "data": load_app_settings()}

    def handle_watchlist_post(self) -> dict:
        length = int(self.headers.get("Content-Length") or "0")
        payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        store = load_watchlist_store()
        items = sanitize_watch_items(payload.get("items") or [])
        old_snapshots = store.get("snapshots") or {}
        store = {
            "items": items,
            "snapshots": {watch_key(row["rawCode"], row.get("focus")): old_snapshots.get(watch_key(row["rawCode"], row.get("focus"))) for row in items if old_snapshots.get(watch_key(row["rawCode"], row.get("focus")))},
            "lastRefresh": store.get("lastRefresh"),
        }
        save_watchlist_store(store)
        refreshed = refresh_watchlist_cache()
        return {"ok": True, "data": {"items": refreshed.get("items") or items, "lastRefresh": refreshed.get("lastRefresh")}}

    def handle_settings_post(self) -> dict:
        length = int(self.headers.get("Content-Length") or "0")
        payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        settings = save_app_settings({
            "thresholds": payload.get("thresholds", {}),
            "axisRanges": payload.get("axisRanges", {}),
            "rules": payload.get("rules", []),
        })
        return {"ok": True, "data": settings}

    def handle_test_alerts(self) -> dict:
        store = load_watchlist_store()
        settings = load_app_settings()
        results = []
        for snapshot in (store.get("snapshots") or {}).values():
            if isinstance(snapshot, dict):
                results.extend(evaluate_alerts_for_snapshot(snapshot, settings, force=True))
        return {
            "ok": True,
            "data": {
                "emailConfigured": email_alert_ready(),
                "results": results,
            },
        }

    def write_json(self, payload: dict):
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8787"))
    start_watchlist_scheduler()
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    print(f"Index valuation app: http://0.0.0.0:{port}/index-valuation-watch.html")
    server.serve_forever()
