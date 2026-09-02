import os, json, hashlib, sqlite3
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware

APP_KEY = os.getenv("APP_API_KEY", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
# Acepta tanto https://proyecto.supabase.co como .../rest/v1
if SUPABASE_URL.endswith("/rest/v1"):
    SUPABASE_URL = SUPABASE_URL[:-8].rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
DB = os.getenv("DATABASE_PATH", "trading_capital.db")

app = FastAPI(title="Trading Capital Connector", version="5.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def auth(x_app_key: Optional[str]):
    if APP_KEY and x_app_key != APP_KEY:
        raise HTTPException(401, "Clave de app incorrecta")

def use_supabase():
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

def sb_headers(prefer=None):
    h = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h

def sqlite_db():
    con = sqlite3.connect(DB)
    con.row_factory = sqlite3.Row
    con.execute("""CREATE TABLE IF NOT EXISTS trades(
      id TEXT PRIMARY KEY, source TEXT NOT NULL, account_id TEXT, ticket INTEGER,
      broker TEXT, server TEXT, open_time TEXT, close_time TEXT, symbol TEXT,
      action TEXT, lots REAL, open_price REAL, close_price REAL, sl REAL,
      final_sl REAL, tp REAL, pips REAL, profit REAL, commission REAL, swap REAL,
      comment TEXT, magic_number INTEGER, raw TEXT, created_at TEXT
    )""")
    con.commit()
    return con

async def save_trade(row: dict):
    if use_supabase():
        url = f"{SUPABASE_URL}/rest/v1/trades?on_conflict=id"
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.post(
                url,
                headers=sb_headers("resolution=merge-duplicates,return=minimal"),
                json=row
            )
        if r.status_code not in (200, 201, 204):
            raise HTTPException(502, f"Supabase error {r.status_code}: {r.text[:300]}")
        return

    con = sqlite_db()
    cols = [
        "id","source","account_id","ticket","broker","server","open_time","close_time",
        "symbol","action","lots","open_price","close_price","sl","final_sl","tp","pips",
        "profit","commission","swap","comment","magic_number","raw","created_at"
    ]
    vals = [row.get(k) for k in cols]
    con.execute(
        f"INSERT OR REPLACE INTO trades ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",
        vals
    )
    con.commit()
    con.close()

async def get_trades(source=None, limit=500):
    limit = max(1, min(int(limit), 2000))
    if use_supabase():
        params = [f"select=*", "order=close_time.desc", f"limit={limit}"]
        if source:
            params.append(f"source=eq.{source}")
        url = f"{SUPABASE_URL}/rest/v1/trades?" + "&".join(params)
        async with httpx.AsyncClient(timeout=25) as c:
            r = await c.get(url, headers=sb_headers())
        if r.status_code != 200:
            raise HTTPException(502, f"Supabase error {r.status_code}: {r.text[:300]}")
        return r.json()

    con = sqlite_db()
    q = "SELECT * FROM trades"
    args = []
    if source:
        q += " WHERE source=?"
        args.append(source)
    q += " ORDER BY close_time DESC, created_at DESC LIMIT ?"
    args.append(limit)
    rows = [dict(r) for r in con.execute(q, args).fetchall()]
    con.close()
    return rows

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "trading-capital-connector",
        "version": "5.1.0",
        "status": "awake",
        "storage": "supabase" if use_supabase() else "sqlite-temporal"
    }

@app.get("/auth-check")
def auth_check(x_app_key: Optional[str] = Header(default=None)):
    auth(x_app_key)
    return {"ok": True, "authorized": True}

@app.get("/storage-status")
async def storage_status(x_app_key: Optional[str] = Header(default=None)):
    auth(x_app_key)
    rows = await get_trades(limit=1)
    return {
        "ok": True,
        "storage": "supabase" if use_supabase() else "sqlite-temporal",
        "supabase_configured": use_supabase(),
        "reachable": True
    }

@app.post("/mt4/trade")
async def mt4_trade(payload: dict, x_app_key: Optional[str] = Header(default=None)):
    auth(x_app_key)

    required = ["ticket", "close_time", "symbol", "action", "profit"]
    missing = [k for k in required if k not in payload]
    if missing:
        raise HTTPException(400, "Faltan campos MT4: " + ", ".join(missing))

    source = str(payload.get("source", "grandcapital"))
    account_id = str(payload.get("account_id", ""))
    ticket = int(payload["ticket"])
    ident = f"{source}:{account_id}:{ticket}"

    row = {
        "id": ident,
        "source": source,
        "account_id": account_id,
        "ticket": ticket,
        "broker": str(payload.get("broker", "")),
        "server": str(payload.get("server", "")),
        "open_time": payload.get("open_time"),
        "close_time": payload.get("close_time"),
        "symbol": payload.get("symbol"),
        "action": payload.get("action"),
        "lots": float(payload.get("lots", 0) or 0),
        "open_price": float(payload.get("open_price", 0) or 0),
        "close_price": float(payload.get("close_price", 0) or 0),
        "sl": float(payload.get("sl", 0) or 0),
        "final_sl": float(payload.get("final_sl", 0) or 0),
        "tp": float(payload.get("tp", 0) or 0),
        "pips": float(payload.get("pips", 0) or 0),
        "profit": float(payload.get("profit", 0) or 0),
        "commission": float(payload.get("commission", 0) or 0),
        "swap": float(payload.get("swap", 0) or 0),
        "comment": str(payload.get("comment", "")),
        "magic_number": int(payload.get("magic_number", 0) or 0),
        "raw": payload if use_supabase() else json.dumps(payload),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    await save_trade(row)
    return {"ok": True, "id": ident, "storage": "supabase" if use_supabase() else "sqlite-temporal"}

@app.get("/trades")
async def trades(source: Optional[str] = None,
                 limit: int = 500,
                 x_app_key: Optional[str] = Header(default=None)):
    auth(x_app_key)
    rows = await get_trades(source=source, limit=limit)
    return {"trades": rows, "count": len(rows), "storage": "supabase" if use_supabase() else "sqlite-temporal"}

@app.get("/validation")
async def validation(x_app_key: Optional[str] = Header(default=None)):
    auth(x_app_key)
    rows = await get_trades(source="validacion", limit=2000)
    if not rows:
        return {"trades": 0, "winRate": None, "profitFactor": None, "net": 0}

    pnl = [
        float(r.get("profit") or 0)
        + float(r.get("commission") or 0)
        + float(r.get("swap") or 0)
        for r in rows
    ]
    wins = sum(1 for x in pnl if x > 0)
    gp = sum(x for x in pnl if x > 0)
    gl = abs(sum(x for x in pnl if x < 0))

    return {
        "trades": len(rows),
        "winRate": wins / len(rows) * 100,
        "profitFactor": gp / gl if gl else None,
        "net": sum(pnl)
    }
