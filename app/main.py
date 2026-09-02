import os, json, sqlite3, hashlib, threading
from datetime import datetime
from typing import Optional
import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware

BASE = "https://www.myfxbook.com/api"
DB = os.getenv("DATABASE_PATH","trading_capital.db")
EMAIL = os.getenv("MYFXBOOK_EMAIL","")
PASSWORD = os.getenv("MYFXBOOK_PASSWORD","")
APP_KEY = os.getenv("APP_API_KEY","")
SESSION = {"value":None}
LOCK = threading.Lock()

app = FastAPI(title="Trading Capital Connector", version="4.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def auth(x_app_key: Optional[str]):
    if APP_KEY and x_app_key != APP_KEY:
        raise HTTPException(401,"Clave de app incorrecta")

def db():
    con=sqlite3.connect(DB)
    con.row_factory=sqlite3.Row
    con.execute("""CREATE TABLE IF NOT EXISTS trades(
      id TEXT PRIMARY KEY, source TEXT NOT NULL, account_id TEXT,
      open_time TEXT, close_time TEXT, symbol TEXT, action TEXT, lots REAL,
      open_price REAL, close_price REAL, sl REAL, tp REAL, pips REAL,
      profit REAL, commission REAL, swap REAL, raw TEXT, created_at TEXT
    )""")
    con.commit()
    return con

def fp(t, account_id):
    s="|".join(str(t.get(k,"")) for k in ["openTime","closeTime","symbol","action","openPrice","closePrice","profit"])
    s += "|"+str(t.get("sizing",{}).get("value",""))+"|"+str(account_id)
    return hashlib.sha256(s.encode()).hexdigest()

async def call(name, params=None):
    """Login y llamada Myfxbook usando el MISMO cliente/conexión HTTP."""
    if not EMAIL or not PASSWORD:
        raise HTTPException(500,"Faltan MYFXBOOK_EMAIL/MYFXBOOK_PASSWORD en el servidor")

    params=dict(params or {})
    limits=httpx.Limits(max_connections=1,max_keepalive_connections=1)

    async with httpx.AsyncClient(
        timeout=30,
        limits=limits,
        follow_redirects=True,
        headers={"User-Agent":"TradingCapitalConnector/2.0","Accept":"application/json"}
    ) as c:
        # Login
        r=await c.get(
            f"{BASE}/login.json",
            params={"email":EMAIL,"password":PASSWORD}
        )
        r.raise_for_status()
        login_data=r.json()

        if login_data.get("error"):
            raise HTTPException(
                502,
                "Myfxbook login: "+str(login_data.get("message","Error"))
            )

        session=login_data.get("session")
        if not session:
            raise HTTPException(502,"Myfxbook no devolvió una sesión")

        # Llamada inmediatamente con la misma conexión/IP
        params["session"]=session
        r2=await c.get(f"{BASE}/{name}.json",params=params)
        r2.raise_for_status()
        d=r2.json()

        if d.get("error"):
            raise HTTPException(
                502,
                "Myfxbook "+name+": "+str(d.get("message","Error"))
            )

        try:
            await c.get(f"{BASE}/logout.json",params={"session":session})
        except Exception:
            pass

        return d

@app.get("/health")
def health():
    return {
        "ok": True,
        "service": "trading-capital-connector",
        "version": "4.0.0",
        "status": "awake"
    }

@app.get("/auth-check")
def auth_check(x_app_key: Optional[str]=Header(default=None)):
    auth(x_app_key)
    return {"ok": True, "authorized": True}

@app.get("/myfxbook/accounts")
async def accounts(x_app_key: Optional[str]=Header(default=None)):
    auth(x_app_key)
    d=await call("get-my-accounts")
    return {"accounts":[{"id":a.get("id"),"name":a.get("name"),"accountId":a.get("accountId"),
                         "balance":a.get("balance"),"gain":a.get("gain"),"drawdown":a.get("drawdown"),
                         "profitFactor":a.get("profitFactor"),"lastUpdateDate":a.get("lastUpdateDate")}
                        for a in d.get("accounts",[])]}

@app.post("/myfxbook/sync/{account_id}")
async def sync(account_id: str, x_app_key: Optional[str]=Header(default=None)):
    auth(x_app_key)
    d=await call("get-history",{"id":account_id})
    hist=d.get("history",[])
    con=db(); imported=0
    for t in hist:
        ident=fp(t,account_id)
        sizing=t.get("sizing") or {}
        vals=(ident,"myfxbook",account_id,t.get("openTime"),t.get("closeTime"),t.get("symbol"),t.get("action"),
              float(sizing.get("value") or 0),float(t.get("openPrice") or 0),float(t.get("closePrice") or 0),
              float(t.get("sl") or 0),float(t.get("tp") or 0),float(t.get("pips") or 0),
              float(t.get("profit") or 0),float(t.get("commission") or 0),
              float(t.get("interest") or t.get("swap") or 0),json.dumps(t),datetime.utcnow().isoformat())
        cur=con.execute("""INSERT OR IGNORE INTO trades
          (id,source,account_id,open_time,close_time,symbol,action,lots,open_price,close_price,sl,tp,pips,profit,commission,swap,raw,created_at)
          VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",vals)
        imported += cur.rowcount
    con.commit()
    total=con.execute("SELECT COUNT(*) n FROM trades WHERE source='myfxbook' AND account_id=?",(account_id,)).fetchone()["n"]
    con.close()
    return {"ok":True,"received":len(hist),"imported":imported,"total":total}

@app.get("/trades")
def trades(source: Optional[str]=None, x_app_key: Optional[str]=Header(default=None)):
    auth(x_app_key)
    con=db()
    q="SELECT * FROM trades"; args=[]
    if source: q+=" WHERE source=?"; args=[source]
    q+=" ORDER BY close_time DESC, created_at DESC LIMIT 500"
    rows=[dict(r) for r in con.execute(q,args).fetchall()]
    con.close()
    for r in rows: r.pop("raw",None)
    return {"trades":rows}

@app.get("/validation")
def validation(x_app_key: Optional[str]=Header(default=None)):
    auth(x_app_key)
    con=db(); rows=[dict(r) for r in con.execute("SELECT * FROM trades WHERE source IN ('myfxbook','validacion')").fetchall()]; con.close()
    if not rows: return {"trades":0,"winRate":None,"profitFactor":None,"net":0}
    pnl=[float(r["profit"] or 0)+float(r["commission"] or 0)+float(r["swap"] or 0) for r in rows]
    wins=sum(1 for x in pnl if x>0); gp=sum(x for x in pnl if x>0); gl=abs(sum(x for x in pnl if x<0))
    return {"trades":len(rows),"winRate":wins/len(rows)*100,"profitFactor":gp/gl if gl else None,"net":sum(pnl)}

# Endpoint already reserved for Phase 2 / MT4 EA
@app.post("/mt4/trade")
def mt4_trade(payload: dict, x_app_key: Optional[str]=Header(default=None)):
    auth(x_app_key)
    required=["ticket","close_time","symbol","action","profit"]
    if any(k not in payload for k in required): raise HTTPException(400,"Faltan campos MT4")
    ident="mt4:"+str(payload["ticket"])
    con=db()
    con.execute("""INSERT OR REPLACE INTO trades
    (id,source,account_id,open_time,close_time,symbol,action,lots,open_price,close_price,sl,tp,pips,profit,commission,swap,raw,created_at)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
    (ident,str(payload.get("source","grandcapital")),str(payload.get("account_id","")),payload.get("open_time"),payload.get("close_time"),
     payload.get("symbol"),payload.get("action"),float(payload.get("lots",0)),float(payload.get("open_price",0)),
     float(payload.get("close_price",0)),float(payload.get("sl",0)),float(payload.get("tp",0)),float(payload.get("pips",0)),
     float(payload.get("profit",0)),float(payload.get("commission",0)),float(payload.get("swap",0)),
     json.dumps(payload),datetime.utcnow().isoformat()))
    con.commit(); con.close()
    return {"ok":True,"id":ident}
