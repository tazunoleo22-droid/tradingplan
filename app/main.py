import os, json, sqlite3
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware

APP_KEY = os.getenv("APP_API_KEY", "")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
if SUPABASE_URL.endswith("/rest/v1"):
    SUPABASE_URL = SUPABASE_URL[:-8].rstrip("/")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
DB = os.getenv("DATABASE_PATH", "trading_capital.db")

app = FastAPI(title="Trading Capital Connector", version="6.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def auth(x_app_key: Optional[str]):
    if APP_KEY and x_app_key != APP_KEY:
        raise HTTPException(401, "Clave de app incorrecta")

def use_supabase():
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)

def sb_headers(prefer=None):
    h={"apikey":SUPABASE_SERVICE_ROLE_KEY,"Authorization":f"Bearer {SUPABASE_SERVICE_ROLE_KEY}","Content-Type":"application/json"}
    if prefer: h["Prefer"]=prefer
    return h

async def sb_get(table, query="select=*"):
    url=f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    async with httpx.AsyncClient(timeout=25) as c:
        r=await c.get(url,headers=sb_headers())
    if r.status_code!=200: raise HTTPException(502,f"Supabase {table} {r.status_code}: {r.text[:300]}")
    return r.json()

async def sb_post(table, row, prefer="return=representation", conflict=None):
    qs=f"?on_conflict={conflict}" if conflict else ""
    url=f"{SUPABASE_URL}/rest/v1/{table}{qs}"
    async with httpx.AsyncClient(timeout=25) as c:
        r=await c.post(url,headers=sb_headers(prefer),json=row)
    if r.status_code not in (200,201,204): raise HTTPException(502,f"Supabase {table} {r.status_code}: {r.text[:300]}")
    if r.status_code==204 or not r.text: return None
    return r.json()

async def sb_patch(table, query, row, prefer="return=representation"):
    url=f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    async with httpx.AsyncClient(timeout=25) as c:
        r=await c.patch(url,headers=sb_headers(prefer),json=row)
    if r.status_code not in (200,204): raise HTTPException(502,f"Supabase {table} {r.status_code}: {r.text[:300]}")
    return r.json() if r.text else None

def f(v, default=0.0):
    try: return float(v)
    except: return default

def i(v, default=0):
    try: return int(v)
    except: return default

def b(v):
    if isinstance(v,bool): return v
    if isinstance(v,str): return v.lower() in ("1","true","yes","si","sí")
    return bool(v)

def sqlite_db():
    con=sqlite3.connect(DB); con.row_factory=sqlite3.Row
    con.execute("""CREATE TABLE IF NOT EXISTS trades(
      id TEXT PRIMARY KEY, source TEXT, account_id TEXT, ticket INTEGER, broker TEXT, server TEXT,
      open_time TEXT, close_time TEXT, symbol TEXT, action TEXT, lots REAL, open_price REAL, close_price REAL,
      sl REAL, final_sl REAL, tp REAL, pips REAL, profit REAL, commission REAL, swap REAL, comment TEXT,
      magic_number INTEGER, risk_amount REAL, result_r REAL, mfe_r REAL, mae_r REAL, be_reached INTEGER,
      partial_close INTEGER, slippage_pips REAL, rule_version TEXT, strategy_tag TEXT, data_quality TEXT,
      raw TEXT, created_at TEXT, updated_at TEXT
    )""")
    con.commit(); return con

async def save_trade(row):
    if use_supabase():
        await sb_post("trades",row,"resolution=merge-duplicates,return=minimal","id")
        return
    con=sqlite_db()
    cols=list(row.keys()); vals=[json.dumps(row[k]) if k=="raw" else row[k] for k in cols]
    con.execute(f"INSERT OR REPLACE INTO trades ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)})",vals)
    con.commit(); con.close()

async def get_trades(source=None, limit=2000):
    limit=max(1,min(int(limit),5000))
    if use_supabase():
        q=f"select=*&order=close_time.desc&limit={limit}"
        if source: q+=f"&source=eq.{quote(source,safe='')}"
        return await sb_get("trades",q)
    con=sqlite_db(); q="SELECT * FROM trades"; args=[]
    if source: q+=" WHERE source=?"; args.append(source)
    q+=" ORDER BY close_time DESC LIMIT ?"; args.append(limit)
    rows=[dict(x) for x in con.execute(q,args).fetchall()]; con.close(); return rows

async def update_sync_state(source, account_id, ticket):
    if not use_supabase(): return
    row={"source":source,"account_id":account_id,"last_ticket":ticket,"last_received_at":now_iso(),"received_count":1}
    existing=await sb_get("sync_state",f"select=*&source=eq.{quote(source,safe='')}&account_id=eq.{quote(account_id,safe='')}&limit=1")
    if existing:
        row["received_count"]=i(existing[0].get("received_count"))+1
    await sb_post("sync_state",row,"resolution=merge-duplicates,return=minimal","source,account_id")

@app.get("/health")
def health():
    return {"ok":True,"service":"trading-capital-connector","version":"6.0.0","status":"awake","storage":"supabase" if use_supabase() else "sqlite-temporal"}

@app.get("/auth-check")
def auth_check(x_app_key: Optional[str]=Header(default=None)):
    auth(x_app_key); return {"ok":True,"authorized":True}

@app.post("/mt4/trade")
async def mt4_trade(payload:dict,x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    required=["ticket","close_time","symbol","action","profit"]
    missing=[k for k in required if k not in payload]
    if missing: raise HTTPException(400,"Faltan campos MT4: "+", ".join(missing))
    source=str(payload.get("source","grandcapital"))
    account_id=str(payload.get("account_id",""))
    ticket=i(payload.get("ticket"))
    ident=f"{source}:{account_id}:{ticket}"
    profit=f(payload.get("profit")); commission=f(payload.get("commission")); swap=f(payload.get("swap"))
    risk=f(payload.get("risk_amount"))
    result_r=payload.get("result_r")
    if result_r is None and risk>0: result_r=(profit+commission+swap)/risk
    row={
      "id":ident,"source":source,"account_id":account_id,"ticket":ticket,
      "broker":str(payload.get("broker","")),"server":str(payload.get("server","")),
      "open_time":payload.get("open_time"),"close_time":payload.get("close_time"),
      "symbol":str(payload.get("symbol","")),"action":str(payload.get("action","")),
      "lots":f(payload.get("lots")),"open_price":f(payload.get("open_price")),"close_price":f(payload.get("close_price")),
      "sl":f(payload.get("sl")),"final_sl":f(payload.get("final_sl")),"tp":f(payload.get("tp")),"pips":f(payload.get("pips")),
      "profit":profit,"commission":commission,"swap":swap,"comment":str(payload.get("comment","")),
      "magic_number":i(payload.get("magic_number")),"risk_amount":risk if risk>0 else None,
      "result_r":f(result_r) if result_r is not None else None,
      "mfe_r":f(payload.get("mfe_r")) if payload.get("mfe_r") is not None else None,
      "mae_r":f(payload.get("mae_r")) if payload.get("mae_r") is not None else None,
      "be_reached":b(payload.get("be_reached")),"partial_close":b(payload.get("partial_close")),
      "slippage_pips":f(payload.get("slippage_pips")) if payload.get("slippage_pips") is not None else None,
      "rule_version":str(payload.get("rule_version","")),"strategy_tag":str(payload.get("strategy_tag","")),
      "data_quality":str(payload.get("data_quality","")),"raw":payload,"created_at":now_iso(),"updated_at":now_iso()
    }
    await save_trade(row); await update_sync_state(source,account_id,ticket)
    return {"ok":True,"id":ident,"storage":"supabase" if use_supabase() else "sqlite-temporal"}

@app.get("/trades")
async def trades(source:Optional[str]=None,limit:int=2000,x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key); rows=await get_trades(source,limit)
    return {"trades":rows,"count":len(rows),"storage":"supabase" if use_supabase() else "sqlite-temporal"}

@app.get("/annotations")
async def annotations(x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    if not use_supabase(): return {"annotations":[]}
    rows=await sb_get("trade_annotations","select=*&order=updated_at.desc&limit=5000")
    return {"annotations":rows}

@app.post("/annotations")
async def save_annotation(payload:dict,x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    tid=str(payload.get("trade_id",""))
    if not tid: raise HTTPException(400,"trade_id requerido")
    row={"trade_id":tid,"execution_status":str(payload.get("execution_status","valid")),
         "include_in_system_stats":b(payload.get("include_in_system_stats",True)),
         "note":str(payload.get("note","")),"rule_version":str(payload.get("rule_version","")),
         "updated_at":now_iso()}
    if use_supabase(): await sb_post("trade_annotations",row,"resolution=merge-duplicates,return=minimal","trade_id")
    return {"ok":True}

@app.get("/audit")
async def audit(limit:int=500,x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    if not use_supabase(): return {"audit":[]}
    return {"audit":await sb_get("audit_log",f"select=*&order=created_at.desc&limit={max(1,min(limit,2000))}")}

@app.post("/audit")
async def audit_add(payload:dict,x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    row={"event_type":str(payload.get("event_type","event")),"entity_id":str(payload.get("entity_id","")),
         "old_value":payload.get("old_value"),"new_value":payload.get("new_value"),
         "note":str(payload.get("note","")),"created_at":now_iso()}
    if use_supabase(): await sb_post("audit_log",row,"return=minimal")
    return {"ok":True}

@app.get("/snapshots")
async def snapshots(x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    if not use_supabase(): return {"snapshots":[]}
    return {"snapshots":await sb_get("snapshots","select=*&order=snapshot_date.desc&limit=500")}

@app.post("/snapshots")
async def snapshot_add(payload:dict,x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    sid=str(payload.get("id",""))
    if not sid: raise HTTPException(400,"id requerido")
    row={k:payload.get(k) for k in ["id","snapshot_date","cut_type","source","account_id","balance","equity","contributions","withdrawals","trading_pnl","trades","win_rate","profit_factor","expectancy_r","dd_current","dd_max","risk_allowed","payload","locked"]}
    if use_supabase(): await sb_post("snapshots",row,"resolution=merge-duplicates,return=minimal","id")
    return {"ok":True}

@app.get("/settings")
async def settings_get(x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    if not use_supabase(): return {"settings":{}}
    rows=await sb_get("app_settings","select=*&limit=500")
    return {"settings":{x["key"]:x["value"] for x in rows}}

@app.post("/settings")
async def settings_set(payload:dict,x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    key=str(payload.get("key",""))
    if not key: raise HTTPException(400,"key requerido")
    row={"key":key,"value":payload.get("value"),"updated_at":now_iso()}
    if use_supabase(): await sb_post("app_settings",row,"resolution=merge-duplicates,return=minimal","key")
    return {"ok":True}

@app.get("/diagnostics")
async def diagnostics(x_app_key:Optional[str]=Header(default=None)):
    auth(x_app_key)
    rows=await get_trades(limit=5000)
    by_source={}
    missing_sl=missing_r=missing_comment=0
    latest=None
    for r in rows:
        s=str(r.get("source") or "unknown"); by_source[s]=by_source.get(s,0)+1
        if not f(r.get("sl")): missing_sl+=1
        if r.get("result_r") is None: missing_r+=1
        if not str(r.get("comment") or "").strip(): missing_comment+=1
        ca=r.get("created_at")
        if ca and (latest is None or ca>latest): latest=ca
    sync=[]
    if use_supabase():
        try: sync=await sb_get("sync_state","select=*&order=last_received_at.desc&limit=50")
        except: sync=[]
    return {"ok":True,"storage":"supabase" if use_supabase() else "sqlite-temporal","total_trades":len(rows),
            "by_source":by_source,"missing_initial_sl":missing_sl,"missing_result_r":missing_r,
            "missing_comment":missing_comment,"duplicates":0,"last_received":latest,"sync_state":sync}
