import json
import asyncio
import traceback
import time
import os
import logging
import re
from datetime import datetime
from pathlib import Path
from aiohttp import web, WSMsgType

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("pocket-server")

BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"
PORT = int(os.environ.get("PORT", 10000))

PO_SERVERS = [
    "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-l.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-spb.po.market/socket.io/?EIO=4&transport=websocket",
]

ALL_PAIRS = {
    "EURUSD": "EURUSD", "USDJPY": "USDJPY", "AUDUSD": "AUDUSD",
    "USDCAD": "USDCAD", "USDCHF": "USDCHF", "EURJPY": "EURJPY",
    "AUDJPY": "AUDJPY", "EURCHF": "EURCHF", "AUDCAD": "AUDCAD",
    "CADCHF": "CADCHF", "CADJPY": "CADJPY", "AUDCHF": "AUDCHF",
    "CHFJPY": "CHFJPY", "EURCAD": "EURCAD", "EURAUD": "EURAUD",
    "EURUSD_otc": "EURUSD_otc", "USDJPY_otc": "USDJPY_otc",
    "AUDUSD_otc": "AUDUSD_otc", "USDCAD_otc": "USDCAD_otc",
    "USDCHF_otc": "USDCHF_otc", "EURJPY_otc": "EURJPY_otc",
    "AUDJPY_otc": "AUDJPY_otc", "AUDCAD_otc": "AUDCAD_otc",
    "CADCHF_otc": "CADCHF_otc", "CADJPY_otc": "CADJPY_otc",
    "AUDCHF_otc": "AUDCHF_otc", "CHFJPY_otc": "CHFJPY_otc",
    "EURCHF_otc": "EURCHF_otc", "EURCAD_otc": "EURCAD_otc",
    "EURAUD_otc": "EURAUD_otc",
}

class POClient:
    def __init__(self):
        self.ws = None
        self.connected = False
        self.balance = 0.0
        self.ssid = ""
        self.server = ""
        self._candles = {}
        self._pending = {}
        self._lock = asyncio.Lock()

    async def connect(self, raw_input):
        import aiohttp
        # محرك الفرز التلقائي المستحدث للـ SSID وحزم الـ JSON الطويلة
        clean_ssid = raw_input.strip()
        if '"session"' in clean_ssid or "auth" in clean_ssid:
            match = re.search(r'"session"\s*:\s*"([^"]+)"', clean_ssid)
            if match:
                clean_ssid = match.group(1)
            else:
                match_ssid = re.search(r'([a-f0-9]{32})', clean_ssid)
                if match_ssid: clean_ssid = match_ssid.group(1)

        self.ssid = clean_ssid
        if len(self.ssid) != 32:
            return False, f"Invalid SSID extracted: Length is {len(self.ssid)}, must be 32"

        if self.ws:
            try: await self.ws.close()
            except: pass

        for url in PO_SERVERS:
            host = url.split("//")[1].split("/")[0]
            try:
                logger.info(f"Trying {host} with SSID: {self.ssid[:6]}...")
                headers = {
                    "Origin": "https://po.trade",
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                }
                session = aiohttp.ClientSession()
                ws = await session.ws_connect(url, headers=headers, timeout=10)
                
                await ws.receive_str()
                await ws.send_str("40")
                await asyncio.sleep(0.5)
                
                auth = {
                    "session": self.ssid, "isDemo": 0, "uid": 0,
                    "platform": 2, "isFastHistory": True, "isOptimized": True
                }
                await ws.send_str(f'42["auth",{json.dumps(auth)}]')

                ok = False
                for _ in range(20):
                    try:
                        msg = await ws.receive(timeout=2)
                        if msg.type == WSMsgType.TEXT:
                            data_str = msg.data
                            if data_str == "2": 
                                await ws.send_str("3")
                                continue
                            r = self._parse(data_str)
                            if r:
                                ev, d = r
                                if ev in ("successauth", "updateBalance", "balanceUpdated"):
                                    if isinstance(d, dict):
                                        b = d.get("balance", d.get("amount", 0))
                                        if b: self.balance = float(b)
                                    ok = True; break
                    except asyncio.TimeoutError:
                        continue

                if ok or self.balance > 0:
                    self.ws = ws
                    self.connected = True
                    self.server = host
                    logger.info(f"Connected! ${self.balance:.2f} via {host}")
                    asyncio.create_task(self._recv_loop())
                    asyncio.create_task(self._ping_loop())
                    return True, f"Connected! ${self.balance:.2f}"

                await ws.close()
                await session.close()
                logger.warning(f"{host}: auth failed")
            except Exception as e:
                logger.error(f"{host}: {str(e)[:60]}")

        return False, "Auth failed on all PO servers. Check your session."

    def _parse(self, msg):
        try:
            for p in ("42", "451-"):
                if msg.startswith(p):
                    d = json.loads(msg[len(p):])
                    if isinstance(d, list) and len(d) >= 2:
                        return d[0], d[1]
        except: pass
        return None

    async def _ping_loop(self):
        while self.connected and self.ws:
            try:
                await asyncio.sleep(20)
                await self.ws.send_str("2")
            except: break
    async def _recv_loop(self):
        while self.connected and self.ws:
            try:
                msg = await self.ws.receive(timeout=40)
                if msg.type == WSMsgType.TEXT:
                    raw_data = msg.data
                    if raw_data == "2": 
                        await self.ws.send_str("3")
                        continue
                    if raw_data in ("3", "40", "41"): continue

                    r = self._parse(raw_data)
                    if not r: continue
                    ev, d = r

                    if ev in ("updateBalance", "successauth", "balanceUpdated"):
                        if isinstance(d, dict):
                            b = d.get("balance", d.get("amount", 0))
                            if b: self.balance = float(b)

                    if ev in ("candles", "loadHistoryPeriod", "history"):
                        sym = ""
                        cndls = []
                        if isinstance(d, dict):
                            sym = d.get("asset", d.get("symbol", d.get("pair", "")))
                            cndls = d.get("candles", d.get("data", d.get("history", [])))
                        if sym and cndls:
                            async with self._lock:
                                self._candles[sym] = cndls
                            k = f"c_{sym}"
                            if k in self._pending:
                                self._pending[k].set()
            except asyncio.TimeoutError:
                try: await self.ws.send_str("2")
                except: break
            except:
                self.connected = False; break

    async def get_candles(self, pair, tf_sec=60, limit=200):
        if not self.connected or not self.ws:
            return None, "not connected"
        try:
            sym = pair
            req = {"asset": sym, "period": tf_sec, "time": int(time.time()), "count": limit}
            async with self._lock:
                self._candles.pop(sym, None)
            ev = asyncio.Event()
            k = f"c_{sym}"
            self._pending[k] = ev
            await self.ws.send_str(f'42["loadHistoryPeriod",{json.dumps(req)}]')
            try:
                await asyncio.wait_for(ev.wait(), timeout=12)
            except asyncio.TimeoutError:
                self._pending.pop(k, None)
                return None, f"timeout ({pair})"
            self._pending.pop(k, None)
            async with self._lock:
                raw = self._candles.get(sym, [])
            if not raw:
                return None, "no data"
            return self._fmt(raw, limit), None
        except Exception as e:
            return None, str(e)[:80]

    def _fmt(self, raw, limit):
        out = []
        for c in raw[-limit:]:
            try:
                if isinstance(c, (list, tuple)) and len(c) >= 5:
                    t,o,h,l,cl = int(c[0]),float(c[1]),float(c[2]),float(c[3]),float(c[4])
                    v = float(c[5]) if len(c) > 5 else 1.0
                elif isinstance(c, dict):
                    t = int(c.get("time", c.get("t", 0)))
                    o = float(c.get("open", c.get("o", 0)))
                    h = float(c.get("high", c.get("h", o)))
                    l = float(c.get("low",  c.get("l", o)))
                    cl= float(c.get("close",c.get("c", 0)))
                    v = float(c.get("volume",c.get("v", 1)))
                else: continue
                if cl > 0: out.append({"t":t,"o":o,"h":h,"l":l,"c":cl,"v":v})
            except: continue
        return out

po = POClient()
clients = set()

async def process(action, data):
    if action == "connect":
        ssid = data.get("ssid", "").strip()
        if not ssid: return {"success": False, "error": "empty ssid"}
        ok, msg = await po.connect(ssid)
        return {"success": ok, "action": "connect", "message": msg,
                "balance": po.balance, "connected": ok, "server": po.server}
                
    elif action == "status":
        return {"success": True, "action": "status",
                "connected": po.connected, "balance": po.balance,
                "server": po.server, "time": datetime.now().strftime("%H:%M:%S")}
                
    elif action == "candles":
        pair = data.get("pair", "EURUSD")
        tf   = int(data.get("tf", 1))
        lim  = int(data.get("limit", 200))
        c, e = await po.get_candles(pair, tf * 60, lim)
        if e:
            return {"success": False, "action": "candles", "pair": pair, "error": e, "id": data.get("id")}
        return {"success": True, "action": "candles", "pair": pair, "count": len(c), "data": c, "id": data.get("id")}

    elif action == "scan":
        pairs = data.get("pairs", list(ALL_PAIRS.keys()))
        tf    = int(data.get("tf", 1))
        lim   = int(data.get("limit", 200))
        results = {}
        sem = asyncio.Semaphore(3)

        async def fp(pair):
            async with sem:
                c, e = await po.get_candles(pair, tf * 60, lim)
                if c:
                    results[pair] = {"success": True, "count": len(c), "data": c, "last_price": c[-1]["c"]}
                else:
                    results[pair] = {"success": False, "error": e or "no data"}
                await asyncio.sleep(0.2)

        await asyncio.gather(*[fp(p) for p in pairs])
        return {"success": True, "action": "scan", "results": results, "time": datetime.now().strftime("%H:%M:%S"), "id": data.get("id")}

    elif action == "disconnect":
        await po.disconnect()
        return {"success": True, "action": "disconnect"}

    return {"success": False, "error": "unknown action"}

async def browser_ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    try:
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    resp = await process(data.get("action", ""), data)
                    await ws.send_str(json.dumps(resp, ensure_ascii=False))
                except Exception as e:
                    await ws.send_str(json.dumps({"success": False, "error": str(e)[:100]}))
    except Exception as e:
        logger.error(f"Browser handler error: {e}")
    finally:
        clients.discard(ws)
    return ws

async def home_handler(request):
    if not INDEX_FILE.exists():
        return web.Response(text="index.html not found", status=500)
    return web.FileResponse(INDEX_FILE)

app = web.Application()
app.router.add_get("/", home_handler)
app.router.add_get("/ws", browser_ws_handler)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
