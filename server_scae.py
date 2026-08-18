"""
server_scae.py - Pocket Option WebSocket Server
يتصل بـ Pocket Option مباشرة ويوفر بيانات الأسعار
"""

import json
import asyncio
import traceback
import time
import os
from datetime import datetime

try:
    import websockets
    from websockets.server import serve
except ImportError:
    print("pip install websockets")
    exit(1)

SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("PORT", 8765))

PO_SERVERS = [
    "wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-l.po.market/socket.io/?EIO=4&transport=websocket",
    "wss://api-spb.po.market/socket.io/?EIO=4&transport=websocket",
]

# كل الأزواج المدعومة
ALL_PAIRS = {
    # فوركس عادي
    "EURUSD": "EURUSD", "USDJPY": "USDJPY", "AUDUSD": "AUDUSD",
    "USDCAD": "USDCAD", "USDCHF": "USDCHF", "EURJPY": "EURJPY",
    "AUDJPY": "AUDJPY", "EURCHF": "EURCHF", "AUDCAD": "AUDCAD",
    "CADCHF": "CADCHF", "CADJPY": "CADJPY", "AUDCHF": "AUDCHF",
    "CHFJPY": "CHFJPY", "EURCAD": "EURCAD", "EURAUD": "EURAUD",
    # OTC
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
        self.balance = 0
        self.ssid = ""
        self.server = ""
        self._candles = {}
        self._pending = {}
        self._lock = asyncio.Lock()

    async def connect(self, ssid):
        self.ssid = ssid
        if self.ws:
            try: await self.ws.close()
            except: pass

        attempts = []  # نجمع سبب فشل كل سيرفر هنا لنرجعه للمتصفح

        for url in PO_SERVERS:
            host = url.split("//")[1].split("/")[0]
            try:
                print(f"Trying {host}...")
                ws, conn_err = await self._try_connect(url)
                if not ws:
                    attempts.append(f"{host}: تعذر فتح اتصال WebSocket ({conn_err or 'سبب غير معروف'})")
                    continue

                await ws.send("40")
                await asyncio.sleep(0.5)
                auth = {
                    "session": ssid, "isDemo": 0, "uid": 0,
                    "platform": 2, "isFastHistory": True, "isOptimized": True
                }
                await ws.send(f'42["auth",{json.dumps(auth)}]')

                ok = False
                last_event = None
                for _ in range(20):
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=2)
                        if msg == "2": await ws.send("3"); continue
                        r = self._parse(msg)
                        if r:
                            ev, d = r
                            last_event = ev
                            if ev in ("successauth", "updateBalance", "balanceUpdated"):
                                if isinstance(d, dict):
                                    b = d.get("balance", d.get("amount", 0))
                                    if b: self.balance = float(b)
                                ok = True; break
                            if ev in ("badauth", "unauthorized", "error", "connect_error"):
                                last_event = f"{ev}: {json.dumps(d)[:120]}"
                                break
                    except asyncio.TimeoutError:
                        continue

                if ok or self.balance > 0:
                    self.ws = ws
                    self.connected = True
                    self.server = host
                    print(f"✅ Connected! ${self.balance:.2f} via {host}")
                    asyncio.ensure_future(self._recv_loop())
                    asyncio.ensure_future(self._ping_loop())
                    return True, f"Connected! ${self.balance:.2f} via {host}"

                await ws.close()
                reason = last_event or "لم يصل رد successauth خلال المهلة (session خاطئ/منتهي على الأغلب)"
                attempts.append(f"{host}: {reason}")
                print(f"  {host}: auth failed - {reason}")

            except Exception as e:
                attempts.append(f"{host}: استثناء - {str(e)[:80]}")
                print(f"  {host}: {str(e)[:60]}")

        detail = " | ".join(attempts) if attempts else "لا توجد تفاصيل"
        return False, f"فشل الاتصال بكل السيرفرات :: {detail}"

    async def _try_connect(self, url, timeout=8):
        headers = [
            ("Origin", "https://po.trade"),
            ("Host", url.split("//")[1].split("/")[0]),
            ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"),
            ("Cache-Control", "no-cache"),
        ]
        last_err = None
        for kw in [{"additional_headers": headers}, {"extra_headers": headers}, {}]:
            try:
                ws = await asyncio.wait_for(
                    websockets.connect(url, ping_interval=None, close_timeout=5, **kw),
                    timeout=timeout
                )
                await asyncio.wait_for(ws.recv(), timeout=5)
                return ws, None
            except TypeError:
                continue
            except Exception as e:
                last_err = f"{type(e).__name__}: {str(e)[:150]}"
        return None, last_err

    def _parse(self, msg):
        try:
            if isinstance(msg, bytes): msg = msg.decode("utf-8", "ignore")
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
                await self.ws.send("2")
            except: break

    async def _recv_loop(self):
        while self.connected and self.ws:
            try:
                msg = await asyncio.wait_for(self.ws.recv(), timeout=40)
                if isinstance(msg, bytes): msg = msg.decode("utf-8", "ignore")
                if msg == "2": await self.ws.send("3"); continue
                if msg in ("3", "40", "41"): continue

                r = self._parse(msg)
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
                try: await self.ws.send("2")
                except: break
            except Exception as e:
                if "closed" not in str(e).lower():
                    print(f"recv: {e}")
                self.connected = False; break

    async def get_candles(self, pair, tf_sec=60, limit=200):
        if not self.connected or not self.ws:
            return None, "not connected"
        try:
            sym = f"#{pair}_otc" if not pair.startswith("#") else pair
            if "_otc" not in pair and pair in ALL_PAIRS:
                sym = pair
            req = {"asset": sym, "period": tf_sec,
                   "time": int(time.time()), "count": limit}
            async with self._lock:
                self._candles.pop(sym, None)
            ev = asyncio.Event()
            k = f"c_{sym}"
            self._pending[k] = ev
            await self.ws.send(f'42["loadHistoryPeriod",{json.dumps(req)}]')
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
                if cl > 0:
                    out.append({"t":t,"o":o,"h":h,"l":l,"c":cl,"v":v})
            except: continue
        return out

    async def get_balance(self):
        return self.balance

    async def disconnect(self):
        self.connected = False
        if self.ws:
            try: await self.ws.close()
            except: pass


po = POClient()
clients = set()


async def handle(ws):
    clients.add(ws)
    print(f"browser connected ({len(clients)})")
    try:
        async for msg in ws:
            try:
                data = json.loads(msg)
                resp = await process(data.get("action", ""), data)
                await ws.send(json.dumps(resp, ensure_ascii=False))
            except Exception as e:
                await ws.send(json.dumps({"success": False, "error": str(e)[:100]}))
    except Exception as e:
        print(f"handle: {e}")
    finally:
        clients.discard(ws)
        print(f"browser disconnected ({len(clients)})")


async def process(action, data):
    if action == "connect":
        ssid = data.get("ssid", "").strip()
        if not ssid:
            return {"success": False, "error": "empty ssid"}
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
            return {"success": False, "action": "candles",
                    "pair": pair, "error": e, "id": data.get("id")}
        return {"success": True, "action": "candles", "pair": pair,
                "count": len(c), "data": c, "id": data.get("id")}

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
                    results[pair] = {"success": True, "count": len(c),
                                     "data": c, "last_price": c[-1]["c"]}
                else:
                    results[pair] = {"success": False, "error": e or "no data"}
                await asyncio.sleep(0.2)

        await asyncio.gather(*[fp(p) for p in pairs])
        return {"success": True, "action": "scan", "results": results,
                "time": datetime.now().strftime("%H:%M:%S"), "id": data.get("id")}

    elif action == "disconnect":
        await po.disconnect()
        return {"success": True, "action": "disconnect"}

    return {"success": False, "error": "unknown action"}


async def main():
    print(f"🚀 Pocket Option Server")
    print(f"ws://0.0.0.0:{SERVER_PORT}")
    async with serve(handle, SERVER_HOST, SERVER_PORT,
                     ping_interval=30, ping_timeout=10,
                     max_size=10 * 1024 * 1024):
        print("Server ready!")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped")
    except Exception:
        traceback.print_exc()
