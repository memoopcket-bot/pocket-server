"""
server_scae.py - Passive Listening & Data Processor Engine (2026)
خادم صامت مستمع للبيانات الممررة من المتصفح البشري لحل الحظر نهائياً
"""

import json
import asyncio
import traceback
import os
from datetime import datetime

SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("PORT", 10000))  # المنفذ الإجباري لمنصة ريندر

try:
    import websockets
    from websockets.server import serve
except ImportError:
    print("pip install websockets")
    exit(1)

# فصل دقيق لأزواج العملات العالمية والـ OTC لمنع تداخل الرموز
ALL_PAIRS = {
    "EURUSD": "EURUSD", "USDJPY": "USDJPY", "AUDUSD": "AUDUSD",
    "USDCAD": "USDCAD", "USDCHF": "USDCHF", "EURJPY": "EURJPY",
    "EURUSD_otc": "EURUSD_otc", "USDJPY_otc": "USDJPY_otc",
    "AUDUSD_otc": "AUDUSD_otc", "USDCAD_otc": "USDCAD_otc",
}

class PassiveProcessor:
    def __init__(self):
        self.connected = False
        self.balance = 0
        self._candles = {}
        self._lock = asyncio.Lock()

    async def process_piped_data(self, event_name, data):
        """
        معالجة وفرز البيانات اللحظية القادمة من المتصفح البشري مباشرة
        """
        self.connected = True
        
        # تحديث الرصيد عند استقبال أحداث الحساب
        if event_name in ("successauth", "updateBalance", "balanceUpdated"):
            if isinstance(data, dict):
                b = data.get("balance", data.get("amount", 0))
                if b: 
                    self.balance = float(b)
            return {"success": True, "event": event_name, "balance": self.balance}

        # استقبال وفرز شموع الأسعار اللحظية والتاريخية
        if event_name in ("candles", "loadHistoryPeriod", "history"):
            sym = ""
            cndls = []
            if isinstance(data, dict):
                sym = data.get("asset", data.get("symbol", data.get("pair", "")))
                cndls = data.get("candles", data.get("data", data.get("history", [])))
            
            if sym and cndls:
                # تنظيف اسم الزوج لعرضه بشكل صحيح في الواجهة
                clean_pair = sym.replace("#", "").replace("_otc", "")
                if sym.startswith("#") and sym.endswith("_otc"):
                    clean_pair = f"{clean_pair}_otc"
                
                async with self._lock:
                    self._candles[clean_pair] = cndls
                
                formatted_data = self._fmt(cndls, 200)
                return {
                    "success": True, 
                    "action": "candles", 
                    "pair": clean_pair, 
                    "count": len(formatted_data), 
                    "data": formatted_data
                }
        
        return {"success": True, "event": event_name, "info": "Data logged successfully"}

    def _fmt(self, raw, limit):
        out = []
        for c in raw[-limit:]:
            try:
                if isinstance(c, (list, tuple)) and len(c) >= 5:
                    t, o, h, l, cl = int(c), float(c), float(c), float(c), float(c)
                    v = float(c) if len(c) > 5 else 1.0
                elif isinstance(c, dict):
                    t = int(c.get("time", c.get("t", 0)))
                    o = float(c.get("open", c.get("o", 0)))
                    h = float(c.get("high", c.get("h", o)))
                    l = float(c.get("low",  c.get("l", o)))
                    cl = float(c.get("close", c.get("c", 0)))
                    v = float(c.get("volume", c.get("v", 1)))
                else: 
                    continue
                if cl > 0:
                    out.append({"t": t, "o": o, "h": h, "l": l, "c": cl, "v": v})
            except: 
                continue
        return out
processor = PassiveProcessor()
clients = set()

async def handle(ws):
    clients.add(ws)
    print(f"📱 Browser connected to Render Backend ({len(clients)})")
    try:
        async for msg in ws:
            try:
                data = json.loads(msg)
                action = data.get("action", "")
                
                # استقبال البيانات الممررة (Piped Data) من المتصفح البشري وإعادتها مفرزة
                if action == "pipe_data":
                    event_name = data.get("event", "")
                    payload = data.get("payload", {})
                    resp = await processor.process_piped_data(event_name, payload)
                    
                    # إذا كانت البيانات تحتوي على شموع مفرزة بنجاح، نرسلها فوراً للواجهة لعرضها
                    if resp and resp.get("action") == "candles":
                        await ws.send(json.dumps(resp, ensure_ascii=False))
                        
                elif action == "status":
                    await ws.send(json.dumps({
                        "success": True, 
                        "action": "status",
                        "connected": processor.connected, 
                        "balance": processor.balance,
                        "time": datetime.now().strftime("%H:%M:%S")
                    }, ensure_ascii=False))
                    
                elif action == "disconnect":
                    processor.connected = False
                    await ws.send(json.dumps({"success": True, "action": "disconnect"}))
                    
            except Exception as e:
                await ws.send(json.dumps({"success": False, "error": str(e)[:100]}))
    except Exception as e:
        print(f"⚠️ handle_error: {e}")
    finally:
        clients.discard(ws)
        print(f"📱 Browser disconnected from Render Backend ({len(clients)})")

async def main():
    print(f"🚀 Passive Pocket Option Server Engine Initialized")
    print(f"ws://0.0.0.0:{SERVER_PORT}")
    async with serve(handle, SERVER_HOST, SERVER_PORT,
                     ping_interval=30, ping_timeout=10,
                     max_size=10 * 1024 * 1024):
        print("⚡ [Passive Backend Listening & Awaiting Piped Data From Client-Side]")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped")
    except Exception:
        traceback.print_exc()
