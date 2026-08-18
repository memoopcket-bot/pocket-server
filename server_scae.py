"""
server_scae.py - High-Efficiency Multi-Protocol Broker Engine (2026)
"""

import json
import asyncio
import traceback
import os
import http
from datetime import datetime

SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("PORT", 10000))

try:
    import websockets
    from websockets.server import serve
except ImportError:
    print("pip install websockets")
    exit(1)

# حقن كود الواجهة المصحح والمثالي داخل البايثون مباشرة لضمان عرضه
with open("index.html", "r", encoding="utf-8") as f:
    HTML_UI = f.read()

class PassiveProcessor:
    def __init__(self):
        self.connected = False
        self.balance = 0
        self._candles = {}
        self._lock = asyncio.Lock()

    async def process_piped_data(self, event_name, data):
        self.connected = True
        if event_name in ("successauth", "updateBalance", "balanceUpdated"):
            if isinstance(data, dict):
                b = data.get("balance", data.get("amount", 0))
                if b: self.balance = float(b)
            return {"success": True, "balance": self.balance}

        if event_name in ("candles", "loadHistoryPeriod", "history"):
            if isinstance(data, dict):
                sym = data.get("asset", data.get("symbol", data.get("pair", "")))
                cndls = data.get("candles", data.get("data", data.get("history", [])))
                if sym and cndls:
                    clean_pair = sym.replace("#", "")
                    async with self._lock: self._candles[clean_pair] = cndls
                    fmt_data = self._fmt(cndls, 200)
                    return {"success": True, "action": "candles", "pair": clean_pair, "count": len(fmt_data), "data": fmt_data}
        return {"success": True}

    def _fmt(self, raw, limit):
        out = []
        for c in raw[-limit:]:
            try:
                if isinstance(c, dict):
                    out.append({"t": int(c.get("time", 0)), "o": float(c.get("open", 0)), "h": float(c.get("high", 0)), "l": float(c.get("low", 0)), "c": float(c.get("close", 0))})
            except: continue
        return out
processor = PassiveProcessor()
clients = set()

# محرك معالجة طلبات الويب الفرعي لفرض عرض الواجهة الرسومية ومنع الصفحة البيضاء
def process_request(path, request_headers):
    # إذا كان الطلب القادم من المتصفح عبارة عن تصفح عادي وليس ترقية سوكيت
    if "Upgrade" not in request_headers:
        body = HTML_UI.encode("utf-8")
        return http.HTTPStatus.OK, [
            ("Content-Type", "text/html; charset=utf-8"),
            ("Content-Length", str(len(body))),
            ("Connection", "close")
        ], body
    return None

async def handle(ws):
    """
    مستقبل نفق البيانات المباشر الممرر من المتصفح (WebSocket Tunneling)
    """
    clients.add(ws)
    print(f"📱 Browser connected to Render Backend ({len(clients)})")
    try:
        async for msg in ws:
            try:
                data = json.loads(msg)
                action = data.get("action", "")
                
                if action == "pipe_data":
                    event_name = data.get("event", "")
                    payload = data.get("payload", {})
                    resp = await processor.process_piped_data(event_name, payload)
                    
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
    print(f"🚀 Multiproto Broker Engine Initialized")
    print(f"Server Route: http://0.0.0:{SERVER_PORT}")
    
    # دمج محرك الاستجابة الويب للتعرف التلقائي على نوع الاتصال بنجاح
    async with serve(handle, SERVER_HOST, SERVER_PORT,
                     process_request=process_request,
                     ping_interval=30, ping_timeout=10,
                     max_size=10 * 1024 * 1024):
        print("⚡ [Passive Backend Listening & Serving Web Page]")
        await asyncio.Future()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped")
    except Exception:
        traceback.print_exc()
