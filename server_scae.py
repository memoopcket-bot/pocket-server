import asyncio
import json
import logging
import os
import time
from pathlib import Path
from aiohttp import web, ClientSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger("pocket-server")

PORT = int(os.getenv("PORT", "10000"))
BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"

prices = {}
state = {
    "connected": False,
    "last_error": None,
    "last_update": None,
    "started_at": int(time.time()),
}

ws_client = None
ping_task = None
receive_task = None

def json_response(data, status=200):
    return web.json_response(data, status=status, dumps=lambda v: json.dumps(v, ensure_ascii=False))

async def home(request):
    if not INDEX_FILE.exists():
        return web.Response(text="index.html not found on server", status=500)
    return web.FileResponse(INDEX_FILE)

async def get_pairs(request):
    return json_response({
        "ok": True,
        "connected": state["connected"],
        "prices": prices,
        "updated_at": state["last_update"]
    })

async def connect_provider(request):
    global ws_client, ping_task, receive_task
    try:
        body = await request.json()
    except:
        return json_response({"ok": False, "error": "Invalid JSON"}, status=400)
    
    auth_token = body.get("auth", "").strip()
    if not auth_token:
        return json_response({"ok": False, "error": "SSID token is required"}, status=400)
    
    # التغليف التلقائي الذكي للـ SSID القصير إذا لم يمرر المستخدم الحزمة كاملة
    if "auth" not in auth_token and len(auth_token) == 32:
        payload = f'42["auth",{{\"session\":\"{auth_token}\",\"isDemo\":0}}]'
    else:
        payload = auth_token

    try:
        await disconnect_all()
        session = ClientSession()
        # الاتصال المباشر بخادم البث الخاص بالمنصة
        ws_client = await session.ws_connect("wss://://pocketoption.com")
        
        # إرسال حزمة التوثيق فوراً
        await ws_client.send_str(payload)
        state["connected"] = True
        state["last_error"] = None
        state["last_update"] = int(time.time())
        
        # بدء مهام الخلفية للاستقبال والـ Ping-Pong
        ping_task = asyncio.create_task(send_ping_loop())
        receive_task = asyncio.create_task(receive_messages_loop())
        
        return json_response({"ok": True, "message": "Stream connected successfully"})
    except Exception as e:
        state["connected"] = False
        state["last_error"] = str(e)
        return json_response({"ok": False, "error": f"Connection failed: {e}"}, status=500)

async def send_ping_loop():
    """محرك الحفاظ على الاتصال حياً تماشياً مع بروتوكول المنصة"""
    global ws_client
    while state["connected"] and ws_client:
        try:
            await ws_client.send_str("3")
            await asyncio.sleep(25)
        except:
            break

async def receive_messages_loop():
    """تفكيك الحزم البرمجية القادمة من المنصة وفرز الأسعار لايف"""
    global ws_client
    async for msg in ws_client:
        if msg.type == web.WSMsgType.TEXT:
            raw_data = msg.data
            if raw_data == "2":
                await ws_client.send_str("3")
                continue
            
            if raw_data.startswith("42"):
                try:
                    parsed = json.loads(raw_data[2:])
                    if isinstance(parsed, list) and len(parsed) > 1:
                        event_name = parsed[0]
                        event_data = parsed[1]
                        
                        # فرز وتحديث كائن الأسعار بناءً على استجابة السيرفر
                        if event_name == "loadHistory" or event_name == "candles":
                            asset = event_data.get("asset")
                            candles = event_data.get("candles", [])
                            if asset and candles:
                                prices[asset] = candles[-1]
                                state["last_update"] = int(time.time())
                except:
                    pass

async def disconnect_all():
    global ws_client, ping_task, receive_task
    state["connected"] = False
    if ping_task: ping_task.cancel()
    if receive_task: receive_task.cancel()
    if ws_client:
        try: await ws_client.close()
        except: pass
        ws_client = None

async def disconnect_provider(request):
    await disconnect_all()
    return json_response({"ok": True, "message": "Disconnected successfully"})

app = web.Application()
app.router.add_get("/", home)
app.router.add_get("/api/pairs", get_pairs)
app.router.add_post("/api/connect", connect_provider)
app.router.add_post("/api/disconnect", disconnect_provider)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
