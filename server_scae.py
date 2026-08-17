import asyncio
import json
import logging
import os
import time
from pathlib import Path
from aiohttp import web
from concurrent.futures import ThreadPoolExecutor

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

api_instance = None
executor = ThreadPoolExecutor(max_workers=2)

def json_response(data, status=200):
    return web.json_response(data, status=status, dumps=lambda v: json.dumps(v, ensure_ascii=False, default=str))

async def home(request):
    if not INDEX_FILE.exists():
        return web.Response(text="index.html not found", status=500)
    return web.FileResponse(INDEX_FILE)

async def get_pairs(request):
    return json_response({
        "ok": True,
        "connected": state["connected"],
        "prices": prices,
        "updated_at": state["last_update"]
    })
def start_api_sync(auth_token):
    global api_instance
    try:
        # الاستيراد الرسمي المتوافق مع وثائق مستند الـ Docs المرفق
        from pocketoptionapi.stable_api import PocketOptionAPI
        api_instance = PocketOptionAPI(auth_token)
        return api_instance.connect()
    except Exception as e:
        logger.error(f"Sync API connection failed: {e}")
        return False

async def connect_provider(request):
    global api_instance
    try: body = await request.json()
    except: return json_response({"ok": False, "error": "Invalid JSON"}, status=400)
    
    auth = body.get("auth") or body.get("ssid") or body.get("session")
    if not auth or not isinstance(auth, str) or len(auth.strip()) < 10:
        return json_response({"ok": False, "error": "Invalid or missing token"}, status=400)
    
    auth_token = auth.strip()
    
    # تشغيل الاتصال التزامني في Thread مستقل تماماً تماشياً مع معايير الملف التعليمي
    loop = asyncio.get_running_loop()
    connection_success = await loop.run_in_executor(executor, start_api_sync, auth_token)
    
    if connection_success:
        state["connected"] = True
        state["last_error"] = None
        state["last_update"] = int(time.time())
        asyncio.create_task(track_prices_loop())
        return json_response({"ok": True, "message": "Successfully connected via stable library!"})
    else:
        state["connected"] = False
        state["last_error"] = "Authentication rejected by platform"
        return json_response({"ok": False, "error": state["last_error"]}, status=400)

async def track_prices_loop():
    global api_instance
    symbols = ["EURUSD", "GBPUSD", "EURUSD_OTC", "GBPUSD_OTC"]
    while state["connected"] and api_instance:
        try:
            if hasattr(api_instance, "get_realtime_candles"):
                for asset in symbols:
                    candles = api_instance.get_realtime_candles(asset)
                    if candles:
                        prices[asset] = list(candles.values())[-1] if isinstance(candles, dict) else candles[-1]
                        state["last_update"] = int(time.time())
            await asyncio.sleep(0.5)
        except Exception as e:
            logger.warning(f"Price tracking warning: {e}")
            await asyncio.sleep(1)

async def disconnect_provider(request):
    global api_instance
    state["connected"] = False
    if api_instance:
        try: api_instance.close()
        except: pass
        api_instance = None
    return json_response({"ok": True, "message": "Disconnected successfully"})

app = web.Application()
app.router.add_get("/", home)
app.router.add_get("/api/pairs", get_pairs)
app.router.add_post("/api/connect", connect_provider)
app.router.add_post("/api/disconnect", disconnect_provider)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
