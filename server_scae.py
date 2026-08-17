import asyncio
import json
import logging
import os
import time
from pathlib import Path
from aiohttp import web
from concurrent.futures import ThreadPoolExecutor

# إعداد السجلات لمراقبة Render Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
)
logger = logging.getLogger("pocket-server")

PORT = int(os.getenv("PORT", "10000"))
BASE_DIR = Path(__file__).resolve().parent
INDEX_FILE = BASE_DIR / "index.html"

# مخزن البيانات والحالة
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
    return web.json_response(
        data,
        status=status,
        dumps=lambda v: json.dumps(v, ensure_ascii=False, default=str),
    )

async def home(request):
    if not INDEX_FILE.exists():
        return web.Response(text="ملف index.html غير موجود في السيرفر", status=500)
    return web.FileResponse(INDEX_FILE)

async def health(request):
    return json_response({
        "ok": True,
        "server_uptime_seconds": int(time.time()) - state["started_at"],
        "pocket_connected": state["connected"],
        "last_error": state["last_error"],
        "last_update": state["last_update"],
        "tracked_pairs_count": len(prices)
    })

async def get_pairs(request):
    # مسار الواجهة لقراءة الأسعار اللحظية بشكل صامت كل ثانية
    return json_response({
        "ok": True,
        "connected": state["connected"],
        "prices": prices,
        "updated_at": state["last_update"]
    })

def start_api_sync(auth_token):
    """تشغيل ربط المكتبة التزامني في Thread منفصل لمنع تجميد السيرفر"""
    global api_instance
    try:
        from pocketoptionapi.stable_api import PocketOptionAPI
        # بناء الكائن باستخدام الـ SSID الممرر من الواجهة
        api_instance = PocketOptionAPI(auth_token)
        return api_instance.connect()
    except Exception as e:
        logger.error(f"خطأ أثناء تشغيل المكتبة التزامنية: {e}")
        return False

async def connect_provider(request):
    global api_instance
    try:
        body = await request.json()
    except Exception:
        return json_response({"ok": False, "error": "JSON حزمة غير صالحة"}, status=400)
    
    auth = body.get("auth") or body.get("ssid")
    if not auth or not isinstance(auth, str) or len(auth.strip()) < 10:
        return json_response({"ok": False, "error": "رمز التوثيق (SSID) غير صالح أو مفقود"}, status=400)
    
    auth_token = auth.strip()
    setStatus("جارٍ محاولة الاتصال بالخوادم البعيدة...")
    
    # تشغيل الاتصال عبر الـ Thread Pool
    loop = asyncio.get_running_loop()
    connection_success = await loop.run_in_executor(executor, start_api_sync, auth_token)
    
    if connection_success:
        state["connected"] = True
        state["last_error"] = None
        state["last_update"] = int(time.time())
        logger.info("تم التوثيق والاتصال بنجاح بـ Pocket Option")
        
        # بدء حلقة قراءة الأسعار في الخلفية بشكل منفصل
        asyncio.create_task(track_prices_loop())
        return json_response({"ok": True, "message": "تم الاتصال وبث الأسعار بدأ بنجاح"})
    else:
        state["connected"] = False
        state["last_error"] = "فشل التوثيق: خوادم المنصة رفضت الـ SSID"
        return json_response({"ok": False, "error": state["last_error"]}, status=400)

async def track_prices_loop():
    """حلقة مستمرة لقراءة الأسعار من كائن الـ API وتحديث الـ State داخلياً"""
    global api_instance
    while state["connected"] and api_instance:
        try:
            # محاولة جلب الشموع اللحظية بناءً على توثيق المستودع المرسل
            if hasattr(api_instance, "get_realtime_candles"):
                for asset in ["EURUSD", "GBPUSD", "EURUSD_OTC", "GBPUSD_OTC"]:
                    candles = api_instance.get_realtime_candles(asset)
                    if candles:
                        # أخذ آخر سعر محدث (الاطار اللحظي)
                        prices[asset] = list(candles.values())[-1] if isinstance(candles, dict) else candles[-1]
                        state["last_update"] = int(time.time())
            await asyncio.sleep(0.5) # تحديث فائق السرعة كل نصف ثانية
        except Exception as e:
            logger.warning(f"تحذير أثناء قراءة الأسعار: {e}")
            await asyncio.sleep(1)

async def disconnect_provider(request):
    global api_instance
    state["connected"] = False
    if api_instance:
        try:
            api_instance.close()
        except Exception:
            pass
        api_instance = None
    return json_response({"ok": True, "message": "تم قطع الاتصال بالخادم بنجاح"})

app = web.Application()
app.router.add_get("/", home)
app.router.add_get("/health", health)
app.router.add_get("/api/pairs", get_pairs)
app.router.add_post("/api/connect", connect_provider)
app.router.add_post("/api/disconnect", disconnect_provider)

if __name__ == "__main__":
    web.run_app(app, host="0.0.0.0", port=PORT)
