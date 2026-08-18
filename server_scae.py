import os, json, logging, asyncio, websockets

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("PO_Dual_Engine_Final")
LIVE_MARKET_DATA = {}

class StreamState:
    def __init__(self):
        self.expect_binary = False
        self.active_event = None
        self.tracked_asset = "UNKNOWN"
        self.current_period = 60
        self.packet_counter = 0

state = StreamState()

# واجهة طوارئ مدمجة تضمن تشغيل الصفحة فوراً حتى لو اختفى الملف الخارجي
EMERGENCY_HTML = b"""<!DOCTYPE html><html lang='ar' dir='rtl'><head><meta charset='UTF-8'><title>Passive Bridge 2026</title><style>body{background-color:#121620;color:white;font-family:sans-serif;display:flex;justify-content:center;align-items:center;height:100vh;margin:0;} .box{background:#1a202e;padding:30px;border-radius:12px;border:1px solid #263147;text-align:center;max-width:400px;}</style></head><body><div class='box'><h2>⚙️ نفق السيرفر المزدوج نشط</h2><p style='color:#8a96a3;'>تم تشغيل بيئة المعالجة الصامتة لعام 2026 بنجاح. يرجى التأكد من ربط النفق البشري عبر المتصفح لبدء استقبال البايتات الثنائية للأسعار.</p></div></body></html>"""

async def process_request(path, request_headers):
    if "upgrade" not in request_headers.get("Connection", "").lower():
        logger.info("🌍 طلب ويب HTTP وارد: جاري محاولة جلب واجهة الـ HTML...")
        
        # فحص المسارات المتوقعة للملف لمنع أي حظر جيو-مكانى
        for filename in ["index.html", "INDEX.HTML", "../index.html"]:
            if os.path.exists(filename):
                try:
                    with open(filename, "rb") as f:
                        return (websockets.http.HTTPStatus.OK, {"Content-Type": "text/html; charset=utf-8"}, f.read())
                except:
                    pass
                    
        # حالة التغطية الحرجة: إذا تعذر قراءة الملف، يتم ضخ واجهة الطوارئ لمنع الشاشة البيضاء
        return (websockets.http.HTTPStatus.OK, {"Content-Type": "text/html; charset=utf-8"}, EMERGENCY_HTML)
    return None
async def parse_and_route_frame(frame, is_binary: bool):
    """المحلل المعماري للباكيتات: يفرز الحزم الحية بكفاءة ثابتة O(1)"""
    state.packet_counter += 1
    p_num = state.packet_counter

    if is_binary:
        if state.expect_binary and state.active_event == "updateStream":
            decoded = frame.decode('utf-8', errors='ignore')
            LIVE_MARKET_DATA[state.tracked_asset] = {"stream": decoded[:100]}
            logger.info(f"📊 [تفكيك #{p_num}] أسعار لـ {state.tracked_asset} -> {decoded[:50]}")
            state.expect_binary = False
            state.active_event = None
        return

    if isinstance(frame, str):
        if frame in ["2", "3"] or frame.startswith("42[\"ps\""): return
        if frame.startswith("42"):
            try:
                data = json.loads(frame[2:])
                if isinstance(data, list) and len(data) >= 2:
                    if data == "changeSymbol": state.tracked_asset = data.get("asset", "UNKNOWN")
                    elif data == "subFor": state.tracked_asset = str(data)
            except: pass
        elif frame.startswith("451-"):
            try:
                data = json.loads(frame[4:])
                if isinstance(data, list) and len(data) >= 2:
                    state.expect_binary = True
                    state.active_event = data
            except: pass

async def bridge_handler(websocket, path=None):
    logger.info("🔌 نفق WebSocket البشري متصل ويستقبل الآن...")
    state.packet_counter = 0
    try:
        async for message in websocket:
            await parse_and_route_frame(message, isinstance(message, bytes))
    except websockets.exceptions.ConnectionClosed:
        logger.warning("🔌 انفصل Nفق البشري الممرر للبيانات.")

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(bridge_handler, "0.0.0.0", port, process_request=process_request):
        logger.info(f"🚀 [ناجح] السيرفر المزدوج مستقر على المنفذ: {port}")
        await asyncio.Future()

if __name__ == "__main__":
    try: asyncio.run(main())
    except Exception as e: logger.error(f"🚨 خطأ حرج: {str(e)}")
