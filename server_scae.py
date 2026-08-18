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

async def process_request(path, request_headers):
    # دالة ذكية تقرأ ملف الواجهة الخارجي وسحق الشاشة البيضاء كلياً
    if "upgrade" not in request_headers.get("Connection", "").lower():
        logger.info("🌍 طلب ويب HTTP وارد: جاري جلب واجهة الـ HTML...")
        html_content = b"<h1>index.html not found in repository</h1>"
        if os.path.exists("index.html"):
            with open("index.html", "rb") as f:
                html_content = f.read()
        return (websockets.http.HTTPStatus.OK, {"Content-Type": "text/html; charset=utf-8"}, html_content)
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
                    if data[0] == "changeSymbol": state.tracked_asset = data[1].get("asset", "UNKNOWN")
                    elif data[0] == "subFor": state.tracked_asset = str(data[1])
            except: pass
        elif frame.startswith("451-"):
            try:
                data = json.loads(frame[4:])
                if isinstance(data, list) and len(data) >= 2:
                    state.expect_binary = True
                    state.active_event = data[0]
            except: pass

async def bridge_handler(websocket, path=None):
    logger.info("🔌 نفق WebSocket البشري متصل ويستقبل الآن...")
    state.packet_counter = 0
    try:
        async for message in websocket:
            await parse_and_route_frame(message, isinstance(message, bytes))
    except websockets.exceptions.ConnectionClosed:
        logger.warning("🔌 انفصل النفق البشري الممرر للبيانات.")

async def main():
    port = int(os.environ.get("PORT", 8765))
    async with websockets.serve(bridge_handler, "0.0.0.0", port, process_request=process_request):
        logger.info(f"🚀 [ناجح] السيرفر المزدوج مستقر على المنفذ: {port}")
        await asyncio.Future()

if __name__ == "__main__":
    try: asyncio.run(main())
    except Exception as e: logger.error(f"🚨 خطأ حرج: {str(e)}")
