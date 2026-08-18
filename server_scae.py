import json
import logging
import asyncio
import websockets

# إعداد السجلات الاحترافية لمراقبة الأداء
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PO_Passive_Bridge_2026")

# جداول تخزين البيانات اللحظية في الذاكرة (مصفوفات سريعة جداً بقيمة O(1))
LIVE_MARKET_DATA = {}
SOCIAL_TRADING_DATA = {}

class StreamState:
    """مراقبة تتابع حزم Socket.io الثنائية لمنع تداخل البيانات"""
    def __init__(self):
        self.expect_binary = False
        self.active_event = None
        self.tracked_asset = "UNKNOWN"
        self.current_period = 60

state = StreamState()

async def parse_and_route_frame(frame, is_binary: bool):
    """
    المحلل المعماري للباكيتات: يتلقى الحزم من نفق المتصفح البشري ويفرزها فوراً.
    """
    # أولاً: إذا كانت الحزمة القادمة عبارة عن بايتات ثنائية (Binary Payload)
    if is_binary:
        if state.expect_binary and state.active_event:
            await decode_binary_payload(state.active_event, frame)
            # إعادة تصفير الراية فوراً للاستعداد للحزمة القادمة في أجزاء من الملي ثانية
            state.expect_binary = False
            state.active_event = None
        return

    # ثانياً: إذا كانت الحزمة نصية (Text Payload)
    if isinstance(frame, str):
        # تغطية الحالات الحرجة: تجاهل رسائل النبض والحفاظ على الاتصال (Ping/Pong)
        if frame in ["2", "3"] or frame.startswith("42[\"ps\""):
            return

        # رصد قذائف الأوامر الصادرة من المتصفح (Outbound Client Actions)
        if frame.startswith("42"):
            try:
                json_str = frame[2:]
                data_array = json.loads(json_str)
                
                if isinstance(data_array, list) and len(data_array) >= 2:
                    action = data_array[0]
                    payload = data_array[1]
                    
                    if action == "changeSymbol" and isinstance(payload, dict):
                        state.tracked_asset = payload.get("asset", "UNKNOWN")
                        state.current_period = payload.get("period", 60)
                        logger.info(f"🔄 المتصفح غير الزوج -> {state.tracked_asset} ({state.current_period}s)")
                        
                    elif action == "subFor":
                        state.tracked_asset = str(payload)
                        logger.info(f"📡 تفعيل اشتراك البث اللحظي للزوج -> {state.tracked_asset}")
            except json.JSONDecodeError:
                pass

        # رصد الإشارات التمهيدية للأحداث الثنائية الواردة من السيرفر (Inbound Binary Events)
        elif frame.startswith("451-"):
            try:
                json_str = frame[4:]
                data_array = json.loads(json_str)
                
                if isinstance(data_array, list) and len(data_array) >= 2:
                    event_name = data_array[0]
                    
                    # تفعيل حالة انتظار المرفق الثنائي للحدث الحالي
                    state.expect_binary = True
                    state.active_event = event_name
            except json.JSONDecodeError:
                pass

async def decode_binary_payload(event_type: str, binary_bytes: bytes):
    """
    تفكيك وقراءة مصفوفة البايتات الحية وتحديث جداول البيانات اللحظية فوراً.
    """
    try:
        # فك ترميز البايتات الممررة إلى نص مقروء لاستخراج قيم الأرقام الكسرية
        decoded_data = binary_bytes.decode('utf-8', errors='ignore')
        
        if event_type == "updateStream":
            # تحديث جدول الأسعار اللحظي في الذاكرة بكفاءة ثابتة O(1)
            LIVE_MARKET_DATA[state.tracked_asset] = {
                "raw_price_stream": decoded_data[:100],
                "period": state.current_period
            }
            logger.info(f"📈 [داتا أسعار ثنائية] {state.tracked_asset} -> {decoded_data[:60]}")
            
        elif event_type == "chafor":
            SOCIAL_TRADING_DATA[state.tracked_asset] = decoded_data[:100]
            logger.info(f"💬 [داتا تداول اجتماعي ثنائية] -> {decoded_data[:50]}")
            
        elif event_type == "updateCharts":
            logger.info(f"📊 [داتا شموع ثنائية] تم التحديث بنجاح.")
            
    except Exception as e:
        logger.error(f"❌ خطأ في معالجة بايتات الحدث {event_type}: {str(e)}")

async def bridge_handler(websocket):
    """المستقبل الصامت لحزم النفق البشري (Passive Bridge Consumer)"""
    logger.info("🔌 نفق المتصفح البشري متصل بنجاح! خادم Render يستقبل التدفق الثنائي الآن...")
    try:
        async for message in websocket:
            is_bin = isinstance(message, bytes)
            await parse_and_route_frame(message, is_bin)
    except websockets.exceptions.ConnectionClosed:
        logger.info("🔌 انفصل النفق البشري مؤقتاً. بانتظار العودة الحية لإعادة المزامنة...")

# تشغيل السيرفر ليتوافق تماماً مع إعدادات بيئة Render الحالية
if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 8765))
    
    start_server = websockets.serve(bridge_handler, "0.0.0.0", port)
    logger.info(f"🚀 خادم الـ Passive Bridge النهائي يعمل ويستمع على المنفذ: {port}")
    
    asyncio.get_event_loop().run_complete(start_server)
    asyncio.get_event_loop().run_forever()
