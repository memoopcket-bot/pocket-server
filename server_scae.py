import json
import logging
import asyncio
import websockets

# إعداد السجلات الاحترافية لمراقبة دقة تدفق البيانات بالملي ثانية
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("PO_Passive_Processor")

# جداول لتخزين الأسعار الحية وبيانات التداول الاجتماعي في الذاكرة
LIVE_MARKET_DATA = {}
SOCIAL_TRADING_DATA = {}

class StreamState:
    """إدارة حالة النفق لتوقع وتوجيه الرسائل الثنائية (Binary Frames) ومنع التداخل"""
    def __init__(self):
        self.expect_binary = False
        self.active_event = None
        self.tracked_asset = "UNKNOWN"
        self.current_period = 60
        self.packet_counter = 0  # عدّاد لمراقبة حجم التدفق اللحظي وكشف الخلل

state = StreamState()

async def parse_and_route_frame(frame, is_binary: bool):
    """
    المحلل المعماري للباكيتات: يستقبل الحزم الممررة من نفق المتصفح البشري ويفرزها فوراً.
    """
    state.packet_counter += 1
    p_num = state.packet_counter

    # --- أولاً: معالجة مجرى البيانات الثنائية (Binary Data Pipeline) ---
    if is_binary:
        logger.info(f"📥 [حزمة #{p_num}] استلمت رسالة ثنائية (Binary Payload) | الحجم: {len(frame)} بايت")
        
        if state.expect_binary and state.active_event:
            await decode_binary_payload(p_num, state.active_event, frame)
            # إعادة تصفير الراية فوراً للاستعداد للحزمة اللحظية التالية في أجزاء من الملي ثانية
            state.expect_binary = False
            state.active_event = None
        else:
            # حالة حرجة: وصول بايتات ثنائية بدون تمهيد نصي مسبق (مشكلة تداخل شبكة)
            logger.warning(f"⚠️ [حزمة #{p_num}] خلل تزامن! وصلت بايتات ثنائية لكن السيرفر لم يكن ينتظر حدثاً ممهداً.")
        return

    # --- ثانياً: معالجة مجرى الرسائل النصية المقروءة (Text Data Pipeline) ---
    if isinstance(frame, str):
        logger.info(f"📝 [حزمة #{p_num}] استلمت رسالة نصية (Text Frame) -> {frame[:120]}")

        # تغطية الحالات الحرجة: تجاهل رسائل النبض والحفاظ على الاتصال (Ping/Pong / Heartbeat)
        if frame in ["2", "3"] or frame.startswith("42[\"ps\""):
            return

        # رصد قذائف الأوامر الصادرة من المتصفح البشري (Outbound Client Actions)
        if frame.startswith("42"):
            try:
                json_str = frame[2:]
                data_array = json.loads(json_str)
                
                if isinstance(data_array, list) and len(data_array) >= 2:
                    action = data_array[0]
                    payload = data_array[1]
                    
                    # التقاط وتوثيق أمر تغيير أصل التداول والتايم فريم
                    if action == "changeSymbol" and isinstance(payload, dict):
                        state.tracked_asset = payload.get("asset", "UNKNOWN")
                        state.current_period = payload.get("period", 60)
                        logger.info(f"🔄 [سجل] المتصفح غيّر أصل التداول -> {state.tracked_asset} ({state.current_period}s)")
                        
                    # التقاط وتوثيق أمر الاشتراك الحي في الأسعار
                    elif action == "subFor":
                        state.tracked_asset = str(payload)
                        logger.info(f"📡 [سجل] تفعيل بث الاشتراك للزوج الحالي -> {state.tracked_asset}")
            except json.JSONDecodeError:
                logger.error(f"❌ [حزمة #{p_num}] خطأ فك JSON لحزمة صادر المتصفح '42': {frame[:50]}")

        # رصد الإشارات التمهيدية للأحداث الثنائية الواردة من السيرفر (Inbound Binary Events)
        elif frame.startswith("451-"):
            try:
                json_str = frame[4:]  # تخطي البادئة "451-"
                data_array = json.loads(json_str)
                
                if isinstance(data_array, list) and len(data_array) >= 2:
                    # تفعيل حالة انتظار المرفق الثنائي للحدث الحالي وحجزه في الذاكرة
                    state.expect_binary = True
                    state.active_event = data_array[0]
                    logger.info(f"🎯 [سجل] إشارة ممهدة للحدث الثنائي القادم: '{state.active_event}'")
            except json.JSONDecodeError:
                logger.error(f"❌ [حزمة #{p_num}] خطأ فك JSON لإشارة التمهيد '451-': {frame[:50]}")

async def decode_binary_payload(packet_id: int, event_type: str, binary_bytes: bytes):
    """
    تفكيك مصفوفة البايتات الحية وتحديث جداول البيانات الفورية في الذاكرة.
    """
    try:
        # فك ترميز البايتات الممررة إلى نص مقروء لاستخراج قيم الأسعار
        decoded_data = binary_bytes.decode('utf-8', errors='ignore')
        
        if event_type == "updateStream":
            # تحديث جدول الأسعار اللحظي في الذاكرة بكفاءة ثابتة O(1)
            LIVE_MARKET_DATA[state.tracked_asset] = {
                "raw_price_stream": decoded_data[:100],
                "period": state.current_period
            }
            logger.info(f"📊 [تفكيك ناجح #{packet_id}] داتا أسعار لـ {state.tracked_asset} -> {decoded_data[:60]}")
            
        elif event_type == "chafor":
            # مسار فك تشفير إحصائيات التداول الاجتماعي والدردشة لمنع تداخلها مع الأسعار
            SOCIAL_TRADING_DATA[state.tracked_asset] = decoded_data[:100]
            logger.info(f"💬 [تفكيك ناجح #{packet_id}] داتا تداول اجتماعي ودردشة ثنائية.")
            
        elif event_type == "updateCharts":
            logger.info(f"📉 [تفكيك ناجح #{packet_id}] داتا شموع ورسوم بيانية ثنائية.")
            
    except Exception as e:
        # تسجيل الخطأ الحرج المسبب للمشكلة دون السماح بانهيار السيرفر
        logger.error(f"🚨 [خطأ حرج في الحزمة #{packet_id}] فشل معالجة بايتات الحدث '{event_type}': {str(e)}")

async def bridge_handler(websocket, path=None):
    """المستقبل الصامت لحزم النفق البشري (Passive Bridge Consumer)"""
    logger.info("🔌 [اتصال] نفق المتصفح البشري متصل بنجاح! خادم Render يستقبل التدفق الآن...")
    state.packet_counter = 0  # إعادة تصفير العداد عند كل اتصال جديد لمراقبة دقيقة
    try:
        async for message in websocket:
            is_bin = isinstance(message, bytes)
            await parse_and_route_frame(message, is_bin)
    except websockets.exceptions.ConnectionClosed:
        logger.warning("🔌 [انفصال] انقطع اتصال النفق البشري الممرر للبيانات. بانتظار العودة...")
    except Exception as e:
        logger.error(f"🚨 [خطأ نفق] حدث خطأ غير متوقع في مجرى السيرفر: {str(e)}")

async def main():
    import os
    port = int(os.environ.get("PORT", 8765))
    
    # الطريقة الحديثة والمستقرة لبدء الخادم لعام 2026 دون تعارض حلقات الأحداث وسحق RuntimeError
    async with websockets.serve(bridge_handler, "0.0.0.0", port):
        logger.info(f"🚀 [تشغيل] سيرفر الـ Passive Processor يعمل بنجاح ويستمع على المنفذ: {port}")
        await asyncio.Future()  # الحفاظ على السيرفر حياً ومعلقاً في وضع الاستماع دون توقف

if __name__ == "__main__":
    try:
        # إطلاق الحلقة بطريقة asyncio.run الآمنة هندسياً لبيئات الاستضافة الحديثة
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 تم إيقاف السيرفر يدوياً.")
    except Exception as e:
        logger.error(f"🚨 خطأ حرج أثناء تشغيل السيرفر: {str(e)}")
