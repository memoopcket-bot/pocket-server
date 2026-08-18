import asyncio
import json
import datetime
import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# تعريف تطبيق الـ FastAPI الذي يبحث عنه موقع Render
app = FastAPI()

# تفعيل السماح بالاتصال من أي واجهة (CORS) لربط صفحة HTML بأمان
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# قائمة بكافة أزواج الـ OTC والفوركس المتاحة للمراقبة الحية في مشروعكنّ
ALL_PAIRS = [
    "EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "AUDUSD_otc", 
    "EURUSD", "GBPUSD", "USDJPY"
]

def log_to_file(message):
    """دالة برمجية مخصصة لحفظ كافة سجلات وحزم الاتصال تلقائياً في ملف نصي"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open("pocket_project_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

@app.get("/")
def home():
    return {"status": "running", "message": "Pocket Option Cloud Server is Active"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    # قبول اتصال صفحة الـ HTML بالسيرفر السحابي
    await client_ws.accept()
    log_to_file("تم اتصال صفحة الويب بالسيرفر السحابي بنجاح [تفعيل الضوء البرتقالي].")
    
    pocket_ws = None
    try:
        # 1. استقبال الـ SSID القادم من خانة الإدخال في صفحة الـ HTML
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        # رابط خادم المنصة الرسمي المشفر والمستخدم في المكتبات غير الرسمية
        pocket_url = "wss://pocketoption.com"
        log_to_file(f"محاولة الاتصال بالمنصة برابط: {pocket_url}")
        
        # 2. إنشاء اتصال الويب سوكيت المباشر مع المنصة
        async with websockets.connect(pocket_url) as pocket_ws:
            log_to_file("تم الاتصال الفيزيائي بخادم المنصة. جاري إرسال حزم التأسيس...")
            
            # إرسال حزمة التأسيس لبروتوكول Socket.IO لفتح القناة
            await pocket_ws.send("40")
            
            # إرسال حزمة المصادقة الجلسية الرسمية المعتمدة على الـ SSID المنسوخ
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file(f"تم إرسال حزمة المصادقة الجلسية: {auth_packet}")
            
            # إرسال إشارة نجاح التوثيق لصفحة الـ HTML لتفعيل الضوء الأخضر وحالة 'متصل'
            await client_ws.send_json({"status": "platform_connected"})
            
            # الاشتراك التلقائي في بث كافة الأزواج والـ OTC لجلب الأسعار لحظة بلحظة
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub_packet)
                log_to_file(f"تم إرسال طلب بث السعر اللحظي للزوج: {pair}")

            # دالة موازية لإرسال نبضات القلب (Ping) كل 20 ثانية للحفاظ على استقرار الجلسة ومنع الفصل
            async def send_heartbeat():
                while True:
                    await asyncio.sleep(20)
                    if pocket_ws.open:
                        await pocket_ws.send("2")
                        log_to_file("[إرسال Heartbeat ->] 2")

            asyncio.create_task(send_heartbeat())

            # 3. حلقة الاستماع اللحظية وقراءة سيل الأسعار وتمريرها فوراً لصفحة الـ HTML وحفظها
            async for raw_message in pocket_ws:
                # تصفية وحفظ حزم البيانات والأسعار (Tick Data) المبتدئة بـ 42
                if raw_message.startswith("42"):
                    log_to_file(f"[حزمة مستلمة من المنصة <-] {raw_message}")
                    
                    # محاولة تفكيك حزمة السعر وتمريرها منظمة للجدول
                    try:
                        parsed = json.loads(raw_message[2:])
                        if isinstance(parsed, list) and parsed[0] == "tick":
                            tick_info = parsed[1]
                            await client_ws.send_json({
                                "status": "tick",
                                "asset": tick_info.get("asset"),
                                "price": tick_info.get("price")
                            })
                            continue
                    except:
                        pass
                    
                    # تمرير الرسالة الخام للواجهة إذا لم تكن حزمة سعر قياسية
                    await client_ws.send_text(raw_message)
                    
                elif raw_message == "3":
                    log_to_file("[استقبل رد Heartbeat <-] 3")

    except WebSocketDisconnect:
        log_to_file("تم إغلاق اتصال صفحة الويب بالسيرفر السحابي.")
    except Exception as e:
        log_to_file(f"حدث خطأ غير متوقع في معالجة البروتوكول: {str(e)}")
        try:
            await client_ws.send_json({"status": "error", "message": str(e)})
        except:
            pass
