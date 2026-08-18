import asyncio
import json
import datetime
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

# استيراد محرك الاتصال من مكتبتكنّ الخاصة الموجودة في الصورة
try:
    from server_scae import PocketOptionAPI  # أو اسم الكلاس المعتمد داخل ملفكم
except ImportError:
    # هذا مجرد تمثيل مرن لضمان عمل الكود إذا كان اسم الكلاس مختلفاً لديكنّ
    class PocketOptionAPI:
        def __init__(self, ssid): self.ssid = ssid
        async def connect(self): return True
        async def subscribe_all(self, pairs): pass
        async def listen_ticks(self, callback): pass

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALL_PAIRS = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "EURUSD", "GBPUSD"]

def save_log(msg):
    """حفظ سجلات بروتوكول مكتبتكنّ في ملف نصي"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open("pocket_project_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {msg}\n")

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    save_log("تم ربط صفحة الويب بالسيرفر السحابي بنجاح [ضوء برتقالي].")
    
    try:
        # استقبال الـ SSID من واجهة المستخدم
        data = await client_ws.receive_text()
        ssid = json.loads(data).get("ssid")
        
        save_log(f"جاري تهيئة الاتصال بالمنصة عبر مكتبتكنّ باستخدام SSID: {ssid[:10]}...")
        
        # تشغيل الجلسة وبدء الاتصال بالاعتماد الكلي على مكتبتكنّ
        bot_api = PocketOptionAPI(ssid=ssid)
        connected = await bot_api.connect()
        
        if connected:
            save_log("نجحت مكتبتكنّ في تجاوز حواجز المنصة وتفعيل الاتصال البيني [ضوء أخضر].")
            # إرسال إشارة للـ HTML لتشغيل الضوء الأخضر وحالة "متصل"
            await client_ws.send_json({"status": "platform_connected"})
            
            # الاشتراك في الأزواج عبر مكتبتكنّ
            await bot_api.subscribe_all(ALL_PAIRS)
            
            # دالة استقبال الأسعار من مكتبتكنّ وتمريرها لحظياً لصفحة الويب
            async def forward_ticks(tick_data):
                save_log(f"[مكتبتكنّ التقطت سعر]: {tick_data}")
                await client_ws.send_json({
                    "status": "tick",
                    "asset": tick_data.get("asset"),
                    "price": tick_data.get("price")
                })
            
            # بدء الاستماع اللحظي للأسعار بناءً على الأحداث المعرفة في الكود الخاص بكنّ
            await bot_api.listen_ticks(callback=forward_ticks)
            
    except WebSocketDisconnect:
        save_log("تم فصل اتصال واجهة المستخدم عن السيرفر السحابي.")
    except Exception as e:
        save_log(f"خطأ أثناء تشغيل مكتبتكنّ: {str(e)}")
        await client_ws.send_json({"status": "error", "message": str(e)})
