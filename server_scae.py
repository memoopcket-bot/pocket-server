import asyncio
import json
import datetime
import websockets
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# تفعيل الـ CORS لتشغيل واجهتك الأمامية بدون أي قيود اتصال
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# قائمة أزواج العملات المراد تتبع أسعارها لحظياً
ALL_PAIRS = [
    "EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "AUDUSD_otc", 
    "EURUSD", "GBPUSD", "USDJPY"
]

def log_to_file(message):
    """دالة لحفظ السجلات وتتبع حركة حزم البيانات دون مسح السجل القديم"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open("pocket_project_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

@app.get("/")
def home():
    return {"status": "running", "message": "Active"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    # قبول اتصال المتصفح أو الواجهة الأمامية الخاصة بك
    await client_ws.accept()
    log_to_file("Web client connected.")
    
    pocket_ws = None
    heartbeat_task = None
    
    try:
        # استقبال حزمة التوثيق الأولى (التي تحتوي على الـ SSID) من عميلك
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        # جلب الرابط من متغيرات بيئة Render
        raw_url = os.getenv("POCKET_URL")
        if not raw_url:
            log_to_file("Error: POCKET_URL variable not found in Render settings.")
            await client_ws.send_json({"status": "error", "message": "POCKET_URL not configured"})
            return

        # تنظيف الرابط تلقائياً من علامات الاقتباس ومسافات النسخ الزائدة لمنع انهيار الخادم
        pocket_url = raw_url.strip('"').strip("'").strip()
        log_to_file(f"Target URL verified and cleaned successfully.")

        # الرؤوس الخاصة بمحاكاة المتصفح لإقناع جدار حماية المنصة بالاتصال
        custom_headers = {
            "Origin": "https://pocketoption.com"
        }
        
        # فتح اتصال WebSocket آمن ومباشر مع خادم المنصة
        async with websockets.connect(pocket_url, extra_headers=custom_headers) as pocket_ws:
            log_to_file("Connection stable with Pocket Option.")
            
            # بروتوكول Socket.io (EIO=3) يستقبل حزمة ترحيبية "40" تلقائياً من السيرفر فور نجاح الاتصال
            first_msg = await pocket_ws.recv()
            log_to_file(f"Server welcome packet received: {first_msg}")
            
            # إرسال حزمة التوثيق بالـ SSID الديناميكي الذي تم استقباله من الواجهة
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file("Auth token delivered successfully.")
            
            # إرسال إشارة نجاح للواجهة الأمامية الخاصة بك
            await client_ws.send_json({"status": "platform_connected"})
            
            # الاشتراك الفوري لتلقي الأسعار لجميع أزواج العملات المطلوبة
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub_packet)
            log_to_file(f"Subscribed to {len(ALL_PAIRS)} asset pairs.")

            # دالة إرسال نبضات الحياة (Heartbeat) لمنع خوادم المنصة من فصل جلستك التداولية
            async def send_heartbeat():
                try:
                    while pocket_ws.open:
                        await asyncio.sleep(25)  # إرسال النبضة كل 25 ثانية تماشياً مع معيار EIO=3
                        await pocket_ws.send("2")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log_to_file(f"Heartbeat loop stopped: {e}")

            # تشغيل مهمة الهارت بيت بشكل منفصل في الخلفية لعدم تعطيل تدفق البيانات
            heartbeat_task = asyncio.create_task(send_heartbeat())

            # بدء استقبال حزم البيانات الحية وتوجيهها للمتصفح الخاص بك
            async for raw_message in pocket_ws:
                # إذا أرسل سيرفر المنصة طلب نبضة "2" نرد عليه فوراً بـ "3" لتأكيد حيويتنا
                if raw_message == "2":
                    await pocket_ws.send("3")
                    continue
                
                # تصفية الحزم القادمة التي تحتوي على أحداث وبيانات تداول (تبدأ بـ 42)
                if raw_message.startswith("42"):
                    try:
                        # حذف معرّف البروتوكول "42" وفك تشفير الـ JSON
                        parsed = json.loads(raw_message[2:])
                        
                        # حزم التيكسات تصل كقائمة، حيث العنصر الأول هو اسم الحدث "tick"
                        if isinstance(parsed, list) and len(parsed) > 1 and parsed[0] == "tick":
                            tick_info = parsed[1]
                            # إرسال بيانات السعر نظيفة ومباشرة لواجهتك الأمامية
                            await client_ws.send_json({
                                "status": "tick",
                                "asset": tick_info.get("asset"),
                                "price": tick_info.get("price")
                            })
                            continue
                    except Exception:
                        pass # في حال كانت حزمة أخرى، سيتم تمرير نصها الخام دون تعديل لعميلك
                    
                    # تمرير الرسالة نصياً ومباشرة إلى المتصفح الخاص بك
                    await client_ws.send_text(raw_message)
                    
                elif raw_message == "3":
                    pass

    except WebSocketDisconnect:
        log_to_file("Client Web interface disconnected.")
    except Exception as e:
        log_to_file(f"Critical Error observed: {str(e)}")
        try:
            await client_ws.send_json({"status": "error", "message": str(e)})
        except:
            pass
    finally:
        # إغلاق آمن وحرص شديد على تنظيف الذاكرة لمنع تراكم المهام (Memory Leak)
        if heartbeat_task:
            heartbeat_task.cancel()
        if pocket_ws and pocket_ws.open:
            await pocket_ws.close()
        log_to_file("Resources cleaned up and connections closed securely.")
