import asyncio
import json
import datetime
import websockets
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# تفعيل الـ CORS للسماح لواجهتك الأمامية بالاتصال بدون قيود
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# قائمة أزواج العملات المراد تتبع أسعارها
ALL_PAIRS = [
    "EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "AUDUSD_otc", 
    "EURUSD", "GBPUSD", "USDJPY"
]

def log_to_file(message):
    """دالة لحفظ السجلات وتتبع الأخطاء بشكل مستمر دون مسح القديم"""
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open("pocket_project_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

@app.get("/")
def home():
    return {"status": "running", "message": "Active"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    # قبول اتصال المتصفح / الواجهة الأمامية
    await client_ws.accept()
    log_to_file("Web client connected.")
    
    pocket_ws = None
    heartbeat_task = None
    
    try:
        # استقبال بيانات التوثيق (SSID) من العميل
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        # جلب الرابط من متغيرات بيئة ريندر
        pocket_url = os.getenv("POCKET_URL")
        if not pocket_url:
            log_to_file("Error: POCKET_URL variable not found in Render settings.")
            await client_ws.send_json({"status": "error", "message": "POCKET_URL not configured"})
            return

        log_to_file("Connecting to Pocket Option WS Server...")
        
        # حزمة الرؤوس (Headers) لمحاكاة اتصال المتصفح وتخطي الحجب
        custom_headers = {
            "Origin": "https://pocketoption.com"
        }
        
        # فتح الاتصال مع سيرفر المنصة الرئيسي ببروتوكول آمن
        async with websockets.connect(pocket_url, extra_headers=custom_headers) as pocket_ws:
            log_to_file("Connection stable with Pocket Option.")
            
            # بروتوكول Socket.io (EIO=3) يستقبل حزمة ترحيبية تلقائية "40" من السيرفر فور الاتصال
            first_msg = await pocket_ws.recv()
            log_to_file(f"Server welcome packet received: {first_msg}")
            
            # إرسال حزمة التوثيق مباشرة باستخدام الـ SSID الديناميكي
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file("Auth token delivered successfully.")
            
            # إعلام واجهتك الأمامية بنجاح الاتصال بالمنصة
            await client_ws.send_json({"status": "platform_connected"})
            
            # الاشتراك في أزواج العملات المحددة لتلقي الـ Ticks
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub_packet)
            log_to_file(f"Subscribed to {len(ALL_PAIRS)} asset pairs.")

            # دالة الهارت بيت (Heartbeat) للحفاظ على حيوية الاتصال ومنع الفصل التلقائي
            async def send_heartbeat():
                try:
                    while pocket_ws.open:
                        await asyncio.sleep(25)  # إرسال نبضة كل 25 ثانية حسب معيار EIO=3
                        await pocket_ws.send("2")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log_to_file(f"Heartbeat loop stopped: {e}")

            # تشغيل الهارت بيت في الخلفية بشكل منفصل
            heartbeat_task = asyncio.create_task(send_heartbeat())

            # بدء حلقة استقبال البيانات وتمريرها لحظياً
            async for raw_message in pocket_ws:
                # إذا أرسل السيرفر طلب نبضة "2"، نرد عليه بـ "3" فوراً لإبقاء الجلسة حية
                if raw_message == "2":
                    await pocket_ws.send("3")
                    continue
                
                # معالجة حزم البيانات القادمة من السيرفر والتي تبدأ بـ 42
                if raw_message.startswith("42"):
                    try:
                        # إزالة مقدمة البروتوكول "42" وتحليل الـ JSON
                        parsed = json.loads(raw_message[2:])
                        
                        # التيكسات تأتي في مصفوفة مكوّنة من [اسم الحدث، البيانات]
                        if isinstance(parsed, list) and len(parsed) > 1 and parsed[0] == "tick":
                            tick_info = parsed[1]
                            # إرسال السعر النظيف لواجهتك الأمامية
                            await client_ws.send_json({
                                "status": "tick",
                                "asset": tick_info.get("asset"),
                                "price": tick_info.get("price")
                            })
                            continue
                    except Exception:
                        pass # في حال كانت الحزمة بتنسيق آخر، يتم تمريرها نصياً للعميل دون تعديل
                    
                    # تمرير أي رسائل أخرى مباشرة كما هي إلى متصفح العميل
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
        # تنظيف الذاكرة وإغلاق كافة الاتصالات بشكل آمن عند حدوث أي فصل
        if heartbeat_task:
            heartbeat_task.cancel()
        if pocket_ws and pocket_ws.open:
            await pocket_ws.close()
        log_to_file("Resources cleaned up and connections closed securely.")
