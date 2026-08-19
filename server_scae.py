import asyncio
import json
import datetime
import websockets
import os
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

ALL_PAIRS = [
    "EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "AUDUSD_otc", 
    "EURUSD", "GBPUSD", "USDJPY"
]

def log_to_file(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # تم تعديل الوضع إلى "a" للحفاظ على سجل الأخطاء كاملاً وتتبع المشاكل
    with open("pocket_project_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

@app.get("/")
def home():
    return {"status": "running", "message": "Active"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    log_to_file("Web client connected.")
    
    pocket_ws = None
    heartbeat_task = None
    
    try:
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        pocket_url = os.getenv("POCKET_URL")
        
        if not pocket_url:
            log_to_file("Error: POCKET_URL variable not found in Render settings.")
            await client_ws.send_json({"status": "error", "message": "POCKET_URL not configured"})
            return

        log_to_file(f"Connecting to Pocket Option WS...")
        
        async with websockets.connect(pocket_url) as pocket_ws:
            log_to_file("Connection stable with Pocket Option.")
            
            # بروتوكول فتح الاتصال في Socket.io
            await pocket_ws.send("40")
            
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file("Auth token delivered.")
            
            await client_ws.send_json({"status": "platform_connected"})
            
            # الاشتراك في أزواج العملات
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub_packet)
            log_to_file(f"Subscribed to {len(ALL_PAIRS)} pairs.")

            # دالة الهارت بيت الآمنة والمربوطة بحالة الاتصال الحقيقية
            async def send_heartbeat():
                try:
                    while pocket_ws.open:
                        await asyncio.sleep(20)
                        await pocket_ws.send("2")
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    log_to_file(f"Heartbeat loop stopped: {e}")

            heartbeat_task = asyncio.create_task(send_heartbeat())

            # استقبال البيانات وتمريرها
            async for raw_message in pocket_ws:
                # الرد على طلب الهارت بيت من السيرفر (إذا أرسل 2 نرد بـ 3)
                if raw_message == "2":
                    await pocket_ws.send("3")
                    continue
                
                if raw_message.startswith("42"):
                    try:
                        # تحويل النص لـ JSON مع تخطي معرف البروتوكول "42"
                        parsed = json.loads(raw_message[2:])
                        
                        # تصحيح قراءة التيكسات (تأتي كقائمة أول عنصر فيها هو الحدث وثاني عنصر البيانات)
                        if isinstance(parsed, list) and len(parsed) > 1 and parsed[0] == "tick":
                            tick_info = parsed[1]
                            await client_ws.send_json({
                                "status": "tick",
                                "asset": tick_info.get("asset"),
                                "price": tick_info.get("price")
                            })
                            continue
                    except Exception as parse_error:
                        pass # في حال كانت الحزمة ليست بتنسيق المتوقع، يتم تمريرها خام للعميل
                    
                    await client_ws.send_text(raw_message)
                    
                elif raw_message == "3":
                    pass

    except WebSocketDisconnect:
        log_to_file("Client Web disconnected.")
    except Exception as e:
        log_to_file(f"Critical Error: {str(e)}")
        try:
            await client_ws.send_json({"status": "error", "message": str(e)})
        except:
            pass
    finally:
        # إغلاق المهام والاتصالات المفتوحة لعدم تسريب الذاكرة (Memory Leak)
        if heartbeat_task:
            heartbeat_task.cancel()
        if pocket_ws and pocket_ws.open:
            await pocket_ws.close()
        log_to_file("Cleaned up resources successfully.")
