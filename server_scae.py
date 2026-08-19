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
        
        raw_url = os.getenv("POCKET_URL")
        if not raw_url:
            log_to_file("Error: POCKET_URL variable not found in Render settings.")
            await client_ws.send_json({"status": "error", "message": "POCKET_URL not configured"})
            return

        pocket_url = raw_url.strip('"').strip("'").strip()
        log_to_file(f"Target URL cleaned.")

        custom_headers = {
            "Origin": "https://po.market",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        log_to_file("Connecting to Pocket Option EIO=4 Server...")
        async with websockets.connect(pocket_url, extra_headers=custom_headers) as pocket_ws:
            log_to_file("Connection stable with Pocket Option.")
            
            # بروتوكول EIO=4 الموضح في صورتك يتطلب إرسال "40" من طرفنا أولاً لفتح قناة الاتصال
            await pocket_ws.send("40")
            
            # إرسال حزمة التوثيق بالـ SSID الخاص بك
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file("Auth token delivered successfully.")
            
            await client_ws.send_json({"status": "platform_connected"})
            
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub_packet)
            log_to_file(f"Subscribed to {len(ALL_PAIRS)} pairs.")

            # دالة الهارت بيت المتوافقة مع سيرفر EIO=4 الحديث لإرسال "3" عند طلب السيرفر
            async def send_heartbeat():
                try:
                    while pocket_ws.open:
                        await asyncio.sleep(20)
                        # إرسال النبضة للحفاظ على استقرار الجلسة الحية
                        await pocket_ws.send("2")
                except asyncio.CancelledError:
                    pass

            heartbeat_task = asyncio.create_task(send_heartbeat())

            async for raw_message in pocket_ws:
                # إذا أرسل السيرفر طلب نبضة "2" نرد بـ "3"
                if raw_message == "2":
                    await pocket_ws.send("3")
                    continue
                
                if raw_message.startswith("42"):
                    try:
                        parsed = json.loads(raw_message[2:])
                        if isinstance(parsed, list) and len(parsed) > 1 and parsed[0] == "tick":
                            tick_info = parsed[1]
                            await client_ws.send_json({
                                "status": "tick",
                                "asset": tick_info.get("asset"),
                                "price": tick_info.get("price")
                            })
                            continue
                    except Exception:
                        pass
                    
                    await client_ws.send_text(raw_message)

    except WebSocketDisconnect:
        log_to_file("Client Web interface disconnected.")
    except Exception as e:
        log_to_file(f"Critical Error: {str(e)}")
        try:
            await client_ws.send_json({"status": "error", "message": str(e)})
        except:
            pass
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
        if pocket_ws and pocket_ws.open:
            await pocket_ws.close()
        log_to_file("Cleaned up resources.")
