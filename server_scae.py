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
    with open("pocket_project_log.txt", "w", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

@app.get("/")
def home():
    return {"status": "running", "message": "Active"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    log_to_file("Web client connected.")
    
    pocket_ws = None
    try:
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        # قراءة الرابط تلقائياً من إعدادات ريندر بأمان تام وبدون أي أخطاء لغة
        pocket_url = os.getenv("POCKET_URL")
        
        if not pocket_url:
            log_to_file("Error: POCKET_URL variable not found in Render settings.")
            return

        log_to_file(f"Connecting to: {pocket_url[:30]}...")
        
        async with websockets.connect(pocket_url) as pocket_ws:
            log_to_file("Connection stable.")
            await pocket_ws.send("40")
            
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file("Auth token delivered.")
            
            await client_ws.send_json({"status": "platform_connected"})
            
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub_packet)

            async def send_heartbeat():
                while True:
                    await asyncio.sleep(20)
                    if pocket_ws.open:
                        await pocket_ws.send("2")

            asyncio.create_task(send_heartbeat())

            async for raw_message in pocket_ws:
                if raw_message.startswith("42"):
                    try:
                        parsed = json.loads(raw_message[2:])
                        if isinstance(parsed, list) and parsed == "tick":
                            tick_info = parsed
                            await client_ws.send_json({
                                "status": "tick",
                                "asset": tick_info.get("asset"),
                                "price": tick_info.get("price")
                            })
                            continue
                    except:
                        pass
                    await client_ws.send_text(raw_message)
                elif raw_message == "3":
                    pass

    except WebSocketDisconnect:
        log_to_file("Disconnected.")
    except Exception as e:
        log_to_file(f"Error: {str(e)}")
        try:
            await client_ws.send_json({"status": "error", "message": str(e)})
        except:
            pass
