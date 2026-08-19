import asyncio
import json
import datetime
import websockets
import base64
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
    return {"status": "running", "message": "Pocket Option Cloud Server is Active"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    log_to_file("Web client connected successfully.")
    
    pocket_ws = None
    try:
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        cipher_text = "d3NzOi8vYXBpLWluLnBvY2tldG9wdGlvbi5jb206ODA5NS9zb2NrZXQuaW8vP0VJTz0zJnRyYW5zcG9ydD13ZWJzb2NrZXQ="
        pocket_url = base64.b64decode(cipher_text).decode("utf-8")
        
        log_to_file("Connecting to platform server...")
        
        async with websockets.connect(pocket_url) as pocket_ws:
            log_to_file("Physical connection established.")
            await pocket_ws.send("40")
            
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file("Auth packet sent successfully.")
            
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
        log_to_file("Web client disconnected.")
    except Exception as e:
        log_to_file(f"Protocol error: {str(e)}")
        try:
            await client_ws.send_json({"status": "error", "message": str(e)})
        except:
            pass
