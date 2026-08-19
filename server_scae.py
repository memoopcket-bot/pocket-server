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

# 5 أزواج فقط للبداية
ALL_PAIRS = [
    "EURUSD_otc",
    "GBPUSD_otc", 
    "USDJPY_otc",
    "EURGBP_otc",
    "AUDUSD_otc"
]

def log_to_file(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open("pocket_project_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")

@app.get("/")
def home():
    return {"status": "running"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    log_to_file("Client connected")
    
    pocket_ws = None
    heartbeat_task = None
    
    try:
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        raw_url = os.getenv("POCKET_URL")
        if not raw_url:
            await client_ws.send_json({"status": "error", "message": "POCKET_URL missing"})
            return

        pocket_url = raw_url.strip('"').strip("'").strip()
        
        headers = {
            "Origin": "https://po.market",
            "User-Agent": "Mozilla/5.0"
        }
        
        log_to_file("Connecting to PO...")
        
        async with websockets.connect(pocket_url, extra_headers=headers) as pocket_ws:
            log_to_file("Connected!")
            
            # انتظر قليلاً
            await asyncio.sleep(2)
            
            # افتح الاتصال
            await pocket_ws.send("40")
            log_to_file("Sent 40")
            
            await asyncio.sleep(2)
            
            # auth
            auth_msg = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 1, "platform": 1}}]'
            await pocket_ws.send(auth_msg)
            log_to_file("Auth sent")
            
            await asyncio.sleep(3)
            
            await client_ws.send_json({"status": "platform_connected"})
            log_to_file("platform_connected sent to client")
            
            # اشترك ببطء
            for pair in ALL_PAIRS:
                sub = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub)
                log_to_file(f"Subscribed: {pair}")
                await asyncio.sleep(2)  # تأخير 2 ثانية
            
            log_to_file("All subscriptions done")
            
            # heartbeat
            async def heartbeat():
                try:
                    while True:
                        await asyncio.sleep(10)
                        await pocket_ws.send("2")
                        log_to_file("Heartbeat sent")
                except:
                    pass
            
            heartbeat_task = asyncio.create_task(heartbeat())
            
            # استقبال
            async for raw in pocket_ws:
                log_to_file(f"RAW: {raw[:100]}")
                
                if raw == "2":
                    await pocket_ws.send("3")
                    continue
                
                if raw.startswith("42"):
                    try:
                        parsed = json.loads(raw[2:])
                        if isinstance(parsed, list) and len(parsed) > 1:
                            data = parsed[1]
                            if isinstance(data, dict):
                                asset = data.get("asset")
                                price = data.get("price")
                                if asset and price:
                                    log_to_file(f"✅ PRICE: {asset} = {price}")
                                    await client_ws.send_json({
                                        "status": "tick",
                                        "asset": asset,
                                        "price": float(price)
                                    })
                    except:
                        pass

    except WebSocketDisconnect:
        log_to_file("Client disconnected")
    except Exception as e:
        log_to_file(f"ERROR: {str(e)}")
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
