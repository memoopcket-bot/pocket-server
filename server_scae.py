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

ALL_PAIRS = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "AUDUSD_otc"]

def log_to_file(message):
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    with open("pocket_project_log.txt", "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

@app.get("/")
def home():
    return {"status": "running"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    log_to_file("Client connected")
    
    try:
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        log_to_file(f"SSID: {ssid[:20]}...")
        
        raw_url = os.getenv("POCKET_URL")
        if not raw_url:
            await client_ws.send_json({"status": "error", "message": "POCKET_URL missing"})
            return
        
        pocket_url = raw_url.strip('"').strip("'").strip()
        
        headers = {
            "Origin": "https://po.market",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        
        log_to_file("Connecting to PO...")
        async with websockets.connect(pocket_url, extra_headers=headers) as pocket_ws:
            log_to_file("Connected to PO!")
            
            # فتح
            await pocket_ws.send("40")
            log_to_file("Sent 40")
            await asyncio.sleep(2)
            
            # Auth
            auth = f'42["auth",{{"session":"{ssid}","isDemo":1}}]'
            await pocket_ws.send(auth)
            log_to_file("Auth sent")
            await asyncio.sleep(3)
            
            await client_ws.send_json({"status": "platform_connected"})
            log_to_file("platform_connected sent")
            
            # اشترك مع تأخير
            for pair in ALL_PAIRS:
                sub = f'42["changeSymbol",{{"asset":"{pair}","timeframe":60}}]'
                await pocket_ws.send(sub)
                log_to_file(f"Subscribed: {pair}")
                await asyncio.sleep(2)
            
            # استقبال كل الرسائل
            async for raw in pocket_ws:
                # سجل كل رسالة
                log_to_file(f"RAW: {raw[:150]}")
                
                if raw == "2":
                    await pocket_ws.send("3")
                    continue
                
                # محاولة استخراج أي سعر من أي رسالة
                if raw.startswith("42"):
                    try:
                        parsed = json.loads(raw[2:])
                        log_to_file(f"PARSED TYPE: {type(parsed)}")
                        
                        if isinstance(parsed, list):
                            log_to_file(f"EVENT: {parsed[0]}")
                            if len(parsed) > 1:
                                log_to_file(f"DATA: {json.dumps(parsed[1])[:100]}")
                                
                                data = parsed[1]
                                if isinstance(data, dict):
                                    # جرب كل الأسماء الممكنة للسعر
                                    asset = data.get("asset") or data.get("symbol") or data.get("active") or data.get("pair")
                                    price = data.get("price") or data.get("bid") or data.get("ask") or data.get("close") or data.get("value")
                                    
                                    if asset and price:
                                        log_to_file(f"✅ PRICE: {asset} = {price}")
                                        await client_ws.send_json({
                                            "status": "tick",
                                            "asset": asset,
                                            "price": float(price)
                                        })
                    except Exception as e:
                        log_to_file(f"Parse error: {e}")

    except Exception as e:
        log_to_file(f"Error: {str(e)}")
