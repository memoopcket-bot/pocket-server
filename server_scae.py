import json
import datetime
import os
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pocketoptionapi import PocketOptionAPI

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    log_to_file("✅ Client connected")
    
    try:
        # استقبال SSID
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        log_to_file(f"📝 SSID: {ssid[:20]}...")
        
        # إنشاء API
        api = PocketOptionAPI(ssid)
        log_to_file("🔧 API created")
        
        # اتصال
        api.connect()
        log_to_file("✅ Connected to Pocket Option")
        
        await client_ws.send_json({"status": "platform_connected"})
        log_to_file("📤 platform_connected sent")
        
        # الأزواج
        pairs = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "AUDUSD_otc"]
        
        # اشتراك
        for pair in pairs:
            try:
                api.subscribe(pair)
                log_to_file(f"📤 Subscribed: {pair}")
            except Exception as e:
                log_to_file(f"⚠️ Subscribe error {pair}: {e}")
            await asyncio.sleep(1)
        
        log_to_file("✅ All subscriptions done")
        
        # استقبال الأسعار
        while True:
            try:
                # جلب الأسعار
                prices = api.get_price()
                
                if prices:
                    if isinstance(prices, dict):
                        asset = prices.get("asset")
                        price = prices.get("price")
                        if asset and price:
                            await client_ws.send_json({
                                "status": "tick",
                                "asset": asset,
                                "price": float(price)
                            })
                    elif isinstance(prices, list):
                        for p in prices:
                            if isinstance(p, dict):
                                asset = p.get("asset")
                                price = p.get("price")
                                if asset and price:
                                    await client_ws.send_json({
                                        "status": "tick",
                                        "asset": asset,
                                        "price": float(price)
                                    })
            except Exception as e:
                log_to_file(f"⚠️ Price error: {e}")
            
            await asyncio.sleep(1)
            
    except WebSocketDisconnect:
        log_to_file("❌ Client disconnected")
    except Exception as e:
        log_to_file(f"❌ Error: {e}")
