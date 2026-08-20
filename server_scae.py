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
    return {"status": "running", "message": "Active"}

@app.websocket("/ws")
async def websocket_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    log_to_file("✅ Client connected")
    
    pocket_ws = None
    heartbeat_task = None
    
    try:
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        log_to_file(f"📝 SSID received: {ssid[:20]}...")
        
        raw_url = os.getenv("POCKET_URL")
        log_to_file(f"🔗 POCKET_URL: {raw_url}")
        
        if not raw_url:
            await client_ws.send_json({"status": "error", "message": "POCKET_URL missing"})
            return
        
        pocket_url = raw_url.strip('"').strip("'").strip()
        
        headers = {
            "Origin": "https://po.market",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache"
        }
        
        log_to_file("🔌 Connecting to Pocket Option...")
        
        async with websockets.connect(pocket_url, extra_headers=headers) as pocket_ws:
            log_to_file("✅ Connected to Pocket Option!")
            
            # فتح اتصال Socket.IO
            await pocket_ws.send("40")
            log_to_file("📤 Sent: 40")
            
            await asyncio.sleep(3)
            
            # Auth - التنسيق الصحيح
            auth_packet = f'42["auth",{{"session":"{ssid}","isDemo":1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file(f"📤 Auth sent: {auth_packet[:60]}...")
            
            await asyncio.sleep(3)
            
            await client_ws.send_json({"status": "platform_connected"})
            log_to_file("✅ platform_connected sent to client")
            
            # اشتراك في الأزواج
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol",{{"asset":"{pair}","timeframe":60}}]'
                await pocket_ws.send(sub_packet)
                log_to_file(f"📤 Subscribed: {pair}")
                await asyncio.sleep(2)
            
            log_to_file("✅ All subscriptions done")
            
            # Heartbeat
            async def send_heartbeat():
                try:
                    while True:
                        await asyncio.sleep(15)
                        await pocket_ws.send("2")
                        log_to_file("💓 Heartbeat sent")
                except:
                    pass
            
            heartbeat_task = asyncio.create_task(send_heartbeat())
            
            # استقبال الرسائل
            async for raw_message in pocket_ws:
                log_to_file(f"📥 RAW: {raw_message[:150]}")
                
                if raw_message == "2":
                    await pocket_ws.send("3")
                    continue
                
                if raw_message.startswith("42"):
                    try:
                        # إزالة "42" من البداية
                        json_str = raw_message[2:]
                        parsed = json.loads(json_str)
                        
                        if isinstance(parsed, list) and len(parsed) > 1:
                            event_name = parsed[0]
                            data = parsed[1]
                            
                            # log event
                            log_to_file(f"📋 Event: {event_name}")
                            
                            # التقاط الأسعار
                            if isinstance(data, dict):
                                asset = data.get("asset")
                                price = data.get("price")
                                
                                if asset and price is not None:
                                    log_to_file(f"💰 PRICE FOUND: {asset} = {price}")
                                    await client_ws.send_json({
                                        "status": "tick",
                                        "asset": asset,
                                        "price": float(price)
                                    })
                    except Exception as e:
                        log_to_file(f"⚠️ Parse error: {e}")
                        
    except WebSocketDisconnect:
        log_to_file("❌ Client disconnected")
    except Exception as e:
        log_to_file(f"❌ Error: {str(e)}")
        try:
            await client_ws.send_json({"status": "error", "message": str(e)})
        except:
            pass
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
        if pocket_ws and pocket_ws.open:
            await pocket_ws.close()
