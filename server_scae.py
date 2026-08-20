import asyncio
import json
import datetime
import websockets
import os
from fastapi import FastAPI, WebSocket
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

def log(msg):
    print(f"[{datetime.datetime.now()}] {msg}")

@app.get("/")
def home():
    return {"status": "running"}

@app.websocket("/ws")
async def ws_endpoint(client_ws: WebSocket):
    await client_ws.accept()
    
    try:
        data = await client_ws.receive_text()
        ssid = json.loads(data).get("ssid")
        
        url = os.getenv("POCKET_URL").strip()
        
        async with websockets.connect(url, extra_headers={"Origin": "https://po.market"}) as po:
            
            await po.send("40")
            await asyncio.sleep(2)
            
            auth = f'42["auth",{{"session":"{ssid}","isDemo":1}}]'
            await po.send(auth)
            await asyncio.sleep(3)
            
            await client_ws.send_json({"status": "platform_connected"})
            
            pairs = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc"]
            
            for pair in pairs:
                # التغيير الوحيد: period بدل timeframe
                sub = f'42["changeSymbol",{{"asset":"{pair}","period":60}}]'
                await po.send(sub)
                log(f"Subscribed: {pair}")
                await asyncio.sleep(1)
            
            while True:
                raw = await po.recv()
                log(f"RAW: {raw[:200]}")
                
                if raw == "2":
                    await po.send("3")
                    continue
                
                await client_ws.send_text(raw)
                
    except Exception as e:
        log(f"Error: {e}")
