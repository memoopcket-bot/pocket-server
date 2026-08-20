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

# تقسيم الأزواج إلى مجموعتين حسب نوع الصفحة
FOREX_PAIRS = ["EURUSD", "GBPUSD", "USDJPY"]
OTC_PAIRS = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "EURGBP_otc", "AUDUSD_otc"]
ALL_PAIRS = FOREX_PAIRS + OTC_PAIRS

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
    log_to_file("A new web page/client connected to the server.")
    
    pocket_ws = None
    heartbeat_task = None
    
    try:
        # استقبال البيانات الأولى من الصفحة
        init_data = await client_ws.receive_text()
        init_json = json.loads(init_data)
        ssid = init_json.get("ssid")
        
        # ميزة التصفية: تفرز الصفحة بناءً على نوع الطلب القادم منها (forex أو otc)
        # إذا لم ترسل الصفحة نوعاً، ستقوم بعرض كل شيء كالعادة
        page_type = init_json.get("page_type", "all") 
        
        raw_url = os.getenv("POCKET_URL")
        if not raw_url:
            log_to_file("Error: POCKET_URL variable not found in Render settings.")
            await client_ws.send_json({"status": "error", "message": "POCKET_URL not configured"})
            return

        pocket_url = raw_url.strip('"').strip("'").strip()
        log_to_file(f"Target URL cleaned for this session.")

        custom_headers = {
            "Origin": "https://po.market",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        log_to_file(f"Connecting page ({page_type}) to Pocket Option Server...")
        async with websockets.connect(pocket_url, extra_headers=custom_headers) as pocket_ws:
            log_to_file("Connection stable with Pocket Option.")
            
            # بروتوكول EIO=4 فتح الاتصال
            await pocket_ws.send("40")
            
            # التوثيق بالـ SSID
            auth_packet = f'42["auth", {{"session": "{ssid}", "isDemo": 1, "uid": 999999, "platform": 1}}]'
            await pocket_ws.send(auth_packet)
            log_to_file("Auth token delivered successfully.")
            
            await client_ws.send_json({"status": "platform_connected"})
            
            # الاشتراك في الأزواج للمنصة كاملة
            for pair in ALL_PAIRS:
                sub_packet = f'42["changeSymbol", {{"asset": "{pair}", "timeframe": 60}}]'
                await pocket_ws.send(sub_packet)

            async def send_heartbeat():
                try:
                    while pocket_ws.open:
                        await asyncio.sleep(20)
                        await pocket_ws.send("2")
                except asyncio.CancelledError:
                    pass

            heartbeat_task = asyncio.create_task(send_heartbeat())

            async for raw_message in pocket_ws:
                if raw_message == "2":
                    await pocket_ws.send("3")
                    continue
                
                if raw_message.startswith("42"):
                    try:
                        parsed = json.loads(raw_message[2:])
                        if isinstance(parsed, list) and len(parsed) > 1 and parsed[0] == "tick":
                            tick_info = parsed[1]
                            asset_name = tick_info.get("asset")
                            
                            # الفلترة الذكية: إرسال الأسعار للصفحة بناءً على نوعها المفضل
                            should_send = False
                            if page_type == "forex" and asset_name in FOREX_PAIRS:
                                should_send = True
                            elif page_type == "otc" and asset_name in OTC_PAIRS:
                                should_send = True
                            elif page_type == "all":
                                should_send = True
                                
                            if should_send:
                                # إرسال البيانات النظيفة للجدول مباشرة
                                await client_ws.send_json({
                                    "status": "tick",
                                    "asset": asset_name,
                                    "price": tick_info.get("price")
                                })
                    except Exception:
                        pass
                    
                    # تمرير النص الخام أيضاً للتوافق الكامل
                    await client_ws.send_text(raw_message)

    except WebSocketDisconnect:
        log_to_file("One of the connected pages closed.")
    except Exception as e:
        log_to_file(f"Error in session: {str(e)}")
    finally:
        if heartbeat_task:
            heartbeat_task.cancel()
        if pocket_ws and pocket_ws.open:
            await pocket_ws.close()
