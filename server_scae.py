import asyncio
import os
import ujson
import time
import re
from aiohttp import web
import websockets

# Global State
current_ssid = ""
live_prices = {}
websocket_task = None

# Supported Asset Lists (Forex & OTC Only)
FOREX_PAIRS = ["EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD"]
OTC_PAIRS = ["EURUSD_otc", "GBPUSD_otc", "USDJPY_otc", "USDCHF_otc", "USDCAD_otc", "AUDUSD_otc", "NZDUSD_otc"]
ALL_ASSETS = FOREX_PAIRS + OTC_PAIRS

# 1. API Endpoints Configuration
async def handle_index(request):
    try:
        with open("index.html", "r", encoding="utf-8") as file:
            return web.Response(text=file.read(), content_type="text/html")
    except Exception:
        return web.Response(text="<h1>Dashboard index.html missing</h1>", content_type="text/html", status=404)

async def handle_connect(request):
    global current_ssid, websocket_task
    data = await request.json()
    ssid_input = data.get("ssid", "").strip()
    
    if not ssid_input:
        return web.json_response({"error": "SSID is required"}, status=400)
    
    # Core Fix: Automatically clean escaped backslashes from the raw input string
    ssid_input = ssid_input.replace('\\"', '"').replace('\\\\', '\\')
    current_ssid = ssid_input
    print("🔄 Cleaned raw SSID payload injected into backend router.")
    
    # Trigger active loop connection
    if websocket_task:
        websocket_task.cancel()
    websocket_task = asyncio.create_task(pocket_option_websocket_loop())
    
    return web.json_response({"status": "connected", "assets_count": len(ALL_ASSETS)})

async def handle_disconnect(request):
    global current_ssid, websocket_task, live_prices
    current_ssid = ""
    live_prices.clear()
    if websocket_task:
        websocket_task.cancel()
    print("🔌 Disconnected via manual dashboard action.")
    return web.json_response({"status": "disconnected"})

async def handle_pairs(request):
    now = time.time()
    result = []
    for asset, price in live_prices.items():
        is_otc_asset = "_otc" in asset.lower()
        result.append({
            "asset": asset,
            "name": asset.replace("_otc", " / OTC" if is_otc_asset else ""),
            "price": price,
            "otc": is_otc_asset,
            "streaming": True,
            "last_update": now
        })
    return web.json_response({"pairs": result, "timestamp": now})

async def handle_status(request):
    return web.json_response({
        "connected": bool(current_ssid),
        "streaming_count": len(live_prices),
        "total_assets": len(ALL_ASSETS)
    })

# 2. Main Verified Pocket Option Connection Loop
async def pocket_option_websocket_loop():
    global current_ssid, live_prices
    uri = "wss://api-eu.po.market/v1/v2/websocket"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Origin": "https://pocketoption.com"
    }
    
    while current_ssid:
        try:
            async with websockets.connect(uri, open_timeout=15, extra_headers=headers) as websocket:
                print("✅ Securely handshake established with Pocket Option pipeline.")
                
                # Format full validation packet with proper current epoch timestamps
                auth_packet = current_ssid
                current_epoch = int(time.time())
                auth_packet = re.sub(r'i:\d+;', f'i:{current_epoch};', auth_packet)
                if not auth_packet.startswith("42"):
                    auth_packet = f"42{auth_packet}"
                
                await websocket.send(auth_packet)
                print("🔑 Authenticated session tracking packet sent.")
                
                # Bulk subscription request for the targeted dynamic assets
                for asset in ALL_ASSETS:
                    sub_msg = f'42["subscribe_candles",_wrap_asset_sub("{asset}")]'
                    sub_msg = f'42["subscribe_candles",{{"asset":"{asset}","period":60}}]'
                    await websocket.send(sub_msg)
                
                while current_ssid:
                    response = await websocket.recv()
                    
                    # Core engine connection maintainer
                    if response == "2":
                        await websocket.send("3")
                        continue
                    
                    if response.startswith("42"):
                        try:
                            parsed = ujson.loads(response[2:])
                            if isinstance(parsed, list) and len(parsed) > 1:
                                msg_type = parsed[0]
                                msg_data = parsed[1]
                                # Capture realtime closing prices or active tick pool values
                                if msg_type == "candles" or msg_type == "tick":
                                    asset_id = msg_data.get("asset")
                                    if asset_id in ALL_ASSETS:
                                        live_prices[asset_id] = msg_data.get("close") or msg_data.get("price", 0.0)
                        except Exception:
                            pass
                    await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            return
        except Exception as e:
            print(f"⚠️ Pipeline connection dropped, retrying: {e}")
            await asyncio.sleep(3)

# 3. Server Factory Initializer
def create_app():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_post("/api/connect", handle_connect)
    app.router.add_post("/api/disconnect", handle_disconnect)
    app.router.add_get("/api/pairs", handle_pairs)
    app.router.add_get("/api/status", handle_status)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app = create_app()
    print(f"Starting standard microserver engine on port {port}")
    web.run_app(app, host="0.0.0.0", port=port)
