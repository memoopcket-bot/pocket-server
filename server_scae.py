#!/usr/bin/env python3
"""
Pocket Option Dashboard - LOCAL VERSION for Windows 8.1 / Python 3.9
Uses pocketoptionapi-async (pure Python, no Rust bindings needed).
Polls get_candles for all pairs in parallel every 2 seconds.
"""
import asyncio
import json
import time
import os
import logging
from aiohttp import web
from pocketoptionapi_async import AsyncPocketOptionClient, ASSETS

# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
log = logging.getLogger("pocket-local")

# ---------------------------------------------------------------------------
client: AsyncPocketOptionClient | None = None
prices: dict = {}
connection_state: dict = {"connected": False, "is_demo": False, "balance": 0.0, "error": None}
poll_task = None
payout_task = None

STALE_THRESHOLD = 30

# Build asset list from the library's ASSETS dict
def get_all_assets():
    return list(ASSETS.keys())

def asset_name(asset: str) -> str:
    names = {
        "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
        "USDCHF": "USD/CHF", "USDCAD": "USD/CAD", "AUDUSD": "AUD/USD",
        "NZDUSD": "NZD/USD", "XAUUSD": "Gold", "XAGUSD": "Silver",
        "UKBrent": "Brent Oil", "USCrude": "WTI Oil",
        "BTCUSD": "Bitcoin", "ETHUSD": "Ethereum",
        "EURGBP": "EUR/GBP", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
        "AUDJPY": "AUD/JPY", "AUDCAD": "AUD/CAD", "AUDCHF": "AUD/CHF",
        "AUDNZD": "AUD/NZD", "CADCHF": "CAD/CHF", "CADJPY": "CAD/JPY",
        "CHFJPY": "CHF/JPY", "EURCHF": "EUR/CHF", "EURNZD": "EUR/NZD",
        "GBPAUD": "GBP/AUD", "NZDJPY": "NZD/JPY", "EURRUB": "EUR/RUB",
        "USDRUB": "USD/RUB", "EURHUF": "EUR/HUF", "CHFNOK": "CHF/NOK",
    }
    base = asset.replace("_otc", "")
    if base in names:
        return names[base] + (" OTC" if asset.endswith("_otc") else "")
    return asset

def is_otc(asset: str) -> bool:
    return "_otc" in asset.lower()

# ---------------------------------------------------------------------------
async def poll_all_prices():
    """Poll get_candles for ALL pairs in parallel every 2 seconds."""
    all_assets = get_all_assets()
    log.info(f"Starting price polling for {len(all_assets)} assets")

    for asset in all_assets:
        prices[asset] = {
            "price": 0.0, "time": 0, "streaming": False,
            "name": asset_name(asset), "otc": is_otc(asset), "payout": None,
        }

    while True:
        if client is None or not client.is_connected():
            await asyncio.sleep(1)
            continue
        try:
            # Poll all pairs in parallel with semaphore to limit concurrency
            sem = asyncio.Semaphore(15)

            async def fetch_one(asset):
                async with sem:
                    try:
                        candles = await client.get_candles(asset, '1m', 1)
                        if candles:
                            c = candles[-1]
                            price = c.close
                            prices[asset] = {
                                "price": price,
                                "time": time.time(),
                                "streaming": True,
                                "name": asset_name(asset),
                                "otc": is_otc(asset),
                                "payout": prices.get(asset, {}).get("payout"),
                            }
                    except Exception as e:
                        if asset in prices:
                            prices[asset]["streaming"] = False

            tasks = [fetch_one(a) for a in all_assets]
            await asyncio.gather(*tasks, return_exceptions=True)
            live = sum(1 for d in prices.values() if d.get("streaming"))
            log.info(f"Polling complete: {live}/{len(all_assets)} live")
        except Exception as e:
            log.error(f"Poll error: {e}")
        await asyncio.sleep(2)

async def fetch_payouts():
    """Periodically try to get payout info."""
    while True:
        if client and client.is_connected():
            try:
                # Try getting candles for known pairs to extract payout
                # pocketoptionapi-async may not have direct payout method
                pass
            except:
                pass
        await asyncio.sleep(60)

# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

async def handle_index(request):
    return web.FileResponse(os.path.join(os.path.dirname(os.path.abspath(__file__)), "index.html"))

async def handle_connect(request):
    global client, connection_state, prices, poll_task, payout_task

    if connection_state["connected"]:
        return web.json_response({"error": "Already connected"}, status=400)

    data = await request.json()
    ssid = data.get("ssid", "").strip()
    if not ssid:
        return web.json_response({"error": "SSID is required"}, status=400)

    prices.clear()
    connection_state = {"connected": False, "is_demo": False, "balance": 0.0, "error": None}

    try:
        log.info("Connecting to Pocket Option...")
        client = AsyncPocketOptionClient(ssid)
        connected = await client.connect()
        await asyncio.sleep(2)

        if not client.is_connected():
            connection_state["error"] = "Failed to connect - check SSID"
            return web.json_response({"error": "Connection failed"}, status=400)

        connection_state["connected"] = True
        try:
            balance = await client.get_balance()
            connection_state["balance"] = balance.amount if hasattr(balance, 'amount') else float(balance)
        except:
            connection_state["balance"] = 0.0
        connection_state["is_demo"] = "demo" in ssid.lower() or '"isDemo":1' in ssid

        all_assets = get_all_assets()
        log.info(f"Connected. Assets: {len(all_assets)}")

        # Start polling
        poll_task = asyncio.create_task(poll_all_prices())
        payout_task = asyncio.create_task(fetch_payouts())

        return web.json_response({
            "status": "connected",
            "is_demo": connection_state["is_demo"],
            "balance": connection_state["balance"],
            "assets_count": len(all_assets),
        })
    except Exception as e:
        log.error(f"Connect error: {e}")
        connection_state["error"] = str(e)
        return web.json_response({"error": str(e)}, status=500)

async def handle_disconnect(request):
    global client, connection_state, prices, poll_task, payout_task

    if poll_task:
        poll_task.cancel()
        poll_task = None
    if payout_task:
        payout_task.cancel()
        payout_task = None
    if client:
        try:
            await client.disconnect()
        except:
            pass
        client = None

    prices.clear()
    connection_state = {"connected": False, "is_demo": False, "balance": 0.0, "error": None}
    return web.json_response({"status": "disconnected"})

async def handle_pairs(request):
    now = time.time()
    result = []
    for asset, data in prices.items():
        last_update = data.get("time", 0)
        stale = (now - last_update > STALE_THRESHOLD) if last_update > 0 else True
        result.append({
            "asset": asset,
            "name": data.get("name", asset),
            "price": data.get("price", 0.0),
            "otc": data.get("otc", False),
            "payout": data.get("payout"),
            "streaming": data.get("streaming", False) and not stale,
            "last_update": last_update,
            "stale": stale,
        })
    result.sort(key=lambda x: (not x["streaming"], x["name"]))
    return web.json_response({"pairs": result, "count": len(result), "timestamp": now})

async def handle_status(request):
    streaming_count = sum(1 for d in prices.values() if d.get("streaming", False))
    return web.json_response({
        "connected": connection_state["connected"],
        "is_demo": connection_state["is_demo"],
        "balance": connection_state["balance"],
        "error": connection_state["error"],
        "streaming_count": streaming_count,
        "total_assets": len(prices),
    })

# ---------------------------------------------------------------------------
async def on_shutdown(app):
    global client, poll_task, payout_task
    if poll_task: poll_task.cancel()
    if payout_task: payout_task.cancel()
    if client:
        try: await client.disconnect()
        except: pass

def create_app():
    app = web.Application()
    app.router.add_get("/", handle_index)
    app.router.add_post("/api/connect", handle_connect)
    app.router.add_post("/api/disconnect", handle_disconnect)
    app.router.add_get("/api/pairs", handle_pairs)
    app.router.add_get("/api/status", handle_status)
    app.on_shutdown.append(on_shutdown)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    app = create_app()
    log.info(f"Starting LOCAL server on http://localhost:{port}")
    log.info(f"Total assets available: {len(get_all_assets())}")
    web.run_app(app, host="0.0.0.0", port=port)
