import asyncio
import threading
import ujson
import urllib.parse
from http.server import SimpleHTTPRequestHandler, HTTPServer
import websockets

current_payload = ""
live_prices_cache = {}
websocket_task = None
loop = None

class MyServer(SimpleHTTPRequestHandler):
    def do_GET(self):
        global current_payload
        parsed_url = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        # API endpoint for frontend UI to fetch live price updates
        if parsed_url.path == '/api/prices':
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(ujson.dumps(live_prices_cache), "utf-8"))
            return

        if 'payload' in params:
            raw_input = params['payload'].strip()
            if raw_input:
                current_payload = raw_input
                print("🔄 Protected raw token update deployed successfully!")
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(restart_websocket(), loop)
        
        try:
            with open("index.html", "r", encoding="utf-8") as file:
                html_content = file.read()
        except Exception:
            html_content = "<h1>Error loading dashboard file (index.html)</h1>"

        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(bytes(html_content, "utf-8"))

def run_http_server():
    try:
        server = HTTPServer(('0.0.0.0', 10000), MyServer)
        print("HTTP Core Dashboard engine online on port 10000...")
        server.serve_forever()
    except Exception as e:
        print(f"Server engine failure: {e}")

async def restart_websocket():
    global websocket_task
    if websocket_task:
        websocket_task.cancel()
        try:
            await websocket_task
        except asyncio.CancelledError:
            pass
    websocket_task = asyncio.create_task(get_live_prices_loop())

async def get_live_prices_loop():
    global current_payload, live_prices_cache
    uris = [
        "wss://api-eu.po.market/v1/v2/websocket",
        "wss://api.po.market/v1/v2/websocket",
        "wss://api-prod.po.market/v1/v2/websocket"
    ]
    
    # Updated headers matching 2006 browser fingerprints
    extra_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Origin": "https://pocketoption.com"
    }
    
    while True:
        if not current_payload:
            await asyncio.sleep(2)
            continue
            
        for uri in uris:
            if not current_payload:
                break
            try:
                # Upgraded connect parameters for modern websockets library
                async with websockets.connect(uri, open_timeout=15, extra_headers=extra_headers) as websocket:
                    print(f"🎯 Successfully connected to live feed pool: {uri}")
                    
                    auth_string = current_payload
                    if auth_string.startswith("42"):
                        auth_string = auth_string[2:]
                    
                    await websocket.send(f"42{auth_string}")
                    print("🔒 Session synchronization payload broadcasted.")
                    
                    # Requesting active ticks stream for OTC pair
                    subscribe_msg = '42["subscribe_candles",{"asset":"EURUSD_otc","period":60}]'
                    await websocket.send(subscribe_msg)
                    
                    while current_payload:
                        response = await websocket.recv()
                        if response.startswith("42"):
                            try:
                                # Ultra-fast JSON stripping using native ujson
                                raw_json = response[2:]
                                parsed_data = ujson.loads(raw_json)
                                if len(parsed_data) > 1:
                                    live_prices_cache["EURUSD"] = parsed_data
                                    print(f"📈 Match incoming tick stream: {parsed_data}")
                            except Exception:
                                pass
                        await asyncio.sleep(0.01)
            except asyncio.CancelledError:
                return
            except Exception as e:
                print(f"⚠️ Feed connection failed: {e}")
                await asyncio.sleep(3)

def start_async_loop():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(get_live_prices_loop())

if __name__ == "__main__":
    threading.Thread(target=run_http_server, daemon=True).start()
    start_async_loop()
