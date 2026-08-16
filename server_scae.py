import asyncio
import threading
import json
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
        
        # If accessing the live stream API for frontend updates
        if parsed_url.path == '/api/prices':
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(bytes(json.dumps(live_prices_cache), "utf-8"))
            return

        # Catch full raw payload input from the HTML form
        if 'payload' in params:
            raw_input = params['payload'].strip()
            if raw_input:
                current_payload = raw_input
                print("🔄 New raw payload received via Dashboard panel!")
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
        print("HTTP Live Data Dashboard running...")
        server.serve_forever()
    except Exception as e:
        print(f"Server error: {e}")

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
    
    while True:
        if not current_payload:
            await asyncio.sleep(2)
            continue
            
        for uri in uris:
            if not current_payload:
                break
            try:
                async with websockets.connect(uri, open_timeout=10) as websocket:
                    print(f"✅ Connected to feed socket: {uri}")
                    
                    # Clean and parse the raw input payload if it has leading characters like 42
                    auth_string = current_payload
                    if auth_string.startswith("42"):
                        auth_string = auth_string[2:]
                    
                    # Send authentication packet directly
                    await websocket.send(f"42{auth_string}")
                    print("🔑 Authentic custom packet transmitted successfully!")
                    
                    # Request real-time prices feed for major asset (e.g., EURUSD)
                    # Payload format for subscribing to assets on Pocket Option
                    subscribe_msg = '42["subscribe_candles",{"asset":"EURUSD","period":60}]'
                    await websocket.send(subscribe_msg)
                    
                    while current_payload:
                        response = await websocket.recv()
                        # Extract ticks/prices if response is a data packet
                        if response.startswith("42"):
                            try:
                                data = json.loads(response[2:])
                                if data[0] == "candles" or data[0] == "tick":
                                    live_prices_cache["EURUSD"] = data[1]
                                    print(f"📈 Price update captured: {data[1]}")
                            except Exception:
                                pass
                        await asyncio.sleep(0.1)
            except asyncio.CancelledError:
                return
            except Exception as e:
                print(f"❌ Connection pipeline dropped: {e}")
                await asyncio.sleep(3)

def start_async_loop():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(get_live_prices_loop())

if __name__ == "__main__":
    threading.Thread(target=run_http_server, daemon=True).start()
    start_async_loop()
