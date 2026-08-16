import asyncio
import threading
import json
import urllib.parse
from http.server import SimpleHTTPRequestHandler, HTTPServer
import websockets

current_ssid = ""
websocket_task = None
loop = None

class MyServer(SimpleHTTPRequestHandler):
    def do_GET(self):
        global current_ssid
        parsed_url = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed_url.query)
        
        if 'ssid' in params:
            new_ssid = params['ssid'].strip()
            if new_ssid:
                current_ssid = new_ssid
                print(f"🔄 Token received: {current_ssid[:10]}...")
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
        print("HTTP Dashboard running...")
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
    global current_ssid
    uris = [
        "wss://api-eu.po.market/v1/v2/websocket",
        "wss://api.po.market/v1/v2/websocket",
        "wss://api-prod.po.market/v1/v2/websocket"
    ]
    
    while True:
        if not current_ssid:
            await asyncio.sleep(5)
            continue
            
        for uri in uris:
            if not current_ssid:
                break
            try:
                async with websockets.connect(uri, open_timeout=10) as websocket:
                    print("✅ Connection established with Pocket Option!")
                    auth_payload = {"auth": {"session": current_ssid}}
                    await websocket.send(json.dumps(auth_payload))
                    
                    while current_ssid:
                        response = await websocket.recv()
                        print(f"Live Data: {response}")
                        await asyncio.sleep(0.5)
            except asyncio.CancelledError:
                return
            except Exception as e:
                print(f"❌ Connection failed: {e}")
                await asyncio.sleep(3)

def start_async_loop():
    global loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(get_live_prices_loop())

if __name__ == "__main__":
    threading.Thread(target=run_http_server, daemon=True).start()
    start_async_loop()
