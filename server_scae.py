import asyncio
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
import websockets

# 1. Web Server for Render Health Check
class MyServer(SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(bytes("Pocket Option Live Price Server is Running Successfully!", "utf-8"))

def run_http_server():
    try:
        server = HTTPServer(('0.0.0.0', 10000), MyServer)
        print("HTTP Server started on port 10000...")
        server.serve_forever()
    except Exception as e:
        print(f"Web server error: {e}")

# 2. Pocket Option WebSocket Price Fetcher
async def get_live_prices():
    # Pocket Option backup WebSocket URIs
    uris = [
        "wss://api-eu.po.market/v1/v2/websocket",
        "wss://api.po.market/v1/v2/websocket",
        "wss://api-prod.po.market/v1/v2/websocket"
    ]
    
    for uri in uris:
        print(f"Trying to connect to: {uri}")
        try:
            async with websockets.connect(uri, open_timeout=10) as websocket:
                print("✅ Successfully connected to Pocket Option Server!")
                
                # Continuous loop to receive live ticks and prices
                while True:
                    response = await websocket.recv()
                    print(f"Data received: {response}")
                    await asyncio.sleep(0.5)
        except Exception as e:
            print(f"❌ Connection failed for {uri} due to: {e}")
            print("Switching to the next backup URI in 3 seconds...")
            await asyncio.sleep(3)

if __name__ == "__main__":
    # Run the HTTP server in a background thread to prevent Render from freezing
    threading.Thread(target=run_http_server, daemon=True).start()
    
    # Start the continuous loop to fetch data
    asyncio.run(get_live_prices())
