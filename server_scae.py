import asyncio
import websockets
import json
import sys

sys.stdout.reconfigure(encoding='utf-8')

class DynamicPassiveBridge:
    def __init__(self):
        self.client_connections = set()
        self.platform_ws = None
        self.is_connected_to_platform = False

    async def start_platform_handshake(self, auth_payload, client_ws):
        platform_url = "wss://api-us-north.po.market/socket.io/?EIO=4&transport=websocket"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Origin": "https://pocketoption.com"
        }

        try:
            print("[BRIDGE] Connecting directly to platform stream...")
            async with websockets.connect(platform_url, extra_headers=headers) as ws:
                self.platform_ws = ws
                self.is_connected_to_platform = True
                
                await self.platform_ws.send("40")
                await asyncio.sleep(1)
                
                print("[AUTH INJECTION] Sending raw SSID to platform...")
                await self.platform_ws.send(auth_payload)
                
                asyncio.create_task(self.keep_alive_loop())

                async for message in self.platform_ws:
                    if isinstance(message, bytes):
                        hex_data = message.hex()
                        broadcast_msg = f'451-["binary_update","{hex_data}"]'
                    else:
                        broadcast_msg = message
                    
                    if client_ws in self.client_connections:
                        await client_ws.send(broadcast_msg)

        except Exception as e:
            print(f"[PLATFORM ERROR] Disconnected or refused: {e}")
            self.is_connected_to_platform = False

    async def keep_alive_loop(self):
        while self.is_connected_to_platform and self.platform_ws:
            try:
                await self.platform_ws.send("2")
                print("[HEARTBEAT] Sent engine ping '2'")
                await asyncio.sleep(20)
            except:
                break

    async def handle_dashboard_signals(self, websocket):
        self.client_connections.add(websocket)
        print(f"[RENDER UI] Connected to frontend dashboard control. Total: {len(self.client_connections)}")
        
        try:
            async for message in websocket:
                data = json.loads(message)
                action = data.get("action")
                
                if action == "init_bridge":
                    auth_payload = data.get("auth_payload")
                    asyncio.create_task(self.start_platform_handshake(auth_payload, websocket))
                    
                elif action == "request_candles" and self.platform_ws:
                    asset = data.get("asset")
                    request_msg = f'42["changeSymbol", {{"asset": "{asset}"}}]'
                    await self.platform_ws.send(request_msg)

        except websockets.exceptions.ConnectionClosed:
            print("[RENDER UI] Closed connection with frontend.")
        finally:
            self.client_connections.remove(websocket)

async def main():
    bridge = DynamicPassiveBridge()
    async with websockets.serve(bridge.handle_dashboard_signals, "0.0.0.0", 10000):
        print("[LAUNCHED] Live Render Backend Service running on port 10000...")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
