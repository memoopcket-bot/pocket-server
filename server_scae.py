import asyncio
import threading
import json
from http.server import SimpleHTTPRequestHandler, HTTPServer
import websockets

# 1. كود تشغيل ويب سيرفر لضمان بقاء السيرفر حياً في Render
class MyServer(SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(bytes("سيرفر جلب أسعار بوكت اوبشن اللحظية يعمل سحابياً بنجاح!", "utf-8"))

def run_http_server():
    server = HTTPServer(('0.0.0.0', 10000), MyServer)
    print("HTTP Server started on port 10000...")
    server.serve_forever()

# 2. كود جلب الأسعار اللحظية المباشر عبر الـ WebSocket
async def get_live_prices():
    # الرابط السريع والخفيف لـ WebSocket منصة بوكت اوبشن
    uri = "wss://api-prod.po.market/v1/v2/websocket"
    
    print("جاري محاولة الاتصال بـ Pocket Option WebSocket...")
    try:
        async with websockets.connect(uri) as websocket:
            print("تم الاتصال بنجاح! السيرفر يستقبل البيانات اللحظية الآن...")
            
            # حلقة مستمرة لقراءة البيانات اللحظية فوراً
            while True:
                response = await websocket.recv()
                print(f"البيانات اللحظية المستلمة: {response}")
                await asyncio.sleep(1)
    except Exception as e:
        print(f"حدث انقطاع أو خطأ في الاتصال: {e}")

if __name__ == "__main__":
    # تشغيل سيرفر الويب في خلفية مستقلة (Thread) حتى لا يتعطل البناء
    threading.Thread(target=run_http_server, daemon=True).start()
    
    # تشغيل حلقة جلب الأسعار اللحظية بشكل مستمر
    asyncio.run(get_live_prices())
