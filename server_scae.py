import asyncio
import threading
from http.server import SimpleHTTPRequestHandler, HTTPServer
from pocketoptionapi.stable_api import PocketOptionAPI  # المكتبة الحديثة لسحب الأسعار

# 1. كود تشغيل السيرفر لـ Render لحمايته من الإغلاق
class MyServer(SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(bytes("سيرفر جلب أسعار بوكت اوبشن يعمل بنجاح سحابياً!", "utf-8"))

def run_http_server():
    server = HTTPServer(('0.0.0.0', 10000), MyServer)
    print("HTTP Server started on port 10000...")
    server.serve_forever()

# 2. كود جلب الأسعار اللحظية عبر الـ WebSocket للمكتبة الحديثة
async def fetch_pocket_option_prices():
    # استبدل الـ SSID بالرمز الخاص بحسابك المستخرج من المتصفح لتوثيق الاتصال
    ssid = 'YOUR_POCKET_OPTION_SSID_HERE' 
    
    print("جاري الاتصال بـ Pocket Option WebSocket...")
    api = PocketOptionAPI(ssid)
    success = await api.connect()
    
    if success:
        print("تم الاتصال بنجاح! جاري سحب الأسعار اللحظية لأزواج العملات...")
        # اشتراك في سحب أسعار زوج معين لحظياً كمثال (اليورو مقابل الدولار)
        await api.subscribe_to_asset("EURUSD")
        
        # حلقة تكرارية لطباعة الأسعار اللحظية فور وصولها
        while True:
            prices = api.get_realtime_candles("EURUSD")
            if prices:
                print(f"السعر اللحظي الحالي: {prices[-1]}")
            await asyncio.sleep(1) # انتظر ثانية وجدد السعر
    else:
        print("فشل الاتصال، يرجى التحقق من صلاحية الـ SSID الخاص بك.")

if __name__ == "__main__":
    # تشغيل سيرفر الويب في خلفية مستقلة (Thread) حتى لا يتعطل Render
    threading.Thread(target=run_http_server, daemon=True).start()
    
    # تشغيل مهمة جلب الأسعار اللحظية المستمرة
    asyncio.run(fetch_pocket_option_prices())

