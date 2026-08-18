"""
server_scae.py - Integrated Web & WebSocket Passive Engine (2026)
"""

import json
import asyncio
import traceback
import os
from datetime import datetime

SERVER_HOST = "0.0.0.0"
SERVER_PORT = int(os.environ.get("PORT", 10000))

try:
    import websockets
    from websockets.server import serve
except ImportError:
    print("pip install websockets")
    exit(1)

# كود الواجهة المدمج بالكامل ليعرضه المتصفح مباشرة عند فتح الرابط
HTML_UI = """
<!DOCTYPE html>
<html lang="ar" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pocket Option Live Dashboard (2026)</title>
    <style>
        :root {
            --bg-main: #0b0e14;
            --bg-card: #151a22;
            --border: #222b36;
            --text: #c5cdd8;
            --primary: #2463eb;
            --success: #10b981;
            --danger: #ef4444;
        }
        body {
            background-color: var(--bg-main);
            color: var(--text);
            font-family: system-ui, -apple-system, sans-serif;
            margin: 0;
            padding: 15px;
        }
        .container { max-width: 800px; margin: 0 auto; }
        .card { background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 15px; margin-bottom: 15px; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
        input, select, button { width: 100%; padding: 12px; box-sizing: border-box; background: #1e2530; border: 1px solid var(--border); color: #fff; border-radius: 8px; font-size: 14px; margin-top: 5px; }
        button { background: var(--primary); font-weight: bold; cursor: pointer; border: none; transition: 0.2s; }
        button:active { transform: scale(0.98); }
        .btn-danger { background: var(--danger); }
        .status-bar { display: flex; justify-content: space-between; align-items: center; font-size: 13px; }
        .badge { padding: 4px 8px; border-radius: 20px; font-weight: bold; }
        .badge-red { background: rgba(239,68,68,0.2); color: var(--danger); }
        .badge-green { background: rgba(16,185,129,0.2); color: var(--success); }
        table { width: 100%; border-collapse: collapse; margin-top: 10px; font-size: 13px; }
        th, td { padding: 10px; text-align: center; border-bottom: 1px solid var(--border); }
        th { background: #1a222d; color: #94a3b8; }
        .log-box { background: #000; color: #38bdf8; font-family: monospace; padding: 10px; border-radius: 8px; height: 160px; overflow-y: auto; font-size: 11px; direction: ltr; text-align: left; }
    </style>
</head>
<body>
<div class="container">
    <div class="card">
        <div class="status-bar">
            <div>حالة الاتصال: <span id="statusBadge" class="badge badge-red">منقطع</span></div>
            <div>الرصيد: <span id="balanceText" style="color:var(--success); font-weight:bold;">$0.00</span></div>
        </div>
        <input type="text" id="ssidInput" placeholder="ضع حزمة التوثيق الكاملة هنا...">
        <div class="grid-2" style="margin-top: 10px;">
            <button id="connectBtn" onclick="initSmartConnection()">اتصال بشري مباشر</button>
            <button class="btn-danger" onclick="disconnectAll()">قطع الاتصال</button>
        </div>
    </div>
    <div class="card">
        <div class="grid-2">
            <div>
                <label>الزوج المختار</label>
                <select id="pairSelect">
                    <option value="EURUSD">EURUSD (عالمي)</option>
                    <option value="EURUSD_otc">EURUSD_otc (منصة)</option>
                </select>
            </div>
            <div>
                <label>عدد الشموع</label>
                <input type="number" id="limitInput" value="50">
            </div>
        </div>
        <button style="margin-top:12px; background:var(--success);" onclick="requestLiveCandles()">جلب الشموع اللحظية</button>
    </div>
    <div class="card">
        <h4 style="margin:0 0 10px 0;">📊 جداول البث المباشر</h4>
        <table>
            <thead><tr><th>الأصل / الزوج</th><th>السعر الحالي</th><th>الحالة اللحظية</th></tr></thead>
            <tbody id="pairsTableBody"><tr><td colspan="3" style="color:#64748b;">في انتظار الاتصال...</td></tr></tbody>
        </table>
    </div>
    <div class="card">
        <div class="status-bar" style="margin-bottom:5px;"><span style="font-weight:bold;">📜 سجل محرك التمويه:</span><span id="serverTime">--:--:--</span></div>
        <div id="logBox" class="log-box">جاهز للتوثيق المباشر...</div>
    </div>
</div>
<script>
    let localWS = null; let renderWS = null; let isConnected = false;
    const RENDER_WS_URL = window.location.origin.replace(/^http/, 'ws');
    function log(msg, isError = false) {
        const box = document.getElementById("logBox");
        const color = isError ? "#ef4444" : "#38bdf8";
        box.innerHTML += `<div style="color:${color}">[${new Date().toLocaleTimeString()}] ${msg}</div>`;
        box.scrollTop = box.scrollHeight;
    }
    function initSmartConnection() {
        let rawInput = document.getElementById("ssidInput").value.trim();
        if (!rawInput) return;
        let ssid = rawInput;
        if (rawInput.includes("session_id")) {
            const match = rawInput.match(/session_id\\\\";s:32:\\\\"([a-f0-9]{32})\\\\"/);
            if (match) { ssid = match[1]; log(`✅ تم اقتناص الـ SSID: ${ssid}`); }
        }
        log("🚀 جاري إطلاق طلب الاتصال وفحص استجابة السيرفر...");
        connectToPocketOption(ssid, rawInput);
    }
    function connectToPocketOption(ssid, originalPacket) {
        if (localWS) localWS.close();
        localWS = new WebSocket("wss://api-eu.po.market/socket.io/?EIO=4&transport=websocket");
        localWS.onopen = function() { localWS.send("40"); };
        localWS.onmessage = function(evt) {
            const msg = evt.data;
            log(`📥 [وارد]: ${msg.substring(0, 100)}`);
            if (msg === "2") { localWS.send("3"); return; }
            if (msg.startsWith("0")) {
                if (originalPacket.startsWith("42")) { localWS.send(originalPacket); }
                else { localWS.send(`42["auth",${JSON.stringify({"session":ssid,"isDemo":0,"uid":0,"platform":2,"isFastHistory":true,"isOptimized":true})}]`); }
            }
            if (msg.startsWith("42")) {
                try {
                    const clean = msg.substring(2); const parsed = JSON.parse(clean);
                    const ev = parsed[0]; const d = parsed[1];
                    if (!isConnected && (ev === "successauth" || ev === "updateBalance" || ev === "updateAssets")) {
                        isConnected = true;
                        document.getElementById("statusBadge").className = "badge badge-green";
                        document.getElementById("statusBadge").innerText = "متصل بنجاح";
                        connectToRenderServer();
                    }
                    if (d && (d.balance !== undefined || d.amount !== undefined)) {
                        document.getElementById("balanceText").innerText = `$${parseFloat(d.balance || d.amount).toFixed(2)}`;
                    }
                    if (renderWS && renderWS.readyState === WebSocket.OPEN) {
                        renderWS.send(JSON.stringify({ action: "pipe_data", event: ev, payload: d }));
                    }
                } catch (e) {}
            }
        };
    }
    function connectToRenderServer() {
        if (renderWS) renderWS.close();
        renderWS = new WebSocket(RENDER_WS_URL);
        renderWS.onmessage = function(evt) {
            try {
                const resp = JSON.parse(evt.data);
                if (resp.action === "candles" && resp.success) { updateTableWithCandles(resp.pair, resp.data); }
            } catch (e) {}
        };
    }
    function requestLiveCandles() {
        if (!isConnected || !localWS) return;
        const pair = document.getElementById("pairSelect").value;
        let sym = pair.endsWith("_otc") ? `#${pair.replace("_otc", "")}_otc` : pair;
        localWS.send(`42["loadHistoryPeriod",${JSON.stringify({"asset":sym,"period":60,"time":Math.floor(Date.now()/1000),"count":50})}]`);
    }
    function updateTableWithCandles(pair, data) {
        if (!data || data.length === 0) return;
        const last = data[data.length - 1]; const tbody = document.getElementById("pairsTableBody");
        if (tbody.innerHTML.includes("في انتظار")) tbody.innerHTML = "";
        let row = document.getElementById(`row_${pair}`) || document.createElement("tr");
        row.id = `row_${pair}`; tbody.appendChild(row);
        const isUp = last.c >= last.o;
        row.innerHTML = `<td><b>${pair}</b></td><td style="color:#38bdf8;font-family:monospace;">${parseFloat(last.c).toFixed(5)}</td><td style="color:${isUp?'#10b981':'#ef4444'}; font-weight:bold;">${isUp?'📈 صعود':'📉 هبوط'}</td>`;
    }
    function disconnectAll() { if (localWS) localWS.close(); if (renderWS) renderWS.close(); isConnected = false; document.getElementById("statusBadge").className = "badge badge-red"; document.getElementById("statusBadge").innerText = "منقطع"; }
    setInterval(() => { document.getElementById("serverTime").innerText = new Date().toLocaleTimeString(); }, 1000);
</script>
</body>
</html>
"""
class PassiveProcessor:
    def __init__(self):
        self.connected = False
        self.balance = 0
        self._candles = {}
        self._lock = asyncio.Lock()

    async def process_piped_data(self, event_name, data):
        self.connected = True
        if event_name in ("successauth", "updateBalance", "balanceUpdated"):
            if isinstance(data, dict):
                b = data.get("balance", data.get("amount", 0))
                if b: self.balance = float(b)
            return {"success": True, "balance": self.balance}

        if event_name in ("candles", "loadHistoryPeriod", "history"):
            if isinstance(data, dict):
                sym = data.get("asset", data.get("symbol", data.get("pair", "")))
                cndls = data.get("candles", data.get("data", data.get("history", [])))
                if sym and cndls:
                    clean_pair = sym.replace("#", "")
                    async with self._lock: self._candles[clean_pair] = cndls
                    fmt_data = self._fmt(cndls, 200)
                    return {"success": True, "action": "candles", "pair": clean_pair, "count": len(fmt_data), "data": fmt_data}
        return {"success": True}

    def _fmt(self, raw, limit):
        out = []
        for c in raw[-limit:]:
            try:
                if isinstance(c, dict):
                    out.append({"t": int(c.get("time", 0)), "o": float(c.get("open", 0)), "h": float(c.get("high", 0)), "l": float(c.get("low", 0)), "c": float(c.get("close", 0))})
            except: continue
        return out


processor = PassiveProcessor()
clients = set()


async def handle(ws):
    """
    محرك معالجة الاتصالات المزدوج (يعرض الواجهة للـ HTTP ويستقبل بيانات الـ WS)
    """
    if hasattr(ws, 'path') and (ws.path == "/" or ws.path == ""):
        try:
            headers = [
                ("Content-Type", "text/html; charset=utf-8"),
                ("Connection", "close"),
            ]
            await ws.respond(200, headers, HTML_UI.encode("utf-8"))
            return
        except Exception:
            pass

    clients.add(ws)
    print(f"📱 Browser connected to Render Backend ({len(clients)})")
    try:
        async for msg in ws:
            try:
                data = json.loads(msg)
                action = data.get("action", "")
                
                if action == "pipe_data":
                    event_name = data.get("event", "")
                    payload = data.get("payload", {})
                    resp = await processor.process_piped_data(event_name, payload)
                    
                    if resp and resp.get("action") == "candles":
                        await ws.send(json.dumps(resp, ensure_ascii=False))
                        
                elif action == "status":
                    await ws.send(json.dumps({
                        "success": True, 
                        "action": "status",
                        "connected": processor.connected, 
                        "balance": processor.balance,
                        "time": datetime.now().strftime("%H:%M:%S")
                    }, ensure_ascii=False))
                    
                elif action == "disconnect":
                    processor.connected = False
                    await ws.send(json.dumps({"success": True, "action": "disconnect"}))
                    
            except Exception as e:
                await ws.send(json.dumps({"success": False, "error": str(e)[:100]}))
    except Exception as e:
        print(f"⚠️ handle_error: {e}")
    finally:
        clients.discard(ws)
        print(f"📱 Browser disconnected from Render Backend ({len(clients)})")


async def main():
    print(f"🚀 Integrated Web/WS Passive Engine Initialized")
    print(f"Server Route: http://0.0.0:{SERVER_PORT}")
    
    async with serve(handle, SERVER_HOST, SERVER_PORT,
                     ping_interval=30, ping_timeout=10,
                     max_size=10 * 1024 * 1024):
        print("⚡ [Integrated Passive Server Listening for Web & Data Requests]")
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Stopped")
    except Exception:
        traceback.print_exc()
