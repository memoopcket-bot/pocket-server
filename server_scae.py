import os
import json
import logging
import asyncio
import websockets

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s.%(msecs)03d | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("PO_Passive_Processor")
LIVE_MARKET_DATA = {}

class StreamState:
    def __init__(self):
        self.expect_binary = False
        self.active_event = None
        self.tracked_asset = "UNKNOWN"
        self.packet_counter = 0

state = StreamState()

async def parse_and_route_frame(frame, is_binary: bool):
    state.packet_counter += 1
    p_num = state.packet_counter

    # معالجة مجرى البيانات الثنائية المستلمة من نفق المتصفح
    if is_binary:
        if state.expect_binary and state.active_event == "updateStream":
            decoded = frame.decode('utf-8', errors='ignore')
            LIVE_MARKET_DATA[state.tracked_asset] = {"stream": decoded[:100]}
            logger.info(f"📊 [Packet #{p_num}] Price for {state.tracked_asset} -> {decoded[:50]}")
            state.expect_binary = False
            state.active_event = None
        return

    # معالجة مجرى الحزم النصية لتوثيق تتابع العدادات
    if isinstance(frame, str):
        if frame in ["2", "3"] or frame.startswith("42[\"ps\""): 
            return
            
        if frame.startswith("42"):
            try:
                data = json.loads(frame[2:])
                if isinstance(data, list) and len(data) >= 2:
                    if data[0] == "changeSymbol": 
                        state.tracked_asset = data[1].get("asset", "UNKNOWN")
                    elif data[0] == "subFor": 
                        state.tracked_asset = str(data[1])
            except: 
                pass
        elif frame.startswith("451-"):
            try:
                data = json.loads(frame[4:])
                if isinstance(data, list) and len(data) >= 2:
                    state.expect_binary = True
                    state.active_event = data[0]
            except: 
                pass

async def bridge_handler(websocket, path=None):
    logger.info("🔌 Human tunnel connected to Render backend successfully!")
    state.packet_counter = 0
    try:
        async for message in websocket:
            await parse_and_route_frame(message, isinstance(message, bytes))
    except websockets.exceptions.ConnectionClosed:
        logger.warning("🔌 Human tunnel disconnected from backend.")

async def main():
    port = int(os.environ.get("PORT", 8765))
    # الطريقة الحديثة والآمنة لإطلاق السيرفر وتفادي RuntimeError
    async with websockets.serve(bridge_handler, "0.0.0.0", port):
        logger.info(f"🚀 Passive Processor running stably on port: {port}")
        await asyncio.Future()

if __name__ == "__main__":
    try: 
        asyncio.run(main())
    except Exception as e: 
        logger.error(f"🚨 Critical Server Error: {str(e)}")
