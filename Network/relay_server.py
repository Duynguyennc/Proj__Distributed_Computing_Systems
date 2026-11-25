"""
TẦNG NETWORK (Thành viên 3): Dumb WebSocket Relay Server
======================================================
Nhiệm vụ DUY NHẤT của Server này:
1. Mở cổng WebSocket (mặc định ws://0.0.0.0:8765/ws) để các Client kết nối vào.
2. Nhận gói tin JSON từ một Client và phát sóng (Broadcast) nguyên văn sang các Client khác.
3. Lưu lại lịch sử các gói tin thô (dưới dạng chuỗi JSON thuần túy) để hỗ trợ đồng bộ
   cho Client mới vào sau hoặc Client vừa mất mạng kết nối lại.

QUAN TRỌNG: Server này HOÀN TOÀN KHÔNG biết gì về thuật toán CRDT, không phân xử xung đột,
không sửa nội dung gói tin. (Thành viên 3 sau này có thể viết lại đúng vai trò này bằng Node.js).
"""

import asyncio
import json
import argparse
from typing import Set, List, Dict, Any
from aiohttp import web, WSMsgType


class DumbRelayServer:
    def __init__(self):
        self.clients: Set[web.WebSocketResponse] = set()
        # Lưu lịch sử các gói tin thô (opaque strings) để replay khi có client reconnect
        self.packet_history: List[str] = []

    async def websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)

        client_name = request.query.get("client", "Unknown")
        self.clients.add(ws)
        print(f"[RELAY] + Kết nối mới: '{client_name}' (Tổng số peers đang online: {len(self.clients)})")

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    raw_text = msg.data
                    try:
                        parsed: Dict[str, Any] = json.loads(raw_text)
                    except json.JSONDecodeError:
                        continue

                    # Xử lý yêu cầu đồng bộ lịch sử gói tin khi client mới vào / kết nối lại
                    if parsed.get("relay_cmd") == "sync":
                        from_offset = int(parsed.get("from_offset", 0))
                        missed = self.packet_history[from_offset:]
                        print(f"[RELAY] <-> Đồng bộ cho '{client_name}' từ offset {from_offset} ({len(missed)} gói tin)")
                        await ws.send_json({
                            "relay_cmd": "sync_batch",
                            "current_offset": len(self.packet_history),
                            "packets": missed,
                        })
                        continue

                    # Lưu gói tin thô vào lịch sử và gắn số thứ tự truyền tải của relay
                    seq_id = len(self.packet_history) + 1
                    self.packet_history.append(raw_text)

                    # Đóng gói envelope nhẹ để client biết offset mới nhất
                    relay_envelope = json.dumps({
                        "relay_seq": seq_id,
                        "payload": parsed,
                    }, ensure_ascii=False)

                    # Phát sóng nguyên văn sang tất cả các client KHÁC (Dumb Broadcast)
                    forwarded_count = 0
                    for peer in list(self.clients):
                        if peer is not ws and not peer.closed:
                            await peer.send_str(relay_envelope)
                            forwarded_count += 1

                    # Xác nhận ngược lại offset cho chính người gửi (không gửi lại payload)
                    await ws.send_json({
                        "relay_cmd": "ack",
                        "relay_seq": seq_id,
                    })

                    action = parsed.get("action", "?")
                    sender = parsed.get("client_id", client_name)
                    print(f"[RELAY] #{seq_id} {sender} ({action}) -> Chuyển tiếp tới {forwarded_count} peer(s)")

                elif msg.type == WSMsgType.ERROR:
                    print(f"[RELAY] ! Lỗi kết nối từ '{client_name}': {ws.exception()}")
        finally:
            self.clients.discard(ws)
            print(f"[RELAY] - Ngắt kết nối: '{client_name}' (Còn lại: {len(self.clients)})")

        return ws


def main():
    parser = argparse.ArgumentParser(description="Dumb WebSocket Relay Server cho Đồ án CRDT")
    parser.add_argument("--host", default="0.0.0.0", help="Địa chỉ IP lắng nghe (mặc định: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8765, help="Cổng WebSocket (mặc định: 8765)")
    args = parser.parse_args()

    relay = DumbRelayServer()
    app = web.Application()
    app.router.add_get("/ws", relay.websocket_handler)

    print("=" * 65)
    print(f"  DUMB WEBSOCKET RELAY SERVER ĐANG CHẠY TẠI: ws://{args.host}:{args.port}/ws")
    print("  Vai trò: Trung chuyển gói tin thuần túy (KHÔNG chứa logic CRDT)")
    print("=" * 65)

    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
