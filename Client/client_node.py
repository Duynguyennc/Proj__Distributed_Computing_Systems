"""
CẦU NỐI CLIENT NODE (Kết nối Giao diện Web UI <-> Lõi CRDT Python <-> Dumb Relay)
================================================================================
Mỗi tiến trình này đại diện cho MỘT MÁY KHÁCH (Client) độc lập trong mạng phân tán:
- Sở hữu riêng một bộ máy CRDTEngine(site_id) trong RAM của chính nó.
- Phục vụ giao diện Web (index.html) qua HTTP và trao đổi sự kiện với trình duyệt qua /ui_ws.
- Kết nối tới Dumb Relay Server (ws://.../ws) để phát và nhận các gói tin JSON CRDT.
- Hỗ trợ giả lập Ngắt mạng (Offline), Đồng bộ lại (Reconnect) và Độ trễ mạng (Network Delay).
"""

import os
import sys
import json
import asyncio
import argparse
from typing import Optional, List, Dict, Any, Set
from aiohttp import web, ClientSession, WSMsgType

# Thêm thư mục Core vào sys.path để import crdt_core.py
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
CORE_DIR = os.path.join(PROJECT_ROOT, "Core")
if CORE_DIR not in sys.path:
    sys.path.insert(0, CORE_DIR)

from crdt_core import CRDTEngine  # noqa: E402


class ClientNode:
    def __init__(self, site_id: str, relay_url: str):
        self.site_id = site_id
        self.relay_url = relay_url
        self.engine = CRDTEngine(site_id=site_id)

        # Trạng thái mạng & hàng đợi khi offline
        self.is_offline_mode: bool = False
        self.relay_connected: bool = False
        self.network_delay_ms: int = 0
        self.last_relay_seq: int = 0
        self.pending_outgoing: List[Dict[str, Any]] = []

        # Kết nối WebSocket tới Relay và tới Browser UI
        self.relay_ws = None
        self.ui_clients: Set[web.WebSocketResponse] = set()
        self.packet_logs: List[Dict[str, Any]] = []

    def _add_log(self, direction: str, summary: str, payload: Any) -> Dict[str, Any]:
        entry = {
            "direction": direction,  # "OUT", "IN", "SYS"
            "summary": summary,
            "payload": payload,
        }
        self.packet_logs.append(entry)
        if len(self.packet_logs) > 80:
            self.packet_logs.pop(0)
        return entry

    async def broadcast_to_ui(self, message: Dict[str, Any]) -> None:
        raw = json.dumps(message, ensure_ascii=False)
        for ws in list(self.ui_clients):
            if not ws.closed:
                await ws.send_str(raw)

    async def send_full_state_to_ui(self, ws: Optional[web.WebSocketResponse] = None) -> None:
        state_msg = {
            "type": "full_state",
            "site_id": self.site_id,
            "text": self.engine.get_text(),
            "atoms": self.engine.get_debug_state(),
            "online": self.relay_connected and not self.is_offline_mode,
            "is_offline_mode": self.is_offline_mode,
            "delay_ms": self.network_delay_ms,
            "pending_count": len(self.pending_outgoing),
            "logs": self.packet_logs[-30:],
        }
        if ws is not None:
            await ws.send_json(state_msg)
        else:
            await self.broadcast_to_ui(state_msg)

    async def _send_op_to_relay(self, op: Dict[str, Any]) -> None:
        """Gửi một gói tin CRDT lên Relay (có áp dụng độ trễ giả lập nếu được cấu hình)."""
        if self.network_delay_ms > 0:
            await asyncio.sleep(self.network_delay_ms / 1000.0)

        if self.is_offline_mode or self.relay_ws is None or self.relay_ws.closed:
            self.pending_outgoing.append(op)
            log = self._add_log("SYS", f"Đang Offline -> Xếp hàng chờ gửi ({len(self.pending_outgoing)} gói)", op)
            await self.broadcast_to_ui({"type": "log_entry", "log": log, "pending_count": len(self.pending_outgoing)})
            return

        await self.relay_ws.send_json(op)
        action = op.get("action", "?")
        detail = op["atom"]["value"] if action == "insert" else f"clock={op['target']['clock']}"
        log = self._add_log("OUT", f"Gửi {action.upper()} ({repr(detail)}) lên Relay", op)
        await self.broadcast_to_ui({"type": "log_entry", "log": log, "pending_count": len(self.pending_outgoing)})

    async def _flush_pending_ops(self) -> None:
        """Gửi toàn bộ gói tin bị kẹt trong lúc Offline khi có mạng trở lại."""
        if not self.pending_outgoing or self.relay_ws is None or self.relay_ws.closed:
            return
        queued = list(self.pending_outgoing)
        self.pending_outgoing.clear()
        for op in queued:
            await self.relay_ws.send_json(op)
            action = op.get("action", "?")
            log = self._add_log("OUT", f"[Reconnect Flush] Đẩy gói {action.upper()} bị tồn đọng", op)
            await self.broadcast_to_ui({"type": "log_entry", "log": log, "pending_count": len(self.pending_outgoing)})

    async def relay_loop(self) -> None:
        """Vòng lặp duy trì kết nối WebSocket tới Dumb Relay Server."""
        async with ClientSession() as session:
            while True:
                if self.is_offline_mode:
                    self.relay_connected = False
                    await asyncio.sleep(0.3)
                    continue

                try:
                    url = f"{self.relay_url}?client={self.site_id}"
                    async with session.ws_connect(url) as ws:
                        self.relay_ws = ws
                        self.relay_connected = True
                        log = self._add_log("SYS", f"Đã kết nối tới Relay Server ({self.relay_url})", None)
                        await self.send_full_state_to_ui()

                        # Xin đồng bộ các gói tin đã lỡ từ last_relay_seq
                        await ws.send_json({
                            "relay_cmd": "sync",
                            "from_offset": self.last_relay_seq,
                        })

                        async for msg in ws:
                            if self.is_offline_mode:
                                break

                            if msg.type == WSMsgType.TEXT:
                                data = json.loads(msg.data)

                                # 1. Nhận batch đồng bộ khi mới kết nối / kết nối lại
                                if data.get("relay_cmd") == "sync_batch":
                                    packets = data.get("packets", [])
                                    new_offset = int(data.get("current_offset", self.last_relay_seq))
                                    applied_any = False
                                    for raw_p in packets:
                                        op = json.loads(raw_p)
                                        res = self.engine.apply_remote_op_with_info(op)
                                        if res["applied"]:
                                            applied_any = True
                                            self._add_log("IN", f"[Sync] Nạp gói {op.get('action','').upper()} từ {op.get('client_id')}", op)
                                    self.last_relay_seq = max(self.last_relay_seq, new_offset)
                                    # Đẩy các gói tin mình gõ lúc offline lên Relay
                                    await self._flush_pending_ops()
                                    if applied_any or packets:
                                        await self.send_full_state_to_ui()
                                    else:
                                        await self.broadcast_to_ui({"type": "status_update", "online": True, "pending_count": len(self.pending_outgoing), "logs": self.packet_logs[-30:]})

                                # 2. Relay xác nhận đã phát gói của chính mình
                                elif data.get("relay_cmd") == "ack":
                                    seq = int(data.get("relay_seq", 0))
                                    if seq > self.last_relay_seq:
                                        self.last_relay_seq = seq

                                # 3. Nhận gói tin CRDT từ peer khác
                                elif "payload" in data:
                                    seq = int(data.get("relay_seq", 0))
                                    if seq > self.last_relay_seq:
                                        self.last_relay_seq = seq
                                    op = data["payload"]
                                    res = self.engine.apply_remote_op_with_info(op)
                                    sender = op.get("client_id", "?")
                                    action = op.get("action", "?")
                                    log = self._add_log("IN", f"Nhận {action.upper()} từ {sender} (applied={res['applied']})", op)
                                    await self.broadcast_to_ui({
                                        "type": "remote_applied",
                                        "text": self.engine.get_text(),
                                        "atoms": self.engine.get_debug_state(),
                                        "op_info": res,
                                        "log": log,
                                    })

                            elif msg.type in (WSMsgType.CLOSED, WSMsgType.ERROR):
                                break

                except Exception as exc:
                    self._add_log("SYS", f"Chưa kết nối được Relay ({exc}). Thử lại sau 2s...", None)
                    await self.broadcast_to_ui({"type": "status_update", "online": False, "logs": self.packet_logs[-30:]})

                self.relay_ws = None
                self.relay_connected = False
                await asyncio.sleep(2.0)

    async def http_index_handler(self, request: web.Request) -> web.Response:
        html_path = os.path.join(CURRENT_DIR, "index.html")
        with open(html_path, "r", encoding="utf-8") as f:
            content = f.read()
        return web.Response(text=content, content_type="text/html")

    async def ui_websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.ui_clients.add(ws)

        await self.send_full_state_to_ui(ws)

        try:
            async for msg in ws:
                if msg.type == WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    msg_type = data.get("type")

                    # Browser gửi diff (xóa delete_count ký tự tại index, sau đó chèn insert_text)
                    if msg_type == "local_diff":
                        index = int(data["index"])
                        delete_count = int(data["delete_count"])
                        insert_text = str(data["insert_text"])

                        generated_ops: List[Dict[str, Any]] = []

                        # 1. Xử lý xóa trước
                        for _ in range(delete_count):
                            op = self.engine.local_delete(index)
                            if op:
                                generated_ops.append(op)

                        # 2. Xử lý chèn từng ký tự
                        for offset, ch in enumerate(insert_text):
                            op = self.engine.local_insert(ch, index + offset)
                            generated_ops.append(op)

                        # Phản hồi ngay trạng thái RAM/Atom table cho UI
                        await self.broadcast_to_ui({
                            "type": "local_ack",
                            "text": self.engine.get_text(),
                            "atoms": self.engine.get_debug_state(),
                        })

                        # Phát các gói tin CRDT qua Relay (chạy nền để không chặn UI)
                        for op in generated_ops:
                            asyncio.create_task(self._send_op_to_relay(op))

                    elif msg_type == "toggle_offline":
                        self.is_offline_mode = bool(data.get("offline", False))
                        status_txt = "BẬT CHẾ ĐỘ OFFLINE (Ngắt khỏi Relay)" if self.is_offline_mode else "ĐANG KẾT NỐI LẠI RELAY..."
                        log = self._add_log("SYS", status_txt, None)
                        if self.is_offline_mode and self.relay_ws and not self.relay_ws.closed:
                            await self.relay_ws.close()
                        await self.broadcast_to_ui({
                            "type": "status_update",
                            "online": self.relay_connected and not self.is_offline_mode,
                            "is_offline_mode": self.is_offline_mode,
                            "pending_count": len(self.pending_outgoing),
                            "log": log,
                        })

                    elif msg_type == "set_delay":
                        self.network_delay_ms = max(0, int(data.get("delay_ms", 0)))
                        log = self._add_log("SYS", f"Đặt độ trễ mạng giả lập: {self.network_delay_ms} ms", None)
                        await self.broadcast_to_ui({
                            "type": "status_update",
                            "delay_ms": self.network_delay_ms,
                            "log": log,
                        })

        finally:
            self.ui_clients.discard(ws)

        return ws


async def start_background_tasks(app: web.Application) -> None:
    node: ClientNode = app["client_node"]
    app["relay_task"] = asyncio.create_task(node.relay_loop())


async def cleanup_background_tasks(app: web.Application) -> None:
    app["relay_task"].cancel()
    try:
        await app["relay_task"]
    except asyncio.CancelledError:
        pass


def main():
    parser = argparse.ArgumentParser(description="Client Node chạy CRDT Engine + Web UI")
    parser.add_argument("--user", default="Alice", help="Site ID / Tên người dùng (ví dụ: Alice, Bob)")
    parser.add_argument("--host", default="127.0.0.1", help="Host chạy giao diện Web (mặc định: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8081, help="Cổng giao diện Web (mặc định: 8081)")
    parser.add_argument("--relay", default="ws://127.0.0.1:8765/ws", help="Địa chỉ Dumb Relay Server")
    args = parser.parse_args()

    node = ClientNode(site_id=args.user, relay_url=args.relay)
    app = web.Application()
    app["client_node"] = node
    app.router.add_get("/", node.http_index_handler)
    app.router.add_get("/ui_ws", node.ui_websocket_handler)
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)

    print(f"[CLIENT:{args.user}] Mở trình duyệt tại: http://{args.host}:{args.port} (Relay: {args.relay})")
    web.run_app(app, host=args.host, port=args.port, print=None)


if __name__ == "__main__":
    main()
