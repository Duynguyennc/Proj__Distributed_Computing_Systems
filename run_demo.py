"""
Script khởi chạy nhanh bản Demo MVP cho buổi họp nhóm.
Khởi động đồng thời:
1. Dumb WebSocket Relay Server (Tầng Network - Port 8765)
2. Client Node Alice (Port 8081)
3. Client Node Bob (Port 8082)
(Có thể truyền --count 3 để mở thêm Client Charlie ở Port 8083)
"""

import os
import sys
import time
import argparse
import subprocess

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
RELAY_SCRIPT = os.path.join(PROJECT_ROOT, "Network", "relay_server.py")
CLIENT_SCRIPT = os.path.join(PROJECT_ROOT, "Client", "client_node.py")

USERS = [
    ("Alice", 8081),
    ("Bob", 8082),
    ("Charlie", 8083),
    ("Dave", 8084),
]


def main():
    parser = argparse.ArgumentParser(description="Chạy nhanh Demo CRDT Collaborative Editor")
    parser.add_argument("--count", type=int, default=2, choices=[2, 3, 4], help="Số lượng Client chạy thử (mặc định: 2)")
    args = parser.parse_args()

    procs = []
    try:
        # 1. Khởi động Dumb Relay Server
        p_relay = subprocess.Popen([sys.executable, RELAY_SCRIPT])
        procs.append(p_relay)
        time.sleep(0.8)

        # 2. Khởi động các Client Node độc lập
        urls = []
        for i in range(args.count):
            name, port = USERS[i]
            p_client = subprocess.Popen([
                sys.executable,
                CLIENT_SCRIPT,
                "--user", name,
                "--port", str(port),
                "--relay", "ws://127.0.0.1:8765/ws",
            ])
            procs.append(p_client)
            urls.append((name, f"http://127.0.0.1:{port}"))

        time.sleep(0.8)
        print("\n" + "=" * 65)
        print("  HỆ THỐNG DEMO MVP ĐÃ SẴN SÀNG! MỞ CÁC ĐƯỜNG DẪN SAU TRÊN TRÌNH DUYỆT:")
        for name, url in urls:
            print(f"    -> Máy {name:<8}: {url}")
        print("=" * 65)
        print("  Nhấn Ctrl + C tại cửa sổ này để tắt toàn bộ hệ thống.\n")

        # Chờ tiến trình
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        print("\n[DEMO] Đang dừng toàn bộ tiến trình...")
    finally:
        for p in procs:
            if p.poll() is None:
                p.terminate()
        for p in procs:
            try:
                p.wait(timeout=3)
            except subprocess.TimeoutExpired:
                p.kill()
        print("[DEMO] Đã tắt sạch sẽ!")


if __name__ == "__main__":
    main()
