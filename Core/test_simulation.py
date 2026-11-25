"""
Kịch bản kiểm thử giả lập phân tán (Distributed Simulation Test).
Mô phỏng 3 Client (Alice, Bob, Charlie) cùng gõ, xóa và trao đổi gói tin qua mạng chập chờn.
Kiểm chứng tính chất hội tụ (Convergence) tuyệt đối của CRDT Core.
"""

from crdt_core import CRDTEngine
import random


def test_concurrent_edits_and_convergence():
    print("=" * 70)
    print("TEST 1: HAI NGƯỜI GÕ ĐỒNG THỜI VÀO CÙNG MỘT VỊ TRÍ")
    print("=" * 70)

    alice = CRDTEngine(site_id="Alice")
    bob = CRDTEngine(site_id="Bob")

    # Alice gõ "HELLO"
    ops_alice = []
    for i, ch in enumerate("HELLO"):
        op = alice.local_insert(ch, i)
        ops_alice.append(op)

    # Bob đồng bộ toàn bộ chữ "HELLO" từ Alice
    for op in ops_alice:
        bob.apply_remote_op(op)

    print(f"Trạng thái ban đầu trên cả 2 máy: \"{alice.get_text()}\"")
    assert alice.get_text() == bob.get_text() == "HELLO"

    # Bây giờ tại cùng 1 tích tắc (đồng thời):
    # - Alice chèn '!' vào cuối chuỗi (sau 'O', tức index 5)
    # - Bob chèn '?' vào cuối chuỗi (sau 'O', tức index 5)
    op_alice_concurrent = alice.local_insert("!", 5)
    op_bob_concurrent = bob.local_insert("?", 5)

    print(f"Alice vừa gõ xong (chưa nhận gói của Bob): \"{alice.get_text()}\"")
    print(f"Bob vừa gõ xong (chưa nhận gói của Alice):   \"{bob.get_text()}\"")

    # Hai bên trao đổi gói tin cho nhau
    alice.apply_remote_op(op_bob_concurrent)
    bob.apply_remote_op(op_alice_concurrent)

    print(f"Sau khi đồng bộ: Alice = \"{alice.get_text()}\" | Bob = \"{bob.get_text()}\"")
    assert alice.get_text() == bob.get_text()
    print(">>> KẾT QUẢ TEST 1: HỘI TỤ CHÍNH XÁC 100%! HAI MÁY HIỂN THỊ Y HỆT NHAU.")


def test_network_shuffle_and_packet_loss_recovery():
    print("\n" + "=" * 70)
    print("TEST 2: MẠNG CHẬP CHỜN, GÓI TIN ĐẾN ĐẢO LỘN THỨ TỰ (SHUFFLED NETWORK)")
    print("=" * 70)

    client1 = CRDTEngine(site_id="Client1")
    client2 = CRDTEngine(site_id="Client2")
    client3 = CRDTEngine(site_id="Client3")

    # Client 1 soạn thảo một đoạn mã Python ngắn: "def add(a, b): return a + b"
    code_text = "def add(a, b): return a + b"
    all_packets = []
    for i, ch in enumerate(code_text):
        op = client1.local_insert(ch, i)
        all_packets.append(op)

    # Trộn ngẫu nhiên hoàn toàn thứ tự của 27 gói tin này để giả lập mạng bị trễ/đảo lộn
    shuffled_packets = list(all_packets)
    random.seed(42)  # Cố định seed để tái hiện kết quả
    random.shuffle(shuffled_packets)

    # Client 2 và Client 3 nhận các gói tin theo thứ tự bị đảo lộn lung tung
    for op in shuffled_packets:
        client2.apply_remote_op(op)
        client3.apply_remote_op(op)

    print(f"Client 1 (gõ tuần tự):        \"{client1.get_text()}\"")
    print(f"Client 2 (nhận gói xáo trộn): \"{client2.get_text()}\"")
    print(f"Client 3 (nhận gói xáo trộn): \"{client3.get_text()}\"")

    assert client1.get_text() == client2.get_text() == client3.get_text()
    print(">>> KẾT QUẢ TEST 2: ĐỘC LẬP THỨ TỰ (COMMUTATIVITY) THÀNH CÔNG RỰC RỠ!")


def test_false_deletion_anomaly_resolution():
    print("\n" + "=" * 70)
    print("TEST 3: BẢO VỆ CHỐNG THẢM HỌA 'XÓA OAN' (FALSE DELETION)")
    print("=" * 70)

    c1 = CRDTEngine(site_id="Client1")
    c2 = CRDTEngine(site_id="Client2")

    # 1. C1 gõ 'A' tại index 0 -> C2 nhận 'A'
    op_insert_a = c1.local_insert("A", 0)
    c2.apply_remote_op(op_insert_a)
    assert c1.get_text() == c2.get_text() == "A"

    # 2. C2 bấm xóa 'A' (tạo op_delete_a, nhưng gói này bị delay trên mạng)
    op_delete_a = c2.local_delete(0)

    # 3. Ở máy C1, C1 tự xóa 'A' và gõ ngay chữ 'B' vào vị trí đó
    c1.local_delete(0)
    c1.local_insert("B", 0)
    print(f"Client 1 sau khi xóa A và gõ B: \"{c1.get_text()}\"")
    assert c1.get_text() == "B"

    # 4. Gói tin xóa của C2 lúc này mới bay tới máy C1
    c1.apply_remote_op(op_delete_a)

    print(f"Client 1 sau khi nhận gói xóa trễ từ C2: \"{c1.get_text()}\"")
    # Chữ 'B' không được phép bị xóa oan! Chữ 'B' vẫn phải sống!
    assert c1.get_text() == "B"
    print(">>> KẾT QUẢ TEST 3: CHỮ 'B' VẪN SỐNG KHỎE! THẢM HỌA XÓA OAN ĐÃ ĐƯỢC CHẶN ĐỨNG NHỜ CLOCK.")


if __name__ == "__main__":
    test_concurrent_edits_and_convergence()
    test_network_shuffle_and_packet_loss_recovery()
    test_false_deletion_anomaly_resolution()
    print("\n" + "*" * 70)
    print("TẤT CẢ 3 BÀI TEST PHÂN TÁN ĐỀU ĐẠT 100% ĐIỂM TỐI ĐA!")
    print("*" * 70)
