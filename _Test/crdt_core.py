from dataclasses import dataclass
import random
import bisect
from typing import List, Optional, Dict, Any


@dataclass
class Atom:
    pos: List[int]
    site_id: str
    clock: int
    value: str
    is_deleted: bool = False

    def __lt__(self, other: "Atom") -> bool:
        """
        Quy tắc thứ tự toàn phần giữa 2 Atom:
        1. So sánh Vector vị trí (pos) theo thứ tự từ điển
        2. Nếu pos giống hệt nhau (do 2 người gõ cùng lúc), so sánh tiếp đến site_id
        3. Nếu site_id cũng giống nhau, so sánh tiếp đến clock
        """
        if self.pos != other.pos:
            return self.pos < other.pos
        
        if self.site_id != other.site_id:
            return self.site_id < other.site_id
        
        return self.clock < other.clock

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Atom):
            return False
        
        return (
            self.pos == other.pos
            and self.site_id == other.site_id
            and self.clock == other.clock
        )

    def to_dict(self) -> Dict[str, Any]:
        # Chuyển đổi thành dictionary tương ứng form JSON
        return {
            "pos": self.pos,
            "site_id": self.site_id,
            "clock": self.clock,
            "value": self.value,
            "is_deleted": self.is_deleted,
        }


class LSEQAllocator:
    """
    Bộ cấp phát tọa độ thích ứng LSEQ
    Sử dụng cây cơ số mở rộng theo hàm mũ: Base(d) = 2^(base_bit + d)
    Nên chọn base_bit là 4 hoặc 5
    """

    def __init__(self, base_bit: int = 4, step: int = 10):
        self.base_bit = base_bit
        self.step = step

        # Hai mốc biên ảo cố định của toàn bộ tài liệu
        self.START_POS = [0]
        self.END_POS = [1 << base_bit]

    def max_val(self, depth: int) -> int:
        # Dung lượng tối đa của tầng depth: 2^(base_bit + depth)
        return 1 << (self.base_bit + depth)

    def allocate_between(self, left_pos: List[int], right_pos: List[int]) -> List[int]:
        # ==================================================================================================
        """
        CẦN CẢI TIẾN: XỬ LÝ BẰNG THUẬT TOÁN MỞ RỘNG CHỐNG TẤN CÔNG PHÁ HOẠI, TRƯỜNG HỢP GÂY HẠI CÓ CHỦ ĐÍCH
        random + md5 + hash --> Tạo được cơ chế luân phiên thay đổi chiến lược Boundary+ và Boundary-
        """

        depth = 0
        prefix: List[int] = []

        while True:
            max_d = self.max_val(depth)
            l_val = left_pos[depth] if depth < len(left_pos) else 0

            if depth < len(right_pos) and left_pos[:depth] == right_pos[:depth]:
                r_val = right_pos[depth]
            else:
                r_val = max_d

            diff = r_val - l_val

            if diff > 1:
                # Còn chỗ trống ở tầng hiện tại
                step_val = min(self.step, diff - 1)
                new_val = l_val + random.randint(1, step_val)
                return prefix + [new_val]
            else:
                # Khoảng cách giữa 2 mốc đã sát nhau, phải mở rộng thêm 1 chiều mới
                prefix.append(l_val)
                depth += 1


class CRDTEngine:
    """
    Lõi xử lý CRDT tại mỗi Client.
    Quản lý danh sách Atom đã sắp xếp, cung cấp API chèn/xóa cục bộ và tích hợp từ xa.
    """

    def __init__(self, site_id: str, base_bit: int = 4):
        self.site_id = site_id
        self.clock = 0  # Sequence number tăng dần cho mỗi thao tác do chính máy này sinh ra
        self.allocator = LSEQAllocator(base_bit=base_bit)

        # Mảng lưu trữ toàn bộ các Atom (kể cả Tombstone) theo thứ tự tăng dần PosID
        # Khởi tạo sẵn 2 cột mốc biên ảo ở hai đầu
        self.sentinel_start = Atom(
            pos=self.allocator.START_POS,
            site_id="",
            clock=0,
            value="",
            is_deleted=True,
        )
        self.sentinel_end = Atom(
            pos=self.allocator.END_POS,
            site_id="",
            clock=0,
            value="",
            is_deleted=True,
        )
        self.atoms: List[Atom] = [self.sentinel_start, self.sentinel_end]

    def _get_visible_atoms(self) -> List[Atom]:
        """Lấy danh sách các Atom đang hiển thị trên màn hình (loại bỏ Tombstone và Sentinel)."""
        return [
            a for a in self.atoms[1:-1] if not a.is_deleted
        ]

    def local_insert(self, char: str, visual_index: int) -> Dict[str, Any]:
        """
        Thực thi khi người dùng gõ 1 ký tự tại vị trí con trỏ visual_index trên màn hình.
        visual_index = 0 nghĩa là chèn vào đầu tài liệu.
        """
        self.clock += 1
        visible = self._get_visible_atoms()

        # Xác định 2 ký tự hàng xóm (trái và phải) kẹp con trỏ
        if visual_index <= 0:
            left_pos = self.sentinel_start.pos
            right_pos = visible[0].pos if visible else self.sentinel_end.pos
        elif visual_index >= len(visible):
            left_pos = visible[-1].pos
            right_pos = self.sentinel_end.pos
        else:
            left_pos = visible[visual_index - 1].pos
            right_pos = visible[visual_index].pos

        # Sinh tọa độ LSEQ mới nằm giữa 2 mốc
        new_pos = self.allocator.allocate_between(left_pos, right_pos)

        # Tạo Atom mới
        new_atom = Atom(
            pos=new_pos,
            site_id=self.site_id,
            clock=self.clock,
            value=char,
            is_deleted=False,
        )

        # Dùng Tìm kiếm nhị phân chèn ngay vào mảng trong RAM
        bisect.insort(self.atoms, new_atom)

        # Trả về gói tin JSON chuẩn tinh giản để tầng mạng gửi đi
        return {
            "action": "insert",
            "client_id": self.site_id,
            "atom": {
                "pos": new_atom.pos,
                "clock": new_atom.clock,
                "value": new_atom.value,
            },
        }

    def local_delete(self, visual_index: int) -> Optional[Dict[str, Any]]:
        """
        Thực thi khi người dùng nhấn Backspace hoặc Delete một ký tự tại visual_index.
        """
        visible = self._get_visible_atoms()
        if visual_index < 0 or visual_index >= len(visible):
            return None

        target_atom = visible[visual_index]
        target_atom.is_deleted = True  # Đánh dấu Tombstone (không xóa vật lý)
        self.clock += 1

        # Trả về gói tin JSON xóa (chỉ mang định danh của Atom bị xóa)
        return {
            "action": "delete",
            "client_id": self.site_id,
            "target": {
                "pos": target_atom.pos,
                "origin_client": target_atom.site_id,
                "clock": target_atom.clock,
            },
        }

    def apply_remote_op(self, op: Dict[str, Any]) -> bool:
        # Nhận gói tin JSON từ mạng WebSocket và tích hợp vào tài liệu cục bộ.
        action = op.get("action")

        if action == "insert":
            atom_data = op["atom"]
            new_atom = Atom(
                pos=atom_data["pos"],
                site_id=op["client_id"],  # Lấy trực tiếp từ lớp vỏ ngoài
                clock=atom_data["clock"],
                value=atom_data["value"],
                is_deleted=False,
            )

            # Kiểm tra xem Atom này đã tồn tại chưa (chống trùng lặp gói tin)
            idx = bisect.bisect_left(self.atoms, new_atom)
            if idx < len(self.atoms) and self.atoms[idx] == new_atom:
                return False

            # Chèn vào mảng theo thứ tự nhị phân
            self.atoms.insert(idx, new_atom)
            return True

        elif action == "delete":
            target = op["target"]
            # Tìm chính xác Atom cần xóa dựa vào bộ 3 định danh: pos, site_id, clock
            for atom in self.atoms:
                if (
                    atom.pos == target["pos"]
                    and atom.site_id == target["origin_client"]
                    and atom.clock == target["clock"]
                ):
                    atom.is_deleted = True
                    return True
            return False

        return False

    def get_text(self) -> str:
        # Xuất ra chuỗi văn bản hoàn chỉnh để tầng giao diện hiển thị cho người dùng.
        return "".join(a.value for a in self._get_visible_atoms())

    def get_debug_state(self) -> List[Dict[str, Any]]:
        # Xuất toàn bộ trạng thái mảng RAM (bao gồm cả Tombstone) để debug và kiểm thử.
        return [a.to_dict() for a in self.atoms[1:-1]]
