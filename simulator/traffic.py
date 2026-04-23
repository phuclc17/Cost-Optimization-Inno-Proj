import math


class NormalTraffic:
    """
    Traffic bình thường: dao động theo giờ trong ngày.
    Peak lúc 12h trưa và 20h tối.
    Base rate: 50 events/giây.
    """
    name = "normal"
    base_rate = 50.0

    def get_rate_at(self, elapsed_seconds: float) -> float:
        # Compress 24h vào thời gian thực nghiệm
        hour = (elapsed_seconds / 3600 * 24) % 24
        # 2 peak: trưa và tối
        lunch  = math.exp(-0.5 * ((hour - 12) / 2) ** 2)
        dinner = math.exp(-0.5 * ((hour - 20) / 2) ** 2)
        multiplier = 1.0 + 2.0 * lunch + 3.0 * dinner
        return self.base_rate * multiplier


class PeakTraffic:
    """
    Flash sale: traffic tăng 10x trong 10 phút.
    Mô phỏng sự kiện 11.11 / Black Friday.
    Base rate: 50 events/giây.
    """
    name = "peak"
    base_rate = 50.0

    def get_rate_at(self, elapsed_seconds: float) -> float:
        minutes = elapsed_seconds / 60
        # Flash sale từ phút 5 đến phút 15
        if 5 <= minutes <= 15:
            if minutes < 7:
                # Ramp up: 2 phút tăng dần
                multiplier = 1 + (minutes - 5) / 2 * 9
            elif minutes <= 13:
                # Peak: giữ 10x
                multiplier = 10.0
            else:
                # Ramp down: 2 phút giảm dần
                multiplier = 10 - (minutes - 13) / 2 * 9
        else:
            multiplier = 1.0
        return self.base_rate * multiplier


class MixedTraffic:
    """
    Kết hợp: normal + 2 mini flash sale nhỏ.
    Realistic nhất so với thực tế.
    Base rate: 50 events/giây.
    """
    name = "mixed"
    base_rate = 50.0

    def get_rate_at(self, elapsed_seconds: float) -> float:
        minutes = elapsed_seconds / 60
        # Base: normal traffic
        base = NormalTraffic().get_rate_at(elapsed_seconds)
        # Mini spike 1: phút 10-13, tăng 3x
        spike1 = base * 2 if 10 <= minutes <= 13 else 0
        # Mini spike 2: phút 25-27, tăng 2x
        spike2 = base * 1 if 25 <= minutes <= 27 else 0
        return base + spike1 + spike2


def get_pattern(name: str):
    """Lấy traffic pattern theo tên."""
    patterns = {
        "normal": NormalTraffic(),
        "peak":   PeakTraffic(),
        "mixed":  MixedTraffic(),
    }
    if name not in patterns:
        raise ValueError(f"Unknown pattern: {name}. "
                         f"Choose: {list(patterns.keys())}")
    return patterns[name]