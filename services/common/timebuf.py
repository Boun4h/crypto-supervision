from collections import deque
from typing import Deque, Tuple, Optional


class TimeBuffer:
    """
    Stocke (ts, price) et permet de retrouver la dernière valeur <= (ts - horizon).
    Simple, efficace, suffisant pour 10s / 1m.
    """
    def __init__(self, maxlen: int = 2000):
        self.q: Deque[Tuple[float, float]] = deque(maxlen=maxlen)

    def add(self, ts: float, price: float):
        self.q.append((ts, price))

    def get_price_ago(self, ts: float, seconds: float) -> Optional[float]:
        target = ts - seconds
        # scan reverse (petit buffer)
        for t, p in reversed(self.q):
            if t <= target:
                return p
        return None
