from pydantic import BaseModel
from typing import Literal, Optional

Exchange = Literal["binance", "kraken", "poloniex"]

class RawTick(BaseModel):
    exchange: Exchange
    symbol_raw: str
    ts: float
    price: float

class NormTick(BaseModel):
    exchange: Exchange
    symbol: str  # canonical e.g. BTC/USDT
    ts: float
    price: float
    delta_10s: Optional[float] = None
    pct_10s: Optional[float] = None
    delta_1m: Optional[float] = None
    pct_1m: Optional[float] = None
