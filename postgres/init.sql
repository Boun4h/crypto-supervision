CREATE TABLE IF NOT EXISTS market_ticks (
  id BIGSERIAL PRIMARY KEY,
  exchange TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_utc TIMESTAMPTZ NOT NULL,
  last NUMERIC NULL,
  bid NUMERIC NULL,
  ask NUMERIC NULL,
  spread_abs NUMERIC NULL,
  spread_pct NUMERIC NULL,
  raw_json JSONB NULL
);

CREATE INDEX IF NOT EXISTS idx_market_ticks_ts ON market_ticks (ts_utc DESC);
CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol ON market_ticks (exchange, symbol, ts_utc DESC);
