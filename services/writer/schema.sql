CREATE TABLE IF NOT EXISTS ticks (
  ts        TIMESTAMPTZ NOT NULL,
  exchange  TEXT NOT NULL,
  symbol    TEXT NOT NULL,
  price     DOUBLE PRECISION NOT NULL,
  delta_10s DOUBLE PRECISION NULL,
  pct_10s   DOUBLE PRECISION NULL,
  delta_1m  DOUBLE PRECISION NULL,
  pct_1m    DOUBLE PRECISION NULL
);

CREATE INDEX IF NOT EXISTS idx_ticks_ts ON ticks(ts);
CREATE INDEX IF NOT EXISTS idx_ticks_sym ON ticks(exchange, symbol, ts DESC);
