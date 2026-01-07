CREATE TABLE prices (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    price NUMERIC,
    ts TIMESTAMP NOT NULL
);

-- Index to speed up time‑based queries
CREATE INDEX idx_prices_ts ON prices(ts);
