import os


def load_symbol_map():
    """
    Mapping minimal. Tu peux le remplacer par YAML/JSON.
    """
    # Canonical panier
    canonical = [s.strip() for s in os.getenv("CANONICAL_SYMBOLS", "").split(",") if s.strip()]

    # Exemple: tu ajustes selon tes needs.
    # Binance miniTicker: "BTCUSDT" -> "BTC/USDT"
    binance = {"BTCUSDT": "BTC/USDT", "ETHUSDT": "ETH/USDT", "SOLUSDT": "SOL/USDT"}

    # Kraken: "XBT/USD" -> on choisit canonical "BTC/USD" ou "BTC/USDT" (à toi de décider)
    # Ici, pour cohérence stablecoin, tu peux mapper Kraken USD vers USDT si tu veux comparer,
    # mais c'est un choix métier. Je te propose plutôt canonical distinct: BTC/USD.
    kraken = {"XBT/USD": "BTC/USD", "ETH/USD": "ETH/USD", "SOL/USD": "SOL/USD"}

    # Poloniex: "BTC_USDT" -> "BTC/USDT"
    poloniex = {"BTC_USDT": "BTC/USDT", "ETH_USDT": "ETH/USDT", "SOL_USDT": "SOL/USDT"}

    return canonical, {
        "binance": binance,
        "kraken": kraken,
        "poloniex": poloniex,
    }
