import pandas as pd
import numpy as np
import requests as r
import yfinance as yf
from datetime import datetime as dt



pd.set_option('display.max_columns', None)

# --- Category tickers (all vs USD) ---
PAYMENT = ["USDT-USD", "USDC-USD", "DAI-USD", "BUSD-USD", "TUSD-USD", "XRP-USD"]

DEFI = ["UNI-USD", "AAVE-USD", "COMP-USD", "SUSHI-USD", "MKR-USD", "CAKE-USD", "CRV-USD"]

LAYER1 = ["BTC-USD", "ETH-USD", "SOL-USD", "AVAX-USD", "ADA-USD", "DOT-USD"]

MEME = ["DOGE-USD", "SHIB-USD", "FLOKI-USD", "PEPE-USD", "CKB-USD"]

NFT = ["SAND-USD", "MANA-USD", "AXS-USD", "ENJ-USD", "THETA-USD"]

INFRA = ["LINK-USD", "GRT-USD", "RUNE-USD", "AGIX-USD", "OCEAN-USD", "NEAR-USD", "TAO-USD"]

ALL_TICKERS = PAYMENT + DEFI + LAYER1 + MEME + NFT + INFRA

CATEGORY_MAP = {
    **{c: "payment" for c in PAYMENT},
    **{c: "defi" for c in DEFI},
    **{c: "layer1" for c in LAYER1},
    **{c: "meme" for c in MEME},
    **{c: "nft" for c in NFT},
    **{c: "infra" for c in INFRA},
}





