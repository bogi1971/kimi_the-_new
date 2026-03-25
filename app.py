"""
Elite Bull Scanner Pro V9.0 - Tech Edition
Fixes: Autopilot, RS vs QQQ, Volume Profile, Dead Ticker Cleanup
Removed: Alpha Vantage, Fallback Meme-Stocks
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import time
import requests
import json
import os
import threading
import random
import warnings
import logging
import pytz

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any, Set
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    genai = None

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ==============================================================================
# PAGE CONFIG
# ==============================================================================

st.set_page_config(
    layout="wide",
    page_title="Elite Bull Scanner Pro V9.0",
    page_icon="🐂",
)

# ==============================================================================
# CSS
# ==============================================================================

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Syne:wght@700;800&display=swap');

*, *::before, *::after { box-sizing: border-box; }

html, body, [data-testid="stApp"] {
    background: #080c10 !important;
    color: #c9d1d9;
    font-family: 'JetBrains Mono', monospace;
}

[data-testid="stSidebar"] {
    background: #0d1117 !important;
    border-right: 1px solid #1c2128;
}

.bull-card {
    border: 1px solid #1c2128;
    border-radius: 12px;
    padding: 18px;
    margin: 10px 0;
    background: linear-gradient(145deg, #0d1117 0%, #0a0f15 100%);
    border-left: 3px solid #00ff88;
    transition: transform 0.15s ease, box-shadow 0.15s ease;
    position: relative;
    overflow: hidden;
}
.bull-card::before {
    content: '';
    position: absolute;
    top: 0; left: 0; right: 0;
    height: 1px;
    background: linear-gradient(90deg, transparent, #00ff8833, transparent);
}
.bull-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 24px rgba(0,255,136,0.08);
}
.bull-card.gold {
    border-left-color: #FFD700;
}
.bull-card.gold::before {
    background: linear-gradient(90deg, transparent, #FFD70033, transparent);
}

.card-symbol {
    font-family: 'Syne', sans-serif;
    font-size: 1.4rem;
    font-weight: 800;
    color: #e6edf3;
    letter-spacing: 0.05em;
}

.price-display {
    font-size: 1.6rem;
    font-weight: 700;
    color: #00ff88;
    margin: 8px 0;
    font-family: 'JetBrains Mono', monospace;
}

.badge {
    display: inline-block;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 0.7rem;
    font-weight: 700;
    margin: 2px;
    letter-spacing: 0.05em;
}
.badge-green  { background: #0a2918; color: #00ff88; border: 1px solid #00ff8844; }
.badge-yellow { background: #2a2000; color: #FFD700; border: 1px solid #FFD70044; }
.badge-red    { background: #2a0a0a; color: #ff6b6b; border: 1px solid #ff6b6b44; }
.badge-blue   { background: #0a1a2a; color: #58a6ff; border: 1px solid #58a6ff44; }
.badge-gray   { background: #161b22; color: #8b949e; border: 1px solid #30363d; }

.metric-row {
    display: flex;
    gap: 12px;
    margin: 8px 0;
    flex-wrap: wrap;
}
.metric-item {
    background: #161b22;
    border-radius: 6px;
    padding: 6px 10px;
    font-size: 0.72rem;
    color: #8b949e;
    border: 1px solid #21262d;
    flex: 1;
    min-width: 80px;
    text-align: center;
}
.metric-item span {
    display: block;
    font-size: 0.9rem;
    color: #e6edf3;
    font-weight: 700;
    margin-top: 2px;
}

.score-bar-bg {
    width: 100%;
    height: 4px;
    background: #21262d;
    border-radius: 2px;
    margin: 6px 0;
    overflow: hidden;
}
.score-bar-fill {
    height: 100%;
    border-radius: 2px;
    transition: width 0.4s ease;
}

.sl-tp-row {
    display: flex;
    gap: 8px;
    margin: 8px 0;
}
.sl-badge {
    background: #2a0a0a;
    color: #ff6b6b;
    border: 1px solid #ff6b6b44;
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 700;
    flex: 1;
    text-align: center;
}
.tp-badge {
    background: #0a2918;
    color: #00ff88;
    border: 1px solid #00ff8844;
    padding: 4px 10px;
    border-radius: 4px;
    font-size: 0.8rem;
    font-weight: 700;
    flex: 1;
    text-align: center;
}

.rs-positive { color: #00ff88; }
.rs-negative { color: #ff6b6b; }

.link-btn {
    display: block;
    background: #161b22;
    color: #58a6ff !important;
    text-decoration: none;
    padding: 7px 12px;
    border-radius: 6px;
    margin: 4px 0;
    font-size: 0.78rem;
    text-align: center;
    border: 1px solid #21262d;
    transition: background 0.15s;
}
.link-btn:hover { background: #1c2128; }

.autopilot-status {
    padding: 10px 16px;
    border-radius: 8px;
    font-size: 0.85rem;
    margin: 8px 0;
    font-weight: 700;
}
.autopilot-on  { background: #0a2918; color: #00ff88; border: 1px solid #00ff8844; }
.autopilot-off { background: #161b22; color: #8b949e; border: 1px solid #21262d; }

.clock-display {
    background: linear-gradient(135deg, #0d1117 0%, #0a0f15 100%);
    border: 1px solid #1c2128;
    border-radius: 16px;
    padding: 24px;
    text-align: center;
    margin-bottom: 20px;
    position: relative;
    overflow: hidden;
}
.clock-display::after {
    content: '';
    position: absolute;
    bottom: 0; left: 0; right: 0;
    height: 2px;
    background: linear-gradient(90deg, transparent, #00ff88, transparent);
}
.clock-time {
    font-family: 'Syne', sans-serif;
    font-size: 2.8rem;
    font-weight: 800;
    color: #00ff88;
    letter-spacing: 0.1em;
    font-variant-numeric: tabular-nums;
}

.volume-bar {
    display: flex;
    align-items: center;
    gap: 8px;
    margin: 4px 0;
    font-size: 0.72rem;
    color: #8b949e;
}
.volume-bar-inner {
    flex: 1;
    height: 3px;
    background: #21262d;
    border-radius: 2px;
    overflow: hidden;
}
.volume-bar-fill {
    height: 100%;
    border-radius: 2px;
}
</style>
""", unsafe_allow_html=True)

# ==============================================================================
# CONSTANTS & CONFIG
# ==============================================================================

MIN_PULLBACK_PCT   = 0.02
MAX_PULLBACK_PCT   = 0.60
AUTO_SCAN_INTERVAL = 1800       # 30 Minuten
ALERT_COOLDOWN_MIN = 60
MIN_SCORE          = 55
MIN_PRICE          = 5.0        # Kein Penny-Stock-Müll
CATALYST_FILE      = "catalysts.json"
DEAD_TICKERS_FILE  = "dead_tickers.json"

# Nur echte Tech-Aktien – keine Crypto/Meme
BASE_WATCHLIST = [
    # Mega-Cap Tech
    "AAPL", "MSFT", "NVDA", "GOOGL", "META", "AMZN", "TSLA", "AMD",

    # Semiconductors
    "AVGO", "QCOM", "MU", "AMAT", "LRCX", "KLAC", "MRVL", "ON", "TXN",
    "INTC", "ASML", "TSM", "ARM", "MPWR", "WOLF", "ONTO", "ENTG", "ACLS",

    # AI / Cloud Infrastructure
    "ORCL", "CRM", "NOW", "SNOW", "PLTR", "DDOG", "NET", "CFLT",
    "MDB", "GTLB", "ESTC", "SMCI", "DELL", "HPE", "PSTG", "NTAP",

    # Cybersecurity
    "CRWD", "PANW", "ZS", "FTNT", "OKTA", "S", "CYBR", "TENB", "QLYS", "VRNS",

    # Software / SaaS
    "ADSK", "ANSS", "CDNS", "TTD", "HUBS", "BILL", "MNDY", "APPN",
    "VEEV", "PCTY", "PAYC", "DOCU", "ZM", "RNG", "SMAR",

    # Fintech / Payments
    "COIN", "HOOD", "AFRM", "SOFI", "SSNC", "FIS", "FISV", "GPN", "PYPL", "MA", "V",

    # Hardware / Consumer Tech
    "ROKU", "LOGI", "STX", "WDC",

    # Biotech Blue-Chip (langsame Bewegungen, gute Setups)
    "LLY", "ABBV", "AMGN", "GILD", "VRTX", "REGN", "BIIB", "MRNA",
    "BMY", "PFE", "ISRG", "DXCM", "IDXX", "ALGN",
]

# ==============================================================================
# DATA CLASSES
# ==============================================================================

class SourceType(str, Enum):
    WATCHLIST = "watchlist"
    CATALYST  = "catalyst"
    GAINERS   = "gainers"
    UNKNOWN   = "unknown"

class CandlestickPattern(str, Enum):
    NONE               = "none"
    HAMMER             = "hammer"
    INVERTED_HAMMER    = "inv_hammer"
    BULLISH_ENGULFING  = "engulfing"
    MORNING_STAR       = "morning_star"
    PIERCING_LINE      = "piercing"
    BULLISH_HARAMI     = "harami"
    THREE_WHITE_SOLDIERS = "3_soldiers"

@dataclass
class CandlestickSignal:
    pattern:      CandlestickPattern
    strength:     int
    confirmation: bool
    description:  str
    entry_quality: str

@dataclass
class ScanResult:
    symbol:        str
    score:         int
    price:         float
    pullback_pct:  float
    recent_high:   float
    stop_loss:     float
    target:        float
    rr_ratio:      float
    rvol:          float
    rs_vs_qqq:     float        # Relative Strength vs QQQ
    vol_profile:   str          # "healthy" | "distribution" | "neutral"
    reasons:       List[str]
    news:          List[Dict]   = field(default_factory=list)
    source:        SourceType   = SourceType.UNKNOWN
    candlestick:   CandlestickSignal = field(default_factory=lambda: CandlestickSignal(
        pattern=CandlestickPattern.NONE, strength=0, confirmation=False,
        description="Kein Signal", entry_quality="weak"
    ))
    structure_intact: bool = False

# ==============================================================================
# PERSISTENT STORAGE
# ==============================================================================

def load_json_file(path: str, default) -> Any:
    if os.path.exists(path):
        try:
            with open(path, "r") as f:
                return json.load(f)
        except Exception:
            pass
    return default

def save_json_file(path: str, data):
    try:
        with open(path, "w") as f:
            json.dump(data, f)
    except Exception as e:
        logger.error(f"Speicherfehler {path}: {e}")

def load_dead_tickers() -> Set[str]:
    return set(load_json_file(DEAD_TICKERS_FILE, []))

def save_dead_tickers(dead: Set[str]):
    save_json_file(DEAD_TICKERS_FILE, list(dead))

def add_dead_ticker(symbol: str):
    """Ticker erst nach 3 fehlgeschlagenen Versuchen als tot markieren."""
    fail_counts = st.session_state.get("fail_counts", {})
    fail_counts[symbol] = fail_counts.get(symbol, 0) + 1
    st.session_state["fail_counts"] = fail_counts

    if fail_counts[symbol] >= 3:
        dead = load_dead_tickers()
        dead.add(symbol)
        save_dead_tickers(dead)
        st.session_state["dead_tickers"].add(symbol)
        logger.warning(f"💀 {symbol} nach 3 Fehlern als toter Ticker gespeichert")
    else:
        logger.debug(f"⚠️ {symbol} Fehler {fail_counts[symbol]}/3 – noch nicht als tot markiert")

# ==============================================================================
# SESSION STATE
# ==============================================================================

def init_session():
    dead = load_dead_tickers()
    catalysts = load_json_file(CATALYST_FILE, [])
    watchlist = [s for s in BASE_WATCHLIST if s not in dead]

    defaults = {
        'catalyst_list':    catalysts,
        'dead_tickers':     dead,
        'sent_alerts':      {},
        'scan_results':     [],
        'last_scan_time':   None,
        'auto_refresh':     False,
        'alert_history':    [],
        'qqq_data':         None,
        'qqq_loaded_at':    0,
        'yahoo_cache':      {},
        'yahoo_cache_ts':   {},
        'fail_counts':      {},
        'last_heartbeat':   None,
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session()

# ==============================================================================
# SECRETS
# ==============================================================================

try:
    TELEGRAM_TOKEN  = st.secrets["telegram"]["bot_token"]
    TELEGRAM_CHAT   = st.secrets["telegram"]["chat_id"]
    FINNHUB_KEYS    = st.secrets["finnhub"]["keys"]
    GEMINI_KEY      = st.secrets["gemini"]["api_key"]
except Exception:
    TELEGRAM_TOKEN  = ""
    TELEGRAM_CHAT   = ""
    FINNHUB_KEYS    = []
    GEMINI_KEY      = ""

_finnhub_idx = 0

def next_finnhub_key() -> Optional[str]:
    global _finnhub_idx
    if not FINNHUB_KEYS:
        return None
    key = FINNHUB_KEYS[_finnhub_idx % len(FINNHUB_KEYS)]
    _finnhub_idx += 1
    return key

# ==============================================================================
# MARKET CLOCK
# ==============================================================================

def get_market_clock() -> Dict:
    et   = pytz.timezone('US/Eastern')
    now  = datetime.now(et)
    open_  = now.replace(hour=9,  minute=30, second=0, microsecond=0)
    close_ = now.replace(hour=16, minute=0,  second=0, microsecond=0)
    pre_   = now.replace(hour=7,  minute=0,  second=0, microsecond=0)

    holidays = {(1,1),(1,19),(2,16),(4,3),(5,25),(6,19),(7,3),(9,7),(11,26),(12,25)}
    is_holiday = (now.month, now.day) in holidays

    if now.weekday() >= 5 or is_holiday:
        return {'time': now.strftime('%I:%M:%S %p'), 'status': 'CLOSED',
                'color': '#ff6b6b', 'countdown': 'Weekend/Holiday', 'is_open': False}
    if now < pre_:
        return {'time': now.strftime('%I:%M:%S %p'), 'status': 'CLOSED',
                'color': '#ff6b6b', 'countdown': f'Pre-market in {str(pre_-now)[:5]}', 'is_open': False}
    if now < open_:
        return {'time': now.strftime('%I:%M:%S %p'), 'status': 'PRE-MARKET',
                'color': '#FFD700', 'countdown': f'Opens in {str(open_-now)[:5]}', 'is_open': False}
    if now <= close_:
        pct = (now - open_) / (close_ - open_)
        return {'time': now.strftime('%I:%M:%S %p'), 'status': 'OPEN',
                'color': '#00ff88', 'countdown': f'Closes in {str(close_-now)[:5]}',
                'is_open': True, 'progress': pct}
    return {'time': now.strftime('%I:%M:%S %p'), 'status': 'CLOSED',
            'color': '#ff6b6b', 'countdown': 'Opens tomorrow', 'is_open': False}

# ==============================================================================
# YAHOO FINANCE CACHE
# ==============================================================================

_yf_lock = threading.Lock()
_yf_last_call = 0.0
_YF_DELAY = 1.2
_YF_CACHE_TTL = 300

def fetch_yf(symbol: str, period: str = '3mo') -> Optional[pd.DataFrame]:
    """Thread-safe Yahoo Finance fetch mit Cache und Dead-Ticker-Erkennung."""
    cache_key = f"{symbol}_{period}"
    now = time.time()

    # Cache-Check
    cache    = st.session_state.get('yahoo_cache', {})
    cache_ts = st.session_state.get('yahoo_cache_ts', {})
    if cache_key in cache and (now - cache_ts.get(cache_key, 0)) < _YF_CACHE_TTL:
        return cache[cache_key]

    # Dead-Ticker-Check
    if symbol in st.session_state.get('dead_tickers', set()):
        return None

    with _yf_lock:
        elapsed = now - _yf_last_call
        if elapsed < _YF_DELAY:
            time.sleep(_YF_DELAY - elapsed)
        globals()['_yf_last_call'] = time.time()

    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period=period, interval='1d')

        if df is None or df.empty or len(df) < 10:
            add_dead_ticker(symbol)
            st.session_state['dead_tickers'].add(symbol)
            return None

        # Pre/Post-Market Preis einsetzen
        try:
            pm = ticker.fast_info.last_price
            if pm and pm > 0:
                df.iloc[-1, df.columns.get_loc('Close')] = pm
        except Exception:
            pass

        cache[cache_key]    = df
        cache_ts[cache_key] = time.time()
        st.session_state['yahoo_cache']    = cache
        st.session_state['yahoo_cache_ts'] = cache_ts
        return df

    except Exception as e:
        err = str(e).lower()
        if 'delisted' in err or 'no price data' in err or '404' in err:
            add_dead_ticker(symbol)
            st.session_state['dead_tickers'].add(symbol)
        return None

def get_qqq_data() -> Optional[pd.DataFrame]:
    """QQQ Daten für Relative Strength – gecacht 1h."""
    now = time.time()
    if (st.session_state.get('qqq_data') is not None and
            now - st.session_state.get('qqq_loaded_at', 0) < 3600):
        return st.session_state['qqq_data']

    df = fetch_yf('QQQ', '3mo')
    if df is not None:
        st.session_state['qqq_data']    = df
        st.session_state['qqq_loaded_at'] = now
    return df

# ==============================================================================
# RELATIVE STRENGTH vs QQQ
# ==============================================================================

def calc_rs_vs_qqq(symbol_df: pd.DataFrame, qqq_df: pd.DataFrame, days: int = 20) -> float:
    """
    Berechnet Relative Strength: wie viel hat der Ticker über/unter QQQ performed
    in den letzten N Tagen. Positiv = outperformance.
    """
    try:
        sym_close = symbol_df['Close'].tail(days + 1)
        qqq_close = qqq_df['Close'].tail(days + 1)

        if len(sym_close) < 2 or len(qqq_close) < 2:
            return 0.0

        sym_ret = (float(sym_close.iloc[-1]) - float(sym_close.iloc[0])) / float(sym_close.iloc[0])
        qqq_ret = (float(qqq_close.iloc[-1]) - float(qqq_close.iloc[0])) / float(qqq_close.iloc[0])

        return round((sym_ret - qqq_ret) * 100, 2)
    except Exception:
        return 0.0

# ==============================================================================
# VOLUME PROFILE ANALYSIS
# ==============================================================================

def analyze_volume_profile(df: pd.DataFrame, pullback_start_idx: int) -> str:
    """
    Analysiert ob Volumen beim Pullback abnimmt (gesund) oder zunimmt (Distribution).
    pullback_start_idx: Index des letzten Hochs
    """
    try:
        if pullback_start_idx < 0 or len(df) < pullback_start_idx + 2:
            return "neutral"

        pullback_vols  = df['Volume'].iloc[pullback_start_idx:].values
        pre_high_vols  = df['Volume'].iloc[max(0, pullback_start_idx-10):pullback_start_idx].values

        if len(pullback_vols) < 2 or len(pre_high_vols) < 2:
            return "neutral"

        avg_pre    = float(np.mean(pre_high_vols))
        avg_pullbk = float(np.mean(pullback_vols))

        if avg_pre <= 0:
            return "neutral"

        ratio = avg_pullbk / avg_pre

        if ratio < 0.7:
            return "healthy"       # Volumen nimmt ab → gesunder Pullback
        elif ratio > 1.2:
            return "distribution"  # Volumen steigt → Verkaufsdruck
        else:
            return "neutral"
    except Exception:
        return "neutral"

# ==============================================================================
# CANDLESTICK ANALYSIS
# ==============================================================================

def analyze_candlestick(df: pd.DataFrame, swing_low: float, recent_high: float) -> CandlestickSignal:
    if len(df) < 5:
        return CandlestickSignal(CandlestickPattern.NONE, 0, False, "Zu wenig Daten", "weak")

    def props(c):
        o, cl, h, l = float(c['Open']), float(c['Close']), float(c['High']), float(c['Low'])
        body = abs(cl - o)
        tr   = h - l
        if tr <= 0:
            return None
        return {
            'o': o, 'c': cl, 'h': h, 'l': l,
            'body': body, 'tr': tr,
            'upper': (h - max(o, cl)) / tr,
            'lower': (min(o, cl) - l) / tr,
            'body_pct': body / tr,
            'bull': cl > o,
        }

    p0 = props(df.iloc[-4]) if len(df) >= 4 else None
    p1 = props(df.iloc[-3])
    p2 = props(df.iloc[-2])
    p3 = props(df.iloc[-1])

    if not all([p1, p2, p3]):
        return CandlestickSignal(CandlestickPattern.NONE, 0, False, "Ungültige Kerzen", "weak")

    near_support = (p3['c'] - swing_low) / p3['c'] < 0.04 if p3['c'] > 0 else False
    in_pullback  = 0.02 < (recent_high - p3['c']) / recent_high < MAX_PULLBACK_PCT if recent_high > 0 else False

    signals, strength, confirms = [], 0, 0

    # Hammer
    if p3['lower'] > 0.60 and p3['body_pct'] < 0.30 and p3['bull'] and near_support:
        signals.append("HAMMER"); strength += 40
        if p3['lower'] > 0.70: strength += 10; confirms += 1

    # Bullish Engulfing
    if p2['c'] < p2['o'] and p3['bull'] and p3['o'] < p2['c'] and p3['c'] > p2['o'] and p3['body'] > p2['body'] * 1.2:
        signals.append("ENGULFING"); strength += 35
        if near_support: strength += 10; confirms += 1

    # Morning Star
    if p1 and p1['c'] < p1['o'] and p1['body_pct'] > 0.5 and p2['body_pct'] < 0.3 and p3['bull'] and p3['c'] > (p1['o'] + p1['c']) / 2:
        signals.append("MORNING_STAR"); strength += 45; confirms += 1

    # Three White Soldiers
    if p0 and all(x['bull'] for x in [p0, p1, p2]) and p1['c'] > p0['c'] and p2['c'] > p1['c'] and all(x['body_pct'] > 0.4 for x in [p0, p1, p2]):
        signals.append("3_SOLDIERS"); strength += 50; confirms += 2

    # Piercing Line
    if p2['c'] < p2['o'] and p3['bull'] and p3['o'] < p2['l'] and p3['c'] > (p2['o'] + p2['c']) / 2 and near_support:
        signals.append("PIERCING"); strength += 30

    # Volume Confirmation
    avg_vol = float(df['Volume'].tail(20).mean())
    if avg_vol > 0 and float(df['Volume'].iloc[-1]) > avg_vol * 1.5:
        confirms += 1; strength += 5

    if near_support:  confirms += 1; strength += 8
    if in_pullback:   confirms += 1; strength += 5

    if not signals:
        return CandlestickSignal(CandlestickPattern.NONE, 0, False, "Kein Signal", "weak")

    pattern_map = {
        "3_SOLDIERS":  CandlestickPattern.THREE_WHITE_SOLDIERS,
        "MORNING_STAR": CandlestickPattern.MORNING_STAR,
        "HAMMER":      CandlestickPattern.HAMMER,
        "ENGULFING":   CandlestickPattern.BULLISH_ENGULFING,
        "PIERCING":    CandlestickPattern.PIERCING_LINE,
    }
    main = next((pattern_map[s] for s in signals if s in pattern_map), CandlestickPattern.NONE)

    if strength >= 80 and confirms >= 3:   quality = "excellent"
    elif strength >= 65 and confirms >= 2: quality = "good"
    elif strength >= 50 and confirms >= 1: quality = "moderate"
    else:                                  quality = "weak"

    return CandlestickSignal(
        pattern=main,
        strength=min(100, strength),
        confirmation=confirms >= 2,
        description=f"{' + '.join(signals)} ({confirms}x confirm)",
        entry_quality=quality,
    )

# ==============================================================================
# MARKET STRUCTURE
# ==============================================================================

def analyze_structure(df: pd.DataFrame) -> Dict:
    default = {'structure_intact': False, 'higher_highs': False, 'higher_lows': False,
               'last_swing_low': 0.0, 'last_swing_high': 0.0, 'high_idx': -1}
    try:
        if len(df) < 15:
            return default

        highs = df['High'].values
        lows  = df['Low'].values
        swing_h, swing_l = [], []

        for i in range(2, len(highs) - 2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_h.append((i, float(highs[i])))
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_l.append((i, float(lows[i])))

        if len(swing_h) < 2 or len(swing_l) < 2:
            return default

        hh = swing_h[-1][1] > swing_h[-2][1]
        hl = swing_l[-1][1] > swing_l[-2][1]

        return {
            'structure_intact': hh and hl,
            'higher_highs':     hh,
            'higher_lows':      hl,
            'last_swing_low':   swing_l[-1][1],
            'last_swing_high':  swing_h[-1][1],
            'high_idx':         swing_h[-1][0],
        }
    except Exception:
        return default

# ==============================================================================
# NEWS
# ==============================================================================

_news_cache: Dict[str, Tuple[List, float]] = {}
_NEWS_TTL = 1800

def get_news(symbol: str) -> List[Dict]:
    now = time.time()
    if symbol in _news_cache and now - _news_cache[symbol][1] < _NEWS_TTL:
        return _news_cache[symbol][0]

    key = next_finnhub_key()
    if not key:
        return []

    try:
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            'symbol': symbol,
            'from': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            'to': datetime.now().strftime('%Y-%m-%d'),
            'token': key,
        }
        r = requests.get(url, params=params, timeout=8)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and data:
                items = sorted(data, key=lambda x: x.get('datetime', 0), reverse=True)[:3]
                result = [{'title': i.get('headline', '')[:80], 'url': i.get('url', ''),
                           'source': i.get('source', 'Finnhub')} for i in items]
                _news_cache[symbol] = (result, now)
                return result
    except Exception:
        pass
    return []

# ==============================================================================
# TELEGRAM
# ==============================================================================

def send_telegram(result: ScanResult) -> bool:
    if not TELEGRAM_TOKEN or len(TELEGRAM_TOKEN) < 10:
        return False

    rs_str = f"+{result.rs_vs_qqq:.1f}%" if result.rs_vs_qqq >= 0 else f"{result.rs_vs_qqq:.1f}%"
    vol_emoji = {"healthy": "✅", "distribution": "⚠️", "neutral": "➖"}.get(result.vol_profile, "➖")
    candle_str = ""
    if result.candlestick.pattern != CandlestickPattern.NONE:
        candle_str = f"\n🕯 {result.candlestick.pattern.value.upper()} ({result.candlestick.strength}/100)"

    news_str = ""
    if result.news:
        news_str = f"\n📰 {result.news[0]['title'][:50]}..."

    tv_url = f"https://www.tradingview.com/chart/?symbol={result.symbol}"

    msg = (
        f"🐂 <b>{result.symbol}</b> – Score {result.score}/100\n\n"
        f"💵 Preis: ${result.price:.2f}\n"
        f"📉 Pullback: -{result.pullback_pct*100:.1f}%\n"
        f"🎯 RS vs QQQ: {rs_str}\n"
        f"{vol_emoji} Volumen: {result.vol_profile}\n"
        f"📊 R:R {result.rr_ratio:.1f}x | RVol {result.rvol:.1f}x\n"
        f"🛑 SL: ${result.stop_loss:.2f} | TP: ${result.target:.2f}"
        f"{candle_str}{news_str}\n\n"
        f"📈 <a href='{tv_url}'>Chart öffnen</a>"
    )

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={'chat_id': TELEGRAM_CHAT, 'text': msg, 'parse_mode': 'HTML',
                  'disable_web_page_preview': True},
            timeout=6
        ).raise_for_status()
        return True
    except Exception:
        return False

def send_telegram_heartbeat(setups: int, alerts: int, elapsed: float) -> bool:
    if not TELEGRAM_TOKEN or len(TELEGRAM_TOKEN) < 10:
        return False
    now = datetime.now()
    msg = (
        f"🤖 <b>Elite Bull Scanner – Heartbeat</b>

"
        f"🕒 {now.strftime('%H:%M')} Uhr
"
        f"📊 Setups gefunden: {setups}
"
        f"🚨 Alerts gesendet: {alerts}
"
        f"⏱ Scan-Dauer: {elapsed:.1f}s

"
        f"✅ Scanner läuft normal"
    )
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=6
        ).raise_for_status()
        return True
    except Exception:
        return False


def should_alert(symbol: str, price: float, score: int) -> bool:
    alerts = st.session_state.get('sent_alerts', {})
    now = datetime.now()
    if symbol not in alerts:
        return True
    last = alerts[symbol]
    if (now - last['ts']).total_seconds() / 60 < ALERT_COOLDOWN_MIN:
        return False
    if abs(price - last['price']) / last['price'] < 0.02 and score - last['score'] < 10:
        return False
    return True

def record_alert(symbol: str, price: float, score: int):
    st.session_state['sent_alerts'][symbol] = {'ts': datetime.now(), 'price': price, 'score': score}

# ==============================================================================
# GEMINI AI
# ==============================================================================

def gemini_analysis(result: ScanResult) -> str:
    if not GEMINI_KEY or not GEMINI_AVAILABLE:
        return "⚠️ Gemini nicht verfügbar."
    try:
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel('gemini-1.5-flash')
        news_title = result.news[0]['title'] if result.news else 'Keine News'
        prompt = (
            f"Du bist ein professioneller Tech-Stock-Daytrader. Analysiere:\n"
            f"Ticker: {result.symbol}\n"
            f"Preis: ${result.price:.2f} | Pullback: -{result.pullback_pct*100:.1f}%\n"
            f"SL: ${result.stop_loss:.2f} | TP: ${result.target:.2f} | R:R: {result.rr_ratio:.1f}x\n"
            f"RS vs QQQ (20d): {result.rs_vs_qqq:+.1f}%\n"
            f"Volumen-Profil: {result.vol_profile}\n"
            f"Candlestick: {result.candlestick.description} ({result.candlestick.strength}/100)\n"
            f"News: {news_title}\n\n"
            f"Gib eine präzise Einschätzung in 3 Sätzen:\n"
            f"1. RS und Setup-Qualität\n"
            f"2. Volumen-Profil Bewertung\n"
            f"3. Entry-Empfehlung"
        )
        resp = model.generate_content(prompt)
        return resp.text.strip() if resp and hasattr(resp, 'text') else "Keine Antwort."
    except Exception as e:
        return f"Fehler: {str(e)[:80]}"

# ==============================================================================
# CORE SCANNER
# ==============================================================================

def analyze_symbol(symbol: str, qqq_df: Optional[pd.DataFrame]) -> Optional[ScanResult]:
    df = fetch_yf(symbol, '3mo')
    if df is None or len(df) < 15:
        return None

    df_clean = df.dropna()
    if len(df_clean) < 15:
        return None

    current_price = float(df_clean['Close'].iloc[-1])

    # Mindestpreis-Filter – kein Penny-Stock
    if current_price < MIN_PRICE:
        return None

    # Struktur-Analyse
    struct = analyze_structure(df_clean)
    if not (struct['structure_intact'] or struct['higher_lows'] or struct['higher_highs']):
        return None

    last_swing_low  = struct['last_swing_low']
    last_swing_high = struct['last_swing_high']

    if last_swing_low <= 0 or last_swing_high <= 0:
        return None

    # Pullback berechnen (vs. letztem Swing-High, nicht altem Allzeithoch)
    lookback   = min(60, len(df_clean) - 5)
    recent     = df_clean.tail(lookback)
    recent_high = float(recent['High'].max())
    pullback   = (recent_high - current_price) / recent_high

    if pullback < MIN_PULLBACK_PCT or pullback > MAX_PULLBACK_PCT:
        return None

    # Preis nicht zu weit unter Swing Low
    if current_price < last_swing_low * 0.88:
        return None

    # Relative Strength vs QQQ
    rs = calc_rs_vs_qqq(df_clean, qqq_df) if qqq_df is not None else 0.0

    # Volumen-Profil
    high_idx    = struct.get('high_idx', -1)
    vol_profile = analyze_volume_profile(df_clean, high_idx)

    # Candlestick
    candle = analyze_candlestick(df_clean, last_swing_low, recent_high)

    # Score berechnen
    score = 30

    if struct['structure_intact']:  score += 15
    elif struct['higher_lows']:     score += 10
    elif struct['higher_highs']:    score += 6

    # RS Bonus/Malus
    if rs > 5:    score += 18
    elif rs > 2:  score += 12
    elif rs > 0:  score += 6
    elif rs < -5: score -= 10
    elif rs < -2: score -= 5

    # Volumen-Profil
    if vol_profile == "healthy":      score += 15
    elif vol_profile == "distribution": score -= 10

    # RVol
    avg_vol    = float(df_clean['Volume'].mean())
    curr_vol   = float(df_clean['Volume'].iloc[-1])
    rvol       = curr_vol / avg_vol if avg_vol > 0 else 1.0
    if rvol > 2:    score += 15
    elif rvol > 1:  score += 8

    # Support-Nähe
    support_dist = (current_price - last_swing_low) / current_price if current_price > 0 else 1.0
    if support_dist < 0.03: score += 12
    elif support_dist < 0.08: score += 6

    # Candlestick
    if candle.strength >= 50:
        score += candle.strength // 5
        if candle.confirmation: score += 8

    # News (nur bei hohem Score)
    news = []
    if score >= 60 and FINNHUB_KEYS:
        news = get_news(symbol)
        if news: score += 8

    if score < MIN_SCORE:
        return None

    # Stop Loss & Target
    try:
        atr = float((df_clean['High'].rolling(14).max() - df_clean['Low'].rolling(14).min()).mean())
        if not np.isfinite(atr) or atr <= 0:
            atr = current_price * 0.02
    except Exception:
        atr = current_price * 0.02

    stop_loss = max(last_swing_low * 0.97, current_price - 2 * atr)
    target    = min(recent_high * 0.97, current_price + (current_price - stop_loss) * 2)

    if stop_loss <= 0 or stop_loss >= current_price or target <= current_price:
        return None

    rr_ratio = (target - current_price) / (current_price - stop_loss) if (current_price - stop_loss) > 0 else 0
    if rr_ratio < 0.8:
        return None

    # Reasons
    reasons = [f"📉 -{pullback*100:.1f}%"]
    if struct['structure_intact']:  reasons.append("📈 HH+HL")
    elif struct['higher_lows']:     reasons.append("📈 HL")
    rs_str = f"+{rs:.1f}%" if rs >= 0 else f"{rs:.1f}%"
    reasons.append(f"🎯 RS {rs_str}")
    if vol_profile == "healthy":        reasons.append("✅ Vol OK")
    elif vol_profile == "distribution": reasons.append("⚠️ Vol!")
    if rvol > 1.5: reasons.append(f"⚡ {rvol:.1f}x Vol")
    if candle.strength >= 50: reasons.append(f"🕯 {candle.pattern.value}")
    if news: reasons.append("📰 News")

    # Source
    source = SourceType.WATCHLIST
    if symbol in st.session_state.get('catalyst_list', []):
        source = SourceType.CATALYST

    return ScanResult(
        symbol=symbol,
        score=min(100, int(score)),
        price=current_price,
        pullback_pct=pullback,
        recent_high=recent_high,
        stop_loss=stop_loss,
        target=target,
        rr_ratio=rr_ratio,
        rvol=rvol,
        rs_vs_qqq=rs,
        vol_profile=vol_profile,
        reasons=reasons,
        news=news,
        source=source,
        candlestick=candle,
        structure_intact=struct['structure_intact'],
    )

def run_scan(symbols: List[str]) -> List[ScanResult]:
    qqq_df  = get_qqq_data()
    results = []
    dead    = st.session_state.get('dead_tickers', set())
    active  = [s for s in symbols if s not in dead]

    ctx = None
    try:
        ctx = get_script_run_ctx()
    except Exception:
        pass

    def worker(sym):
        if ctx:
            try:
                add_script_run_ctx(threading.current_thread(), ctx)
            except Exception:
                pass
        return analyze_symbol(sym, qqq_df)

    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = {ex.submit(worker, s): s for s in active}
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r:
                    results.append(r)
            except Exception as e:
                logger.error(f"Scanner Fehler {futures[fut]}: {e}")

    return sorted(results, key=lambda x: x.score, reverse=True)

# ==============================================================================
# CARD RENDERER
# ==============================================================================

def render_card(r: ScanResult):
    score_color = '#FFD700' if r.score >= 85 else '#00ff88' if r.score >= 70 else '#58a6ff'
    card_class  = 'bull-card gold' if r.score >= 85 else 'bull-card'
    rs_class    = 'rs-positive' if r.rs_vs_qqq >= 0 else 'rs-negative'
    rs_str      = f"+{r.rs_vs_qqq:.1f}%" if r.rs_vs_qqq >= 0 else f"{r.rs_vs_qqq:.1f}%"
    vol_badge   = {'healthy': ('badge-green', '✅ Healthy Vol'), 'distribution': ('badge-red', '⚠️ Distribution'), 'neutral': ('badge-gray', '➖ Vol Neutral')}.get(r.vol_profile, ('badge-gray', '➖'))
    src_badge   = {'watchlist': ('badge-blue', '📋 WL'), 'catalyst': ('badge-yellow', '🧬 CATALYST'), 'gainers': ('badge-green', '🚀 GAINER')}.get(r.source.value, ('badge-gray', '📊'))
    has_candle  = r.candlestick.pattern != CandlestickPattern.NONE

    news_html = ""
    if r.news:
        n = r.news[0]
        url = n.get('url') or f"https://finance.yahoo.com/quote/{r.symbol}"
        news_html = f'<a href="{url}" target="_blank" class="link-btn">📰 {n["title"][:45]}...</a>'

    candle_html = ""
    if has_candle:
        c_color = '#00ff88' if r.candlestick.strength >= 65 else '#FFD700'
        candle_html = f'<span class="badge" style="background:#0a1a0a;color:{c_color};border:1px solid {c_color}44;">🕯 {r.candlestick.pattern.value.upper()} {r.candlestick.strength}/100</span>'

    struct_html = '<span class="badge badge-green">📈 HH+HL</span>' if r.structure_intact else '<span class="badge badge-gray">📈 HL</span>'

    reasons_html = ' '.join([f'<span class="badge badge-gray">{x}</span>' for x in r.reasons[:5]])

    vol_pct = min(100, int(r.rvol / 3 * 100))
    vol_color = '#00ff88' if r.rvol > 1.5 else '#FFD700' if r.rvol > 1 else '#8b949e'

    st.markdown(f"""
    <div class="{card_class}">
        <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">
            <span class="card-symbol">{r.symbol}</span>
            <span class="badge {src_badge[0]}">{src_badge[1]}</span>
        </div>

        <div class="price-display">${r.price:.2f}</div>

        <div style="margin:6px 0;">
            {struct_html}
            <span class="badge {vol_badge[0]}">{vol_badge[1]}</span>
            {candle_html}
        </div>

        <div class="metric-row">
            <div class="metric-item">Pullback<span>-{r.pullback_pct*100:.1f}%</span></div>
            <div class="metric-item">RS vs QQQ<span class="{rs_class}">{rs_str}</span></div>
            <div class="metric-item">R:R<span>{r.rr_ratio:.1f}x</span></div>
            <div class="metric-item">RVol<span>{r.rvol:.1f}x</span></div>
        </div>

        <div class="volume-bar">
            <span>Vol</span>
            <div class="volume-bar-inner">
                <div class="volume-bar-fill" style="width:{vol_pct}%;background:{vol_color};"></div>
            </div>
            <span>{r.rvol:.1f}x</span>
        </div>

        <div class="sl-tp-row">
            <div class="sl-badge">🛑 SL ${r.stop_loss:.2f}</div>
            <div class="tp-badge">🎯 TP ${r.target:.2f}</div>
        </div>

        <div style="font-size:0.72rem;color:{score_color};font-weight:700;margin:6px 0;">
            Score: {r.score}/100
        </div>
        <div class="score-bar-bg">
            <div class="score-bar-fill" style="width:{r.score}%;background:{score_color};"></div>
        </div>

        <div style="margin:8px 0;">{reasons_html}</div>

        {news_html}
        <a href="https://www.tradingview.com/chart/?symbol={r.symbol}" target="_blank" class="link-btn">📈 TradingView</a>
    </div>
    """, unsafe_allow_html=True)

    if st.button(f"🤖 Gemini", key=f"gem_{r.symbol}_{random.randint(1000,9999)}"):
        with st.spinner(f"Analysiere {r.symbol}..."):
            st.info(gemini_analysis(r), icon="💡")

# ==============================================================================
# MAIN UI
# ==============================================================================

def main():
    clock = get_market_clock()

    # Clock Display
    progress_html = ""
    if clock.get('progress'):
        pct = int(clock['progress'] * 100)
        progress_html = f'<div style="width:100%;height:2px;background:#21262d;border-radius:1px;margin-top:12px;"><div style="width:{pct}%;height:100%;background:#00ff88;border-radius:1px;transition:width 1s;"></div></div>'

    st.markdown(f"""
    <div class="clock-display">
        <div class="clock-time">{clock['time']}</div>
        <div style="margin:10px 0;">
            <span style="padding:6px 20px;border-radius:20px;font-weight:700;color:#080c10;background:{clock['color']};font-family:'Syne',sans-serif;">
                {clock['status']}
            </span>
        </div>
        <div style="color:#8b949e;font-size:0.9rem;">{clock['countdown']}</div>
        {progress_html}
    </div>
    """, unsafe_allow_html=True)

    # ── SIDEBAR ──────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("### 🤖 Autopilot")

        auto = st.toggle("Autopilot (alle 30 Min)", value=st.session_state.get('auto_refresh', False))
        st.session_state['auto_refresh'] = auto

        if auto:
            last = st.session_state.get('last_scan_time')
            if last:
                age = (datetime.now() - last).total_seconds()
                remaining = max(0, AUTO_SCAN_INTERVAL - age)
                st.markdown(f'<div class="autopilot-status autopilot-on">✅ AN – nächster Scan in {int(remaining//60)}:{int(remaining%60):02d}</div>', unsafe_allow_html=True)
            else:
                st.markdown('<div class="autopilot-status autopilot-on">✅ AN – erster Scan läuft gleich</div>', unsafe_allow_html=True)
        else:
            st.markdown('<div class="autopilot-status autopilot-off">⏸ AUS</div>', unsafe_allow_html=True)

        st.divider()

        # Catalyst Manager
        st.markdown("### 🧬 Catalyst Manager")
        with st.expander("Liste verwalten"):
            new_t = st.text_input("Ticker hinzufügen").strip().upper()
            if st.button("➕ Hinzufügen") and new_t:
                if new_t not in st.session_state['catalyst_list']:
                    st.session_state['catalyst_list'].append(new_t)
                    save_json_file(CATALYST_FILE, st.session_state['catalyst_list'])
                    st.success(f"{new_t} hinzugefügt!")
                    st.rerun()

            cats = st.session_state.get('catalyst_list', [])
            if cats:
                st.write(", ".join(cats))
                rm = st.multiselect("Entfernen:", cats)
                if st.button("🗑 Löschen") and rm:
                    st.session_state['catalyst_list'] = [x for x in cats if x not in rm]
                    save_json_file(CATALYST_FILE, st.session_state['catalyst_list'])
                    st.rerun()

        st.divider()

        # Filter
        st.markdown("### 🎛 Filter")
        min_score_ui = st.slider("Min Score", 40, 85, MIN_SCORE)
        min_rs_ui    = st.slider("Min RS vs QQQ (%)", -10, 10, -5)
        vol_filter   = st.selectbox("Volumen-Profil", ["Alle", "Nur Healthy", "Kein Distribution"])

        st.divider()

        # Dead Tickers
        dead = st.session_state.get('dead_tickers', set())
        if dead:
            st.markdown(f"### 💀 Tote Ticker ({len(dead)})")
            with st.expander("Anzeigen"):
                st.write(", ".join(sorted(dead)))
                if st.button("🔄 Liste leeren"):
                    st.session_state['dead_tickers'] = set()
                    save_dead_tickers(set())
                    st.rerun()

        st.divider()

        # Manuelle Analyse
        st.markdown("### 🔍 Einzelanalyse")
        manual = st.text_input("Symbol:", placeholder="z.B. NVDA").upper()
        if st.button("Analysieren") and manual:
            with st.spinner(f"Analysiere {manual}..."):
                qqq = get_qqq_data()
                r   = analyze_symbol(manual, qqq)
                if r:
                    st.success(f"Score: {r.score}/100 | RS: {r.rs_vs_qqq:+.1f}% | R:R {r.rr_ratio:.1f}x")
                else:
                    st.error("Kein Setup gefunden")

    # ── AUTO-TRIGGER (FIX) ────────────────────────────────────────────────────
    scan_triggered = False

    if st.session_state.get('auto_refresh'):
        last = st.session_state.get('last_scan_time')
        if last is None:
            scan_triggered = True
        elif (datetime.now() - last).total_seconds() >= AUTO_SCAN_INTERVAL:
            scan_triggered = True
        else:
            # Countdown anzeigen und nach 5s neu laden
            age       = (datetime.now() - last).total_seconds()
            remaining = max(0, AUTO_SCAN_INTERVAL - age)
            mins = int(remaining // 60)
            secs = int(remaining % 60)
            st.info(f"⏳ Nächster Scan in {mins}:{secs:02d} Minuten...")
            time.sleep(5)
            st.rerun()

    # ── MANUAL SCAN BUTTON ────────────────────────────────────────────────────
    col1, col2 = st.columns([3, 1])
    with col1:
        if st.button("🚀 SCAN STARTEN", type="primary", use_container_width=True):
            scan_triggered = True
    with col2:
        if st.button("🔄 Cache leeren", use_container_width=True):
            st.session_state['yahoo_cache']    = {}
            st.session_state['yahoo_cache_ts'] = {}
            st.session_state['qqq_data']       = None
            st.success("Cache geleert!")

    # ── RUN SCAN ──────────────────────────────────────────────────────────────
    if scan_triggered:
        dead      = st.session_state.get('dead_tickers', set())
        catalysts = st.session_state.get('catalyst_list', [])
        symbols   = list({s for s in (BASE_WATCHLIST + catalysts) if s not in dead})

        with st.spinner(f"🔍 Scanne {len(symbols)} Symbole..."):
            progress = st.progress(0)
            status   = st.empty()

            start   = time.time()
            results = run_scan(symbols)
            elapsed = time.time() - start

            progress.empty()
            status.empty()

            # Filter anwenden
            filtered = [r for r in results if r.score >= min_score_ui]
            if min_rs_ui > -10:
                filtered = [r for r in filtered if r.rs_vs_qqq >= min_rs_ui]
            if vol_filter == "Nur Healthy":
                filtered = [r for r in filtered if r.vol_profile == "healthy"]
            elif vol_filter == "Kein Distribution":
                filtered = [r for r in filtered if r.vol_profile != "distribution"]

            st.session_state['scan_results']  = filtered
            st.session_state['last_scan_time'] = datetime.now()

            # Alerts senden
            alerts_sent = 0
            for r in filtered[:10]:
                if should_alert(r.symbol, r.price, r.score):
                    if send_telegram(r):
                        record_alert(r.symbol, r.price, r.score)
                        alerts_sent += 1

            # Stündlicher Heartbeat
            last_hb = st.session_state.get("last_heartbeat")
            now_hb  = datetime.now()
            if last_hb is None or (now_hb - last_hb).total_seconds() >= 3600:
                send_telegram_heartbeat(len(filtered), alerts_sent, elapsed)
                st.session_state["last_heartbeat"] = now_hb

            st.success(f"✅ {len(filtered)} Setups in {elapsed:.1f}s | {alerts_sent} Alerts gesendet")

    # ── RESULTS ───────────────────────────────────────────────────────────────
    results = st.session_state.get('scan_results', [])

    if results:
        last_scan = st.session_state.get('last_scan_time')
        if last_scan:
            st.caption(f"Letzter Scan: {last_scan.strftime('%H:%M:%S')} · {len(results)} Setups")

        # Stats Row
        avg_rs   = np.mean([r.rs_vs_qqq for r in results])
        avg_rr   = np.mean([r.rr_ratio for r in results])
        healthy  = sum(1 for r in results if r.vol_profile == "healthy")
        with_can = sum(1 for r in results if r.candlestick.pattern != CandlestickPattern.NONE)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Setups",        len(results))
        c2.metric("Ø RS vs QQQ",  f"{avg_rs:+.1f}%")
        c3.metric("Ø R:R",        f"{avg_rr:.1f}x")
        c4.metric("Healthy Vol",  f"{healthy}/{len(results)}")

        st.divider()

        cols = st.columns(3)
        for i, r in enumerate(results[:15]):
            with cols[i % 3]:
                render_card(r)

    else:
        st.markdown("""
        <div style="text-align:center;padding:60px 20px;color:#8b949e;">
            <div style="font-size:3rem;margin-bottom:16px;">🐂</div>
            <div style="font-family:'Syne',sans-serif;font-size:1.2rem;color:#e6edf3;">
                Bereit zum Scannen
            </div>
            <div style="margin-top:8px;font-size:0.85rem;">
                Klicke "SCAN STARTEN" oder aktiviere den Autopilot
            </div>
        </div>
        """, unsafe_allow_html=True)

    # Auto-Rerun für Clock-Update
    if clock.get('is_open'):
        time.sleep(1)
        st.rerun()

if __name__ == "__main__":
    main()
