"""
Elite Bull Scanner Pro V8.1 - BALANCED Edition
Candlestick als Bonus statt Pflicht, adaptive Filter
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any, Set, Union
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
import warnings
import pytz
import logging
import random
import os
import json
import threading
import numpy as np
from google import genai
from streamlit.runtime.scriptrunner import add_script_run_ctx, get_script_run_ctx

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================== CSS Styles ==============================
st.markdown("""
<style>
.bull-card { 
    border: 1px solid #333; 
    border-radius: 10px; 
    padding: 15px; 
    margin: 10px 0; 
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    box-shadow: 0 4px 6px rgba(0,0,0,0.3);
    transition: transform 0.2s;
    border-left: 4px solid #00FF00;
}
.bull-card:hover {
    transform: translateY(-2px);
    box-shadow: 0 6px 12px rgba(0,0,0,0.4);
}
.pullback-badge { 
    padding: 5px 10px; 
    border-radius: 5px; 
    color: white; 
    font-weight: bold;
    display: inline-block;
    margin: 5px 0;
}
.candlestick-badge {
    background: linear-gradient(90deg, #00FF00, #00aa00);
    color: white;
    padding: 3px 8px;
    border-radius: 3px;
    font-size: 0.75rem;
    margin: 0 2px;
    font-weight: bold;
    display: inline-block;
}
.candlestick-badge.weak {
    background: linear-gradient(90deg, #ff6b6b, #ffa502);
    opacity: 0.7;
}
.tier-badge { 
    background: #444; 
    padding: 2px 8px; 
    border-radius: 3px; 
    font-size: 0.7rem; 
    margin: 0 2px;
    color: #fff;
    border: 1px solid #555;
}
.cache-badge {
    background: #2d5a2d;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 0.7rem;
    margin: 0 2px;
    color: #90EE90;
}
.price {
    font-size: 1.5rem;
    font-weight: bold;
    color: #00FF00;
    margin: 10px 0;
}
.stop-loss {
    background: #ff4b4b;
    color: white;
    padding: 3px 8px;
    border-radius: 3px;
    font-size: 0.8rem;
    margin-right: 5px;
}
.target {
    background: #00FF00;
    color: black;
    padding: 3px 8px;
    border-radius: 3px;
    font-size: 0.8rem;
}
.confidence-bar {
    width: 100%;
    height: 8px;
    background: #333;
    border-radius: 4px;
    overflow: hidden;
    margin: 5px 0;
}
.confidence-fill {
    height: 100%;
    transition: width 0.3s ease;
}
.news-link-btn {
    display: block;
    background: #1f4068;
    color: #fff;
    text-decoration: none;
    padding: 8px;
    border-radius: 5px;
    margin: 5px 0;
    font-size: 0.8rem;
    text-align: center;
}
.news-link-btn:hover {
    background: #2a5585;
}
.btn-link {
    display: block;
    background: #4a4a4a;
    color: #fff;
    text-decoration: none;
    padding: 8px;
    border-radius: 5px;
    margin: 5px 0;
    font-size: 0.8rem;
    text-align: center;
}
.btn-link:hover {
    background: #5a5a5a;
}
.market-clock-container {
    background: linear-gradient(135deg, #1a1a2e 0%, #0f0f23 100%);
    padding: 20px;
    border-radius: 15px;
    text-align: center;
    margin: 20px 0;
    border: 1px solid #333;
}
.market-time {
    font-size: 2.5rem;
    font-weight: bold;
    color: #00FF00;
    font-family: 'Courier New', monospace;
}
.market-status {
    padding: 8px 20px;
    border-radius: 20px;
    font-weight: bold;
    color: white;
    display: inline-block;
    margin: 10px 0;
}
.market-countdown {
    font-size: 1.2rem;
    color: #FFD700;
    margin: 10px 0;
}
.market-progress {
    width: 100%;
    height: 6px;
    background: #333;
    border-radius: 3px;
    overflow: hidden;
    margin-top: 10px;
}
.market-progress-bar {
    height: 100%;
    background: linear-gradient(90deg, #00FF00, #FFD700);
    transition: width 1s ease;
}
.holiday-banner {
    background: linear-gradient(90deg, #ff4b4b, #ff6b6b);
    color: white;
    padding: 20px;
    border-radius: 10px;
    text-align: center;
    font-size: 1.2rem;
    margin: 20px 0;
    border: 2px solid #ff3333;
}
.info-box {
    background: #1a1a2e;
    padding: 15px;
    border-radius: 8px;
    border-left: 4px solid #00FF00;
    margin: 10px 0;
}
.error-box {
    background: #2d1a1a;
    padding: 15px;
    border-radius: 8px;
    border-left: 4px solid #ff4b4b;
    margin: 10px 0;
    color: #ff9999;
}
.api-stat {
    background: #1a1a2e;
    padding: 10px;
    border-radius: 5px;
    margin: 5px 0;
    font-size: 0.9rem;
}
.key-indicator {
    padding: 8px;
    margin: 5px 0;
    border-radius: 5px;
    background: #2a2a3e;
    font-size: 0.85rem;
}
.key-active {
    border-left: 3px solid #00FF00;
    background: #1a2a1a;
}
.key-exhausted {
    border-left: 3px solid #ff4b4b;
    opacity: 0.6;
}
.mover-badge {
    background: linear-gradient(90deg, #ff6b6b, #ffa502);
    color: white;
    padding: 3px 8px;
    border-radius: 3px;
    font-size: 0.7rem;
    margin: 0 2px;
    font-weight: bold;
}
.source-watchlist { border-left: 3px solid #00FF00; }
.source-gainers { border-left: 3px solid #ff6b6b; }
.source-mostactive { border-left: 3px solid #FFD700; }
.filter-active {
    background: linear-gradient(90deg, #00FF00, #00aa00);
    color: black;
    padding: 10px;
    border-radius: 5px;
    font-weight: bold;
    margin: 10px 0;
}
.structure-badge {
    background: #2d5a2d;
    padding: 2px 8px;
    border-radius: 3px;
    font-size: 0.7rem;
    margin: 0 2px;
    color: #90EE90;
}
.structure-badge.weak {
    background: #5a2d2d;
    color: #ff9999;
}
</style>
""", unsafe_allow_html=True)

# ============================== Type Definitions ==============================

class SourceType(str, Enum):
    WATCHLIST = "watchlist"
    GAINERS = "gainers"
    MOST_ACTIVE = "most_active"
    UNKNOWN = "unknown"

class CandlestickPattern(str, Enum):
    NONE = "none"
    HAMMER = "hammer"
    INVERTED_HAMMER = "inverted_hammer"
    BULLISH_ENGULFING = "bullish_engulfing"
    MORNING_STAR = "morning_star"
    PIERCING_LINE = "piercing_line"
    BULLISH_HARAMI = "bullish_harami"
    THREE_WHITE_SOLDIERS = "three_white_soldiers"

@dataclass
class CandlestickSignal:
    pattern: CandlestickPattern
    strength: int  # 0-100
    confirmation: bool
    description: str
    entry_quality: str  # "excellent", "good", "moderate", "weak"

@dataclass
class RateLimitConfig:
    calls_per_second: float = 1.0
    calls_per_minute: int = 60
    calls_per_day: int = 25
    burst_size: int = 3
    
    def get_min_delay(self) -> float:
        return 1.0 / self.calls_per_second

@dataclass 
class ScanResult:
    symbol: str
    tier: int
    score: int
    price: float
    pullback_pct: float
    recent_high: float
    stop_loss: float
    target: float
    rr_ratio: float
    rvol: float
    reasons: List[str]
    news: List[Dict[str, Any]] = field(default_factory=list)
    pe_ratio: Optional[float] = None
    api_sources: List[str] = field(default_factory=list)
    from_cache: bool = False
    source: SourceType = SourceType.UNKNOWN
    candlestick: CandlestickSignal = field(default_factory=lambda: CandlestickSignal(
        pattern=CandlestickPattern.NONE, strength=0, confirmation=False, 
        description="Kein Signal", entry_quality="weak"
    ))
    has_candlestick_confirm: bool = False
    structure_intact: bool = False  # NEU: Für Anzeige

# ============================== KONFIGURATION - GELockERT ==============================

st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V8.1 BALANCED", page_icon="🐂")

# 🔥 GELockERTE FILTER - Hier sind die Änderungen!
MIN_PULLBACK_PERCENT = 0.02      # Von 0.05 auf 0.02 (2% reichen!)
MAX_PULLBACK_PERCENT = 0.70      # Von 0.50 auf 0.70 (tiefere Pullbacks ok)
AUTO_REFRESH_INTERVAL = 3600
ALERT_COOLDOWN_MINUTES = 60
MIN_SCORE_THRESHOLD = 55         # Von 70 auf 55 (deutlich niedriger!)
MIN_CANDLESTICK_STRENGTH = 40    # Von 60 auf 40
REQUIRE_CANDLESTICK_CONFIRM = False  # WICHTIG: Immer False! Candlestick ist Bonus, nicht Pflicht
REQUIRE_STRUCTURE_INTACT = False     # NEU: Higher Lows reichen, nicht beides nötig
MAX_WATCHLIST_SIZE = 100

# API Keys - AUS SECRETS LADEN!
try:
    TELEGRAM_BOT_TOKEN = st.secrets["telegram"]["bot_token"]
    TELEGRAM_CHAT_ID = st.secrets["telegram"]["chat_id"]
    FINNHUB_KEYS = st.secrets["finnhub"]["keys"]
    ALPHA_VANTAGE_KEYS = st.secrets["alpha_vantage"]["keys"]
except Exception as e:
    logger.warning(f"Secrets nicht gefunden oder unvollständig: {e}")
    TELEGRAM_BOT_TOKEN = ""
    TELEGRAM_CHAT_ID = ""
    FINNHUB_KEYS = []
    ALPHA_VANTAGE_KEYS = []

DEFAULT_WATCHLIST = sorted(list(set([
    "NVDA", "TSLA", "AMD", "PLTR", "COIN", "MSTR", "HOOD", "CRWD", "AAPL", "MSFT", 
    "AMZN", "MARA", "SAP", "LLY", "ABBV", "JNJ", "PFE", "MRK", "BMY", "GILD", "AMGN", 
    "BIIB", "VRTX", "REGN", "MRNA", "BNTX", "GSK", "AZN", "SNY", "JAZZ", "ALNY", "IONS", 
    "NTLA", "EDIT", "CRSP", "BEAM", "VNDA", "SAVA", "GERN", "FATE", "IOVA", "SRPT", 
    "RCKT", "APLS", "HALO", "AQST", "IBRX", "ASND", "DNLI", "ALDX", "LNTH", "REPL", 
    "CING", "ACHV", "ATRA", "TBPH", "ROG", "ETON", "BMRN", "CRVS", "NVAX", "UUUU", 
    "CELC", "RAPT", "ACRS"
])))

FALLBACK_MOVERS = {
    'gainers': ["MARA", "RIOT", "HUT", "COIN", "HOOD", "MSTR", "NVDA", "AMD", "PLTR", "TSLA"],
    'most_active': ["TSLA", "NVDA", "AMD", "PLTR", "COIN", "MSTR", "HOOD", "AAPL", "MSFT", "AMZN"]
}

# ============================== Session State ==============================

def init_session_state():
    defaults = {
        'watchlist': DEFAULT_WATCHLIST,
        'sent_alerts': {},
        'api_stats': {'yahoo': 0, 'finnhub': 0, 'alpha_vantage': 0, 'cache_hits': 0, 'alpha_rotation_count': 0},
        'scan_results': [],
        'last_scan_time': None,
        'auto_refresh': False,
        'refresh_count': 0,
        'last_auto_refresh': 0,
        'last_movers_check': 0,
        'alert_history': [],
        'scan_debug': [],
        'top_movers_cache': FALLBACK_MOVERS,
        'combined_universe': set(DEFAULT_WATCHLIST + [s for sublist in FALLBACK_MOVERS.values() for s in sublist]),
        'movers_source': 'fallback',
        'hard_filter_active': False,  # 🔥 GEÄNDERT: Default False statt True!
        'show_only_candlestick': False,  # 🔥 GEÄNDERT: Default False
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()

# ============================== Cache & API Manager ==============================

class SmartCache:
    def __init__(self):
        self.cache = {}
        self.timestamps = {}
        
    def get(self, key: str, ttl: int = 600) -> Optional[Any]:
        if key in self.cache:
            age = time.time() - self.timestamps.get(key, 0)
            if age < ttl:
                return self.cache[key]
            del self.cache[key]
            del self.timestamps[key]
        return None
        
    def set(self, key: str, value: Any):
        self.cache[key] = value
        self.timestamps[key] = time.time()

news_cache = SmartCache()
fundamentals_cache = SmartCache()
structure_cache = SmartCache()
market_context_cache = SmartCache()
movers_cache = SmartCache()

class RateLimiter:
    def __init__(self, max_calls: int, window_seconds: int):
        self.max_calls = max_calls
        self.window = window_seconds
        self.calls = []
        self._lock = threading.Lock()
        
    def can_call(self) -> bool:
        with self._lock:
            now = time.time()
            self.calls = [c for c in self.calls if now - c < self.window]
            return len(self.calls) < self.max_calls
            
    def record_call(self) -> int:
        with self._lock:
            self.calls.append(time.time())
            return len(self.calls)
            
    def get_status(self) -> str:
        with self._lock:
            now = time.time()
            self.calls = [c for c in self.calls if now - c < self.window]
            return f"{len(self.calls)}/{self.max_calls}"

class AlphaVantageManager:
    def __init__(self, keys: List[str]):
        self.keys = [k for k in keys if k and len(k) > 10]
        self.current_index = 0
        self.limiters = {i: {'calls_today': 0, 'calls_per_min': [], 'key': k, 'exhausted': False} for i, k in enumerate(self.keys)}
        self._lock = threading.Lock()
        
    def get_current_key(self) -> Optional[str]:
        return self.keys[self.current_index] if self.keys else None
        
    def rotate_key(self) -> Optional[str]:
        if not self.keys:
            return None
        with self._lock:
            for _ in range(len(self.keys)):
                self.current_index = (self.current_index + 1) % len(self.keys)
                if not self.limiters[self.current_index]['exhausted']:
                    stats = st.session_state.get('api_stats', {})
                    if isinstance(stats, dict):
                        stats['alpha_rotation_count'] = stats.get('alpha_rotation_count', 0) + 1
                        st.session_state['api_stats'] = stats
                    return self.get_current_key()
        return None
        
    def can_call(self, key_index: Optional[int] = None) -> bool:
        if not self.keys:
            return False
        idx = key_index if key_index is not None else self.current_index
        limiter = self.limiters[idx]
        now = time.time()
        limiter['calls_per_min'] = [c for c in limiter['calls_per_min'] if now - c < 60]
        if len(limiter['calls_per_min']) >= 5:
            return False
        if limiter['calls_today'] >= 25:
            limiter['exhausted'] = True
            return False
        return True
        
    def record_call(self, key_index: Optional[int] = None) -> int:
        with self._lock:
            idx = key_index if key_index is not None else self.current_index
            limiter = self.limiters[idx]
            limiter['calls_per_min'].append(time.time())
            limiter['calls_today'] += 1
            stats = st.session_state.get('api_stats', {})
            if isinstance(stats, dict):
                stats['alpha_vantage'] = stats.get('alpha_vantage', 0) + 1
                st.session_state['api_stats'] = stats
            return limiter['calls_today']
            
    def get_status(self) -> List[Dict]:
        return [{
            'index': i,
            'key': f"{k[:4]}...{k[-4:]}" if len(k) > 8 else k,
            'active': i == self.current_index,
            'calls_today': self.limiters[i]['calls_today'],
            'exhausted': self.limiters[i]['exhausted'],
            'can_call': self.can_call(i)
        } for i, k in enumerate(self.keys)]

finnhub_limiter = RateLimiter(60, 60)
alpha_manager = AlphaVantageManager(ALPHA_VANTAGE_KEYS)

# ============================== Helper Functions ==============================

def safe_requests_get(url: str, params: Optional[Dict] = None, headers: Optional[Dict] = None, timeout: int = 10) -> Optional[requests.Response]:
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"API-Request Fehler: {e}")
        return None

def get_market_clock() -> Dict[str, Any]:
    et = pytz.timezone('US/Eastern')
    now = datetime.now(et)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    pre_market = now.replace(hour=4, minute=0, second=0, microsecond=0)

    holidays_2026 = [(1, 1), (1, 19), (2, 16), (4, 3), (5, 25), (6, 19), (7, 3), (9, 7), (11, 26), (12, 25)]
    is_holiday = (now.month, now.day) in holidays_2026

    if now.weekday() >= 5 or is_holiday:
        status = "CLOSED" if now.weekday() >= 5 else "HOLIDAY"
        color = "#ff4b4b"
        countdown = "Weekend" if now.weekday() >= 5 else "Holiday"
        next_event = "Tuesday 09:30 ET" if is_holiday and now.weekday() == 0 else "Monday 09:30 ET"
        progress = 0
    elif now < pre_market:
        status = "CLOSED"
        color = "#ff4b4b"
        countdown = f"Pre-market in {str(pre_market - now)[:8]}"
        next_event = "04:00 ET"
        progress = 0
    elif now < market_open:
        status = "PRE-MARKET"
        color = "#FFD700"
        countdown = f"Opens in {str(market_open - now)[:8]}"
        next_event = "09:30 ET"
        progress = 0
    elif market_open <= now <= market_close:
        status = "OPEN"
        color = "#00FF00"
        countdown = f"Closes in {str(market_close - now)[:8]}"
        next_event = "16:00 ET"
        progress = (now - market_open) / (market_close - market_open)
    else:
        status = "CLOSED"
        color = "#ff4b4b"
        countdown = "Opens tomorrow"
        next_event = "09:30 ET"
        progress = 0

    return {
        'time': now.strftime('%I:%M:%S %p'),
        'status': status,
        'color': color,
        'countdown': countdown,
        'next_event': next_event,
        'progress': progress,
        'is_open': status == "OPEN",
        'is_holiday': is_holiday
    }

def get_market_context() -> Dict[str, Any]:
    cache_key = "market_ctx"
    cached = market_context_cache.get(cache_key, 3600)
    if cached:
        return cached
    
    try:
        time.sleep(1)
        spy = yf.Ticker("SPY")
        spy_data = spy.history(period="5d")
        
        if len(spy_data) < 2:
            result = {'risk_off': False, 'spy_change': 0, 'market_closed': True}
            market_context_cache.set(cache_key, result)
            return result
        
        spy_change = (spy_data['Close'].iloc[-1] - spy_data['Close'].iloc[-2]) / spy_data['Close'].iloc[-2]
        
        vix_level = 20
        if abs(spy_change) > 0.01:
            try:
                time.sleep(0.5)
                vix = yf.Ticker("^VIX")
                vix_data = vix.history(period="2d")
                vix_level = vix_data['Close'].iloc[-1] if not vix_data.empty else 20
            except:
                pass
        
        risk_off = (spy_change < -0.02) or (vix_level > 30)
        result = {
            'risk_off': risk_off, 
            'spy_change': spy_change, 
            'vix_level': vix_level, 
            'market_closed': False
        }
        market_context_cache.set(cache_key, result)
        return result
        
    except Exception as e:
        logger.error(f"Fehler beim Marktkontext: {e}")
        result = {'risk_off': False, 'spy_change': 0, 'vix_level': 20, 'market_closed': True}
        market_context_cache.set(cache_key, result)
        return result

# ============================== Top Movers Functions ==============================

def fetch_yahoo_movers() -> Tuple[Dict[str, List[str]], str]:
    cache_key = "yahoo_movers"
    cached = movers_cache.get(cache_key, AUTO_REFRESH_INTERVAL)
    if cached:
        return cached, 'cache'
    
    movers = {'gainers': [], 'most_active': []}
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    
    urls = {
        'gainers': 'https://finance.yahoo.com/gainers',
        'most_active': 'https://finance.yahoo.com/most-active'
    }
    
    success_count = 0
    
    for category, url in urls.items():
        try:
            time.sleep(2)
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                try:
                    tables = pd.read_html(StringIO(response.text))
                    if tables and len(tables) > 0:
                        df = tables[0]
                        if 'Symbol' in df.columns:
                            symbols = df['Symbol'].head(15).tolist()
                            symbols = [s for s in symbols if isinstance(s, str) and len(s) <= 5 and s.isalpha()]
                            movers[category] = symbols[:10]
                            success_count += 1
                            logger.info(f"{category}: {len(symbols)} Symbole geladen")
                except Exception as parse_error:
                    logger.error(f"Parsing Fehler {category}: {parse_error}")
                    continue
        except Exception as e:
            logger.error(f"Fehler beim Laden {category}: {e}")
            continue
    
    if success_count >= 1: 
        movers_cache.set(cache_key, movers)
        return movers, 'yahoo'
    else:
        logger.warning("Verwende Fallback Movers")
        return FALLBACK_MOVERS, 'fallback'

def get_combined_universe(force_refresh: bool = False) -> Tuple[Set[str], str]:
    last_check = st.session_state.get('last_movers_check', 0)
    now = time.time()
    
    if force_refresh or (now - last_check >= AUTO_REFRESH_INTERVAL):
        st.session_state['last_movers_check'] = now
        
        movers, source = fetch_yahoo_movers()
        st.session_state['top_movers_cache'] = movers
        st.session_state['movers_source'] = source
        
        combined = set(st.session_state['watchlist'])
        for category, symbols in movers.items():
            combined.update(symbols)
        
        st.session_state['combined_universe'] = combined
        logger.info(f"Universum aktualisiert: {len(combined)} Symbole (Source: {source})")
        return combined, source
    
    return st.session_state['combined_universe'], st.session_state.get('movers_source', 'fallback')

def get_symbol_source(symbol: str) -> SourceType:
    if symbol in st.session_state['watchlist']:
        return SourceType.WATCHLIST
    movers = st.session_state.get('top_movers_cache', {})
    if symbol in movers.get('gainers', []):
        return SourceType.GAINERS
    if symbol in movers.get('most_active', []):
        return SourceType.MOST_ACTIVE
    return SourceType.UNKNOWN

# ============================== News Functions ==============================

def get_finnhub_news_smart(symbol: str) -> Tuple[Optional[List[Dict]], bool]:
    cache_key = f"news_{symbol}"
    cached = news_cache.get(cache_key, AUTO_REFRESH_INTERVAL)
    if cached:
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['cache_hits'] = stats.get('cache_hits', 0) + 1
            st.session_state['api_stats'] = stats
        return cached, True
    
    if not FINNHUB_KEYS:
        return None, False
        
    current_finnhub_key = random.choice(FINNHUB_KEYS)
    
    if not finnhub_limiter.can_call():
        return None, False
    
    try:
        url = f"https://finnhub.io/api/v1/company-news"
        params = {
            'symbol': symbol,
            'from': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            'to': datetime.now().strftime('%Y-%m-%d'),
            'token': current_finnhub_key
        }
        response = safe_requests_get(url, params, timeout=10)
        if response:
            data = response.json()
            finnhub_limiter.record_call()
            stats = st.session_state.get('api_stats', {})
            if isinstance(stats, dict):
                stats['finnhub'] = stats.get('finnhub', 0) + 1
                st.session_state['api_stats'] = stats
            
            if isinstance(data, list) and len(data) > 0:
                sorted_news = sorted(data, key=lambda x: x.get('datetime', 0), reverse=True)[:5]
                formatted_news = []
                for item in sorted_news:
                    formatted_news.append({
                        'title': item.get('headline', 'No Title'),
                        'url': item.get('url', ''),
                        'source': item.get('source', 'Finnhub'),
                        'datetime': item.get('datetime', 0),
                        'score': 10
                    })
                news_cache.set(cache_key, formatted_news)
                return formatted_news, False
    except Exception as e:
        logger.error(f"Finnhub Fehler für {symbol}: {e}")
    
    return None, False

def analyze_news_tiered(symbol: str, tier: int, score: int) -> Tuple[List[Dict], List[str], bool]:
    if tier <= 20 or score > 60:
        news, cached = get_finnhub_news_smart(symbol)
        if news:
            sources = ['FH']
            bullish_kws = ['bullish', 'upgrade', 'beat', 'growth', 'positive', 'strong', 'zulassung', 'übernahmeangebot', 'quartalszahlen']
            bearish_kws = ['bearish', 'downgrade', 'miss', 'loss', 'negative', 'weak']
            
            for item in news[:1]:
                title_lower = item['title'].lower()
                if any(word in title_lower for word in bullish_kws):
                    item['score'] = 15
                elif any(word in title_lower for word in bearish_kws):
                    item['score'] = 5
            return news, sources, cached
    
    return [], [], False

# ============================== CANDLESTICK ANALYSIS ==============================

def analyze_candlestick(df: pd.DataFrame, swing_low: float, recent_high: float) -> CandlestickSignal:
    """
    Candlestick-Analyse - jetzt als Bonus-System, nicht als Pflicht!
    """
    if len(df) < 5:
        return CandlestickSignal(
            pattern=CandlestickPattern.NONE,
            strength=0,
            confirmation=False,
            description="Zu wenig Daten",
            entry_quality="weak"
        )
    
    # Letzte 3 Kerzen für Muster-Erkennung
    c1 = df.iloc[-3]
    c2 = df.iloc[-2]
    c3 = df.iloc[-1]
    
    def candle_properties(c):
        open_p = float(c['Open'])
        close_p = float(c['Close'])
        high_p = float(c['High'])
        low_p = float(c['Low'])
        
        body = abs(close_p - open_p)
        upper_shadow = high_p - max(open_p, close_p)
        lower_shadow = min(open_p, close_p) - low_p
        total_range = high_p - low_p
        
        return {
            'open': open_p, 'close': close_p, 'high': high_p, 'low': low_p,
            'body': body, 'upper_shadow': upper_shadow, 'lower_shadow': lower_shadow,
            'total_range': total_range,
            'bullish': close_p > open_p,
            'bearish': close_p < open_p,
            'body_pct': body / total_range if total_range > 0 else 0,
            'upper_pct': upper_shadow / total_range if total_range > 0 else 0,
            'lower_pct': lower_shadow / total_range if total_range > 0 else 0
        }
    
    p1 = candle_properties(c1)
    p2 = candle_properties(c2)
    p3 = candle_properties(c3)
    
    dist_to_support = (p3['close'] - swing_low) / p3['close'] if p3['close'] > 0 else 1.0
    near_support = dist_to_support < 0.03
    dist_from_high = (recent_high - p3['close']) / recent_high
    in_pullback = 0.02 < dist_from_high < 0.70  # 🔥 Angepasst an neue Grenzen
    
    signals_found = []
    strength = 0
    confirmations = 0
    
    # === PATTERN 1: HAMMER ===
    is_hammer = (
        p3['lower_pct'] > 0.60 and
        p3['body_pct'] < 0.30 and
        p3['bullish'] and
        near_support and
        p3['low'] <= swing_low * 1.02
    )
    
    if is_hammer:
        signals_found.append("HAMMER")
        strength += 40
        if p3['lower_pct'] > 0.70:
            strength += 10
            confirmations += 1
    
    # === PATTERN 2: INVERTED HAMMER ===
    is_inverted_hammer = (
        p3['upper_pct'] > 0.60 and
        p3['body_pct'] < 0.30 and
        p3['bearish'] and
        p2['bullish'] and
        near_support
    )
    
    if is_inverted_hammer and not is_hammer:
        signals_found.append("INVERTED_HAMMER")
        strength += 25
    
    # === PATTERN 3: BULLISH ENGULFING ===
    is_engulfing = (
        p2['bearish'] and
        p3['bullish'] and
        p3['open'] < p2['close'] and
        p3['close'] > p2['open'] and
        p3['body'] > p2['body'] * 1.2
    )
    
    if is_engulfing:
        signals_found.append("ENGULFING")
        strength += 35
        if near_support:
            strength += 10
            confirmations += 1
    
    # === PATTERN 4: MORNING STAR ===
    is_morning_star = (
        p1['bearish'] and
        p1['body_pct'] > 0.50 and
        p2['body_pct'] < 0.30 and
        p3['bullish'] and
        p3['close'] > (p1['open'] + p1['close']) / 2
    )
    
    if is_morning_star:
        signals_found.append("MORNING_STAR")
        strength += 45
        confirmations += 1
    
    # === PATTERN 5: PIERCING LINE ===
    is_piercing = (
        p2['bearish'] and
        p3['bullish'] and
        p3['open'] < p2['low'] and
        p3['close'] > (p2['open'] + p2['close']) / 2 and
        near_support
    )
    
    if is_piercing:
        signals_found.append("PIERCING")
        strength += 30
    
    # === PATTERN 6: BULLISH HARAMI ===
    is_harami = (
        p2['bearish'] and
        p2['body_pct'] > 0.50 and
        p3['bullish'] and
        p3['body'] < p2['body'] * 0.6 and
        p3['high'] < p2['high'] and
        p3['low'] > p2['low'] and
        near_support
    )
    
    if is_harami:
        signals_found.append("HARAMI")
        strength += 20
    
    # === PATTERN 7: THREE WHITE SOLDIERS ===
    p0 = candle_properties(df.iloc[-4]) if len(df) >= 4 else None
    
    is_three_soldiers = (
        p0 is not None and
        p0['bullish'] and p1['bullish'] and p2['bullish'] and
        p1['close'] > p0['close'] and
        p2['close'] > p1['close'] and
        p2['open'] > p1['open'] and
        all(c['body_pct'] > 0.40 for c in [p0, p1, p2])
    )
    
    if is_three_soldiers:
        signals_found.append("3_SOLDIERS")
        strength += 50
        confirmations += 2
    
    # === ZUSÄTZLICHE BESTÄTIGUNGEN ===
    avg_vol = df['Volume'].tail(20).mean()
    current_vol = float(df['Volume'].iloc[-1])
    if current_vol > avg_vol * 1.5:
        confirmations += 1
        strength += 5
    
    if near_support:
        confirmations += 1
        strength += 10
    
    if in_pullback:
        confirmations += 1
        strength += 5
    
    prev_close = p2['close']
    if p3['open'] >= prev_close * 0.99:
        confirmations += 1
        strength += 5
    
    # === ERGEBNIS ===
    if not signals_found:
        return CandlestickSignal(
            pattern=CandlestickPattern.NONE,
            strength=0,
            confirmation=False,
            description="Kein klares Candlestick-Signal",
            entry_quality="weak"
        )
    
    pattern_priority = {
        "3_SOLDIERS": CandlestickPattern.THREE_WHITE_SOLDIERS,
        "MORNING_STAR": CandlestickPattern.MORNING_STAR,
        "HAMMER": CandlestickPattern.HAMMER,
        "ENGULFING": CandlestickPattern.BULLISH_ENGULFING,
        "PIERCING": CandlestickPattern.PIERCING_LINE,
        "INVERTED_HAMMER": CandlestickPattern.INVERTED_HAMMER,
        "HARAMI": CandlestickPattern.BULLISH_HARAMI
    }
    
    main_pattern = CandlestickPattern.NONE
    for sig in signals_found:
        if sig in pattern_priority:
            main_pattern = pattern_priority[sig]
            break
    
    if strength >= 80 and confirmations >= 3:
        entry_quality = "excellent"
    elif strength >= 65 and confirmations >= 2:
        entry_quality = "good"
    elif strength >= 50 and confirmations >= 1:
        entry_quality = "moderate"
    else:
        entry_quality = "weak"
    
    return CandlestickSignal(
        pattern=main_pattern,
        strength=min(100, strength),
        confirmation=confirmations >= 2,
        description=f"{' + '.join(signals_found)} ({confirmations}x Confirm)",
        entry_quality=entry_quality
    )

# ============================== Analyse Funktionen ==============================

def analyze_structure(df: Optional[pd.DataFrame], symbol: Optional[str] = None) -> Dict[str, Any]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return _default_structure_result()
    required_cols = ['High', 'Low', 'Close']
    for col in required_cols:
        if col not in df.columns:
            return _default_structure_result()
    if len(df) < 10:
        return _default_structure_result()
    df_clean = df[['High', 'Low', 'Close']].dropna()
    if len(df_clean) < 10:
        return _default_structure_result()
    if symbol:
        cache_key = f"structure_{symbol}"
        cached = structure_cache.get(cache_key, 300)
        if cached:
            return cached
    try:
        highs = df_clean['High'].values
        lows = df_clean['Low'].values
        swing_highs = []
        swing_lows = []
        for i in range(2, len(highs)-2):
            if not all(np.isfinite([highs[i], highs[i-1], highs[i-2], highs[i+1], highs[i+2]])):
                continue
            if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and 
                highs[i] > highs[i+1] and highs[i] > highs[i+2]):
                swing_highs.append((i, float(highs[i])))
            if not all(np.isfinite([lows[i], lows[i-1], lows[i-2], lows[i+1], lows[i+2]])):
                continue
            if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and 
                lows[i] < lows[i+1] and lows[i] < lows[i+2]):
                swing_lows.append((i, float(lows[i])))
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            hh = swing_highs[-1][1] > swing_highs[-2][1]
            hl = swing_lows[-1][1] > swing_lows[-2][1]
            slope = 0.0
            if len(swing_highs) >= 3:
                x = [float(swing_highs[-3][0]), float(swing_highs[-2][0]), float(swing_highs[-1][0])]
                y = [float(swing_highs[-3][1]), float(swing_highs[-2][1]), float(swing_highs[-1][1])]
                if all(np.isfinite(val) for val in x + y):
                    n = len(x)
                    x_mean = sum(x) / n
                    y_mean = sum(y) / n
                    numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
                    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
                    if denominator != 0 and np.isfinite(denominator):
                        slope = numerator / denominator
            result = {
                'higher_highs': bool(hh),
                'higher_lows': bool(hl),
                'trend_slope': float(slope) if np.isfinite(slope) else 0.0,
                'structure_intact': bool(hh and hl),
                'last_swing_low': float(swing_lows[-1][1]),
                'last_swing_high': float(swing_highs[-1][1])
            }
        else:
            result = _default_structure_result(df_clean)
        if symbol:
            structure_cache.set(cache_key, result)
        return result
    except:
        return _default_structure_result()

def _default_structure_result(df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    if df is not None and not df.empty and 'Low' in df.columns and 'High' in df.columns:
        try:
            last_low = float(df['Low'].tail(5).min()) if len(df['Low']) > 0 else 0.0
            last_high = float(df['High'].tail(20).max()) if len(df['High']) > 0 else 0.0
            return {
                'structure_intact': False,
                'higher_highs': False,
                'higher_lows': False,
                'trend_slope': 0.0,
                'last_swing_low': last_low,
                'last_swing_high': last_high
            }
        except:
            pass
    return {
        'structure_intact': False,
        'higher_highs': False,
        'higher_lows': False,
        'trend_slope': 0.0,
        'last_swing_low': 0.0,
        'last_swing_high': 0.0
    }

# ============================== Alpha Vantage Smart Fetch ==============================

def get_alpha_vantage_smart(symbol: str) -> Tuple[Optional[Dict], bool]:
    cache_key = f"av_fund_{symbol}"
    cached = fundamentals_cache.get(cache_key, AUTO_REFRESH_INTERVAL)
    if cached:
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['cache_hits'] = stats.get('cache_hits', 0) + 1
            st.session_state['api_stats'] = stats
        return cached, True
    if not alpha_manager.keys:
        return None, False
    attempts = 0
    max_attempts = len(alpha_manager.keys)
    while attempts < max_attempts:
        if alpha_manager.can_call():
            current_key = alpha_manager.get_current_key()
            if not current_key:
                break
            url = "https://www.alphavantage.co/query"
            params = {
                'function': 'OVERVIEW',
                'symbol': symbol,
                'apikey': current_key
            }
            try:
                response = safe_requests_get(url, params, timeout=15)
                if response:
                    data = response.json()
                    if 'Note' in data or 'Information' in data:
                        alpha_manager.limiters[alpha_manager.current_index]['exhausted'] = True
                        alpha_manager.rotate_key()
                        attempts += 1
                        time.sleep(1)
                        continue
                    if 'Error Message' in data:
                        return None, False
                    if 'Symbol' in data and data['Symbol']:
                        result = {
                            'pe_ratio': float(data.get('PERatio', 0)) if data.get('PERatio') and data.get('PERatio') not in ['None', '0'] else None,
                            'sector': data.get('Sector', ''),
                            'industry': data.get('Industry', ''),
                            'market_cap': int(float(data.get('MarketCapitalization', 0))) if data.get('MarketCapitalization') and data.get('MarketCapitalization') not in ['None', '0'] else 0
                        }
                        fundamentals_cache.set(cache_key, result)
                        alpha_manager.record_call()
                        return result, False
                    else:
                        return None, False
                else:
                    alpha_manager.rotate_key()
                    attempts += 1
                    time.sleep(0.5)
            except:
                alpha_manager.rotate_key()
                attempts += 1
                time.sleep(0.5)
        else:
            alpha_manager.rotate_key()
            attempts += 1
    return None, False

# ============================== Gemini AI Integration ==============================

def get_gemini_entry_analysis(item_data: dict) -> str:
    if "gemini" not in st.secrets or "api_key" not in st.secrets["gemini"]:
        return "⚠️ Gemini API-Key fehlt in den Secrets. Bitte eintragen."
        
    client = genai.Client(api_key=st.secrets["gemini"]["api_key"])
    
    news_list = item_data.get('news', [])
    news_title = news_list[0].get('title', 'Keine relevanten News') if news_list else 'Keine relevanten News'
    
    candle = item_data.get('candlestick')
    candle_desc = candle.description if candle else 'Kein Signal'
    candle_quality = candle.entry_quality if candle else 'weak'
    
    prompt = f"""
    Du bist ein professioneller Daytrader. Analysiere folgendes Setup für einen kurzfristigen Long-Einstieg:
    
    Ticker: {item_data['symbol']}
    Preis: ${item_data['price']:.2f}
    Pullback: -{item_data['pullback_pct']*100:.1f}% vom Hoch
    Geplanter Stop Loss: ${item_data['stop_loss']:.2f}
    Geplantes Target: ${item_data['target']:.2f}
    Rel. Volumen (RVol): {item_data['rvol']:.1f}x
    Candlestick-Signal: {candle_desc}
    Entry Quality: {candle_quality}
    News Catalyst: {news_title}

    Gib mir eine extrem präzise Einschätzung (max. 3 Sätze): 
    1. Wie bewertest du das CRV (R:R) und die Volatilität?
    2. Ist das Candlestick-Signal überzeugend für einen Einstieg heute?
    3. Konkreter Entry-Trigger: Limit Order, Market Order oder warten?
    """
    
    try:
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        return response.text.strip()
    except Exception as e:
        return f"❌ Gemini API Fehler: {str(e)}"

# ============================== ThreadPool Scanner ==============================

class ThreadPoolBullScanner:
    """
    ThreadPool-basierter Scanner mit BALANCIERTEN Filtern
    """
    
    def __init__(self, max_workers: int = 4, min_delay: float = 1.0):
        self.max_workers = max_workers
        self.min_delay = min_delay
        self._yahoo_lock = threading.Lock()
        self._last_yahoo_call = 0.0
        self._yahoo_cache = SmartCache()
        
    def _fetch_yahoo_with_rate_limit(self, symbol: str) -> Optional[pd.DataFrame]:
        cache_key = f"yh_{symbol}"
        cached = self._yahoo_cache.get(cache_key, 300)
        if cached is not None:
            return cached
        
        with self._yahoo_lock:
            now = time.time()
            time_since_last = now - self._last_yahoo_call
            if time_since_last < self.min_delay:
                time.sleep(self.min_delay - time_since_last)
            self._last_yahoo_call = time.time()
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = yf.Ticker(symbol).history(period='3mo', interval='1d')
                
                if not df.empty:
                    stats = st.session_state.get('api_stats', {})
                    stats['yahoo'] = stats.get('yahoo', 0) + 1
                    st.session_state['api_stats'] = stats
                    
                    self._yahoo_cache.set(cache_key, df)
                    return df
                break
                
            except Exception as e:
                if attempt < max_retries - 1:
                    sleep_time = 2 ** attempt
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Yahoo Fehler für {symbol}: {e}")
            
        return None
    
    def analyze_single_symbol(self, symbol: str, tier: int, total: int) -> Optional[Dict[str, Any]]:
        debug_info = {'symbol': symbol, 'tier': tier, 'errors': [], 'checks': {}}
        
        if tier > 30:
            time.sleep(0.5)
        if tier > 60:
            time.sleep(1.0)
        
        df = self._fetch_yahoo_with_rate_limit(symbol)
        if df is None or df.empty:
            debug_info['errors'].append("Yahoo Fehler / Empty Data")
            _log_scan_debug(debug_info)
            return None
        if len(df) < 15:
            debug_info['errors'].append(f"Zu wenig Daten: {len(df)}")
            _log_scan_debug(debug_info)
            return None
        
        df_clean = df.dropna()
        if len(df_clean) < 10:
            debug_info['errors'].append("Zu wenig gültige Daten")
            _log_scan_debug(debug_info)
            return None
        
        current_price = float(df_clean['Close'].iloc[-1])
        if not np.isfinite(current_price) or current_price <= 0:
            debug_info['errors'].append("Ungültiger Preis")
            _log_scan_debug(debug_info)
            return None
        
        lookback = min(60, len(df_clean)-5)
        recent = df_clean.tail(lookback)
        recent_high = float(recent['High'].max())
        if not np.isfinite(recent_high) or recent_high <= 0:
            debug_info['errors'].append("Kein High")
            _log_scan_debug(debug_info)
            return None
        
        pullback_pct = (recent_high - current_price) / recent_high
        if pullback_pct < MIN_PULLBACK_PERCENT or pullback_pct > MAX_PULLBACK_PERCENT:
            debug_info['errors'].append(f"Pullback {pullback_pct:.2%} außerhalb Grenzen ({MIN_PULLBACK_PERCENT:.0%}-{MAX_PULLBACK_PERCENT:.0%})")
            _log_scan_debug(debug_info)
            return None
        
        structure = analyze_structure(df_clean, symbol)
        debug_info['checks']['structure'] = structure.get('structure_intact', False)
        debug_info['checks']['higher_lows'] = structure.get('higher_lows', False)
        
        # 🔥 GEÄNDERT: Structure-Filter lockerer
        # ALT: if not structure.get('structure_intact', False) and not structure.get('higher_lows', False):
        # NEU: Nur prüfen ob überhaupt ein Trend da ist (Higher Lows ODER Higher Highs)
        has_bullish_structure = (
            structure.get('structure_intact', False) or 
            structure.get('higher_lows', False) or
            structure.get('higher_highs', False)
        )
        
        if not has_bullish_structure:
            debug_info['errors'].append("Kein bullischer Trend (weder HH noch HL)")
            _log_scan_debug(debug_info)
            return None
        
        last_swing_low = structure.get('last_swing_low')
        if last_swing_low is None or not np.isfinite(last_swing_low) or last_swing_low <= 0:
            debug_info['errors'].append("Kein Swing Low")
            _log_scan_debug(debug_info)
            return None
        
        if current_price < last_swing_low * 0.85:  # 🔥 GEÄNDERT: Von 0.90 auf 0.85 (15% statt 10% Puffer)
            debug_info['errors'].append("Preis zu weit unter Swing Low")
            _log_scan_debug(debug_info)
            return None
        
        # === CANDLESTICK ANALYSE ===
        candlestick = analyze_candlestick(df_clean, last_swing_low, recent_high)
        debug_info['checks']['candlestick_pattern'] = candlestick.pattern.value
        debug_info['checks']['candlestick_strength'] = candlestick.strength
        debug_info['checks']['candlestick_quality'] = candlestick.entry_quality
        
        # 🔥 GEÄNDERT: Candlestick ist BONUS, nicht Pflicht!
        hard_filter_active = st.session_state.get('hard_filter_active', False)
        
        candlestick_penalty = 0
        if hard_filter_active and REQUIRE_CANDLESTICK_CONFIRM:
            # Nur im echten Hard Mode prüfen (der jetzt default OFF ist)
            if candlestick.strength < MIN_CANDLESTICK_STRENGTH:
                debug_info['errors'].append(f"Candlestick zu schwach: {candlestick.strength}/100 (min: {MIN_CANDLESTICK_STRENGTH})")
                _log_scan_debug(debug_info)
                return None
        
        # === SCORING (Candlestick als Bonus) ===
        score = 30  # 🔥 GEÄNDERT: Basis von 25 auf 30 erhöht
        
        # Trend-Struktur
        if structure.get('structure_intact', False):
            score += 15
        elif structure.get('higher_lows', False):
            score += 12  # 🔥 GEÄNDERT: Von 10 auf 12
        elif structure.get('higher_highs', False):
            score += 8   # 🔥 NEU: Auch HH allein gibt Punkte
        
        trend_slope = structure.get('trend_slope', 0)
        if trend_slope is not None and np.isfinite(trend_slope) and trend_slope > 0.005:
            score += 5
        
        # Volumen
        avg_vol = df_clean['Volume'].mean()
        current_vol = df_clean['Volume'].iloc[-1]
        rvol = current_vol / avg_vol if avg_vol > 0 else 1.0
        if rvol > 2:
            score += 20
        elif rvol > 1.0:
            score += 10
        
        # Support
        support_dist = (current_price - last_swing_low) / current_price if current_price > 0 else 1.0
        if support_dist < 0.03:
            score += 15
        elif support_dist < 0.08:
            score += 8
        
        # 🔥 Candlestick Bonus (statt Pflicht)
        if candlestick.strength >= MIN_CANDLESTICK_STRENGTH:
            score += candlestick.strength // 4  # 0-25 Punkte extra (statt //5)
            if candlestick.confirmation:
                score += 10
        else:
            # Kleiner Malus für fehlendes Candlestick, aber kein Ausschluss
            candlestick_penalty = 5
            score -= candlestick_penalty
        
        # News
        news, sources, cached_news = analyze_news_tiered(symbol, tier, score)
        if news:
            score += news[0]['score']
        
        # Fundamentals
        fundamentals, fund_cached = None, False
        if score > 50 and tier <= 10:  # 🔥 GEÄNDERT: Von 55 auf 50
            fundamentals, fund_cached = get_alpha_vantage_smart(symbol)
        
        pe_ratio = None
        if fundamentals:
            pe_ratio = fundamentals.get('pe_ratio')
            if pe_ratio is not None and np.isfinite(pe_ratio):
                if pe_ratio < 15:
                    score += 8
                elif pe_ratio > 100:
                    score -= 5
        
        # ATR & Risk Management
        try:
            atr = float((df_clean['High'].rolling(14).max() - df_clean['Low'].rolling(14).min()).mean())
            if not np.isfinite(atr) or atr <= 0:
                atr = current_price * 0.02
        except:
            atr = current_price * 0.02
        
        stop_loss = max(last_swing_low * 0.97, current_price - (2*atr))
        target = min(recent_high * 0.97, current_price + (current_price - stop_loss)*2)
        if stop_loss <= 0 or stop_loss >= current_price or target <= current_price:
            debug_info['errors'].append("Ungültige SL/TP")
            _log_scan_debug(debug_info)
            return None
        
        rr_ratio = (target - current_price) / (current_price - stop_loss) if (current_price - stop_loss) > 0 else 0
        if rr_ratio < 0.8:  # 🔥 GEÄNDERT: Von 1.0 auf 0.8 (auch kleinere R:R erlauben)
            debug_info['errors'].append(f"R:R {rr_ratio:.2f} zu niedrig (min: 0.8)")
            _log_scan_debug(debug_info)
            return None
        
        # Threshold prüfen
        effective_threshold = MIN_SCORE_THRESHOLD
        if score < effective_threshold:
            debug_info['errors'].append(f"Score {score} < {effective_threshold} (Threshold)")
            _log_scan_debug(debug_info)
            return None
        
        # Reasons
        reasons = [f"📉 -{pullback_pct:.1%}"]
        if structure.get('structure_intact', False):
            reasons.append("📈 Trend stark")
        elif structure.get('higher_lows', False):
            reasons.append("📈 HL ok")  # 🔥 Kürzer
        elif structure.get('higher_highs', False):
            reasons.append("📈 HH ok")  # 🔥 NEU
        
        if rvol > 1.0:
            reasons.append(f"⚡ Vol {rvol:.1f}x")
        if support_dist < 0.03:
            reasons.append("🎯 Support nah")
        
        # Candlestick in Reasons
        if candlestick.strength >= MIN_CANDLESTICK_STRENGTH:
            reasons.append(f"🕯️ {candlestick.pattern.value}")
            if candlestick.confirmation:
                reasons.append("✅ Confirm")
        else:
            reasons.append("⚠️ Kein Candlestick")  # 🔥 Transparent zeigen
        
        if news:
            reasons.append(f"📰 {news[0]['source']}")
        if pe_ratio is not None:
            reasons.append(f"{'💰' if pe_ratio<15 else '📊'} PE {pe_ratio:.1f}")
        
        source = get_symbol_source(symbol)
        
        return {
            'symbol': symbol,
            'tier': tier,
            'score': min(100, int(score)),
            'price': current_price,
            'pullback_pct': pullback_pct,
            'recent_high': recent_high,
            'stop_loss': stop_loss,
            'target': target,
            'rr_ratio': rr_ratio,
            'rvol': rvol,
            'reasons': reasons,
            'news': news,
            'pe_ratio': pe_ratio,
            'api_sources': list(set([s for s in sources] + (['AV'] if fundamentals else []))),
            'from_cache': cached_news or fund_cached,
            'source': source,
            'candlestick': candlestick,
            'has_candlestick_confirm': candlestick.strength >= MIN_CANDLESTICK_STRENGTH,
            'structure_intact': structure.get('structure_intact', False),  # 🔥 NEU: Für Anzeige
        }
    
    def scan_batch(self, symbols: List[Tuple[str, SourceType]], progress_callback=None) -> List[Dict]:
        results = []
        completed = 0
        error_count = 0
        success_count = 0
        candlestick_filtered = 0
        
        try:
            ctx = get_script_run_ctx()
        except:
            ctx = None
            
        def wrapper(sym, tier, total):
            if ctx:
                add_script_run_ctx(threading.current_thread(), ctx)
            return self.analyze_single_symbol(sym, tier, total)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_symbol = {
                executor.submit(wrapper, sym, i+1, len(symbols)): (sym, src) 
                for i, (sym, src) in enumerate(symbols)
            }
            
            for future in as_completed(future_to_symbol):
                symbol, source = future_to_symbol[future]
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                        success_count += 1
                    else:
                        debug = st.session_state.get('scan_debug', [])
                        if debug and debug[-1].get('errors'):
                            if 'Candlestick' in str(debug[-1]['errors']):
                                candlestick_filtered += 1
                        error_count += 1
                except Exception as e:
                    logger.error(f"Fehler bei {symbol}: {e}")
                    error_count += 1
                
                completed += 1
                if progress_callback:
                    progress_callback(completed, len(symbols), success_count, error_count, candlestick_filtered, symbol, source)
        
        return sorted(results, key=lambda x: (x['score'], x['pullback_pct']), reverse=True)

def _log_scan_debug(debug_info: Dict):
    scan_debug = st.session_state.get('scan_debug', [])
    scan_debug.append(debug_info)
    st.session_state['scan_debug'] = scan_debug[-100:]

# ============================== Alert Management ==============================

def should_send_alert(symbol: str, current_price: float, current_score: int) -> bool:
    sent_alerts = st.session_state.get('sent_alerts', {})
    now = datetime.now()
    if symbol not in sent_alerts:
        return True
    last_alert = sent_alerts[symbol]
    time_diff = (now - last_alert['timestamp']).total_seconds() / 60
    if time_diff < ALERT_COOLDOWN_MINUTES:
        return False
    price_change = abs(current_price - last_alert['price']) / last_alert['price']
    score_change = current_score - last_alert['score']
    if price_change < 0.02 and score_change < 10:
        return False
    return True

def record_alert(symbol: str, price: float, score: int, setup_type: str):
    st.session_state['sent_alerts'][symbol] = {
        'timestamp': datetime.now(),
        'price': price,
        'score': score,
        'setup_type': setup_type
    }
    st.session_state['alert_history'].append({
        'timestamp': datetime.now(),
        'symbol': symbol,
        'price': price,
        'score': score,
        'setup_type': setup_type
    })
    st.session_state['alert_history'] = st.session_state['alert_history'][-20:]

# ============================== Telegram Alert ==============================

def send_telegram_alert(symbol: str, price: float, pullback_pct: float, news_item: Optional[Dict], 
                       setup_type: str, pe_ratio: Optional[float] = None, api_sources: Optional[List] = None, 
                       tier: Optional[int] = None, source: Optional[SourceType] = None,
                       candlestick: Optional[CandlestickSignal] = None) -> bool:
    if not TELEGRAM_BOT_TOKEN or len(TELEGRAM_BOT_TOKEN) < 10:
        return False
    news_title = news_item.get('title','')[:40] + '...' if news_item else 'Keine News'
    news_url = news_item.get('url','') if news_item else f'https://finance.yahoo.com/quote/{symbol}'
    emoji = "🕯️" if candlestick and candlestick.strength >= 80 else "🏆" if setup_type == "GOLD" else "🐂"
    
    source_emoji = {
        SourceType.WATCHLIST: '📋',
        SourceType.GAINERS: '🚀',
        SourceType.MOST_ACTIVE: '🔥'
    }.get(source, '📊')
    
    pe_info = f"\n📊 P/E: {pe_ratio:.1f}" if pe_ratio else ""
    api_info = f"\n📡 {','.join(api_sources)}" if api_sources else ""
    tier_info = f"\n🎯 Tier {tier}" if tier else ""
    source_info = f"\n{source_emoji} Quelle: {source.value if source else 'unknown'}" if source else ""
    
    candle_info = ""
    if candlestick and candlestick.pattern != CandlestickPattern.NONE:
        candle_info = f"\n🕯️ {candlestick.pattern.value.upper()} ({candlestick.strength}/100)"
        if candlestick.confirmation:
            candle_info += " ✅Confirm"
    
    msg = f"""{emoji} <b>{setup_type}: {symbol}</b> {emoji}
📉 Pullback: <b>-{pullback_pct:.1f}%</b>
💵 Preis: ${price:.2f}{pe_info}{api_info}{tier_info}{source_info}{candle_info}
📰 {news_title}
👉 <a href='{news_url}'>News</a> | <a href='https://www.tradingview.com/chart/?symbol={symbol}'>Chart</a>"""
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        requests.post(url, data=payload, timeout=5).raise_for_status()
        return True
    except:
        return False

# ============================== Karten HTML ==============================

def render_card_html(item: Dict) -> str:
    sym = item['symbol']
    price = item['price']
    pullback = item['pullback_pct']
    sl = item['stop_loss']
    target = item['target']
    rr = item['rr_ratio']
    rvol = item['rvol']
    score = item['score']
    reasons = ' | '.join(item['reasons'][:4])
    news_item = item.get('news', [{}])[0] if item.get('news') else None
    news_title = news_item['title'][:40] + '...' if news_item else 'Keine News'
    news_url = news_item['url'] if news_item else f'https://finance.yahoo.com/quote/{sym}'
    tv_url = f'https://www.tradingview.com/chart/?symbol={sym}'
    tier = item.get('tier', '-')
    source = item.get('source', SourceType.UNKNOWN)
    apis = item.get('api_sources', [])
    cached = item.get('from_cache', False)
    candlestick = item.get('candlestick', None)
    structure_intact = item.get('structure_intact', False)  # 🔥 NEU
    
    has_candle = candlestick and candlestick.pattern != CandlestickPattern.NONE
    pullback_color = '#ff6b6b' if pullback > 0.15 else '#ffa502'
    conf_color = '#9933ff' if score > 85 else '#FFD700' if score > 70 else '#00FF00'
    tier_html = f'<div class="tier-badge">T{tier}</div>'
    api_html = ''.join([f'<div class="tier-badge">{a}</div>' for a in apis])
    cache_html = '<div class="cache-badge">CACHE</div>' if cached else ''
    
    # 🔥 Structure Badge
    structure_html = f'<div class="structure-badge{" weak" if not structure_intact else ""}">{"📈 HH+HL" if structure_intact else "📈 HL"}</div>'
    
    # Candlestick Badge
    candle_html = ''
    if has_candle:
        candle_color = '#00FF00' if candlestick.strength >= 80 else '#FFD700' if candlestick.strength >= 65 else '#ff6b6b'
        candle_html = f'<div class="candlestick-badge" style="background: {candle_color};">{candlestick.pattern.value.upper()} {candlestick.strength}</div>'
    else:
        candle_html = f'<div class="candlestick-badge weak">NO CANDLE</div>'
    
    source_badges = {
        SourceType.WATCHLIST: '<div class="tier-badge" style="background:#2d5a2d;">📋 WL</div>',
        SourceType.GAINERS: '<div class="mover-badge">🚀 GAINER</div>',
        SourceType.MOST_ACTIVE: '<div class="mover-badge" style="background:linear-gradient(90deg, #FFD700, #FFA500);">🔥 ACTIVE</div>'
    }
    source_html = source_badges.get(source, '')
    
    quality_html = ''
    if has_candle:
        quality_colors = {'excellent': '#00FF00', 'good': '#90EE90', 'moderate': '#FFD700', 'weak': '#ff6b6b'}
        q_color = quality_colors.get(candlestick.entry_quality, '#888')
        quality_html = f'<div style="font-size: 0.7rem; color: {q_color}; margin: 3px 0;">Quality: {candlestick.entry_quality.upper()}</div>'
    
    return f"""
    <div class="bull-card source-{source.value if hasattr(source, 'value') else str(source)}">
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <h3 style="margin:0;">🐂 {sym}</h3>
            {source_html}
        </div>
        <div class="pullback-badge" style="background: {pullback_color};">-{pullback:.1%}</div>
        <div style="margin: 5px 0;">{tier_html}{api_html}{cache_html}{structure_html}</div>
        {candle_html}
        {quality_html}
        <div class="price">${price:.2f}</div>
        <div style="font-size: 0.8rem; color: #aaa; margin: 5px 0;">{reasons}</div>
        <div style="margin: 8px 0;">
            <span class="stop-loss">SL: ${sl:.2f}</span>
            <span class="target">TP: ${target:.2f}</span>
        </div>
        <div style="font-size: 0.8rem; color: {conf_color}; margin: 5px 0;">Score: {score}/100</div>
        <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
        <div style="font-size: 0.75rem; color: #888; margin: 5px 0;">R:R {rr:.1f}x | Vol {rvol:.1f}x</div>
        <a href="{news_url}" target="_blank" class="news-link-btn">📰 {news_title}</a>
        <a href="{tv_url}" target="_blank" class="btn-link">📈 TradingView</a>
    </div>
    """

def render_card(item: Dict, container):
    html = render_card_html(item)
    with container:
        st.markdown(html, unsafe_allow_html=True)
        
        unique_btn_key = f"gemini_btn_{item['symbol']}_{int(time.time()*1000)}"  # 🔥 Eindeutiger Key
        if st.button(f"🤖 Gemini Check", key=unique_btn_key):
            with st.spinner(f"Gemini analysiert {item['symbol']}..."):
                analysis = get_gemini_entry_analysis(item)
                st.info(analysis, icon="💡")

# ============================== Main UI ==============================

def main():
    clock = get_market_clock()

    if clock.get('is_holiday'):
        st.markdown(f"""
        <div class="holiday-banner">
            🎌 US MARKET HOLIDAY 🎌<br>
            <small>Markt ist geschlossen. Daten können unvollständig sein.</small>
        </div>
        """, unsafe_allow_html=True)

    st.markdown(f"""
    <div class="market-clock-container">
        <div class="market-time">{clock['time']}</div>
        <div style="margin: 10px 0;">
            <span class="market-status" style="background: {clock['color']};">{clock['status']}</span>
        </div>
        <div class="market-countdown">{clock['countdown']}</div>
        {f'<div class="market-progress"><div class="market-progress-bar" style="width: {clock["progress"]*100}%;"></div></div>' if clock['is_open'] else ''}
    </div>
    """, unsafe_allow_html=True)

    # Automatisches Refresh
    if st.session_state.get('auto_refresh'):
        last = st.session_state.get('last_auto_refresh', 0)
        last_movers = st.session_state.get('last_movers_check', 0)
        now = time.time()
        
        if now - last_movers >= AUTO_REFRESH_INTERVAL:
            st.session_state['last_movers_check'] = now
            try:
                movers, source = fetch_yahoo_movers()
                st.session_state['top_movers_cache'] = movers
                st.session_state['movers_source'] = source
                combined = set(st.session_state['watchlist'])
                for category, symbols in movers.items():
                    combined.update(symbols)
                st.session_state['combined_universe'] = combined
            except Exception as e:
                logger.error(f"Fehler beim Movers-Update: {e}")
        
        if now - last >= AUTO_REFRESH_INTERVAL:
            st.session_state['last_auto_refresh'] = now
            st.session_state['refresh_count'] = st.session_state.get('refresh_count', 0) + 1
            st.rerun()

    # Sidebar
    with st.sidebar:
        st.header("🤖 Autopilot (24/7)")
        auto_pilot = st.toggle("Autopilot aktivieren", value=st.session_state.get('auto_refresh', False), help="Scannt automatisch alle 30 Minuten")
        st.session_state['auto_refresh'] = auto_pilot
        st.divider()

        st.header("🎛️ Filter Einstellungen")
        
        # 🔥 Hard Filter Toggle - jetzt default OFF
        hard_filter = st.toggle("🔥 Hard Mode (Nur Candlestick)", 
                                value=st.session_state.get('hard_filter_active', False),
                                help="Nur Setups mit starken Candlestick-Signalen (60+ Punkte)")
        st.session_state['hard_filter_active'] = hard_filter
        
        if hard_filter:
            st.markdown("""
            <div class="filter-active">
                🔥 HARD MODE AKTIV<br>
                <small>Min. Candlestick: 40/100<br>
                Nur excellent/good Quality</small>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.info("✅ Balanced Mode: Candlestick als Bonus, nicht Pflicht")
        
        # 🔥 NEU: Manuelle Filter-Anpassung
        with st.expander("⚙️ Filter feinjustieren"):
            global MIN_PULLBACK_PERCENT, MAX_PULLBACK_PERCENT, MIN_SCORE_THRESHOLD
            min_pull = st.slider("Min. Pullback %", 1, 10, int(MIN_PULLBACK_PERCENT*100)) / 100
            max_pull = st.slider("Max. Pullback %", 30, 80, int(MAX_PULLBACK_PERCENT*100)) / 100
            min_score = st.slider("Min. Score", 40, 80, MIN_SCORE_THRESHOLD)
            
            if st.button("Filter anwenden"):
                MIN_PULLBACK_PERCENT = min_pull
                MAX_PULLBACK_PERCENT = max_pull
                MIN_SCORE_THRESHOLD = min_score
                st.success(f"Filter aktualisiert: {min_pull:.0%}-{max_pull:.0%}, Score {min_score}+")
        
        st.divider()
        
        st.header("📡 API Status")
        stats = st.session_state.get('api_stats', {'yahoo':0,'finnhub':0,'alpha_vantage':0,'cache_hits':0,'alpha_rotation_count':0})
        
        yahoo_calls = stats.get('yahoo', 0)
        st.markdown(f"""
        <div class="info-box">
        🟢 <b>Yahoo Finance</b><br>
        Kursdaten: {yahoo_calls} Calls<br>
        <small>Unbegrenzt kostenlos</small>
        </div>
        """, unsafe_allow_html=True)
        
        fh_status = "🟢" if finnhub_limiter.can_call() else "🔴"
        st.markdown(f"""
        <div class="api-stat">
            <div style="display:flex; justify-content:space-between;">
                <span>Finnhub News</span>
                <span>{fh_status} {finnhub_limiter.get_status()}/60 pro Minute</span>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        alpha_status_list = alpha_manager.get_status()
        all_alpha_exhausted = all(s['exhausted'] for s in alpha_status_list)
        if all_alpha_exhausted:
            st.markdown("""
            <div class="error-box">
            ⚠️ <b>Alpha Vantage erschöpft!</b><br>
            Limit: 25/Tag pro Key<br>
            Morgen wieder verfügbar
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<div style='margin:10px 0;'><b>Alpha Vantage (25/Tag):</b></div>", unsafe_allow_html=True)
        for status in alpha_status_list:
            cls = "key-active" if status['active'] else "key-exhausted" if status['exhausted'] else ""
            indicator = "▶️" if status['active'] else "✅" if not status['exhausted'] else "❌"
            st.markdown(f"""
            <div class="key-indicator {cls}">
                {indicator} Key {status['index']+1}: {status['calls_today']}/25
            </div>
            """, unsafe_allow_html=True)
        
        rotations = stats.get('alpha_rotation_count',0)
        cache_hits = stats.get('cache_hits',0)
        st.markdown(f'<div style="font-size:0.8rem;margin:5px 0;">🔄 Rotationen: {rotations}</div>', unsafe_allow_html=True)
        st.markdown(f'<div style="font-size:0.8rem;margin:5px 0;">📦 Cache Hits: {cache_hits}</div>', unsafe_allow_html=True)

        st.divider()
        st.header("🚀 Top Movers")
        movers = st.session_state.get('top_movers_cache', {})
        movers_source = st.session_state.get('movers_source', 'fallback')
        
        if movers:
            cols = st.columns(2)
            with cols[0]:
                st.metric("Gainers", len(movers.get('gainers', [])))
            with cols[1]:
                st.metric("Active", len(movers.get('most_active', [])))
            
            source_color = "🟢" if movers_source == "yahoo" else "🟡" if movers_source == "cache" else "🔴"
            st.caption(f"{source_color} Quelle: {movers_source.upper()}")
            
            last_movers_check = st.session_state.get('last_movers_check', 0)
            if last_movers_check:
                ago = int((time.time() - last_movers_check) / 60)
                st.caption(f"Letztes Update: vor {ago} Min")
        else:
            st.info("Noch keine Movers geladen")
        
        st.divider()
        st.header("🧪 API Tests")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Test Yahoo", use_container_width=True):
                try:
                    time.sleep(1)
                    data = yf.Ticker("AAPL").history(period="5d")
                    if not data.empty:
                        st.success(f"✅ Yahoo OK! {len(data)} Tage")
                        stats = st.session_state.get('api_stats', {})
                        stats['yahoo'] = stats.get('yahoo', 0) + 1
                        st.session_state['api_stats'] = stats
                    else:
                        st.error("❌ Keine Daten")
                except Exception as e:
                    st.error(f"❌ Fehler: {str(e)[:50]}")
        with col2:
            if st.button("Test Finnhub", use_container_width=True):
                news, cached = get_finnhub_news_smart("TSLA")
                if news:
                    st.success(f"✅ Finnhub OK! {len(news)} News")
                else:
                    st.error("❌ Keine News")
        
        if st.button("🔄 Movers jetzt laden", use_container_width=True):
            with st.spinner("Lade Movers..."):
                try:
                    movers, source = fetch_yahoo_movers()
                    st.session_state['top_movers_cache'] = movers
                    st.session_state['movers_source'] = source
                    combined = set(st.session_state['watchlist'])
                    for category, symbols in movers.items():
                        combined.update(symbols)
                    st.session_state['combined_universe'] = combined
                    st.success(f"✅ {len(combined)} Symbole ({source})")
                    st.rerun()
                except Exception as e:
                    st.error(f"❌ Fehler: {str(e)[:100]}")
        
        st.divider()
        st.header("🔍 Manuelle Abfrage")
        manual_symbol = st.text_input("Symbol:", placeholder="z.B. NVDA", key="manual").upper()
        
        # 🔥 Debug-Option für manuelle Abfrage
        show_debug = st.checkbox("Debug-Info anzeigen", value=True)
        
        if st.button("📊 Analyse starten") and manual_symbol:
            # 🔥 Temporär Hard Filter deaktivieren für manuellen Test
            old_hard_filter = st.session_state.get('hard_filter_active', False)
            st.session_state['hard_filter_active'] = False
            
            with st.spinner(f"Analysiere {manual_symbol}..."):
                try:
                    time.sleep(1)
                    scanner = ThreadPoolBullScanner(max_workers=1, min_delay=0.5)
                    result = scanner.analyze_single_symbol(manual_symbol, 1, 1)
                    if result:
                        st.success(f"✅ Setup gefunden für {manual_symbol}!")
                        
                        # Alle Details anzeigen
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Score:** {result['score']}/100")
                            st.write(f"**Price:** ${result['price']:.2f}")
                            st.write(f"**Pullback:** {result['pullback_pct']:.2%}")
                            st.write(f"**R:R:** {result['rr_ratio']:.1f}x")
                        with col2:
                            candle = result.get('candlestick')
                            if candle:
                                st.write(f"**Candlestick:** {candle.pattern.value}")
                                st.write(f"**Strength:** {candle.strength}/100")
                                st.write(f"**Quality:** {candle.entry_quality}")
                            else:
                                st.write("**Candlestick:** Kein Signal")
                            st.write(f"**Structure:** {'Intakt' if result.get('structure_intact') else 'Nur HL'}")
                        
                        st.write(f"**Reasons:** {' | '.join(result['reasons'])}")
                        
                        # 🔥 Wichtig: Zeige ob Hard Filter blockiert hätte
                        if old_hard_filter and candle and candle.strength < MIN_CANDLESTICK_STRENGTH:
                            st.warning(f"⚠️ Im Hard Mode wäre dieses Setup ausgeschlossen (Candlestick {candle.strength}/100)")
                    else:
                        st.error(f"❌ Kein Setup für {manual_symbol}")
                        
                        if show_debug:
                            scan_debug = st.session_state.get('scan_debug', [])
                            if scan_debug:
                                last = scan_debug[-1]
                                with st.expander("🔍 Debug Details"):
                                    st.write("**Checks:**")
                                    st.json(last.get('checks', {}))
                                    st.write("**Fehler:**")
                                    for err in last.get('errors', []):
                                        st.write(f"- {err}")
                                        
                                    # 🔥 Hilfreiche Tipps basierend auf Fehlern
                                    errors = last.get('errors', [])
                                    if any('Pullback' in e for e in errors):
                                        st.info("💡 Tipp: Pullback außerhalb 2%-70% Bereich. Prüfe ob Aktueller Preis nahe am Hoch ist.")
                                    if any('Candlestick' in e for e in errors):
                                        st.info("💡 Tipp: Kein Candlestick-Muster erkannt. Hard Filter deaktivieren um trotzdem zu sehen.")
                                    if any('Score' in e for e in errors):
                                        st.info("💡 Tipp: Score zu niedrig. In den Einstellungen den Min-Score Slider reduzieren.")
                finally:
                    # 🔥 Wiederherstellen
                    st.session_state['hard_filter_active'] = old_hard_filter
        
        if st.button("🔄 Stats zurücksetzen"):
            st.session_state['api_stats'] = {'yahoo':0,'finnhub':0,'alpha_vantage':0,'cache_hits':0,'alpha_rotation_count':0}
            st.session_state['scan_debug'] = []
            st.session_state['top_movers_cache'] = FALLBACK_MOVERS
            st.session_state['movers_source'] = 'fallback'
            st.session_state['last_movers_check'] = 0
            st.session_state['combined_universe'] = set(DEFAULT_WATCHLIST + [s for sublist in FALLBACK_MOVERS.values() for s in sublist])
            for i in alpha_manager.limiters:
                alpha_manager.limiters[i]['exhausted'] = False
                alpha_manager.limiters[i]['calls_today'] = 0
                alpha_manager.limiters[i]['calls_per_min'] = []
            st.success("Zurückgesetzt!")
            st.rerun()

    # Haupt-Scan Button
    scan_triggered = False
    col1, col2 = st.columns([3, 1])
    with col1:
        mode = "HARD" if st.session_state.get('hard_filter_active', False) else "BALANCED"
        if st.button(f'🚀 {mode} SCAN Starten', type="primary"):
            scan_triggered = True
    with col2:
        if st.button('⚡ Quick Scan (Soft)'):
            st.session_state['hard_filter_active'] = False
            scan_triggered = True

    # --- AUTOPILOT LOGIK ---
    if st.session_state.get('auto_refresh', False):
        last_scan = st.session_state.get('last_scan_time')
        if last_scan is None:
            scan_triggered = True
        else:
            elapsed_minutes = (datetime.now() - last_scan).total_seconds() / 60
            if elapsed_minutes >= 30:
                scan_triggered = True

    if scan_triggered:
        with st.spinner("🔍 Scanne mit balancierten Filtern..."):
            time.sleep(1)
            market_ctx = get_market_context()
            if market_ctx.get('market_closed'):
                st.warning("⚠️ Markt ist möglicherweise geschlossen.")
            
            universe, source = get_combined_universe()
            watchlist_count = len(st.session_state['watchlist'])
            movers_count = len(universe) - watchlist_count
            
            filter_mode = "🔥 HARD (Nur Candlestick)" if st.session_state.get('hard_filter_active', False) else "✅ BALANCED (Candlestick als Bonus)"
            st.info(f"📊 Modus: **{filter_mode}** | Scanne {len(universe)} Symbole ({watchlist_count} Watchlist + {movers_count} Movers)")
            
            # 🔥 Zeige aktuelle Filter-Einstellungen
            with st.expander("Aktive Filter"):
                st.write(f"- Pullback: {MIN_PULLBACK_PERCENT:.0%} - {MAX_PULLBACK_PERCENT:.0%}")
                st.write(f"- Min Score: {MIN_SCORE_THRESHOLD}")
                st.write(f"- Candlestick: {'Pflicht (60+)' if st.session_state.get('hard_filter_active') else 'Bonus (40+)'}")
            
            st.session_state['scan_debug'] = []
            
            scan_list = [(sym, get_symbol_source(sym)) for sym in universe]
            scan_list.sort(key=lambda x: 0 if x[1] == SourceType.WATCHLIST else 1)
            
            scanner = ThreadPoolBullScanner(max_workers=4, min_delay=1.0)
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            stats_text = st.empty()
            
            filtered_by_candlestick = [0]
            
            def update_progress(completed, total, success, errors, candle_filtered, current_sym, current_src):
                progress = completed / total
                progress_bar.progress(min(progress, 0.99))
                filtered_by_candlestick[0] = candle_filtered
                status_text.text(f"Analysiere: {current_sym} ({completed}/{total})")
                stats_text.markdown(f"✅ **{success}** Setups | ❌ {errors} Ausschlüsse | 🕯️ {candle_filtered} (Candlestick)")
            
            start_time = time.time()
            results = scanner.scan_batch(scan_list, update_progress)
            elapsed = time.time() - start_time
            
            progress_bar.empty()
            status_text.empty()
            stats_text.empty()
            
            st.session_state['scan_results'] = results
            st.session_state['last_scan_time'] = datetime.now()
            
            hard_filter_active = st.session_state.get('hard_filter_active', False)
            if hard_filter_active:
                st.success(f"✅ {len(results)} HARD-SETUPS in {elapsed:.1f}s gefunden")
            else:
                st.success(f"✅ {len(results)} BALANCED-SETUPS in {elapsed:.1f}s gefunden")
                if filtered_by_candlestick[0] > 0:
                    st.info(f"💡 {filtered_by_candlestick[0]} hätten durch Hard Filter ausgeschlossen werden")
            
            # Alerts
            alerts_sent = 0
            for item in results:
                if item['score'] > 75:
                    symbol = item['symbol']
                    price = item['price']
                    score = item['score']
                    if should_send_alert(symbol, price, score):
                        setup_type = "CATALYST" if (item.get('news') and item['news'][0].get('tier', 0) == 1) else "GOLD"
                        candle = item.get('candlestick')
                        success = send_telegram_alert(
                            symbol, price, item['pullback_pct'],
                            item['news'][0] if item.get('news') else None,
                            setup_type, item.get('pe_ratio'),
                            item.get('api_sources'), item.get('tier'),
                            item.get('source'), candle
                        )
                        if success:
                            record_alert(symbol, price, score, setup_type)
                            alerts_sent += 1
                            if alerts_sent <= 3:
                                st.toast(f"🚨 {setup_type} Alert: {symbol} @ ${price:.2f} (Score: {score})")

    # Ergebnisse anzeigen
    results = st.session_state.get('scan_results', [])
    if results:
        hard_filter_active = st.session_state.get('hard_filter_active', False)
        
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        with col1:
            mode_text = "🔥 HARD" if hard_filter_active else "✅ BALANCED"
            st.subheader(f"📊 Gefundene Setups: {len(results)} ({mode_text})")
        with col2:
            sources = {}
            for r in results:
                src = r.get('source', SourceType.UNKNOWN)
                src_val = src.value if hasattr(src, 'value') else str(src)
                sources[src_val] = sources.get(src_val, 0) + 1
            source_text = " | ".join([f"{k}: {v}" for k, v in sources.items()])
            st.caption(f"Quellen: {source_text}")
        with col3:
            candlestick_count = sum(1 for r in results if r.get('has_candlestick_confirm', False))
            st.caption(f"🕯️ Mit Candlestick: {candlestick_count}")
        with col4:
            if st.session_state.get('auto_refresh'):
                count = st.session_state.get('refresh_count', 0)
                st.markdown(f'<div style="background:#1a1a2e;padding:10px;border-radius:8px;border-left:4px solid #00FF00;">🔴 LIVE #{count}</div>', unsafe_allow_html=True)
            else:
                last_time = st.session_state.get('last_scan_time')
                if last_time:
                    st.caption(f"Letzter Scan: {last_time.strftime('%H:%M:%S')}")
        
        sent_alerts = st.session_state.get('sent_alerts', {})
        active_alerts = len([a for a in sent_alerts.values() if (datetime.now() - a['timestamp']).total_seconds() / 3600 < 24])
        st.info(f"📱 Aktive Alerts (24h): {active_alerts} | In Cooldown: {len(sent_alerts) - active_alerts}")
        
        stats = st.session_state.get('api_stats', {})
        api_summary = {}
        for r in results:
            for s in r.get('api_sources', []):
                api_summary[s] = api_summary.get(s, 0) + 1
        cache_count = sum(1 for r in results if r.get('from_cache'))
        
        cols = st.columns(4)
        cols[0].metric("Setups", len(results))
        cols[1].metric("Yahoo Calls", stats.get('yahoo', 0))
        cols[2].metric("Finnhub", stats.get('finnhub', 0))
        cols[3].metric("Alpha Vantage", stats.get('alpha_vantage', 0))
        
        # Candlestick-Statistik
        if results:
            quality_dist = {'excellent': 0, 'good': 0, 'moderate': 0, 'weak': 0}
            pattern_dist = {}
            for r in results:
                c = r.get('candlestick')
                if c:
                    quality_dist[c.entry_quality] = quality_dist.get(c.entry_quality, 0) + 1
                    pattern_dist[c.pattern.value] = pattern_dist.get(c.pattern.value, 0) + 1
            
            st.write("**🕯️ Candlestick-Qualität:**")
            q_cols = st.columns(4)
            for i, (q, count) in enumerate(quality_dist.items()):
                if count > 0:
                    q_cols[i].metric(q.upper(), count)
            
            st.write("**📊 Erkannte Muster:**")
            st.write(", ".join([f"{p}: {c}" for p, c in pattern_dist.items() if c > 0]))
        
        st.success(f"✅ APIs in Ergebnissen: {api_summary} | Cache: {cache_count}")
        
        # Ergebnis-Grid
        results_sorted = sorted(results, key=lambda x: (x['score'], x['pullback_pct']), reverse=True)
        cols = st.columns(4)
        for i, r in enumerate(results_sorted[:16]):
            with cols[i % 4]:
                render_card(r, st.container())
        
        with st.expander("📡 API Details"):
            st.write(f"**Yahoo Finance:** {stats.get('yahoo', 0)} Calls (unbegrenzt)")
            st.write(f"**Finnhub:** {finnhub_limiter.get_status()}/60 pro Minute")
            st.write(f"**Alpha Vantage:** {stats.get('alpha_vantage', 0)}/25 pro Tag")
            ctx = get_market_context()
            st.write(f"**Marktkontext:** {'Risk-Off' if ctx.get('risk_off') else 'Risk-On'}")
            st.write(f"**Hard Filter:** {'AKTIV' if hard_filter_active else 'INAKTIV'}")
            
            universe = st.session_state.get('combined_universe', set())
            movers_source = st.session_state.get('movers_source', 'fallback')
            st.write(f"**Scan-Universum:** {len(universe)} Symbole (Movers: {movers_source})")
            st.write(f"- Watchlist: {len(st.session_state['watchlist'])}")
            movers = st.session_state.get('top_movers_cache', {})
            if movers:
                st.write(f"- Gainers: {len(movers.get('gainers', []))}")
                st.write(f"- Most Active: {len(movers.get('most_active', []))}")
            
            st.write("---")
            st.write("**Letzte Alerts:**")
            for symbol, alert in list(st.session_state.get('sent_alerts', {}).items())[:5]:
                ago = int((datetime.now() - alert['timestamp']).total_seconds() / 60)
                st.write(f"  • {symbol}: {alert['setup_type']} vor {ago}min @ ${alert['price']:.2f}")

    alerts = st.session_state.get('sent_alerts', {})
    if alerts:
        st.write("**Letzte Alerts:**")
        for symbol, alert in list(alerts.items())[:5]:
            ago = int((datetime.now() - alert['timestamp']).total_seconds() / 60)
            st.write(f"  • {symbol}: {alert['setup_type']} vor {ago}min @ ${alert['price']:.2f}")
    else:
        st.info("👆 Klicke '🚀 BALANCED SCAN' um Setups zu finden! (Candlestick ist jetzt Bonus, nicht Pflicht)")

    # --- AUTOPILOT KEEP-ALIVE LOOP ---
    if st.session_state.get('auto_refresh', False):
        last_scan = st.session_state.get('last_scan_time')
        if last_scan:
            next_scan = last_scan + timedelta(minutes=30)
            remaining = (next_scan - datetime.now()).total_seconds()
            
            if remaining > 0:
                mins, secs = divmod(int(remaining), 60)
                st.sidebar.info(f"⏳ Nächster Autopilot-Scan in: {mins:02d}:{secs:02d} Min")
                time.sleep(10)
                st.rerun()
            else:
                st.rerun()

if __name__ == "__main__":
    main()
