"""
Elite Bull Scanner Pro V9.1 - PRECISION Edition
================================================
FIXES gegenüber V9.0:
- Yahoo 429: max_workers 4→3, min_delay 1.0→1.5s
- Yahoo 429: Backoff bei Rate Limit 2^n → 10*n Sekunden
- Yahoo 429: Marktkontext Delays erhöht (2s + 3s statt 1s + 0.5s)
- Yahoo 429: 3s Pause vor Scan-Start
- Gemini 429: Rate Limiter 10/Min + Retry mit Backoff (aus V9.0)
- Gemini 429: record_call() VOR dem API-Call (nicht danach)
- $SAVA delisted: harmloser Fehler, wird still übersprungen
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import time
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any, Set
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
    .bull-card:hover { transform: translateY(-2px); box-shadow: 0 6px 12px rgba(0,0,0,0.4); }
    .bull-card.catalyst { border-left: 4px solid #FFD700; }
    .pullback-badge { padding: 5px 10px; border-radius: 5px; color: white; font-weight: bold; display: inline-block; margin: 5px 0; }
    .candlestick-badge { background: linear-gradient(90deg, #00FF00, #00aa00); color: white; padding: 3px 8px; border-radius: 3px; font-size: 0.75rem; margin: 0 2px; font-weight: bold; display: inline-block; }
    .candlestick-badge.weak { background: linear-gradient(90deg, #ff6b6b, #ffa502); opacity: 0.7; }
    .tier-badge { background: #444; padding: 2px 8px; border-radius: 3px; font-size: 0.7rem; margin: 0 2px; color: #fff; border: 1px solid #555; display: inline-block; }
    .cache-badge { background: #2d5a2d; padding: 2px 8px; border-radius: 3px; font-size: 0.7rem; margin: 0 2px; color: #90EE90; display: inline-block; }
    .structure-badge { background: #2d5a2d; padding: 2px 8px; border-radius: 3px; font-size: 0.7rem; margin: 0 2px; color: #90EE90; display: inline-block; }
    .structure-badge.weak { background: #5a2d2d; color: #ff9999; }
    .price { font-size: 1.5rem; font-weight: bold; color: #00FF00; margin: 10px 0; }
    .stop-loss { background: #ff4b4b; color: white; padding: 3px 8px; border-radius: 3px; font-size: 0.8rem; margin-right: 5px; display: inline-block; }
    .target { background: #00FF00; color: black; padding: 3px 8px; border-radius: 3px; font-size: 0.8rem; display: inline-block; }
    .confidence-bar { width: 100%; height: 8px; background: #333; border-radius: 4px; overflow: hidden; margin: 5px 0; }
    .confidence-fill { height: 100%; transition: width 0.3s ease; }
    .news-link-btn { display: block; background: #1f4068; color: #fff; text-decoration: none; padding: 8px; border-radius: 5px; margin: 5px 0; font-size: 0.8rem; text-align: center; }
    .news-link-btn:hover { background: #2a5585; }
    .btn-link { display: block; background: #4a4a4a; color: #fff; text-decoration: none; padding: 8px; border-radius: 5px; margin: 5px 0; font-size: 0.8rem; text-align: center; }
    .btn-link:hover { background: #5a5a5a; }
    .mover-badge { background: linear-gradient(90deg, #ff6b6b, #ffa502); color: white; padding: 3px 8px; border-radius: 3px; font-size: 0.7rem; margin: 0 2px; font-weight: bold; display: inline-block; }
    .gemini-score-high { background: linear-gradient(90deg, #00FF00, #00aa00); color: black; padding: 4px 10px; border-radius: 5px; font-size: 0.8rem; font-weight: bold; display: inline-block; margin: 3px 0; }
    .gemini-score-mid { background: linear-gradient(90deg, #FFD700, #FFA500); color: black; padding: 4px 10px; border-radius: 5px; font-size: 0.8rem; font-weight: bold; display: inline-block; margin: 3px 0; }
    .gemini-score-low { background: linear-gradient(90deg, #ff6b6b, #cc0000); color: white; padding: 4px 10px; border-radius: 5px; font-size: 0.8rem; font-weight: bold; display: inline-block; margin: 3px 0; }
    .gemini-reasoning { background: #0d1117; border-left: 3px solid #FFD700; padding: 8px 12px; border-radius: 0 5px 5px 0; font-size: 0.78rem; color: #ccc; margin: 5px 0; line-height: 1.5; }
    .vol-warning { background: #2d2a1a; border-left: 3px solid #FFA500; padding: 4px 8px; border-radius: 3px; font-size: 0.75rem; color: #FFD700; display: inline-block; margin: 2px 0; }
</style>
""", unsafe_allow_html=True)

# ============================== Type Definitions ==============================

class SourceType(str, Enum):
    WATCHLIST = "watchlist"
    CATALYST = "catalyst"
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
    strength: int
    confirmation: bool
    description: str
    entry_quality: str

@dataclass
class GeminiNewsScore:
    score: int
    catalyst_type: str
    reasoning: str
    is_real_catalyst: bool
    from_cache: bool = False

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
    avg_daily_volume: float
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
    structure_intact: bool = False
    gemini_news: Optional[GeminiNewsScore] = None

# ============================== KONFIGURATION ==============================

st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V9.1", page_icon="🐂")

MIN_PULLBACK_PERCENT = 0.02
MAX_PULLBACK_PERCENT = 0.70
AUTO_REFRESH_INTERVAL = 3600
ALERT_COOLDOWN_MINUTES = 60
MIN_SCORE_THRESHOLD = 70
MIN_CANDLESTICK_STRENGTH = 40
REQUIRE_CANDLESTICK_CONFIRM = False
MIN_AVG_DAILY_VOLUME = 100_000
GEMINI_MIN_CATALYST_SCORE = 50

CATALYST_FILE = "catalysts.json"

try:
    TELEGRAM_BOT_TOKEN = st.secrets["telegram"]["bot_token"]
    TELEGRAM_CHAT_ID = st.secrets["telegram"]["chat_id"]
    FINNHUB_KEYS = st.secrets["finnhub"]["keys"]
    ALPHA_VANTAGE_KEYS = st.secrets["alpha_vantage"]["keys"]
except Exception:
    TELEGRAM_BOT_TOKEN = ""
    TELEGRAM_CHAT_ID = ""
    FINNHUB_KEYS = []
    ALPHA_VANTAGE_KEYS = []

BASE_WATCHLIST = [
    "NVDA", "TSLA", "AMD", "PLTR", "COIN", "MSTR", "HOOD", "CRWD", "AAPL", "MSFT",
    "AMZN", "MARA", "SAP", "LLY", "ABBV", "JNJ", "PFE", "MRK", "BMY", "GILD", "AMGN",
    "BIIB", "VRTX", "REGN", "MRNA", "BNTX", "GSK", "AZN", "SNY", "JAZZ", "ALNY", "IONS",
    "NTLA", "EDIT", "CRSP", "BEAM", "VNDA", "GERN", "FATE", "IOVA", "SRPT",
    "RCKT", "APLS", "HALO", "AQST", "IBRX", "ASND", "DNLI", "ALDX", "LNTH", "REPL",
    "CING", "ACHV", "ATRA", "TBPH", "ROG", "ETON", "BMRN", "CRVS", "NVAX", "UUUU",
    "CELC", "RAPT", "ACRS"
    # SAVA entfernt — delisted
]

FALLBACK_MOVERS = {
    'gainers': ["MARA", "RIOT", "HUT", "COIN", "HOOD", "MSTR", "NVDA", "AMD", "PLTR", "TSLA"],
    'most_active': ["TSLA", "NVDA", "AMD", "PLTR", "COIN", "MSTR", "HOOD", "AAPL", "MSFT", "AMZN"]
}

# ============================== JSON Management ==============================

def load_catalysts() -> List[str]:
    if os.path.exists(CATALYST_FILE):
        try:
            with open(CATALYST_FILE, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Fehler beim Laden der Catalyst-Liste: {e}")
    return []

def save_catalysts(catalysts: List[str]):
    try:
        with open(CATALYST_FILE, "w") as f:
            json.dump(list(set(catalysts)), f)
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Catalyst-Liste: {e}")

# ============================== Session State ==============================

def init_session_state():
    if 'catalyst_list' not in st.session_state:
        st.session_state['catalyst_list'] = load_catalysts()

    combined_watchlist = sorted(list(set(BASE_WATCHLIST + st.session_state['catalyst_list'])))

    defaults = {
        'watchlist': combined_watchlist,
        'sent_alerts': {},
        'api_stats': {
            'yahoo': 0, 'finnhub': 0, 'alpha_vantage': 0,
            'cache_hits': 0, 'alpha_rotation_count': 0, 'gemini_news': 0
        },
        'scan_results': [],
        'last_scan_time': None,
        'auto_refresh': False,
        'refresh_count': 0,
        'last_auto_refresh': 0,
        'last_movers_check': 0,
        'alert_history': [],
        'scan_debug': [],
        'top_movers_cache': FALLBACK_MOVERS,
        'combined_universe': set(
            combined_watchlist + [s for sublist in FALLBACK_MOVERS.values() for s in sublist]
        ),
        'movers_source': 'fallback',
        'hard_filter_active': False,
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
gemini_news_cache = SmartCache()

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
        self.limiters = {
            i: {'calls_today': 0, 'calls_per_min': [], 'key': k, 'exhausted': False}
            for i, k in enumerate(self.keys)
        }
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
# FIX V9.1: Gemini Rate Limiter — konservativ 10/Min (Free Tier = 15/Min)
gemini_limiter = RateLimiter(10, 60)

# ============================== Helper Functions ==============================

def safe_requests_get(url: str, params: Optional[Dict] = None,
                      headers: Optional[Dict] = None, timeout: int = 10) -> Optional[requests.Response]:
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
    pre_market = now.replace(hour=7, minute=0, second=0, microsecond=0)

    holidays_2026 = [(1,1),(1,19),(2,16),(4,3),(5,25),(6,19),(7,3),(9,7),(11,26),(12,25)]
    is_holiday = (now.month, now.day) in holidays_2026

    if now.weekday() >= 5 or is_holiday:
        status, color = "CLOSED", "#ff4b4b"
        countdown = "Weekend" if now.weekday() >= 5 else "Holiday"
        progress = 0
    elif now < pre_market:
        status, color = "CLOSED", "#ff4b4b"
        countdown = f"Pre-market in {str(pre_market - now)[:8]}"
        progress = 0
    elif now < market_open:
        status, color = "PRE-MARKET", "#FFD700"
        countdown = f"Opens in {str(market_open - now)[:8]}"
        progress = 0
    elif market_open <= now <= market_close:
        status, color = "OPEN", "#00FF00"
        countdown = f"Closes in {str(market_close - now)[:8]}"
        progress = (now - market_open) / (market_close - market_open)
    else:
        status, color = "CLOSED", "#ff4b4b"
        countdown = "Opens tomorrow"
        progress = 0

    return {
        'time': now.strftime('%I:%M:%S %p'),
        'status': status, 'color': color, 'countdown': countdown,
        'progress': progress, 'is_open': status == "OPEN", 'is_holiday': is_holiday
    }

def get_market_context() -> Dict[str, Any]:
    cache_key = "market_ctx"
    cached = market_context_cache.get(cache_key, 3600)
    if cached:
        return cached
    try:
        # FIX V9.1: 2s statt 1s — Yahoo braucht mehr Abstand
        time.sleep(2)
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
                # FIX V9.1: 3s statt 0.5s
                time.sleep(3)
                vix = yf.Ticker("^VIX")
                vix_data = vix.history(period="2d")
                vix_level = vix_data['Close'].iloc[-1] if not vix_data.empty else 20
            except Exception:
                vix_level = 20  # VIX nicht kritisch, weitermachen

        risk_off = (spy_change < -0.02) or (vix_level > 30)
        result = {
            'risk_off': risk_off, 'spy_change': spy_change,
            'vix_level': vix_level, 'market_closed': False
        }
        market_context_cache.set(cache_key, result)
        return result

    except Exception as e:
        logger.error(f"Fehler beim Marktkontext: {e}")
        # FIX V9.1: Bei 429 nicht market_closed=True — einfach Risk-On annehmen
        result = {'risk_off': False, 'spy_change': 0, 'vix_level': 20, 'market_closed': False}
        market_context_cache.set(cache_key, result)
        return result

# ============================== Top Movers ==============================

def fetch_yahoo_movers() -> Tuple[Dict[str, List[str]], str]:
    cache_key = "yahoo_movers"
    cached = movers_cache.get(cache_key, AUTO_REFRESH_INTERVAL)
    if cached:
        return cached, 'cache'

    movers = {'gainers': [], 'most_active': []}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
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
                tables = pd.read_html(StringIO(response.text))
                if tables:
                    df = tables[0]
                    if 'Symbol' in df.columns:
                        symbols = [
                            s for s in df['Symbol'].head(15).tolist()
                            if isinstance(s, str) and len(s) <= 5 and s.isalpha()
                        ]
                        movers[category] = symbols[:10]
                        success_count += 1
        except Exception as e:
            logger.error(f"Fehler beim Laden {category}: {e}")
            continue

    if success_count >= 1:
        movers_cache.set(cache_key, movers)
        return movers, 'yahoo'
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
        combined.update(st.session_state.get('catalyst_list', []))
        for symbols in movers.values():
            combined.update(symbols)
        st.session_state['combined_universe'] = combined
        return combined, source

    combined = set(st.session_state['watchlist'])
    combined.update(st.session_state.get('catalyst_list', []))
    for symbols in st.session_state.get('top_movers_cache', {}).values():
        combined.update(symbols)
    return combined, st.session_state.get('movers_source', 'fallback')

def get_symbol_source(symbol: str) -> SourceType:
    if symbol in st.session_state.get('catalyst_list', []):
        return SourceType.CATALYST
    if symbol in st.session_state.get('watchlist', []):
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
        stats['cache_hits'] = stats.get('cache_hits', 0) + 1
        st.session_state['api_stats'] = stats
        return cached, True

    if not FINNHUB_KEYS or not finnhub_limiter.can_call():
        return None, False

    current_finnhub_key = random.choice(FINNHUB_KEYS)
    try:
        url = "https://finnhub.io/api/v1/company-news"
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
            stats['finnhub'] = stats.get('finnhub', 0) + 1
            st.session_state['api_stats'] = stats

            if isinstance(data, list) and len(data) > 0:
                sorted_news = sorted(data, key=lambda x: x.get('datetime', 0), reverse=True)[:5]
                formatted_news = [{
                    'title': item.get('headline', 'No Title'),
                    'url': item.get('url', ''),
                    'source': item.get('source', 'Finnhub'),
                    'datetime': item.get('datetime', 0),
                    'score': 10
                } for item in sorted_news]
                news_cache.set(cache_key, formatted_news)
                return formatted_news, False
    except Exception as e:
        logger.error(f"Finnhub Fehler für {symbol}: {e}")
    return None, False

# ============================== GEMINI NEWS SCORING ==============================

def get_gemini_news_score(symbol: str, news_items: List[Dict]) -> GeminiNewsScore:
    """
    V9.1: Rate Limiter + Retry mit Backoff bei 429.
    Cache verhindert doppelte Calls für gleiche News.
    """
    if not news_items:
        return GeminiNewsScore(
            score=0, catalyst_type="NONE",
            reasoning="Keine News verfügbar", is_real_catalyst=False
        )

    latest_title = news_items[0].get('title', '')
    cache_key = f"gemini_news_{symbol}_{hash(latest_title) % 100000}"
    cached = gemini_news_cache.get(cache_key, AUTO_REFRESH_INTERVAL)
    if cached:
        cached['from_cache'] = True
        return GeminiNewsScore(**cached)

    if "gemini" not in st.secrets or "api_key" not in st.secrets.get("gemini", {}):
        return GeminiNewsScore(
            score=30, catalyst_type="NO_API",
            reasoning="Gemini API nicht konfiguriert — neutrale Bewertung",
            is_real_catalyst=False
        )

    # FIX V9.1: Warten bis Rate Limit-Slot frei (max 60s)
    wait_attempts = 0
    while not gemini_limiter.can_call() and wait_attempts < 6:
        logger.info(f"Gemini Rate Limit — warte 10s ({symbol})")
        time.sleep(10)
        wait_attempts += 1

    if not gemini_limiter.can_call():
        return GeminiNewsScore(
            score=25, catalyst_type="RATE_LIMITED",
            reasoning="Gemini Rate Limit — Bewertung übersprungen",
            is_real_catalyst=False
        )

    news_text = "\n".join([
        f"- [{item.get('source', '')}] {item.get('title', '')}"
        for item in news_items[:3]
    ])

    prompt = f"""Du bist ein professioneller Aktien-Analyst. Bewerte die folgenden News für {symbol}.

NEWS:
{news_text}

Antworte NUR mit diesem JSON-Format (kein Markdown, keine Erklärung davor/danach):
{{
  "score": <0-100>,
  "catalyst_type": "<FDA_APPROVAL|EARNINGS_BEAT|EARNINGS_MISS|MA_DEAL|ANALYST_UPGRADE|ANALYST_DOWNGRADE|CLINICAL_TRIAL|PARTNERSHIP|LEGAL|MACRO|NONE>",
  "reasoning": "<max. 2 Sätze warum dieser Score>",
  "is_real_catalyst": <true|false>
}}

Scoring-Regeln:
- 80-100: Echter fundamentaler Catalyst (FDA-Zulassung, Übernahme, starke Earnings)
- 50-79: Relevante News (Upgrade, Partnerschaft, Studien-Erfolg)
- 20-49: Schwache oder mehrdeutige News
- 0-19: Keine relevante News, Clickbait oder bearish

is_real_catalyst = true NUR bei: FDA-Zulassung, M&A, Earnings-Überraschung, klinischer Studienerfolg"""

    # FIX V9.1: Retry mit Backoff 15s → 30s → 60s
    max_retries = 3
    for attempt in range(max_retries):
        try:
            gemini_limiter.record_call()
            client = genai.Client(api_key=st.secrets["gemini"]["api_key"])
            response = client.models.generate_content(
                model='gemini-2.5-flash',
                contents=prompt,
            )
            raw = response.text.strip().replace('```json', '').replace('```', '').strip()
            data = json.loads(raw)

            result = GeminiNewsScore(
                score=int(data.get('score', 0)),
                catalyst_type=str(data.get('catalyst_type', 'NONE')),
                reasoning=str(data.get('reasoning', '')),
                is_real_catalyst=bool(data.get('is_real_catalyst', False)),
                from_cache=False
            )

            gemini_news_cache.set(cache_key, {
                'score': result.score,
                'catalyst_type': result.catalyst_type,
                'reasoning': result.reasoning,
                'is_real_catalyst': result.is_real_catalyst,
                'from_cache': False
            })

            stats = st.session_state.get('api_stats', {})
            stats['gemini_news'] = stats.get('gemini_news', 0) + 1
            st.session_state['api_stats'] = stats
            return result

        except Exception as e:
            err_str = str(e)
            is_rate_limit = '429' in err_str or 'RESOURCE_EXHAUSTED' in err_str
            if is_rate_limit and attempt < max_retries - 1:
                wait_time = 15 * (2 ** attempt)  # 15s, 30s, 60s
                logger.warning(f"Gemini 429 für {symbol} — warte {wait_time}s (Versuch {attempt+1})")
                time.sleep(wait_time)
                continue
            logger.error(f"Gemini Fehler für {symbol}: {e}")
            return GeminiNewsScore(
                score=20, catalyst_type="ERROR",
                reasoning="Gemini nicht verfügbar — technische Bewertung",
                is_real_catalyst=False
            )

    return GeminiNewsScore(
        score=20, catalyst_type="ERROR",
        reasoning="Gemini max. Retries erreicht",
        is_real_catalyst=False
    )


def get_gemini_entry_analysis(item_data: dict) -> str:
    if "gemini" not in st.secrets or "api_key" not in st.secrets.get("gemini", {}):
        return "⚠️ Gemini API-Key fehlt in den Secrets."

    client = genai.Client(api_key=st.secrets["gemini"]["api_key"])
    news_list = item_data.get('news', [])
    candle = item_data.get('candlestick')
    candle_desc = candle.description if candle else 'Kein Signal'
    candle_quality = candle.entry_quality if candle else 'weak'
    gemini_news = item_data.get('gemini_news')
    news_context = (
        f"Catalyst: {gemini_news.catalyst_type} (Score {gemini_news.score}/100) — {gemini_news.reasoning}"
        if gemini_news
        else (news_list[0].get('title', 'Keine News') if news_list else 'Keine News')
    )

    prompt = f"""Du bist ein professioneller Daytrader. Analysiere folgendes Setup:

Ticker: {item_data['symbol']}
Preis: ${item_data['price']:.2f}
Pullback: -{item_data['pullback_pct']*100:.1f}% vom Hoch
Stop Loss: ${item_data['stop_loss']:.2f}
Target: ${item_data['target']:.2f}
R:R: {item_data['rr_ratio']:.1f}x
RVol: {item_data['rvol']:.1f}x
Avg. Daily Volume: {item_data.get('avg_daily_volume', 0):,.0f}
Candlestick: {candle_desc} (Quality: {candle_quality})
News: {news_context}

Gib mir 3 Sätze:
1. R:R und Liquidität ausreichend?
2. Timing jetzt oder warten?
3. Entry: Limit, Market oder Skip?"""

    try:
        response = client.models.generate_content(model='gemini-2.5-flash', contents=prompt)
        return response.text.strip()
    except Exception as e:
        return f"❌ Gemini Fehler: {str(e)}"

# ============================== CANDLESTICK ANALYSIS ==============================

def analyze_candlestick(df: pd.DataFrame, swing_low: float, recent_high: float) -> CandlestickSignal:
    if len(df) < 5:
        return CandlestickSignal(
            pattern=CandlestickPattern.NONE, strength=0,
            confirmation=False, description="Zu wenig Daten", entry_quality="weak"
        )

    def candle_properties(c):
        o, cl = float(c['Open']), float(c['Close'])
        h, l = float(c['High']), float(c['Low'])
        body = abs(cl - o)
        upper_shadow = h - max(o, cl)
        lower_shadow = min(o, cl) - l
        total_range = h - l
        return {
            'open': o, 'close': cl, 'high': h, 'low': l,
            'body': body, 'upper_shadow': upper_shadow,
            'lower_shadow': lower_shadow, 'total_range': total_range,
            'bullish': cl > o, 'bearish': cl < o,
            'body_pct': body / total_range if total_range > 0 else 0,
            'upper_pct': upper_shadow / total_range if total_range > 0 else 0,
            'lower_pct': lower_shadow / total_range if total_range > 0 else 0
        }

    p1 = candle_properties(df.iloc[-3])
    p2 = candle_properties(df.iloc[-2])
    p3 = candle_properties(df.iloc[-1])

    dist_to_support = (p3['close'] - swing_low) / p3['close'] if p3['close'] > 0 else 1.0
    near_support = dist_to_support < 0.03
    dist_from_high = (recent_high - p3['close']) / recent_high
    in_pullback = 0.02 < dist_from_high < 0.70

    signals_found = []
    strength = 0
    confirmations = 0

    if (p3['lower_pct'] > 0.60 and p3['body_pct'] < 0.30 and
            p3['bullish'] and near_support and p3['low'] <= swing_low * 1.02):
        signals_found.append("HAMMER")
        strength += 40
        if p3['lower_pct'] > 0.70:
            strength += 10
            confirmations += 1

    if (p3['upper_pct'] > 0.60 and p3['body_pct'] < 0.30 and
            p3['bearish'] and p2['bullish'] and near_support and "HAMMER" not in signals_found):
        signals_found.append("INVERTED_HAMMER")
        strength += 25

    if (p2['bearish'] and p3['bullish'] and
            p3['open'] < p2['close'] and p3['close'] > p2['open'] and
            p3['body'] > p2['body'] * 1.2):
        signals_found.append("ENGULFING")
        strength += 35
        if near_support:
            strength += 10
            confirmations += 1

    if (p1['bearish'] and p1['body_pct'] > 0.50 and p2['body_pct'] < 0.30 and
            p3['bullish'] and p3['close'] > (p1['open'] + p1['close']) / 2):
        signals_found.append("MORNING_STAR")
        strength += 45
        confirmations += 1

    if (p2['bearish'] and p3['bullish'] and p3['open'] < p2['low'] and
            p3['close'] > (p2['open'] + p2['close']) / 2 and near_support):
        signals_found.append("PIERCING")
        strength += 30

    if (p2['bearish'] and p2['body_pct'] > 0.50 and p3['bullish'] and
            p3['body'] < p2['body'] * 0.6 and
            p3['high'] < p2['high'] and p3['low'] > p2['low'] and near_support):
        signals_found.append("HARAMI")
        strength += 20

    p0 = candle_properties(df.iloc[-4]) if len(df) >= 4 else None
    if (p0 and p0['bullish'] and p1['bullish'] and p2['bullish'] and
            p1['close'] > p0['close'] and p2['close'] > p1['close'] and
            p2['open'] > p1['open'] and
            all(c['body_pct'] > 0.40 for c in [p0, p1, p2])):
        signals_found.append("3_SOLDIERS")
        strength += 50
        confirmations += 2

    avg_vol = df['Volume'].tail(20).mean()
    if float(df['Volume'].iloc[-1]) > avg_vol * 1.5:
        confirmations += 1
        strength += 5
    if near_support:
        confirmations += 1
        strength += 10
    if in_pullback:
        confirmations += 1
        strength += 5
    if p3['open'] >= p2['close'] * 0.99:
        confirmations += 1
        strength += 5

    if not signals_found:
        return CandlestickSignal(
            pattern=CandlestickPattern.NONE, strength=0,
            confirmation=False, description="Kein Candlestick-Signal", entry_quality="weak"
        )

    pattern_map = {
        "3_SOLDIERS": CandlestickPattern.THREE_WHITE_SOLDIERS,
        "MORNING_STAR": CandlestickPattern.MORNING_STAR,
        "HAMMER": CandlestickPattern.HAMMER,
        "ENGULFING": CandlestickPattern.BULLISH_ENGULFING,
        "PIERCING": CandlestickPattern.PIERCING_LINE,
        "INVERTED_HAMMER": CandlestickPattern.INVERTED_HAMMER,
        "HARAMI": CandlestickPattern.BULLISH_HARAMI
    }
    main_pattern = next(
        (pattern_map[s] for s in signals_found if s in pattern_map),
        CandlestickPattern.NONE
    )

    if strength >= 80 and confirmations >= 3:
        entry_quality = "excellent"
    elif strength >= 65 and confirmations >= 2:
        entry_quality = "good"
    elif strength >= 50 and confirmations >= 1:
        entry_quality = "moderate"
    else:
        entry_quality = "weak"

    return CandlestickSignal(
        pattern=main_pattern, strength=min(100, strength),
        confirmation=confirmations >= 2,
        description=f"{' + '.join(signals_found)} ({confirmations}x Confirm)",
        entry_quality=entry_quality
    )

# ============================== Struktur-Analyse ==============================

def analyze_structure(df: Optional[pd.DataFrame], symbol: Optional[str] = None) -> Dict[str, Any]:
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return _default_structure_result()
    for col in ['High', 'Low', 'Close']:
        if col not in df.columns:
            return _default_structure_result()
    if len(df) < 10:
        return _default_structure_result()
    df_clean = df[['High', 'Low', 'Close']].dropna()
    if len(df_clean) < 10:
        return _default_structure_result()
    if symbol:
        cached = structure_cache.get(f"structure_{symbol}", 300)
        if cached:
            return cached
    try:
        highs = df_clean['High'].values
        lows = df_clean['Low'].values
        swing_highs, swing_lows = [], []

        for i in range(2, len(highs) - 2):
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
                if all(np.isfinite(v) for v in x + y):
                    n = len(x)
                    xm, ym = sum(x)/n, sum(y)/n
                    num = sum((x[i]-xm)*(y[i]-ym) for i in range(n))
                    den = sum((x[i]-xm)**2 for i in range(n))
                    if den != 0:
                        slope = num / den

            result = {
                'higher_highs': bool(hh), 'higher_lows': bool(hl),
                'trend_slope': float(slope) if np.isfinite(slope) else 0.0,
                'structure_intact': bool(hh and hl),
                'last_swing_low': float(swing_lows[-1][1]),
                'last_swing_high': float(swing_highs[-1][1])
            }
        else:
            result = _default_structure_result(df_clean)

        if symbol:
            structure_cache.set(f"structure_{symbol}", result)
        return result
    except Exception:
        return _default_structure_result()

def _default_structure_result(df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
    base = {
        'structure_intact': False, 'higher_highs': False,
        'higher_lows': False, 'trend_slope': 0.0,
        'last_swing_low': 0.0, 'last_swing_high': 0.0
    }
    if df is not None and not df.empty and 'Low' in df.columns and 'High' in df.columns:
        try:
            base['last_swing_low'] = float(df['Low'].tail(5).min())
            base['last_swing_high'] = float(df['High'].tail(20).max())
        except Exception:
            pass
    return base

# ============================== Alpha Vantage ==============================

def get_alpha_vantage_smart(symbol: str) -> Tuple[Optional[Dict], bool]:
    cache_key = f"av_fund_{symbol}"
    cached = fundamentals_cache.get(cache_key, AUTO_REFRESH_INTERVAL)
    if cached:
        stats = st.session_state.get('api_stats', {})
        stats['cache_hits'] = stats.get('cache_hits', 0) + 1
        st.session_state['api_stats'] = stats
        return cached, True
    if not alpha_manager.keys:
        return None, False
    attempts = 0
    while attempts < len(alpha_manager.keys):
        if alpha_manager.can_call():
            current_key = alpha_manager.get_current_key()
            if not current_key:
                break
            try:
                response = safe_requests_get(
                    "https://www.alphavantage.co/query",
                    {'function': 'OVERVIEW', 'symbol': symbol, 'apikey': current_key},
                    timeout=15
                )
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
                            'pe_ratio': float(data['PERatio']) if data.get('PERatio') not in ['None', '0', None] else None,
                            'sector': data.get('Sector', ''),
                            'market_cap': int(float(data['MarketCapitalization'])) if data.get('MarketCapitalization') not in ['None', '0', None] else 0
                        }
                        fundamentals_cache.set(cache_key, result)
                        alpha_manager.record_call()
                        return result, False
                    return None, False
                alpha_manager.rotate_key()
                attempts += 1
                time.sleep(0.5)
            except Exception:
                alpha_manager.rotate_key()
                attempts += 1
                time.sleep(0.5)
        else:
            alpha_manager.rotate_key()
            attempts += 1
    return None, False

# ============================== ThreadPool Scanner ==============================

class ThreadPoolBullScanner:
    # FIX V9.1: max_workers 4→3, min_delay 1.0→1.5
    def __init__(self, max_workers: int = 3, min_delay: float = 1.5):
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
            # Cache nochmal prüfen — anderer Thread könnte es inzwischen geladen haben
            cached = self._yahoo_cache.get(cache_key, 300)
            if cached is not None:
                return cached

            now = time.time()
            elapsed = now - self._last_yahoo_call
            if elapsed < self.min_delay:
                time.sleep(self.min_delay - elapsed)
            self._last_yahoo_call = time.time()

        # FIX V9.1: Längeres Backoff bei 429 — 10s, 20s, 30s
        for attempt in range(3):
            try:
                ticker = yf.Ticker(symbol)
                df = ticker.history(period='3mo', interval='1d')

                try:
                    pm_data = ticker.history(period='1d', interval='1m')
                    if not pm_data.empty:
                        pm_price = float(pm_data['Close'].iloc[-1])
                        if pm_price > 0 and np.isfinite(pm_price):
                            df.iloc[-1, df.columns.get_loc('Close')] = pm_price
                            df.iloc[-1, df.columns.get_loc('High')] = max(float(df.iloc[-1]['High']), pm_price)
                            df.iloc[-1, df.columns.get_loc('Low')] = min(float(df.iloc[-1]['Low']), pm_price)
                except Exception:
                    pass

                if not df.empty:
                    stats = st.session_state.get('api_stats', {})
                    stats['yahoo'] = stats.get('yahoo', 0) + 1
                    st.session_state['api_stats'] = stats
                    self._yahoo_cache.set(cache_key, df)
                    return df
                break

            except Exception as e:
                err_str = str(e)
                is_rate = '429' in err_str or 'Too Many' in err_str or 'rate' in err_str.lower()
                if is_rate and attempt < 2:
                    wait = 10 * (attempt + 1)  # 10s, 20s
                    logger.warning(f"Yahoo 429 für {symbol} — warte {wait}s")
                    time.sleep(wait)
                else:
                    logger.error(f"Yahoo Fehler für {symbol}: {e}")
                    break
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

        avg_daily_volume = float(df_clean['Volume'].tail(20).mean())
        if avg_daily_volume < MIN_AVG_DAILY_VOLUME:
            debug_info['errors'].append(f"Volumen zu niedrig: {avg_daily_volume:,.0f}")
            _log_scan_debug(debug_info)
            return None

        current_price = float(df_clean['Close'].iloc[-1])
        if not np.isfinite(current_price) or current_price <= 0:
            debug_info['errors'].append("Ungültiger Preis")
            _log_scan_debug(debug_info)
            return None

        lookback = min(60, len(df_clean) - 5)
        recent_high = float(df_clean.tail(lookback)['High'].max())
        if not np.isfinite(recent_high) or recent_high <= 0:
            debug_info['errors'].append("Kein High")
            _log_scan_debug(debug_info)
            return None

        pullback_pct = (recent_high - current_price) / recent_high
        if not (MIN_PULLBACK_PERCENT <= pullback_pct <= MAX_PULLBACK_PERCENT):
            debug_info['errors'].append(f"Pullback {pullback_pct:.2%} außerhalb Grenzen")
            _log_scan_debug(debug_info)
            return None

        structure = analyze_structure(df_clean, symbol)
        debug_info['checks'].update({
            'structure_intact': structure.get('structure_intact', False),
            'higher_highs': structure.get('higher_highs', False),
            'higher_lows': structure.get('higher_lows', False)
        })

        # V9: HH allein reicht nicht — muss mind. HL haben
        has_bullish_structure = (
            structure.get('structure_intact', False) or
            structure.get('higher_lows', False)
        )
        if not has_bullish_structure:
            debug_info['errors'].append("Kein bullischer Trend (mind. HL benötigt)")
            _log_scan_debug(debug_info)
            return None

        last_swing_low = structure.get('last_swing_low', 0)
        if not last_swing_low or not np.isfinite(last_swing_low) or last_swing_low <= 0:
            debug_info['errors'].append("Kein Swing Low")
            _log_scan_debug(debug_info)
            return None

        if current_price < last_swing_low * 0.85:
            debug_info['errors'].append("Preis zu weit unter Swing Low")
            _log_scan_debug(debug_info)
            return None

        candlestick = analyze_candlestick(df_clean, last_swing_low, recent_high)
        debug_info['checks'].update({
            'candlestick_pattern': candlestick.pattern.value,
            'candlestick_strength': candlestick.strength,
            'candlestick_quality': candlestick.entry_quality
        })

        if st.session_state.get('hard_filter_active') and REQUIRE_CANDLESTICK_CONFIRM:
            if candlestick.strength < MIN_CANDLESTICK_STRENGTH:
                debug_info['errors'].append(f"Candlestick zu schwach: {candlestick.strength}")
                _log_scan_debug(debug_info)
                return None

        # ====== SCORING ======
        score = 30

        if structure.get('structure_intact', False):
            score += 15
        elif structure.get('higher_lows', False):
            score += 10

        trend_slope = structure.get('trend_slope', 0)
        if trend_slope and np.isfinite(trend_slope) and trend_slope > 0.005:
            score += 5

        current_vol = float(df_clean['Volume'].iloc[-1])
        rvol = current_vol / avg_daily_volume if avg_daily_volume > 0 else 1.0
        if rvol > 2:
            score += 20
        elif rvol > 1.5:
            score += 12
        elif rvol > 1.0:
            score += 6

        support_dist = (current_price - last_swing_low) / current_price if current_price > 0 else 1.0
        if support_dist < 0.03:
            score += 15
        elif support_dist < 0.08:
            score += 8

        if candlestick.strength >= MIN_CANDLESTICK_STRENGTH:
            score += candlestick.strength // 4
            if candlestick.confirmation:
                score += 10
        else:
            score -= 5

        # ====== NEWS + GEMINI ======
        news, sources, cached_news = [], [], False
        gemini_news_result = None

        news_raw, cached_news = get_finnhub_news_smart(symbol)
        if news_raw:
            news = news_raw
            sources.append('FH')
            gemini_news_result = get_gemini_news_score(symbol, news)
            debug_info['checks']['gemini_news_score'] = gemini_news_result.score
            debug_info['checks']['gemini_catalyst_type'] = gemini_news_result.catalyst_type

            if gemini_news_result.score >= 80:
                score += 25
            elif gemini_news_result.score >= 60:
                score += 15
            elif gemini_news_result.score >= GEMINI_MIN_CATALYST_SCORE:
                score += 8
            elif gemini_news_result.score < 20:
                score -= 10

            for item in news:
                item['gemini_score'] = gemini_news_result.score
                item['catalyst_type'] = gemini_news_result.catalyst_type
        else:
            score -= 3

        fundamentals, fund_cached = None, False
        if score > 55 and tier <= 10:
            fundamentals, fund_cached = get_alpha_vantage_smart(symbol)

        pe_ratio = None
        if fundamentals:
            pe_ratio = fundamentals.get('pe_ratio')
            if pe_ratio and np.isfinite(pe_ratio):
                score += 8 if pe_ratio < 15 else (-5 if pe_ratio > 100 else 0)

        # ====== STOP-LOSS: Support-basiert ======
        stop_loss = last_swing_low * 0.97
        max_stop_dist = current_price * 0.15
        if (current_price - stop_loss) > max_stop_dist:
            stop_loss = current_price - max_stop_dist

        risk = current_price - stop_loss
        target = min(current_price + risk * 2, recent_high * 1.05)

        if stop_loss <= 0 or stop_loss >= current_price or target <= current_price:
            debug_info['errors'].append("Ungültige SL/TP")
            _log_scan_debug(debug_info)
            return None

        rr_ratio = (target - current_price) / (current_price - stop_loss) if (current_price - stop_loss) > 0 else 0
        if rr_ratio < 1.0:
            debug_info['errors'].append(f"R:R {rr_ratio:.2f} < 1.0")
            _log_scan_debug(debug_info)
            return None

        if score < MIN_SCORE_THRESHOLD:
            debug_info['errors'].append(f"Score {score} < {MIN_SCORE_THRESHOLD}")
            _log_scan_debug(debug_info)
            return None

        reasons = [f"📉 -{pullback_pct:.1%}"]
        if structure.get('structure_intact'):
            reasons.append("📈 HH+HL")
        elif structure.get('higher_lows'):
            reasons.append("📈 HL")
        if rvol > 1.0:
            reasons.append(f"⚡ Vol {rvol:.1f}x")
        if support_dist < 0.03:
            reasons.append("🎯 Support nah")
        if candlestick.strength >= MIN_CANDLESTICK_STRENGTH:
            reasons.append(f"🕯️ {candlestick.pattern.value}")
            if candlestick.confirmation:
                reasons.append("✅ Confirm")
        else:
            reasons.append("⚠️ Kein Candle")
        if gemini_news_result and gemini_news_result.score >= GEMINI_MIN_CATALYST_SCORE:
            reasons.append(f"🤖 {gemini_news_result.catalyst_type}")
        elif news:
            reasons.append("📰 News")
        if pe_ratio:
            reasons.append(f"{'💰' if pe_ratio < 15 else '📊'} PE {pe_ratio:.1f}")

        return {
            'symbol': symbol, 'tier': tier, 'score': min(100, int(score)),
            'price': current_price, 'pullback_pct': pullback_pct,
            'recent_high': recent_high, 'stop_loss': stop_loss,
            'target': target, 'rr_ratio': rr_ratio, 'rvol': rvol,
            'avg_daily_volume': avg_daily_volume, 'reasons': reasons,
            'news': news, 'pe_ratio': pe_ratio,
            'api_sources': list(set(sources + (['AV'] if fundamentals else []))),
            'from_cache': cached_news or fund_cached,
            'source': get_symbol_source(symbol),
            'candlestick': candlestick,
            'has_candlestick_confirm': candlestick.strength >= MIN_CANDLESTICK_STRENGTH,
            'structure_intact': structure.get('structure_intact', False),
            'gemini_news': gemini_news_result,
        }

    def scan_batch(self, symbols: List[Tuple[str, SourceType]], progress_callback=None) -> List[Dict]:
        results = []
        completed = 0
        error_count = 0
        success_count = 0

        try:
            ctx = get_script_run_ctx()
        except Exception:
            ctx = None

        def wrapper(sym, tier, total):
            if ctx:
                add_script_run_ctx(threading.current_thread(), ctx)
            return self.analyze_single_symbol(sym, tier, total)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_symbol = {
                executor.submit(wrapper, sym, i + 1, len(symbols)): (sym, src)
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
                        error_count += 1
                except Exception as e:
                    logger.error(f"Fehler bei {symbol}: {e}")
                    error_count += 1
                completed += 1
                if progress_callback:
                    progress_callback(completed, len(symbols), success_count, error_count, symbol, source)

        return sorted(results, key=lambda x: (x['score'], x['pullback_pct']), reverse=True)


def _log_scan_debug(debug_info: Dict):
    scan_debug = st.session_state.get('scan_debug', [])
    scan_debug.append(debug_info)
    st.session_state['scan_debug'] = scan_debug[-100:]

# ============================== Alert Management ==============================

def should_send_alert(symbol: str, current_price: float, current_score: int) -> bool:
    sent_alerts = st.session_state.get('sent_alerts', {})
    if symbol not in sent_alerts:
        return True
    last_alert = sent_alerts[symbol]
    if (datetime.now() - last_alert['timestamp']).total_seconds() / 60 < ALERT_COOLDOWN_MINUTES:
        return False
    price_change = abs(current_price - last_alert['price']) / last_alert['price']
    return price_change >= 0.02 or (current_score - last_alert['score']) >= 10

def record_alert(symbol: str, price: float, score: int, setup_type: str):
    st.session_state['sent_alerts'][symbol] = {
        'timestamp': datetime.now(), 'price': price, 'score': score, 'setup_type': setup_type
    }
    st.session_state['alert_history'].append({
        'timestamp': datetime.now(), 'symbol': symbol,
        'price': price, 'score': score, 'setup_type': setup_type
    })
    st.session_state['alert_history'] = st.session_state['alert_history'][-20:]

# ============================== Telegram ==============================

def send_telegram_alert(symbol, price, pullback_pct, news_item, setup_type,
                        pe_ratio=None, api_sources=None, tier=None,
                        source=None, candlestick=None, gemini_news=None) -> bool:
    if not TELEGRAM_BOT_TOKEN or len(TELEGRAM_BOT_TOKEN) < 10:
        return False

    news_url = news_item.get('url', '') if news_item else f'https://finance.yahoo.com/quote/{symbol}'
    emoji = "🧬" if setup_type == "CATALYST" else "🏆" if setup_type == "GOLD" else "🐂"
    source_emoji = {
        SourceType.WATCHLIST: '📋', SourceType.CATALYST: '🧬',
        SourceType.GAINERS: '🚀', SourceType.MOST_ACTIVE: '🔥'
    }.get(source, '📊')

    candle_info = ""
    if candlestick and candlestick.pattern != CandlestickPattern.NONE:
        candle_info = f"\n🕯️ {candlestick.pattern.value.upper()} ({candlestick.strength}/100)"
        if candlestick.confirmation:
            candle_info += " ✅"

    gemini_info = ""
    if gemini_news and gemini_news.score >= GEMINI_MIN_CATALYST_SCORE:
        gemini_info = f"\n🤖 {gemini_news.catalyst_type} ({gemini_news.score}/100)\n   {gemini_news.reasoning[:80]}"

    msg = (f"{emoji} <b>{setup_type}: {symbol}</b> {emoji}\n"
           f"📉 Pullback: <b>-{pullback_pct:.1f}%</b>\n"
           f"💵 Preis: ${price:.2f}\n"
           f"{source_emoji} {source.value if source else 'unknown'}"
           f"{candle_info}{gemini_info}\n"
           f"{'📊 PE: ' + f'{pe_ratio:.1f}' if pe_ratio else ''}\n"
           f"👉 <a href='{news_url}'>News</a> | "
           f"<a href='https://www.tradingview.com/chart/?symbol={symbol}'>Chart</a>")

    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                  "parse_mode": "HTML", "disable_web_page_preview": True},
            timeout=5
        ).raise_for_status()
        return True
    except Exception:
        return False

# ============================== KARTEN RENDERING ==============================

def render_card(item: Dict, container):
    with container:
        sym = item['symbol']
        price = item['price']
        pullback = item['pullback_pct']
        sl, target = item['stop_loss'], item['target']
        rr, rvol = item['rr_ratio'], item['rvol']
        avg_vol = item.get('avg_daily_volume', 0)
        score = item['score']
        reasons = ' | '.join(item['reasons'][:4])
        news_item = item.get('news', [{}])[0] if item.get('news') else None
        news_title = (news_item['title'][:35] + '...') if news_item else 'Keine News'
        news_url = news_item['url'] if news_item else f'https://finance.yahoo.com/quote/{sym}'
        tv_url = f'https://www.tradingview.com/chart/?symbol={sym}'
        tier = item.get('tier', '-')
        source = item.get('source', SourceType.UNKNOWN)
        apis = item.get('api_sources', [])
        cached = item.get('from_cache', False)
        candlestick = item.get('candlestick')
        structure_intact = item.get('structure_intact', False)
        gemini_news = item.get('gemini_news')

        has_candle = candlestick and candlestick.pattern != CandlestickPattern.NONE
        pullback_color = '#ff6b6b' if pullback > 0.15 else '#ffa502'
        conf_color = '#9933ff' if score > 85 else '#FFD700' if score > 70 else '#00FF00'

        source_map = {
            SourceType.WATCHLIST: ('📋 WL', 'tier-badge'),
            SourceType.CATALYST: ('🧬 CATALYST', 'tier-badge'),
            SourceType.GAINERS: ('🚀 GAINER', 'mover-badge'),
            SourceType.MOST_ACTIVE: ('🔥 ACTIVE', 'mover-badge'),
        }
        source_badge, source_class = source_map.get(source, ('📊', 'tier-badge'))

        candle_text = f"{candlestick.pattern.value.upper()} {candlestick.strength}" if has_candle else "NO CANDLE"
        candle_class = 'candlestick-badge' if has_candle else 'candlestick-badge weak'
        candle_style = '' if not has_candle else (
            'background:linear-gradient(90deg,#FFD700,#FFA500);'
            if candlestick.strength < 65
            else 'background:linear-gradient(90deg,#00FF00,#00aa00);'
        )

        api_badges = ''.join([f'<span class="tier-badge">{a}</span>' for a in apis])
        cache_badge = '<span class="cache-badge">CACHE</span>' if cached else ''

        gemini_html = ""
        if gemini_news:
            g_class = (
                "gemini-score-high" if gemini_news.score >= 80
                else "gemini-score-mid" if gemini_news.score >= 50
                else "gemini-score-low"
            )
            cache_ind = " 💾" if gemini_news.from_cache else ""
            gemini_html = (
                f'<div style="margin:8px 0;">'
                f'<span class="{g_class}">🤖 {gemini_news.catalyst_type} {gemini_news.score}/100{cache_ind}</span>'
                f'<div class="gemini-reasoning">{gemini_news.reasoning}</div>'
                f'</div>'
            )

        vol_warning = (
            f'<span class="vol-warning">⚠️ Vol dünn: {avg_vol/1000:.0f}k/Tag</span>'
            if avg_vol < 500_000 else ""
        )
        card_class = "bull-card catalyst" if source == SourceType.CATALYST else "bull-card"
        struct_text = '📈 HH+HL' if structure_intact else '📈 HL'
        struct_class = 'structure-badge' if structure_intact else 'structure-badge weak'

        st.markdown(f"""
        <div class="{card_class}">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <h3 style="margin:0;color:#fff;">🐂 {sym}</h3>
                <span class="{source_class}">{source_badge}</span>
            </div>
            <div style="margin-bottom:8px;">
                <span class="pullback-badge" style="background:{pullback_color};">-{pullback:.1%}</span>
                {vol_warning}
            </div>
            <div style="margin:5px 0;">
                <span class="tier-badge">T{tier}</span>{api_badges}{cache_badge}
                <span class="{struct_class}">{struct_text}</span>
            </div>
            <div style="margin:5px 0;">
                <span class="{candle_class}" style="{candle_style}">{candle_text}</span>
            </div>
            {gemini_html}
            <div class="price">${price:.2f}</div>
            <div style="font-size:0.8rem;color:#aaa;margin:8px 0;line-height:1.4;">{reasons}</div>
            <div style="margin:8px 0;">
                <span class="stop-loss">SL ${sl:.2f}</span>
                <span class="target">TP ${target:.2f}</span>
            </div>
            <div style="font-size:0.8rem;color:{conf_color};margin:5px 0;font-weight:bold;">Score: {score}/100</div>
            <div class="confidence-bar">
                <div class="confidence-fill" style="width:{score}%;background:{conf_color};"></div>
            </div>
            <div style="font-size:0.75rem;color:#888;margin:5px 0;">
                R:R {rr:.1f}x | Vol {rvol:.1f}x | Avg {avg_vol/1000:.0f}k/Tag
            </div>
            <a href="{news_url}" target="_blank" class="news-link-btn">📰 {news_title}</a>
            <a href="{tv_url}" target="_blank" class="btn-link">📈 TradingView</a>
        </div>""", unsafe_allow_html=True)

        if st.button("🤖 Gemini Entry-Check", key=f"gemini_{sym}_{random.randint(0,999999)}"):
            with st.spinner(f"Analysiere {sym}..."):
                st.info(get_gemini_entry_analysis(item), icon="💡")

# ============================== MAIN ==============================

def main():
    clock = get_market_clock()

    if clock.get('is_holiday'):
        st.markdown("""
        <div style="background:linear-gradient(90deg,#ff4b4b,#ff6b6b);color:white;padding:20px;
             border-radius:10px;text-align:center;font-size:1.2rem;margin:20px 0;">
            🎌 US MARKET HOLIDAY — Markt geschlossen
        </div>""", unsafe_allow_html=True)

    progress_html = (
        f'<div style="width:100%;height:6px;background:#333;border-radius:3px;overflow:hidden;margin-top:10px;">'
        f'<div style="height:100%;background:linear-gradient(90deg,#00FF00,#FFD700);width:{clock["progress"]*100:.1f}%;"></div></div>'
        if clock['is_open'] else ''
    )
    st.markdown(f"""
    <div style="background:linear-gradient(135deg,#1a1a2e,#0f0f23);padding:20px;border-radius:15px;
         text-align:center;margin:20px 0;border:1px solid #333;">
        <div style="font-size:2.5rem;font-weight:bold;color:#00FF00;font-family:'Courier New',monospace;">
            {clock['time']}
        </div>
        <div style="margin:10px 0;">
            <span style="padding:8px 20px;border-radius:20px;font-weight:bold;color:white;
                  display:inline-block;background:{clock['color']};">{clock['status']}</span>
        </div>
        <div style="font-size:1.2rem;color:#FFD700;margin:10px 0;">{clock['countdown']}</div>
        {progress_html}
    </div>""", unsafe_allow_html=True)

    if st.session_state.get('auto_refresh'):
        now = time.time()
        if now - st.session_state.get('last_auto_refresh', 0) >= AUTO_REFRESH_INTERVAL:
            st.session_state['last_auto_refresh'] = now
            st.session_state['refresh_count'] = st.session_state.get('refresh_count', 0) + 1
            st.rerun()

    # ============================== SIDEBAR ==============================
    with st.sidebar:
        st.header("🤖 Autopilot")
        auto_pilot = st.toggle("Autopilot aktivieren", value=st.session_state.get('auto_refresh', False))
        st.session_state['auto_refresh'] = auto_pilot
        st.divider()

        st.header("🧬 Catalyst Manager")
        with st.expander("Pharma/Biotech Liste", expanded=False):
            new_ticker = st.text_input("Ticker hinzufügen:").strip().upper()
            if st.button("➕ Speichern"):
                if new_ticker and new_ticker not in st.session_state['catalyst_list']:
                    st.session_state['catalyst_list'].append(new_ticker)
                    save_catalysts(st.session_state['catalyst_list'])
                    st.success(f"{new_ticker} hinzugefügt!")
                    st.rerun()
                elif new_ticker:
                    st.warning("Ticker existiert bereits.")
            st.divider()
            if st.session_state['catalyst_list']:
                st.write(", ".join(st.session_state['catalyst_list']))
                to_remove = st.multiselect("Entfernen:", st.session_state['catalyst_list'])
                if st.button("🗑️ Löschen") and to_remove:
                    for t in to_remove:
                        st.session_state['catalyst_list'].remove(t)
                    save_catalysts(st.session_state['catalyst_list'])
                    st.rerun()
            else:
                st.info("Liste ist leer.")
        st.divider()

        st.header("🎛️ Filter")
        hard_filter = st.toggle("🔥 Hard Mode", value=st.session_state.get('hard_filter_active', False))
        st.session_state['hard_filter_active'] = hard_filter
        if hard_filter:
            st.markdown('<div style="background:linear-gradient(90deg,#00FF00,#00aa00);color:black;'
                        'padding:10px;border-radius:5px;font-weight:bold;">🔥 HARD MODE AKTIV</div>',
                        unsafe_allow_html=True)
        else:
            st.info("✅ Balanced Mode")

        with st.expander("⚙️ Filter feinjustieren"):
            global MIN_PULLBACK_PERCENT, MAX_PULLBACK_PERCENT, MIN_SCORE_THRESHOLD, MIN_AVG_DAILY_VOLUME
            min_pull = st.slider("Min. Pullback %", 1, 10, int(MIN_PULLBACK_PERCENT * 100)) / 100
            max_pull = st.slider("Max. Pullback %", 30, 80, int(MAX_PULLBACK_PERCENT * 100)) / 100
            min_score = st.slider("Min. Score", 55, 85, MIN_SCORE_THRESHOLD)
            min_vol_k = st.slider("Min. Avg. Volume (k/Tag)", 50, 500, int(MIN_AVG_DAILY_VOLUME / 1000))
            if st.button("Filter anwenden"):
                MIN_PULLBACK_PERCENT = min_pull
                MAX_PULLBACK_PERCENT = max_pull
                MIN_SCORE_THRESHOLD = min_score
                MIN_AVG_DAILY_VOLUME = min_vol_k * 1000
                st.success("Aktualisiert!")
        st.divider()

        st.header("📡 API Status")
        stats = st.session_state.get('api_stats', {})
        st.markdown(f"""
        <div style="background:#1a1a2e;padding:12px;border-radius:8px;border-left:4px solid #00FF00;margin:8px 0;">
            🟢 <b>Yahoo Finance</b> — {stats.get('yahoo', 0)} Calls<br>
            {'🟢' if finnhub_limiter.can_call() else '🔴'} <b>Finnhub</b> — {finnhub_limiter.get_status()}/60 pro Min<br>
            🤖 <b>Gemini News</b> — {stats.get('gemini_news', 0)} Calls | {gemini_limiter.get_status()}/10 pro Min<br>
            📦 Cache Hits: {stats.get('cache_hits', 0)}
        </div>""", unsafe_allow_html=True)

        for status in alpha_manager.get_status():
            ind = "▶️" if status['active'] else "✅" if not status['exhausted'] else "❌"
            color = '#00FF00' if status['active'] else '#ff4b4b' if status['exhausted'] else '#666'
            st.markdown(
                f'<div style="background:#2a2a3e;padding:7px;margin:3px 0;border-radius:5px;'
                f'border-left:3px solid {color};">{ind} Key {status["index"]+1}: {status["calls_today"]}/25</div>',
                unsafe_allow_html=True
            )
        st.divider()

        st.header("🔍 Manuelle Abfrage")
        manual_symbol = st.text_input("Symbol:", placeholder="z.B. NVDA", key="manual").upper()
        show_debug = st.checkbox("Debug anzeigen", value=True)

        if st.button("📊 Analyse starten") and manual_symbol:
            old_hard = st.session_state.get('hard_filter_active', False)
            st.session_state['hard_filter_active'] = False
            with st.spinner(f"Analysiere {manual_symbol}..."):
                try:
                    time.sleep(1)
                    result = ThreadPoolBullScanner(max_workers=1, min_delay=1.0).analyze_single_symbol(
                        manual_symbol, 1, 1
                    )
                    if result:
                        st.success(f"✅ {manual_symbol}")
                        c1, c2 = st.columns(2)
                        with c1:
                            st.write(f"**Score:** {result['score']}/100")
                            st.write(f"**Price:** ${result['price']:.2f}")
                            st.write(f"**Pullback:** {result['pullback_pct']:.2%}")
                            st.write(f"**R:R:** {result['rr_ratio']:.1f}x")
                            st.write(f"**Avg Vol:** {result.get('avg_daily_volume', 0)/1000:.0f}k/Tag")
                        with c2:
                            candle = result.get('candlestick')
                            if candle and candle.pattern != CandlestickPattern.NONE:
                                st.write(f"**Candle:** {candle.pattern.value} ({candle.strength}/100)")
                                st.write(f"**Quality:** {candle.entry_quality}")
                            gn = result.get('gemini_news')
                            if gn:
                                st.write(f"**Catalyst:** {gn.catalyst_type} ({gn.score}/100)")
                                st.write(f"**Reasoning:** {gn.reasoning[:80]}")
                    else:
                        st.error(f"❌ Kein Setup für {manual_symbol}")
                        if show_debug:
                            debug = st.session_state.get('scan_debug', [])
                            if debug:
                                with st.expander("🔍 Debug"):
                                    st.json(debug[-1].get('checks', {}))
                                    for err in debug[-1].get('errors', []):
                                        st.write(f"❌ {err}")
                finally:
                    st.session_state['hard_filter_active'] = old_hard

        if st.button("🔄 Reset Stats"):
            st.session_state['api_stats'] = {
                'yahoo': 0, 'finnhub': 0, 'alpha_vantage': 0,
                'cache_hits': 0, 'alpha_rotation_count': 0, 'gemini_news': 0
            }
            st.session_state['scan_debug'] = []
            for i in alpha_manager.limiters:
                alpha_manager.limiters[i].update({'exhausted': False, 'calls_today': 0, 'calls_per_min': []})
            st.success("Zurückgesetzt!")
            st.rerun()

    # ============================== SCAN BUTTONS ==============================

    scan_triggered = False
    col1, col2 = st.columns([3, 1])
    with col1:
        mode = "HARD" if st.session_state.get('hard_filter_active') else "PRECISION"
        if st.button(f'🚀 {mode} SCAN (Score ≥ {MIN_SCORE_THRESHOLD})', type="primary"):
            scan_triggered = True
    with col2:
        if st.button('⚡ Quick Scan'):
            st.session_state['hard_filter_active'] = False
            scan_triggered = True

    if st.session_state.get('auto_refresh'):
        last_scan = st.session_state.get('last_scan_time')
        if last_scan is None or (datetime.now() - last_scan).total_seconds() / 60 >= 30:
            scan_triggered = True

    if scan_triggered:
        # FIX V9.1: 3s Pause vor Scan — Yahoo nach Marktkontext-Call erholen lassen
        time.sleep(3)
        with st.spinner("🔍 Precision Scan läuft..."):
            market_ctx = get_market_context()
            if market_ctx.get('market_closed'):
                st.warning("⚠️ Markt möglicherweise geschlossen — Daten können veraltet sein.")

            universe, src = get_combined_universe()
            st.info(
                f"📊 {len(universe)} Symbole | Score ≥ {MIN_SCORE_THRESHOLD} | "
                f"Min Vol {MIN_AVG_DAILY_VOLUME/1000:.0f}k/Tag | 🤖 Gemini aktiv"
            )

            st.session_state['scan_debug'] = []
            scan_list = [(sym, get_symbol_source(sym)) for sym in universe]
            scan_list.sort(key=lambda x: (
                0 if x[1] == SourceType.CATALYST else
                1 if x[1] == SourceType.WATCHLIST else 2
            ))

            scanner = ThreadPoolBullScanner(max_workers=3, min_delay=1.5)
            progress_bar = st.progress(0)
            status_text = st.empty()
            stats_text = st.empty()

            def update_progress(completed, total, success, errors, current_sym, current_src):
                progress_bar.progress(min(completed / total, 0.99))
                status_text.text(f"Analysiere: {current_sym} ({completed}/{total})")
                stats_text.markdown(f"✅ **{success}** Setups | ❌ {errors} Ausschlüsse")

            start_time = time.time()
            results = scanner.scan_batch(scan_list, update_progress)
            elapsed = time.time() - start_time

            progress_bar.empty()
            status_text.empty()
            stats_text.empty()

            st.session_state['scan_results'] = results
            st.session_state['last_scan_time'] = datetime.now()

            real_cats = sum(1 for r in results if r.get('gemini_news') and r['gemini_news'].is_real_catalyst)
            st.success(f"✅ {len(results)} Setups in {elapsed:.1f}s | 🧬 {real_cats} echte Catalysts")

            alerts_sent = 0
            for item in results:
                if item['score'] > 75 and should_send_alert(item['symbol'], item['price'], item['score']):
                    gn = item.get('gemini_news')
                    setup_type = "CATALYST" if (gn and gn.is_real_catalyst) else "GOLD"
                    if send_telegram_alert(
                        item['symbol'], item['price'], item['pullback_pct'],
                        item['news'][0] if item.get('news') else None,
                        setup_type, item.get('pe_ratio'), item.get('api_sources'),
                        item.get('tier'), item.get('source'),
                        item.get('candlestick'), gn
                    ):
                        record_alert(item['symbol'], item['price'], item['score'], setup_type)
                        alerts_sent += 1
                        if alerts_sent <= 3:
                            st.toast(f"🚨 {setup_type}: {item['symbol']} @ ${item['price']:.2f}")

    # ============================== ERGEBNISSE ==============================

    results = st.session_state.get('scan_results', [])
    if results:
        col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
        with col1:
            st.subheader(f"📊 {len(results)} Setups")
        with col2:
            st.metric("🧬 Echte Catalysts", sum(
                1 for r in results if r.get('gemini_news') and r['gemini_news'].is_real_catalyst
            ))
        with col3:
            st.metric("🕯️ Mit Candlestick", sum(1 for r in results if r.get('has_candlestick_confirm')))
        with col4:
            last_time = st.session_state.get('last_scan_time')
            if last_time:
                st.caption(f"Scan: {last_time.strftime('%H:%M:%S')}")

        catalyst_types = {}
        for r in results:
            gn = r.get('gemini_news')
            if gn:
                catalyst_types[gn.catalyst_type] = catalyst_types.get(gn.catalyst_type, 0) + 1

        if catalyst_types:
            st.write("**🤖 Catalyst-Verteilung:**")
            ct_cols = st.columns(min(len(catalyst_types), 5))
            for i, (ct, count) in enumerate(sorted(catalyst_types.items(), key=lambda x: -x[1])):
                ct_cols[i % 5].metric(ct, count)

        cols = st.columns(4)
        for i, r in enumerate(sorted(results, key=lambda x: (x['score'], x['pullback_pct']), reverse=True)[:16]):
            with cols[i % 4]:
                render_card(r, st.container())

        with st.expander("📡 Scan Details"):
            s = st.session_state.get('api_stats', {})
            st.write(
                f"Yahoo: {s.get('yahoo',0)} | Finnhub: {s.get('finnhub',0)} | "
                f"AV: {s.get('alpha_vantage',0)} | Gemini: {s.get('gemini_news',0)} | "
                f"Cache: {s.get('cache_hits',0)}"
            )
            ctx = get_market_context()
            st.write(f"Markt: {'⚠️ Risk-Off' if ctx.get('risk_off') else '✅ Risk-On'}")
            st.write(f"Filter: Score≥{MIN_SCORE_THRESHOLD} | Vol≥{MIN_AVG_DAILY_VOLUME/1000:.0f}k/Tag")

        if st.session_state.get('auto_refresh'):
            last_scan = st.session_state.get('last_scan_time')
            if last_scan:
                remaining = (last_scan + timedelta(minutes=30) - datetime.now()).total_seconds()
                if remaining > 0:
                    mins, secs = divmod(int(remaining), 60)
                    st.sidebar.info(f"⏳ Nächster Scan in: {mins:02d}:{secs:02d}")
                    time.sleep(10)
                    st.rerun()
                else:
                    st.rerun()
    else:
        st.info(f"👆 Klicke '🚀 PRECISION SCAN' — Score ≥ {MIN_SCORE_THRESHOLD}, Gemini News-Scoring aktiv")


if __name__ == "__main__":
    main()
