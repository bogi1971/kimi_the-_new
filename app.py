import streamlit as st
import yfinance as yf
import pandas as pd
import time
import requests
from datetime import datetime, timedelta
import numpy as np
from io import StringIO
import warnings
import pytz
import logging
import random
import os
import threading

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
.source-losers { border-left: 3px solid #ffa502; }
.source-mostactive { border-left: 3px solid #FFD700; }
</style>
""", unsafe_allow_html=True)

# ============================== Helper Functions ==============================
def safe_requests_get(url, params=None, headers=None, timeout=10):
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"API-Request Fehler: {e}")
        return None

def get_market_clock():
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

def get_market_context():
    """Hole Marktkontext mit Caching (5 Minuten)"""
    cache_key = "market_ctx"
    cached = market_context_cache.get(cache_key, 300)
    if cached:
        logger.info("Market Context aus Cache")
        return cached
    
    try:
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

# ============================== Konfiguration ==============================
st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V6.0", page_icon="🐂")

# Filter Einstellungen
MIN_PULLBACK_PERCENT = 0.05
MAX_PULLBACK_PERCENT = 0.50
AUTO_REFRESH_INTERVAL = 900  # 15 Minuten für Movers Check
ALERT_COOLDOWN_MINUTES = 60
MIN_SCORE_THRESHOLD = 60  # Nur Setups mit Score >= 60 anzeigen
MAX_WATCHLIST_SIZE = 40  # Reduzierte Watchlist

# ============================================
# HIER DEINE ECHTEN API KEYS EINTRAGEN!
# ============================================

# Telegram Configuration
TELEGRAM_BOT_TOKEN = "8317204351:AAHRu-mYYU0_NRIxNGEQ5voneIQaDKeQuF8"
TELEGRAM_CHAT_ID = "5338135874"

# Finnhub API Key (kostenlos auf finnhub.io)
FINNHUB_API_KEY = "d652vnpr01qqbln5m9cgd652vnpr01qqbln5m9d0"

# Alpha Vantage API Keys (kostenlos auf alphavantage.co - bis zu 3 Keys für 75 Calls/Tag)
ALPHA_VANTAGE_KEYS = [
    "N6PM9UCXL55JZTN9",
    "4ebfbdb3c8374c99abbf259c168d93c1",
    "6898e81a60be40a092710d0349f95110",
]

# Kompakte Premium-Watchlist (40 Ticker)
DEFAULT_WATCHLIST = sorted(list(set([
    "NVDA", "TSLA", "AMD", "PLTR", "COIN", "MSTR", "HOOD", "CRWD",
    "LLY", "ABBV", "JNJ", "PFE", "MRK", "BMY", "GILD", "AMGN",
    "BIIB", "VRTX", "REGN", "MRNA", "BNTX", "GSK", "AZN", "SNY",
    "JAZZ", "ALNY", "IONS", "NTLA", "EDIT", "CRSP", "BEAM", "VNDA",
    "SAVA", "GERN", "FATE", "IOVA", "SRPT", "RCKT", "APLS", "HALO"
])))

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
        'last_movers_check': 0,  # Neuer Timestamp für Movers
        'alert_history': [],
        'scan_debug': [],
        'top_movers_cache': [],  # Cache für Top Movers
        'combined_universe': set(DEFAULT_WATCHLIST),  # Kombiniertes Universum
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
    def get(self, key, ttl=600):
        if key in self.cache:
            age = time.time() - self.timestamps.get(key, 0)
            if age < ttl:
                return self.cache[key]
            else:
                del self.cache[key]
                del self.timestamps[key]
        return None
    def set(self, key, value):
        self.cache[key] = value
        self.timestamps[key] = time.time()

news_cache = SmartCache()
fundamentals_cache = SmartCache()
structure_cache = SmartCache()
market_context_cache = SmartCache()
movers_cache = SmartCache()  # Neuer Cache für Movers

class RateLimiter:
    def __init__(self, max_calls, window_seconds):
        self.max_calls = max_calls
        self.window = window_seconds
        self.calls = []
    def can_call(self):
        now = time.time()
        self.calls = [c for c in self.calls if now - c < self.window]
        return len(self.calls) < self.max_calls
    def record_call(self):
        self.calls.append(time.time())
        return len(self.calls)
    def get_status(self):
        now = time.time()
        self.calls = [c for c in self.calls if now - c < self.window]
        return f"{len(self.calls)}/{self.max_calls}"

class AlphaVantageManager:
    def __init__(self, keys):
        self.keys = [k for k in keys if k and len(k) > 10]
        self.current_index = 0
        self.limiters = {i: {'calls_today': 0, 'calls_per_min': [], 'key': k, 'exhausted': False} for i, k in enumerate(self.keys)}
    def get_current_key(self):
        return self.keys[self.current_index] if self.keys else None
    def rotate_key(self):
        if not self.keys:
            return None
        for _ in range(len(self.keys)):
            self.current_index = (self.current_index + 1) % len(self.keys)
            if not self.limiters[self.current_index]['exhausted']:
                stats = st.session_state.get('api_stats', {})
                if isinstance(stats, dict):
                    stats['alpha_rotation_count'] = stats.get('alpha_rotation_count', 0) + 1
                    st.session_state['api_stats'] = stats
                return self.get_current_key()
        return None
    def can_call(self, key_index=None):
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
    def record_call(self, key_index=None):
        idx = key_index if key_index is not None else self.current_index
        limiter = self.limiters[idx]
        limiter['calls_per_min'].append(time.time())
        limiter['calls_today'] += 1
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['alpha_vantage'] = stats.get('alpha_vantage', 0) + 1
            st.session_state['api_stats'] = stats
        return limiter['calls_today']
    def get_status(self):
        return [{
            'index': i,
            'key': f"{k[:4]}...{k[-4:]}" if len(k)>8 else k,
            'active': i == self.current_index,
            'calls_today': self.limiters[i]['calls_today'],
            'exhausted': self.limiters[i]['exhausted'],
            'can_call': self.can_call(i)
        } for i, k in enumerate(self.keys)]

finnhub_limiter = RateLimiter(60, 60)
alpha_manager = AlphaVantageManager(ALPHA_VANTAGE_KEYS)

# ============================== NEU: Top Movers Functions ==============================
def fetch_yahoo_movers():
    """Hole Top Movers von Yahoo Finance (Gainers, Losers, Most Active)"""
    cache_key = "yahoo_movers"
    cached = movers_cache.get(cache_key, 600)  # 10 Min Cache
    if cached:
        return cached
    
    movers = {'gainers': [], 'losers': [], 'most_active': []}
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
    
    # URLs für verschiedene Kategorien
    urls = {
        'gainers': 'https://finance.yahoo.com/gainers',
        'losers': 'https://finance.yahoo.com/losers',
        'most_active': 'https://finance.yahoo.com/most-active'
    }
    
    for category, url in urls.items():
        try:
            response = requests.get(url, headers=headers, timeout=10)
            if response.status_code == 200:
                tables = pd.read_html(StringIO(response.text))
                if tables and len(tables) > 0:
                    df = tables[0]
                    if 'Symbol' in df.columns:
                        # Top 15 pro Kategorie
                        symbols = df['Symbol'].head(15).tolist()
                        movers[category] = symbols
                        logger.info(f"{category}: {len(symbols)} Symbole geladen")
        except Exception as e:
            logger.error(f"Fehler beim Laden {category}: {e}")
    
    movers_cache.set(cache_key, movers)
    return movers

def get_combined_universe():
    """Kombiniere Watchlist mit Top Movers"""
    # Prüfe ob Movers-Update fällig (alle 15 Min)
    last_check = st.session_state.get('last_movers_check', 0)
    now = time.time()
    
    if now - last_check >= 900:  # 15 Minuten
        st.session_state['last_movers_check'] = now
        
        with st.spinner("🔄 Lade Top Movers..."):
            movers = fetch_yahoo_movers()
            st.session_state['top_movers_cache'] = movers
            
            # Kombiniere alle Symbole
            combined = set(st.session_state['watchlist'])
            for category, symbols in movers.items():
                combined.update(symbols)
            
            st.session_state['combined_universe'] = combined
            logger.info(f"Universum aktualisiert: {len(combined)} Symbole")
    
    return st.session_state['combined_universe']

def get_symbol_source(symbol):
    """Bestimme die Quelle eines Symbols"""
    if symbol in st.session_state['watchlist']:
        return 'watchlist'
    movers = st.session_state.get('top_movers_cache', {})
    if symbol in movers.get('gainers', []):
        return 'gainers'
    if symbol in movers.get('losers', []):
        return 'losers'
    if symbol in movers.get('most_active', []):
        return 'most_active'
    return 'unknown'

# ============================== News Functions ==============================
def get_finnhub_news_smart(symbol):
    """Hole News von Finnhub mit Caching und Rate Limiting"""
    cache_key = f"news_{symbol}"
    cached = news_cache.get(cache_key, 300)
    if cached:
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['cache_hits'] = stats.get('cache_hits', 0) + 1
            st.session_state['api_stats'] = stats
        return cached, True
    
    if not FINNHUB_API_KEY or len(FINNHUB_API_KEY) < 10:
        return None, False
    
    if not finnhub_limiter.can_call():
        return None, False
    
    try:
        url = f"https://finnhub.io/api/v1/company-news"
        params = {
            'symbol': symbol,
            'from': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
            'to': datetime.now().strftime('%Y-%m-%d'),
            'token': FINNHUB_API_KEY
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

def analyze_news_tiered(symbol, tier, score):
    """
    Analysiere News für ein Symbol basierend auf Tier und aktuellem Score
    Returns: (news_list, sources_list, was_cached)
    """
    if tier <= 20 or score > 60:
        news, cached = get_finnhub_news_smart(symbol)
        if news:
            sources = ['FH']
            for item in news[:1]:
                if any(word in item['title'].lower() for word in ['bullish', 'upgrade', 'beat', 'growth', 'positive', 'strong']):
                    item['score'] = 15
                elif any(word in item['title'].lower() for word in ['bearish', 'downgrade', 'miss', 'loss', 'negative', 'weak']):
                    item['score'] = 5
            return news, sources, cached
    
    return [], [], False

# ============================== Analyse Funktionen ==============================
def analyze_structure(df, symbol=None):
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

def _default_structure_result(df=None):
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
def get_alpha_vantage_smart(symbol):
    cache_key = f"av_fund_{symbol}"
    cached = fundamentals_cache.get(cache_key, 3600)
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
                            'eps': float(data.get('EPS', 0)) if data.get('EPS') and data.get('EPS') not in ['None', '0'] else None,
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

# ============================== Analyse Funktion ==============================
def analyze_smart(symbol, tier, total_tickers, market_ctx=None):
    debug_info = {'symbol': symbol, 'tier': tier, 'errors': [], 'checks': {}}
    
    # Progressive Pause für höhere Tiers
    if tier > 50:
        time.sleep(0.3)
    if tier > 80:
        time.sleep(0.6)
    
    max_retries = 3
    df = None
    last_error = None
    for attempt in range(max_retries):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(period='3mo', interval='1d')
            stats = st.session_state.get('api_stats', {})
            stats['yahoo'] = stats.get('yahoo', 0) + 1
            st.session_state['api_stats'] = stats
            break
        except Exception as e:
            last_error = str(e)
            if "Too Many Requests" in last_error or "Rate limit" in last_error:
                wait_time = (attempt + 1) * 3 + random.uniform(0, 2)
                time.sleep(wait_time)
            else:
                time.sleep(0.5)
    
    if df is None or df.empty:
        debug_info['errors'].append(f"Yahoo Fehler: {last_error}")
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
        debug_info['errors'].append(f"Pullback {pullback_pct:.2%} außerhalb Grenzen")
        _log_scan_debug(debug_info)
        return None
    
    structure = analyze_structure(df_clean, symbol)
    debug_info['checks']['structure'] = structure.get('structure_intact', False)
    debug_info['checks']['higher_lows'] = structure.get('higher_lows', False)
    
    if not structure.get('structure_intact', False) and not structure.get('higher_lows', False):
        debug_info['errors'].append("Kein bullischer Trend")
        _log_scan_debug(debug_info)
        return None
    
    last_swing_low = structure.get('last_swing_low')
    if last_swing_low is None or not np.isfinite(last_swing_low) or last_swing_low <= 0:
        debug_info['errors'].append("Kein Swing Low")
        _log_scan_debug(debug_info)
        return None
    
    if current_price < last_swing_low * 0.90:
        debug_info['errors'].append("Preis zu weit unter Swing Low")
        _log_scan_debug(debug_info)
        return None
    
    score = 25
    if structure.get('structure_intact', False):
        score += 15
    elif structure.get('higher_lows', False):
        score += 10
    
    trend_slope = structure.get('trend_slope', 0)
    if trend_slope is not None and np.isfinite(trend_slope) and trend_slope > 0.005:
        score += 5
    
    avg_vol = df_clean['Volume'].mean()
    current_vol = df_clean['Volume'].iloc[-1]
    rvol = current_vol / avg_vol if avg_vol > 0 else 1.0
    if rvol > 2:
        score += 20
    elif rvol > 1.0:
        score += 10
    
    support_dist = (current_price - last_swing_low) / current_price if current_price > 0 else 1.0
    if support_dist < 0.03:
        score += 15
    elif support_dist < 0.08:
        score += 8
    
    news, sources, cached_news = analyze_news_tiered(symbol, tier, score)
    if news:
        score += news[0]['score']
    
    fundamentals, fund_cached = None, False
    if score > 55 and tier <= 10:
        fundamentals, fund_cached = get_alpha_vantage_smart(symbol)
    
    pe_ratio = None
    if fundamentals:
        pe_ratio = fundamentals.get('pe_ratio')
        if pe_ratio is not None and np.isfinite(pe_ratio):
            if pe_ratio < 15:
                score += 8
            elif pe_ratio > 100:
                score -= 5
    
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
    if rr_ratio < 1.0:
        debug_info['errors'].append(f"R:R {rr_ratio:.2f} zu niedrig")
        _log_scan_debug(debug_info)
        return None
    
    # NEU: Filter - Nur Score >= 60
    if score < MIN_SCORE_THRESHOLD:
        debug_info['errors'].append(f"Score {score} < {MIN_SCORE_THRESHOLD} (Threshold)")
        _log_scan_debug(debug_info)
        return None
    
    reasons = [f"📉 -{pullback_pct:.1%}"]
    if structure.get('structure_intact', False):
        reasons.append("📈 Trend stark")
    elif structure.get('higher_lows', False):
        reasons.append("📈 Trend schwach")
    if rvol > 1.0:
        reasons.append(f"⚡ Vol {rvol:.1f}x")
    if support_dist < 0.03:
        reasons.append("🎯 Support nah")
    if news:
        reasons.append(f"📰 {news[0]['source']}")
    if pe_ratio is not None:
        reasons.append(f"{'💰' if pe_ratio<15 else '📊'} PE {pe_ratio:.1f}")
    
    # Quelle hinzufügen
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
        'source': source  # NEU: Quelle des Symbols
    }

def _log_scan_debug(debug_info):
    scan_debug = st.session_state.get('scan_debug', [])
    scan_debug.append(debug_info)
    st.session_state['scan_debug'] = scan_debug[-100:]

# ============================== Alert Management ==============================
def should_send_alert(symbol, current_price, current_score):
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

def record_alert(symbol, price, score, setup_type):
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
def send_telegram_alert(symbol, price, pullback_pct, news_item, setup_type, pe_ratio=None, api_sources=None, tier=None, source=None):
    if not TELEGRAM_BOT_TOKEN or len(TELEGRAM_BOT_TOKEN)<10:
        return False
    news_title = news_item.get('title','')[:40] + '...' if news_item else 'Keine News'
    news_url = news_item.get('url','') if news_item else f'https://finance.yahoo.com/quote/{symbol}'
    emoji = "🟣" if setup_type=="CATALYST" else "🏆" if setup_type=="GOLD" else "🐂"
    
    # Quelle-Emoji
    source_emoji = {
        'watchlist': '📋',
        'gainers': '🚀',
        'losers': '💎',
        'most_active': '🔥'
    }.get(source, '📊')
    
    pe_info = f"\n📊 P/E: {pe_ratio:.1f}" if pe_ratio else ""
    api_info = f"\n📡 {','.join(api_sources)}" if api_sources else ""
    tier_info = f"\n🎯 Tier {tier}" if tier else ""
    source_info = f"\n{source_emoji} Quelle: {source}" if source else ""
    
    msg = f"""{emoji} <b>{setup_type}: {symbol}</b> {emoji}
📉 Pullback: <b>-{pullback_pct:.1f}%</b>
💵 Preis: ${price:.2f}{pe_info}{api_info}{tier_info}{source_info}
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
def render_card_html(sym, price, pullback, sl, target, rr, reasons, news_item, tier_html, api_html, cache_html, conf_color, tv_url, score, rvol, pullback_color, source):
    news_title = news_item['title'][:40] + '...' if news_item else 'Keine News'
    news_url = news_item['url'] if news_item else f'https://finance.yahoo.com/quote/{sym}'
    
    # Quelle-Badge
    source_badges = {
        'watchlist': '<div class="tier-badge" style="background:#2d5a2d;">📋 WL</div>',
        'gainers': '<div class="mover-badge">🚀 GAINER</div>',
        'losers': '<div class="mover-badge" style="background:linear-gradient(90deg, #4ecdc4, #44a3aa);">💎 DIP</div>',
        'most_active': '<div class="mover-badge" style="background:linear-gradient(90deg, #FFD700, #FFA500);">🔥 ACTIVE</div>'
    }
    source_html = source_badges.get(source, '')
    
    html = f"""
    <div class="bull-card source-{source}">
        <div style="display:flex; justify-content:space-between; align-items:center;">
            <h3 style="margin:0;">🐂 {sym}</h3>
            {source_html}
        </div>
        <div class="pullback-badge" style="background: {pullback_color};">-{pullback:.1%}</div>
        <div style="margin: 5px 0;">{tier_html}{api_html}{cache_html}</div>
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
    return html

def render_card(item, container):
    score = item['score']
    sym = item['symbol']
    price = item['price']
    pullback = item['pullback_pct']
    sl = item['stop_loss']
    target = item['target']
    rr = item['rr_ratio']
    rvol = item['rvol']
    pe = item.get('pe_ratio')
    tier = item.get('tier', '-')
    source = item.get('source', 'unknown')
    reasons_txt = ' | '.join(item['reasons'][:3])
    news_found = item.get('news', [])
    apis = item.get('api_sources', [])
    cached = item.get('from_cache', False)
    tv_url = f'https://www.tradingview.com/chart/?symbol={sym}'

    pullback_color = '#ff6b6b' if pullback > 0.15 else '#ffa502'
    conf_color = '#9933ff' if score > 85 else '#FFD700' if score > 70 else '#00FF00'
    tier_html = f'<div class="tier-badge">T{tier}</div>'
    api_html = ''.join([f'<div class="tier-badge">{a}</div>' for a in apis])
    cache_html = '<div class="cache-badge">CACHE</div>' if cached else ''
    html = render_card_html(sym, price, pullback, sl, target, rr, reasons_txt, 
                            news_found[0] if news_found else None, tier_html, api_html, cache_html, conf_color, tv_url, score, rvol, pullback_color, source)
    with container:
        st.markdown(html, unsafe_allow_html=True)

# ============================== Main ==============================
clock = get_market_clock()

if clock.get('is_holiday'):
    st.markdown(f"""
    <div class="holiday-banner">
        🎌 US MARKET HOLIDAY - Presidents' Day 🎌<br>
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

# ============================== Automatisches Refresh ==============================
# NEU: Prüfe auf Movers-Update alle 15 Min
if st.session_state.get('auto_refresh'):
    last = st.session_state.get('last_auto_refresh', 0)
    last_movers = st.session_state.get('last_movers_check', 0)
    now = time.time()
    
    # Movers-Check alle 15 Min
    if now - last_movers >= AUTO_REFRESH_INTERVAL:
        st.session_state['last_movers_check'] = now
        # Lade Movers im Hintergrund
        movers = fetch_yahoo_movers()
        st.session_state['top_movers_cache'] = movers
        # Aktualisiere kombiniertes Universum
        combined = set(st.session_state['watchlist'])
        for category, symbols in movers.items():
            combined.update(symbols)
        st.session_state['combined_universe'] = combined
        logger.info(f"Movers aktualisiert: {len(combined)} Symbole")
        st.rerun()
    
    # Normaler Refresh
    if now - last >= AUTO_REFRESH_INTERVAL:
        st.session_state['last_auto_refresh'] = now
        st.session_state['refresh_count'] = st.session_state.get('refresh_count', 0) + 1
        st.rerun()

# ============================== Sidebar & API Status ==============================
with st.sidebar:
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

    # NEU: Movers Status
    st.divider()
    st.header("🚀 Top Movers")
    movers = st.session_state.get('top_movers_cache', {})
    if movers:
        cols = st.columns(3)
        with cols[0]:
            st.metric("Gainers", len(movers.get('gainers', [])))
        with cols[1]:
            st.metric("Losers", len(movers.get('losers', [])))
        with cols[2]:
            st.metric("Active", len(movers.get('most_active', [])))
        
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
                data = yf.Ticker("AAPL").history(period="5d")
                if not data.empty:
                    st.success(f"✅ Yahoo OK! {len(data)} Tage")
                    stats = st.session_state.get('api_stats', {})
                    stats['yahoo'] = stats.get('yahoo', 0) + 1
                    st.session_state['api_stats'] = stats
                else:
                    st.error("❌ Keine Daten")
            except:
                st.error("❌ Fehler")
    with col2:
        if st.button("Test Finnhub", use_container_width=True):
            news, cached = get_finnhub_news_smart("TSLA")
            if news:
                st.success(f"✅ Finnhub OK! {len(news)} News")
            else:
                st.error("❌ Keine News")
    
    # NEU: Manuelle Movers-Aktualisierung
    if st.button("🔄 Movers jetzt laden", use_container_width=True):
        with st.spinner("Lade Movers..."):
            movers = fetch_yahoo_movers()
            st.session_state['top_movers_cache'] = movers
            combined = set(st.session_state['watchlist'])
            for category, symbols in movers.items():
                combined.update(symbols)
            st.session_state['combined_universe'] = combined
            st.success(f"✅ {len(combined)} Symbole geladen")
            st.rerun()
    
    st.divider()
    st.header("🔍 Manuelle Abfrage")
    manual_symbol = st.text_input("Symbol:", placeholder="z.B. NVDA", key="manual").upper()
    if st.button("📊 Analyse starten") and manual_symbol:
        with st.spinner(f"Analysiere {manual_symbol}..."):
            result = analyze_smart(manual_symbol, 1, 1)
            if result:
                st.success(f"✅ Setup gefunden für {manual_symbol}!")
                st.json({
                    'Symbol': result['symbol'],
                    'Score': result['score'],
                    'Price': result['price'],
                    'Pullback': f"{result['pullback_pct']:.2%}",
                    'R:R': f"{result['rr_ratio']:.1f}x",
                    'APIs': result['api_sources'],
                    'Source': result.get('source', 'unknown')
                })
            else:
                st.error(f"❌ Kein Setup für {manual_symbol} (Score < {MIN_SCORE_THRESHOLD} oder keine Daten)")
                scan_debug = st.session_state.get('scan_debug', [])
                if scan_debug:
                    last = scan_debug[-1]
                    st.write("Checks:", last.get('checks', {}))
                    st.write("Fehler:", last.get('errors', []))
    
    if st.button("🔄 Stats zurücksetzen"):
        st.session_state['api_stats'] = {'yahoo':0,'finnhub':0,'alpha_vantage':0,'cache_hits':0,'alpha_rotation_count':0}
        st.session_state['scan_debug'] = []
        st.session_state['top_movers_cache'] = {}
        st.session_state['last_movers_check'] = 0
        for i in alpha_manager.limiters:
            alpha_manager.limiters[i]['exhausted'] = False
            alpha_manager.limiters[i]['calls_today'] = 0
            alpha_manager.limiters[i]['calls_per_min'] = []
        st.success("Zurückgesetzt!")
        st.rerun()

# ============================== Haupt-Scan Button ==============================
scan_triggered = False
if st.button('🚀 Smart Scan Starten'):
    scan_triggered = True

if scan_triggered:
    with st.spinner("🔍 Scanne mit Yahoo Finance..."):
        market_ctx = get_market_context()
        if market_ctx.get('market_closed'):
            st.warning("⚠️ Markt ist möglicherweise geschlossen.")
        
        # NEU: Hole kombiniertes Universum (Watchlist + Movers)
        universe = get_combined_universe()
        st.info(f"📊 Scanne {len(universe)} Symbole ({len(st.session_state['watchlist'])} Watchlist + {len(universe) - len(st.session_state['watchlist'])} Movers)")
        
        st.session_state['scan_debug'] = []
        results = []
        progress = st.progress(0)
        status_text = st.empty()
        error_count = 0
        success_count = 0
        
        # Konvertiere zu Liste mit Quellen-Info für Anzeige
        scan_list = []
        for sym in universe:
            source = get_symbol_source(sym)
            scan_list.append((sym, source))
        
        # Sortiere: Watchlist zuerst, dann Movers
        scan_list.sort(key=lambda x: 0 if x[1] == 'watchlist' else 1)

        for i, (sym, source) in enumerate(scan_list):
            tier = i + 1
            status_text.text(f"Analysiere: {sym} [{source}] ({tier}/{len(scan_list)}) - OK:{success_count} Fehler:{error_count}")
            
            try:
                res = analyze_smart(sym, tier, len(scan_list), market_ctx)
                if res:
                    existing = [r for r in results if r['symbol'] == sym]
                    if not existing or res['score'] > existing[0]['score']:
                        results = [r for r in results if r['symbol'] != sym]
                        results.append(res)
                        success_count += 1
                else:
                    error_count += 1
            except:
                error_count += 1
            
            progress.progress((i+1)/len(scan_list))
            
            # VERBESSERTES Rate Limiting
            if i % 3 == 0 and i > 0:
                time.sleep(1.5)
            if i % 10 == 0 and i > 0:
                time.sleep(3.0)
        
        progress.empty()
        status_text.empty()
        st.session_state['scan_results'] = results
        st.session_state['last_scan_time'] = datetime.now()

        # Alerts nur für Score > 75
        alerts_sent_this_scan = 0
        for item in results:
            if item['score'] > 75:
                symbol = item['symbol']
                price = item['price']
                score = item['score']
                if should_send_alert(symbol, price, score):
                    setup_type = "CATALYST" if (item.get('news') and item['news'][0].get('tier', 0) == 1) else "GOLD"
                    success = send_telegram_alert(
                        symbol, price, item['pullback_pct'],
                        item['news'][0] if item.get('news') else None,
                        setup_type, item.get('pe_ratio'),
                        item.get('api_sources'), item.get('tier'),
                        item.get('source')
                    )
                    if success:
                        record_alert(symbol, price, score, setup_type)
                        alerts_sent_this_scan += 1
                        if alerts_sent_this_scan <= 3:
                            st.toast(f"🚨 {setup_type} Alert: {symbol} @ ${price:.2f} (Score: {score})")

# ============================== Ergebnisse anzeigen ==============================
results = st.session_state.get('scan_results', [])
if results:
    # NEU: Filter-Info
    col1, col2, col3 = st.columns([2,1,1])
    with col1:
        st.subheader(f"📊 Gefundene Setups: {len(results)} (Score ≥ {MIN_SCORE_THRESHOLD})")
    with col2:
        # Zeige Verteilung nach Quelle
        sources = {}
        for r in results:
            src = r.get('source', 'unknown')
            sources[src] = sources.get(src, 0) + 1
        
        source_text = " | ".join([f"{k}: {v}" for k, v in sources.items()])
        st.caption(f"Quellen: {source_text}")
    with col3:
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
    
    # NEU: Quellen-Statistik
    st.success(f"✅ APIs in Ergebnissen: {api_summary} | Cache: {cache_count}")
    
    def render_results_grid(results):
        results_sorted = sorted(results, key=lambda x: (x['score'], x['pullback_pct']), reverse=True)
        cols = st.columns(4)
        for i, r in enumerate(results_sorted[:16]):
            with cols[i % 4]:
                render_card(r, st.container())
    
    render_results_grid(results)
    
    with st.expander("📡 API Details"):
        st.write(f"**Yahoo Finance:** {stats.get('yahoo', 0)} Calls (unbegrenzt)")
        st.write(f"**Finnhub:** {finnhub_limiter.get_status()}/60 pro Minute")
        st.write(f"**Alpha Vantage:** {stats.get('alpha_vantage', 0)}/25 pro Tag")
        ctx = get_market_context()
        st.write(f"**Marktkontext:** {'Risk-Off' if ctx.get('risk_off') else 'Risk-On'}")
        
        # NEU: Zeige Universum-Zusammensetzung
        universe = st.session_state.get('combined_universe', set())
        st.write(f"**Scan-Universum:** {len(universe)} Symbole")
        st.write(f"- Watchlist: {len(st.session_state['watchlist'])}")
        movers = st.session_state.get('top_movers_cache', {})
        if movers:
            st.write(f"- Gainers: {len(movers.get('gainers', []))}")
            st.write(f"- Losers: {len(movers.get('losers', []))}")
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
    st.info("👆 Klicke 'Smart Scan Starten' um die Watchlist + Top Movers zu analysieren!")
