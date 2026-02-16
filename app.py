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
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================== Helper Functions ==============================
def safe_requests_get(url, params=None, headers=None, timeout=10):
    try:
        response = requests.get(url, params=params, headers=headers, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"API-Request Fehler: {e}")
        st.error(f"API-Request Fehler: {e}")
        return None

def get_market_clock():
    et = pytz.timezone('US/Eastern')
    now = datetime.now(et)
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    pre_market = now.replace(hour=4, minute=0, second=0, microsecond=0)
    after_hours = now.replace(hour=20, minute=0, second=0, microsecond=0)

    if now.weekday() >= 5:
        status = "CLOSED"
        color = "#ff4b4b"
        bg_color = "#3a1e1e"
        countdown = "Weekend"
        next_event = "Monday 09:30 ET"
        progress = 0
    elif now < pre_market:
        status = "CLOSED"
        color = "#ff4b4b"
        bg_color = "#3a1e1e"
        countdown = f"Pre-market in {str(pre_market - now)[:8]}"
        next_event = "04:00 ET"
        progress = 0
    elif now < market_open:
        status = "PRE-MARKET"
        color = "#FFD700"
        bg_color = "#2b2b00"
        countdown = f"Opens in {str(market_open - now)[:8]}"
        next_event = "09:30 ET"
        progress = 0
    elif market_open <= now <= market_close:
        status = "OPEN"
        color = "#00FF00"
        bg_color = "#0d1f12"
        countdown = f"Closes in {str(market_close - now)[:8]}"
        next_event = "16:00 ET"
        progress = (now - market_open) / (market_close - market_open)
    elif now < after_hours:
        status = "AFTER HOURS"
        color = "#58a6ff"
        bg_color = "#1a1a2e"
        countdown = f"Ext. hours {str(after_hours - now)[:8]}"
        next_event = "20:00 ET"
        progress = 0
    else:
        status = "CLOSED"
        color = "#ff4b4b"
        bg_color = "#3a1e1e"
        countdown = "Opens tomorrow"
        next_event = "09:30 ET"
        progress = 0

    return {
        'time': now.strftime('%I:%M:%S %p'),
        'status': status,
        'color': color,
        'bg_color': bg_color,
        'countdown': countdown,
        'next_event': next_event,
        'progress': progress,
        'is_open': status == "OPEN"
    }

def get_market_context():
    """Analysiert den generellen Marktkontext (Risk-On/Off)"""
    try:
        spy = yf.Ticker("SPY")
        spy_data = spy.history(period="5d")
        if len(spy_data) < 2:
            return {'risk_off': False, 'spy_change': 0}
        spy_change = (spy_data['Close'].iloc[-1] - spy_data['Close'].iloc[-2]) / spy_data['Close'].iloc[-2]
        try:
            vix = yf.Ticker("^VIX")
            vix_data = vix.history(period="2d")
            vix_level = vix_data['Close'].iloc[-1] if not vix_data.empty else 20
        except:
            vix_level = 20
        risk_off = (spy_change < -0.02) or (vix_level > 30)
        return {'risk_off': risk_off, 'spy_change': spy_change, 'vix_level': vix_level}
    except Exception as e:
        logger.error(f"Fehler beim Marktkontext: {e}")
        return {'risk_off': False, 'spy_change': 0, 'vix_level': 20}

# ============================== Konfiguration ==============================
st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V5.5 - Market Clock", page_icon="🐂")
MIN_PULLBACK_PERCENT = 0.10
MAX_PULLBACK_PERCENT = 0.25
AUTO_REFRESH_INTERVAL = 30
MAX_WORKERS = 4
# NEU: Alert-Cooldown in Minuten (verhindert Spam)
ALERT_COOLDOWN_MINUTES = 60

try:
    TELEGRAM_BOT_TOKEN = st.secrets["telegram"]["bot_token"]
    TELEGRAM_CHAT_ID = st.secrets["telegram"]["chat_id"]
    FINNHUB_API_KEY = st.secrets["api_keys"]["finnhub"]
    ALPHA_VANTAGE_KEYS = [
        st.secrets["api_keys"]["alpha_vantage_1"],
        st.secrets["api_keys"]["alpha_vantage_2"],
        st.secrets["api_keys"]["alpha_vantage_3"]
    ]
except:
    logger.warning("Secrets nicht gefunden, benutze Defaults.")
    TELEGRAM_BOT_TOKEN = ""
    TELEGRAM_CHAT_ID = ""
    FINNHUB_API_KEY = ""
    ALPHA_VANTAGE_KEYS = ["", "", ""]

DEFAULT_WATCHLIST = sorted(list(set([
    "ABBV", "ACHV", "ACRS", "ADMA", "ALDX", "ALNY", "AMD", "AMGN", "AMRN", "APLS", "AQST", "ASND",
    "ATOS", "ATRA", "AVXL", "AZN", "BCRX", "BEAM", "BIIB", "BLTE", "BMRN", "BMY", "BNTX", "CELC",
    "CHRS", "CING", "COIN", "CRSP", "CRVS", "CRWD", "CURE", "DNLI", "EDIT", "ETON", "EVFM", "EXEL",
    "FATE", "GERN", "GILD", "GOSS", "GSK", "HALO", "HOOD", "HUT", "IBRX", "INO", "IONS", "IOVA",
    "JAZZ", "JNJ", "LCTX", "LLY", "LNTH", "MARA", "MCRB", "MNKD", "MRK", "MRNA", "MSTR", "NKTR",
    "NTLA", "NVAX", "NVDA", "NVS", "OCGN", "PFE", "PLRX", "PLTR", "QGEN", "RAPT", "RCKT", "REGN",
    "REPL", "RIGL", "RIOT", "RLAY", "ROG", "RYTM", "SAP", "SAVA", "SENS", "SNY", "SRPT", "TAK",
    "TBPH", "TSLA", "TXMD", "UUUU", "VIVK", "VNDA", "VRTX", "VTYX", "VXRT", "XERS", "ZLAB"
])))

# ============================== Session State ==============================
def init_session_state():
    defaults = {
        'watchlist': DEFAULT_WATCHLIST,
        # NEU: Alerts mit Zeitstempel statt nur Set
        'sent_alerts': {},  # Format: {symbol: {'timestamp': datetime, 'price': float, 'score': int}}
        'api_stats': {
            'finnhub': 0,
            'alpha_vantage': 0,
            'alpha_rotation_count': 0,
            'cache_hits': 0
        },
        'scan_results': [],
        'last_scan_time': None,
        'auto_refresh': False,
        'refresh_count': 0,
        'last_auto_refresh': 0,
        # NEU: Alert-History für UI-Anzeige
        'alert_history': [],
    }
    for k, v in defaults.items():
        if k not in st.session_state:
            st.session_state[k] = v

init_session_state()

# ============================== CSS Styles ==============================
st.markdown("""
<style>
.stMetric { background-color: #0E1117; padding: 10px; border-radius: 5px; }
.market-clock-container {
    background: linear-gradient(135deg, #161b22 0%, #0d1117 100%);
    border: 2px solid #30363d;
    border-radius: 15px;
    padding: 20px;
    margin: 10px 0;
    text-align: center;
}
.market-time {
    font-size: 2.5rem;
    font-weight: bold;
    color: white;
    font-family: 'Courier New', monospace;
    text-shadow: 0 0 10px rgba(255,255,255,0.1);
}
.market-status {
    display: inline-block;
    padding: 8px 20px;
    border-radius: 20px;
    font-weight: bold;
    font-size: 1.2rem;
    margin: 10px 0;
}
.market-countdown {
    font-size: 1.1rem;
    color: #FFD700;
    margin-top: 5px;
}
.market-progress {
    width: 100%;
    height: 6px;
    background: #333;
    border-radius: 3px;
    margin-top: 10px;
    overflow: hidden;
}
.market-progress-bar {
    height: 100%;
    background: linear-gradient(90deg, #238636, #00FF00);
    border-radius: 3px;
    transition: width 1s ease;
}
@keyframes greenPulse { 0% { box-shadow: 0 0 5px #00FF00; } 100% { box-shadow: 0 0 15px #00FF00; } }
@keyframes purplePulse { 0% { box-shadow: 0 0 5px #9933ff; } 100% { box-shadow: 0 0 30px #bf80ff; } }
@keyframes goldPulse { 0% { box-shadow: 0 0 5px #FFD700; } 100% { box-shadow: 0 0 25px #FFD700; } }
.bull-card { background-color: #0d1f12; border: 2px solid #00FF00; border-radius: 10px; padding: 15px; text-align: center; margin-bottom: 10px; animation: greenPulse 2.0s infinite alternate; }
.gold-card { background-color: #2b2b00; border: 3px solid #FFD700; border-radius: 10px; padding: 15px; text-align: center; margin-bottom: 10px; animation: goldPulse 1.5s infinite alternate; }
.purple-card { background-color: #1a0033; border: 3px solid #9933ff; border-radius: 10px; padding: 15px; text-align: center; margin-bottom: 10px; animation: purplePulse 0.8s infinite alternate; }
.bull-card h3 { color: #00FF00 !important; margin: 0; }
.gold-card h3 { color: #FFD700 !important; margin: 0; text-shadow: 0 0 10px #FFD700; }
.purple-card h3 { color: #bf80ff !important; margin: 0; text-shadow: 0 0 10px #9933ff; }
.price { font-size: 1.8rem; font-weight: bold; color: white; margin: 10px 0; }
.pullback-badge { background: linear-gradient(45deg, #ff6b6b, #ee5a24); color: white; padding: 4px 12px; border-radius: 12px; font-size: 0.9rem; font-weight: bold; display: inline-block; margin: 5px 0; }
.tier-badge { background: linear-gradient(45deg, #667eea, #764ba2); color: white; padding: 2px 8px; border-radius: 8px; font-size: 0.7rem; display: inline-block; margin: 2px; }
.cache-badge { background: linear-gradient(45deg, #11998e, #38ef7d); color: white; padding: 2px 6px; border-radius: 6px; font-size: 0.65rem; display: inline-block; margin: 2px; }
.rotation-badge { background: linear-gradient(45deg, #ff6b6b, #feca57); color: white; padding: 2px 8px; border-radius: 8px; font-size: 0.7rem; display: inline-block; margin: 2px; }
.stop-loss { color: #ff9999; font-weight: bold; font-size: 0.9rem; border: 1px solid #ff4b4b; border-radius: 4px; padding: 2px 8px; display: inline-block; }
.target { color: #90EE90; font-weight: bold; font-size: 0.9rem; border: 1px solid #00FF00; border-radius: 4px; padding: 2px 8px; display: inline-block; margin-left: 5px; }
.btn-link { display: inline-block; background-color: #262730; padding: 5px 15px; border-radius: 5px; text-decoration: none; font-size: 0.9rem; border: 1px solid #555; color: white !important; }
.confidence-bar { width: 100%; height: 4px; background: #333; margin: 5px 0; border-radius: 2px; }
.confidence-fill { height: 100%; border-radius: 2px; }
.api-stat { background: #1c1c1c; padding: 8px; border-radius: 5px; margin: 2px 0; font-size: 0.8rem; }
.news-link-btn { display: block; background-color: rgba(255,255,255,0.1); color: #fff !important; padding: 8px; border-radius: 5px; font-size: 0.8rem; margin: 8px 0; border: 1px solid #777; text-decoration: none; text-align: left; }
.key-indicator { font-size: 0.7rem; padding: 2px 6px; border-radius: 4px; background: #2d2d2d; border: 1px solid #444; display: inline-block; margin: 2px; }
.key-active { background: #1e3a1e; border-color: #00FF00; color: #00FF00; }
.key-exhausted { background: #3a1e1e; border-color: #ff4b4b; color: #ff4b4b; }
/* NEU: Alert-History Styles */
.alert-history-item {
    background: #1a1a2e;
    border-left: 4px solid #00FF00;
    padding: 10px;
    margin: 5px 0;
    border-radius: 5px;
    font-size: 0.85rem;
}
</style>
""", unsafe_allow_html=True)

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
        self.limiters = {}
        for i, key in enumerate(self.keys):
            self.limiters[i] = {'calls_today': 0, 'calls_per_min': [], 'key': key, 'exhausted': False}
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
        if limiter['calls_today'] >= 500:
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
        status = []
        for i, key in enumerate(self.keys):
            limiter = self.limiters[i]
            status.append({
                'index': i,
                'key': f"{key[:4]}...{key[-4:]}" if len(key)>8 else key,
                'active': i == self.current_index,
                'calls_today': limiter['calls_today'],
                'exhausted': limiter['exhausted'],
                'can_call': self.can_call(i)
            })
        return status

finnhub_limiter = RateLimiter(60, 60)
alpha_manager = AlphaVantageManager(ALPHA_VANTAGE_KEYS)

# ============================== Analyse Funktionen ==============================
def analyze_structure(df, symbol=None):
    """Swing Highs / Lows Analyse"""
    if symbol:
        cache_key = f"structure_{symbol}"
        cached = structure_cache.get(cache_key, 300)
        if cached:
            return cached
    try:
        highs = df['High'].values
        lows = df['Low'].values
        swing_highs = []
        swing_lows = []
        for i in range(2, len(highs)-2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append((i, highs[i]))
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append((i, lows[i]))
        if len(swing_highs)>=2 and len(swing_lows)>=2:
            hh = swing_highs[-1][1] > swing_highs[-2][1]
            hl = swing_lows[-1][1] > swing_lows[-2][1]
            slope = 0
            if len(swing_highs)>=3:
                x = [swing_highs[-3][0], swing_highs[-2][0], swing_highs[-1][0]]
                y = [swing_highs[-3][1], swing_highs[-2][1], swing_highs[-1][1]]
                slope = simple_slope(x,y)
            result = {
                'higher_highs': hh,
                'higher_lows': hl,
                'trend_slope': slope,
                'structure_intact': hh and hl,
                'last_swing_low': swing_lows[-1][1],
                'last_swing_high': swing_highs[-1][1]
            }
        else:
            result = {
                'structure_intact': False,
                'last_swing_low': df['Low'].tail(5).min(),
                'last_swing_high': df['High'].tail(20).max()
            }
        if symbol:
            structure_cache.set(cache_key, result)
        return result
    except Exception as e:
        logger.error(f"Fehler in analyze_structure: {e}")
        return {
            'structure_intact': False,
            'last_swing_low': df['Low'].tail(5).min(),
            'last_swing_high': df['High'].tail(20).max()
        }

def simple_slope(x_list, y_list):
    """Berechnet die Steigung"""
    n = len(x_list)
    if n<2:
        return 0
    x_mean = sum(x_list)/n
    y_mean = sum(y_list)/n
    numerator = sum((x_list[i]-x_mean)*(y_list[i]-y_mean) for i in range(n))
    denominator = sum((x_list[i]-x_mean)**2 for i in range(n))
    return numerator/denominator if denominator != 0 else 0

def get_yahoo_news_fallback(symbol):
    """Yahoo News fallback"""
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        if news:
            return [{'headline': n.get('title'), 'url': n.get('link'), 'source': 'Yahoo', 'datetime': 0} for n in news[:5]]
    except Exception as e:
        logger.error(f"Yahoo News Fehler für {symbol}: {e}")
    return []

def get_finnhub_news_smart(symbol):
    """Finnhub News mit Cache"""
    cache_key = f"fh_news_{symbol}"
    cached = news_cache.get(cache_key, 600)
    if cached:
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['cache_hits'] = stats.get('cache_hits', 0)+1
            st.session_state['api_stats'] = stats
        return cached, True
    if not finnhub_limiter.can_call():
        return [], False
    if not FINNHUB_API_KEY or len(FINNHUB_API_KEY)<10:
        return [], False
    try:
        end = int(time.time())
        start = end - 3600*48
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            'symbol': symbol,
            'from': datetime.fromtimestamp(start).strftime('%Y-%m-%d'),
            'to': datetime.fromtimestamp(end).strftime('%Y-%m-%d'),
            'token': FINNHUB_API_KEY
        }
        response = safe_requests_get(url, params)
        if response:
            finnhub_limiter.record_call()
            stats = st.session_state.get('api_stats', {})
            if isinstance(stats, dict):
                stats['finnhub'] = stats.get('finnhub',0)+1
                st.session_state['api_stats'] = stats
            data = response.json()
            if isinstance(data, list):
                data.sort(key=lambda x:x.get('datetime',0), reverse=True)
                result = data[:5]
                news_cache.set(cache_key, result)
                return result, False
    except Exception as e:
        logger.error(f"Finnhub News Fehler: {e}")
    return [], False

def get_alpha_vantage_smart(symbol):
    """AlphaVantage mit Cache & Rotation"""
    cache_key = f"av_fund_{symbol}"
    cached = fundamentals_cache.get(cache_key, 3600)
    if cached:
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['cache_hits'] = stats.get('cache_hits',0)+1
            st.session_state['api_stats'] = stats
        return cached, True
    if not alpha_manager.keys:
        return None, False
    attempts = 0
    max_attempts = len(alpha_manager.keys)
    while attempts<max_attempts:
        if alpha_manager.can_call():
            current_key = alpha_manager.get_current_key()
            url = "https://www.alphavantage.co/query"
            params = {'function':'OVERVIEW','symbol':symbol,'apikey':current_key}
            response = safe_requests_get(url, params)
            if response:
                data = response.json()
                if 'Note' in data or 'Information' in data:
                    alpha_manager.limiters[alpha_manager.current_index]['exhausted'] = True
                    alpha_manager.rotate_key()
                    attempts +=1
                    time.sleep(0.5)
                    continue
                if 'Symbol' in data:
                    result = {
                        'pe_ratio': float(data.get('PERatio',0)) if data.get('PERatio') else None,
                        'eps': float(data.get('EPS',0)) if data.get('EPS') else None,
                        'sector': data.get('Sector',''),
                        'industry': data.get('Industry',''),
                        'market_cap': int(data.get('MarketCapitalization',0)) if data.get('MarketCapitalization') else 0
                    }
                    fundamentals_cache.set(cache_key, result)
                    return result, False
            else:
                alpha_manager.rotate_key()
                attempts += 1
                continue
        else:
            alpha_manager.rotate_key()
            attempts +=1
    return None, False

def analyze_news_tiered(symbol, tier, prelim_score):
    """Tiered News-Analyse"""
    keywords_tier1 = ['fda approval', 'fda approved', 'phase 3 success', 'merger', 'acquisition', 'buyout']
    keywords_tier2 = ['earnings beat', 'guidance raised', 'upgrade', 'partnership']
    news_items = []
    sources = []
    from_cache = False

    if tier <= 10 and prelim_score > 60:
        fh_news, cached = get_finnhub_news_smart(symbol)
        from_cache = cached
        if fh_news:
            sources.append("FH")
            for item in fh_news:
                title = item.get('headline', '').lower()
                if any(k in title for k in keywords_tier1):
                    news_items.append({'title': item['headline'], 'url': item['url'], 'tier': 1, 'score': 40, 'source': 'FH'})
                elif any(k in title for k in keywords_tier2):
                    news_items.append({'title': item['headline'], 'url': item['url'], 'tier': 2, 'score': 25, 'source': 'FH'})

    if not news_items and tier <= 30:
        yh_news = get_yahoo_news_fallback(symbol)
        if yh_news:
            sources.append("YF")
            for item in yh_news:
                title = item.get('headline', '').lower()
                if any(k in title for k in keywords_tier1):
                    news_items.append({'title': item['headline'], 'url': item['url'], 'tier': 1, 'score': 35, 'source': 'YF'})
                elif any(k in title for k in keywords_tier2):
                    news_items.append({'title': item['headline'], 'url': item['url'], 'tier': 2, 'score': 20, 'source': 'YF'})
    news_items.sort(key=lambda x: x['score'], reverse=True)
    return news_items[:3], sources, from_cache

# ============================== Analyse Hauptfunktion ==============================
def analyze_smart(symbol, tier, total_tickers, market_ctx=None):
    """Hauptanalyse Funktion"""
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period='3mo', interval='1h')
        if df.empty or len(df)<50:
            return None
        if df['Volume'].mean() < 50000:
            return None
        hourly_ret = df['Close'].pct_change().abs()
        if hourly_ret.max() > 0.3:
            return None
        current_price = df['Close'].iloc[-1]
        lookback = min(70, len(df)-10)
        recent = df.tail(lookback)
        recent_high = recent['High'].max()
        pullback_pct = (recent_high - current_price) / recent_high
        if pullback_pct < MIN_PULLBACK_PERCENT or pullback_pct > MAX_PULLBACK_PERCENT:
            return None
        structure = analyze_structure(df, symbol)
        if not structure['structure_intact']:
            return None
        if current_price < structure['last_swing_low']*0.98:
            return None
        # Score
        score = 30
        if structure['trend_slope'] > 0.02:
            score += 15
        # Volatilität
        avg_vol = df['Volume'].tail(20).mean()
        current_vol = df['Volume'].iloc[-1]
        rvol = current_vol/avg_vol if avg_vol>0 else 1
        if rvol>2:
            score += 20
        elif rvol>1.5:
            score += 10
        # Unterstützung
        support_dist = (current_price - structure['last_swing_low'])/current_price
        if support_dist < 0.03:
            score += 15
        elif support_dist < 0.05:
            score += 8
        # News & fundamentale Daten
        news, sources, cached_news = analyze_news_tiered(symbol, tier, score)
        if news:
            score += news[0]['score']
        fundamentals, fund_cached = None, False
        if tier <= 15 and score >= 65:
            fundamentals, fund_cached = get_alpha_vantage_smart(symbol)
        pe_ratio = None
        if fundamentals:
            pe_ratio = fundamentals.get('pe_ratio')
            if pe_ratio:
                if pe_ratio < 15:
                    score += 8
                elif pe_ratio > 100:
                    score -= 5
        # ATR & Stop-Loss
        atr = df['High'].rolling(14).max().mean() - df['Low'].rolling(14).min().mean()
        technical_stop = structure['last_swing_low']*0.985
        atr_stop = current_price - (2*atr)
        stop_loss = max(technical_stop, atr_stop)
        recent_high = df['High'].max()
        target1 = recent_high*0.98
        target2 = current_price + (current_price - stop_loss)*2
        target = min(target1, target2)
        risk = current_price - stop_loss
        reward = target - current_price
        rr_ratio = reward/risk if risk>0 else 0

        if rr_ratio<1.5:
            return None
        if market_ctx and market_ctx.get('risk_off'):
            score -= 10
        if score<50:
            return None
        # Gründe & Quellen
        reasons = [f"📉 -{pullback_pct:.1f}%"]
        if structure['trend_slope'] > 0.02:
            reasons.append("📈 Trend")
        if rvol>1.5:
            reasons.append(f"⚡ Vol {rvol:.1f}x")
        if support_dist<0.03:
            reasons.append("🎯 Support")
        if news:
            reasons.append(f"📰 {news[0]['source']}")
        if pe_ratio:
            reasons.append(f"{'💰' if pe_ratio<15 else '📊'} PE {pe_ratio:.1f}")
        all_sources = [s for s in sources]
        # Ergebnis
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
            'api_sources': list(set(all_sources)),
            'from_cache': cached_news or fund_cached
        }
    except Exception as e:
        logger.error(f"Analyse Fehler {symbol}: {e}")
        return None

# ============================== Alert Management ==============================
# NEU: Verbesserte Alert-Logik mit Cooldown
def should_send_alert(symbol, current_price, current_score):
    """
    Prüft ob ein Alert gesendet werden soll basierend auf:
    1. Cooldown-Zeit (keine Alerts für X Minuten nach letztem Alert)
    2. Signifikanter Preisänderung (>2%)
    3. Score-Verbesserung (>10 Punkte)
    """
    sent_alerts = st.session_state.get('sent_alerts', {})
    now = datetime.now()
    
    if symbol not in sent_alerts:
        return True
    
    last_alert = sent_alerts[symbol]
    time_diff = (now - last_alert['timestamp']).total_seconds() / 60  # Minuten
    
    # Cooldown noch aktiv?
    if time_diff < ALERT_COOLDOWN_MINUTES:
        return False
    
    # Signifikante Änderung?
    price_change = abs(current_price - last_alert['price']) / last_alert['price']
    score_change = current_score - last_alert['score']
    
    # Nur alerten wenn sich etwas signifikant geändert hat
    if price_change < 0.02 and score_change < 10:
        logger.info(f"Alert für {symbol} unterdrückt: Keine signifikante Änderung (Preis: {price_change:.2%}, Score: +{score_change})")
        return False
    
    return True

def record_alert(symbol, price, score, setup_type):
    """Speichert einen Alert mit Zeitstempel"""
    st.session_state['sent_alerts'][symbol] = {
        'timestamp': datetime.now(),
        'price': price,
        'score': score,
        'setup_type': setup_type
    }
    # Auch in History für UI-Anzeige
    st.session_state['alert_history'].append({
        'timestamp': datetime.now(),
        'symbol': symbol,
        'price': price,
        'score': score,
        'setup_type': setup_type
    })
    # History auf letzte 20 Einträge begrenzen
    st.session_state['alert_history'] = st.session_state['alert_history'][-20:]

# ============================== Telegram Alarm ==============================
def send_telegram_alert(symbol, price, pullback_pct, news_item, setup_type, pe_ratio=None, api_sources=None, tier=None):
    if not TELEGRAM_BOT_TOKEN or len(TELEGRAM_BOT_TOKEN)<10:
        return False  # NEU: Return False bei Fehler
    
    news_title = news_item.get('title','')[:40] + '...' if news_item else 'Keine News'
    news_url = news_item.get('url','') if news_item else f'https://finance.yahoo.com/quote/{symbol}'
    emoji = "🟣" if setup_type=="CATALYST" else "🏆" if setup_type=="GOLD" else "🐂"
    pe_info = f"\n📊 P/E: {pe_ratio:.1f}" if pe_ratio else ""
    api_info = f"\n📡 {','.join(api_sources)}" if api_sources else ""
    tier_info = f"\n🎯 Tier {tier}" if tier else ""
    msg = f"""{emoji} <b>{setup_type}: {symbol}</b> {emoji}
📉 Pullback: <b>-{pullback_pct:.1f}%</b>
💵 Preis: ${price:.2f}{pe_info}{api_info}{tier_info}
📰 {news_title}
👉 <a href='{news_url}'>News</a> | <a href='https://www.tradingview.com/chart/?symbol={symbol}'>Chart</a>"""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        requests.post(url, data=payload, timeout=5).raise_for_status()
        return True  # NEU: Return True bei Erfolg
    except Exception as e:
        logger.error(f"Telegram Fehlschlag: {e}")
        return False

# ============================== Karten HTML ==============================
def render_card_html(sym, price, pullback, sl, target, rr, reasons, news_item, tier_html, api_html, cache_html, conf_color, tv_url, score, rvol, pullback_color):
    news_title = news_item['title'][:40] + '...' if news_item else 'Keine News'
    news_url = news_item['url'] if news_item else f'https://finance.yahoo.com/quote/{sym}'
    html = f"""
    <div class="bull-card">
        <h3>🐂 {sym}</h3>
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
    """Karte rendern"""
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
                            news_found[0] if news_found else None, tier_html, api_html, cache_html, conf_color, tv_url, score, rvol, pullback_color)
    with container:
        st.markdown(html, unsafe_allow_html=True)

# ============================== Main ==============================
# Marktzeit
clock = get_market_clock()
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

st.title('🐂 Elite Bull Scanner Pro V5.5')
st.caption(f"3-Key Alpha Rotation | {len(alpha_manager.keys)} Keys aktiv | Market Clock | Echtzeit ET")

# Auto-Refresh
if st.session_state.get('auto_refresh'):
    last = st.session_state.get('last_auto_refresh', 0)
    if time.time() - last >= AUTO_REFRESH_INTERVAL:
        st.session_state['last_auto_refresh'] = time.time()
        st.session_state['refresh_count'] = st.session_state.get('refresh_count', 0) + 1
        st.rerun()

# Sidebar
with st.sidebar:
    st.header("📡 Smart API Status")
    stats = st.session_state.get('api_stats', {'finnhub':0,'alpha_vantage':0,'cache_hits':0,'alpha_rotation_count':0})
    fh_status = "🟢" if finnhub_limiter.can_call() else "🔴"
    alpha_status_list = alpha_manager.get_status()
    st.markdown(f"""
<div class="api-stat">
    <div style="display:flex; justify-content:space-between;">
        <span>Finnhub</span>
        <span>{fh_status} {finnhub_limiter.get_status()}/min</span>
    </div>
</div>
""", unsafe_allow_html=True)

    st.markdown("<div style='margin:10px 0;'><b>Alpha Vantage Keys:</b></div>", unsafe_allow_html=True)
    for status in alpha_status_list:
        cls = "key-active" if status['active'] else "key-exhausted" if status['exhausted'] else ""
        indicator = "▶️" if status['active'] else "✅" if not status['exhausted'] else "❌"
        st.markdown(f"""
<div class="key-indicator {cls}">
    {indicator} Key {status['index']+1}: {status['calls_today']}/500
</div>
""", unsafe_allow_html=True)

    rotations = stats.get('alpha_rotation_count',0)
    cache_hits = stats.get('cache_hits',0)
    st.markdown(f'<div class="rotation-badge">🔄 Rotationen: {rotations}</div>', unsafe_allow_html=True)
    st.markdown(f"""
<div class="api-stat">
    <div style="display:flex; justify-content:space-between;">
        <span>Cache Hits</span>
        <span>🟢 {cache_hits}</span>
    </div>
</div>
""", unsafe_allow_html=True)

    st.divider()
    st.header("🕐 Market Info")
    st.markdown(f"**Status:** <span style='color:{clock['color']}'>{clock['status']}</span>", unsafe_allow_html=True)
    st.markdown(f"**Next:** {clock['next_event']}")
    if clock['is_open']:
        st.progress(clock['progress'])
        st.caption(f"{int(clock['progress']*100)}% des Handelstags")
    st.divider()
    st.header("🔄 Auto Refresh")
    auto_mode = st.toggle("Live-Modus aktivieren", value=st.session_state.get('auto_refresh', False))
    if auto_mode != st.session_state.get('auto_refresh'):
        st.session_state['auto_refresh'] = auto_mode
        st.rerun()

    # NEU: Alert-History in Sidebar
    st.divider()
    st.header("🚨 Alert History")
    alert_history = st.session_state.get('alert_history', [])
    if alert_history:
        for alert in reversed(alert_history[-5:]):  # Letzte 5 Alerts
            time_ago = int((datetime.now() - alert['timestamp']).total_seconds() / 60)
            st.markdown(f"""
<div class="alert-history-item">
    <b>{alert['symbol']}</b> | {alert['setup_type']}<br>
    <small>${alert['price']:.2f} | Score: {alert['score']} | vor {time_ago}min</small>
</div>
""", unsafe_allow_html=True)
    else:
        st.caption("Keine Alerts heute")

    # Watchlist
    st.divider()
    st.header("📋 Watchlist")
    tab1, tab2 = st.tabs(["Einzeln", "Massen-Import"])
    with tab1:
        new_ticker = st.text_input("Ticker:", placeholder="z.B. AAPL").upper()
        if st.button("➕ Hinzufügen") and new_ticker:
            current = st.session_state['watchlist']
            if new_ticker not in current:
                current.append(new_ticker)
                st.session_state['watchlist'] = sorted(current)
                st.success(f"{new_ticker} hinzugefügt!")
                time.sleep(0.5)
                st.rerun()
    with tab2:
        bulk_input = st.text_area("Ticker Liste:", height=150, placeholder="AAPL\nTSLA\nNVDA")
        if st.button("📥 Import"):
            if bulk_input:
                raw = bulk_input.replace(',', '\n').replace(';', '\n').split('\n')
                clean = [t.strip().upper() for t in raw if t.strip()]
                current = st.session_state['watchlist']
                added = 0
                for t in clean:
                    if t not in current:
                        current.append(t)
                        added += 1
                if added:
                    st.session_state['watchlist'] = sorted(current)
                    st.success(f"✅ {added} Ticker importiert!")
                    time.sleep(1)
                    st.rerun()
    current_list = st.session_state['watchlist']
    active_list = st.multiselect("Aktive Watchlist:", options=current_list, default=current_list)
    if len(active_list)!=len(current_list):
        st.session_state['watchlist'] = active_list
        st.rerun()
    st.metric("Anzahl", len(st.session_state['watchlist']))

# Scan Button
scan_triggered = False
if st.button('🚀 Smart Scan Starten'):
    scan_triggered=True

# Scan & Analyse
if scan_triggered:
    with st.spinner("🔍 Scanne..."):
        market_ctx = get_market_context()
        gainers = []
        try:
            headers={'User-Agent':'Mozilla/5.0'}
            r = requests.get('https://finance.yahoo.com/gainers', headers=headers, timeout=10)
            if r.status_code==200:
                try:
                    tables = pd.read_html(StringIO(r.text))
                    if tables:
                        gainers = tables[0]['Symbol'].head(20).tolist()
                except:
                    pass
        except:
            pass
        scan_list = [(s, '📋') for s in st.session_state['watchlist']]
        seen = set(st.session_state['watchlist'])
        for g in gainers:
            if g not in seen and isinstance(g,str):
                scan_list.append((g, '🌍'))
                seen.add(g)
        results = []
        progress = st.progress(0)
        status_text = st.empty()
        use_parallel = len(scan_list)>20
        if use_parallel:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(analyze_smart, sym, i+1, len(scan_list), market_ctx): (sym, i) for i, (sym,_) in enumerate(scan_list)}
                for future in as_completed(futures):
                    sym, idx = futures[future]
                    try:
                        res = future.result()
                        if res:
                            existing = [r for r in results if r['symbol']==sym]
                            if not existing or res['score']>existing[0]['score']:
                                results = [r for r in results if r['symbol']!=sym]
                                results.append(res)
                    except:
                        pass
                    progress.progress((idx+1)/len(scan_list))
                    status_text.text(f"Analysiert: {sym} ({idx+1}/{len(scan_list)})")
        else:
            for i, (sym, _) in enumerate(scan_list):
                tier = i+1
                status_text.text(f"Tier {tier}/{len(scan_list)}: {sym}")
                res = analyze_smart(sym, tier, len(scan_list), market_ctx)
                if res:
                    existing = [r for r in results if r['symbol']==sym]
                    if not existing or res['score']>existing[0]['score']:
                        results = [r for r in results if r['symbol']!=sym]
                        results.append(res)
                progress.progress((i+1)/len(scan_list))
                if i%10==0:
                    time.sleep(0.5)
        progress.empty()
        status_text.empty()
        st.session_state['scan_results']=results
        st.session_state['last_scan_time']=datetime.now()

        # KORRIGIERT: Alert-Logik mit Cooldown und Deduplizierung
        alerts_sent_this_scan = 0
        for item in results:
            if item['score'] > 75:
                symbol = item['symbol']
                price = item['price']
                score = item['score']
                
                # Prüfen ob Alert gesendet werden soll
                if should_send_alert(symbol, price, score):
                    setup_type = "CATALYST" if (item.get('news') and item['news'][0]['tier']==1) else "GOLD"
                    
                    # Telegram senden
                    success = send_telegram_alert(
                        symbol, price, item['pullback_pct'], 
                        item['news'][0] if item.get('news') else None, 
                        setup_type, item.get('pe_ratio'), 
                        item.get('api_sources'), item.get('tier')
                    )
                    
                    if success:
                        # Alert speichern
                        record_alert(symbol, price, score, setup_type)
                        alerts_sent_this_scan += 1
                        
                        # Toast nur bei ersten 3 Alerts dieses Scans
                        if alerts_sent_this_scan <= 3:
                            st.toast(f"🚨 {setup_type} Alert: {symbol} @ ${price:.2f} (Score: {score})")
                else:
                    logger.info(f"Alert für {symbol} unterdrückt (Cooldown aktiv oder keine signifikante Änderung)")

# Ergebnisse Anzeige
results = st.session_state.get('scan_results', [])
if results:
    col1, col2 = st.columns([3,1])
    with col1:
        st.subheader(f"📊 Gefundene Setups: {len(results)}")
    with col2:
        if st.session_state.get('auto_refresh'):
            count = st.session_state.get('refresh_count', 0)
            st.markdown(f'<div style="background:#1a1a2e;padding:10px;border-radius:8px;border-left:4px solid #00FF00;">🔴 LIVE #{count}</div>', unsafe_allow_html=True)
        else:
            last_time=st.session_state.get('last_scan_time')
            if last_time:
                st.caption(f"Letzter Scan: {last_time.strftime('%H:%M:%S')}")
    
    # NEU: Alert-Statistik anzeigen
    sent_alerts = st.session_state.get('sent_alerts', {})
    active_alerts = len([a for a in sent_alerts.values() if (datetime.now() - a['timestamp']).total_seconds() / 3600 < 24])
    st.info(f"📱 Aktive Alerts (24h): {active_alerts} | In Cooldown: {len(sent_alerts) - active_alerts}")
    
    stats = st.session_state.get('api_stats', {})
    api_summary = {}
    for r in results:
        for s in r.get('api_sources', []):
            api_summary[s]=api_summary.get(s,0)+1
    cache_count = sum(1 for r in results if r.get('from_cache'))
    colA, colB, colC, colD=st.columns(4)
    with colA:
        st.metric("Setups", len(results))
    with colB:
        st.metric("API Calls", f"FH:{stats.get('finnhub',0)} AV:{stats.get('alpha_vantage',0)}")
    with colC:
        st.metric("Cache Hits", stats.get('cache_hits',0))
    with colD:
        st.metric("Rotationen", stats.get('alpha_rotation_count',0))
    alpha_status = [s for s in alpha_manager.get_status()]
    alpha_detail = ", ".join([f"K{i+1}:{s['calls_today']}" for i, s in enumerate(alpha_status)])
    st.success(f"✅ APIs: {api_summary} | Cache: {cache_count} | Alpha: {alpha_detail}")

    st.divider()
    def render_results_grid(results):
        results_sorted = sorted(results, key=lambda x: (x['score'], x['pullback_pct']), reverse=True)
        cols = st.columns(4)
        for i, r in enumerate(results_sorted[:16]):
            with cols[i%4]:
                render_card(r, st.container())
    render_results_grid(results)

    with st.expander("📡 API Details"):
        st.write(f"**Finnhub:** {finnhub_limiter.get_status()}/60 pro Minute")
        st.write("**Alpha Keys:**")
        for s in alpha_manager.get_status():
            active_mark = "▶️" if s['active'] else ""
            st.write(f"  {active_mark} Key {s['index']+1}: {s['calls_today']}/500")
        st.write(f"**Cache Hits:** {stats.get('cache_hits',0)}")
        st.write(f"**Rotationen:** {stats.get('alpha_rotation_count',0)}")
        ctx = get_market_context()
        st.write(f"**Marktkontext:** {'Risk-Off' if ctx.get('risk_off') else 'Risk-On'} (SPY: {ctx.get('spy_change',0):.2%}, VIX: {ctx.get('vix_level',0):.1f})")
        
        # NEU: Alert-Details
        st.write("---")
        st.write("**Alert Status:**")
        for symbol, alert in list(st.session_state.get('sent_alerts', {}).items())[:5]:
            ago = int((datetime.now() - alert['timestamp']).total_seconds() / 60)
            st.write(f"  • {symbol}: {alert['setup_type']} vor {ago}min @ ${alert['price']:.2f}")
else:
    if not scan_triggered:
        st.info("👆 Klicke 'Smart Scan Starten' oder aktiviere 'Live-Modus' in der Sidebar.")
