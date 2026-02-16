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
warnings.filterwarnings('ignore')

# =============================================================================
# US MARKET CLOCK - NEU HINZUGEFÜGT
# =============================================================================

def get_market_clock():
    """Zeigt aktuelle US Marktzeit und Status"""
    et = pytz.timezone('US/Eastern')
    now = datetime.now(et)
    
    # Marktzeiten definieren
    market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
    pre_market = now.replace(hour=4, minute=0, second=0, microsecond=0)
    after_hours = now.replace(hour=20, minute=0, second=0, microsecond=0)
    
    # Status bestimmen
    if now.weekday() >= 5:  # Wochenende
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
        countdown = f"Opens tomorrow"
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

# =============================================================================
# KONFIGURATION
# =============================================================================

st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V5.5 - Market Clock", page_icon="🐂")

# API KEYS
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
    TELEGRAM_BOT_TOKEN = "8317204351:AAHRu-mYYU0_NRIxNGEQ5voneIQaDKeQuF8"
    TELEGRAM_CHAT_ID = "5338135874"
    FINNHUB_API_KEY = "d652vnpr01qqbln5m9cgd652vnpr01qqbln5m9d0"
    ALPHA_VANTAGE_KEYS = [
        "N6PM9UCXL55JZTN9",
        "03d0c05583534ec8a42a9f470b4f5451",
        "a2dd09107d62438c8546c31d36b33458"
    ]

MIN_PULLBACK_PERCENT = 0.10
MAX_PULLBACK_PERCENT = 0.25
AUTO_REFRESH_INTERVAL = 30

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

# SESSION STATE
def init_session_state():
    defaults = {
        'watchlist': DEFAULT_WATCHLIST,
        'sent_alerts': set(),
        'api_stats': {
            'finnhub': 0,
            'alpha_vantage': 0,
            'alpha_key_index': 0,
            'yahoo': 0,
            'cache_hits': 0,
            'alpha_rotation_count': 0
        },
        'scan_results': [],
        'last_scan_time': None,
        'auto_refresh': False,
        'refresh_count': 0,
        'last_auto_refresh': 0,
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

init_session_state()

# =============================================================================
# CSS MIT MARKET CLOCK STYLES
# =============================================================================

st.markdown("""
    <style>
    .stMetric { background-color: #0E1117; padding: 10px; border-radius: 5px; }
    
    /* MARKET CLOCK STYLES - NEU */
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
    
    .bull-card { background-color: #0d1f12; border: 2px solid #00FF00; border-radius: 10px; padding: 15px; 
                 text-align: center; margin-bottom: 10px; animation: greenPulse 2.0s infinite alternate; }
    .gold-card { background-color: #2b2b00; border: 3px solid #FFD700; border-radius: 10px; padding: 15px; 
                 text-align: center; margin-bottom: 10px; animation: goldPulse 1.5s infinite alternate; }
    .purple-card { background-color: #1a0033; border: 3px solid #9933ff; border-radius: 10px; padding: 15px; 
                   text-align: center; margin-bottom: 10px; animation: purplePulse 0.8s infinite alternate; }
    
    .bull-card h3 { color: #00FF00 !important; margin: 0; }
    .gold-card h3 { color: #FFD700 !important; margin: 0; text-shadow: 0 0 10px #FFD700; }
    .purple-card h3 { color: #bf80ff !important; margin: 0; text-shadow: 0 0 10px #9933ff; }
    
    .price { font-size: 1.8rem; font-weight: bold; color: white; margin: 10px 0; }
    .pullback-badge { background: linear-gradient(45deg, #ff6b6b, #ee5a24); color: white; padding: 4px 12px; 
                      border-radius: 12px; font-size: 0.9rem; font-weight: bold; display: inline-block; margin: 5px 0; }
    .tier-badge { background: linear-gradient(45deg, #667eea, #764ba2); color: white; padding: 2px 8px; 
                  border-radius: 8px; font-size: 0.7rem; display: inline-block; margin: 2px; }
    .cache-badge { background: linear-gradient(45deg, #11998e, #38ef7d); color: white; padding: 2px 6px; 
                   border-radius: 6px; font-size: 0.65rem; display: inline-block; margin: 2px; }
    .rotation-badge { background: linear-gradient(45deg, #ff6b6b, #feca57); color: white; padding: 2px 8px; 
                      border-radius: 8px; font-size: 0.7rem; display: inline-block; margin: 2px; }
    .stop-loss { color: #ff9999; font-weight: bold; font-size: 0.9rem; border: 1px solid #ff4b4b; 
                 border-radius: 4px; padding: 2px 8px; display: inline-block; }
    .target { color: #90EE90; font-weight: bold; font-size: 0.9rem; border: 1px solid #00FF00; 
              border-radius: 4px; padding: 2px 8px; display: inline-block; margin-left: 5px; }
    .btn-link { display: inline-block; background-color: #262730; padding: 5px 15px; border-radius: 5px; 
                text-decoration: none; font-size: 0.9rem; border: 1px solid #555; color: white !important; }
    .confidence-bar { width: 100%; height: 4px; background: #333; margin: 5px 0; border-radius: 2px; }
    .confidence-fill { height: 100%; border-radius: 2px; }
    .api-stat { background: #1c1c1c; padding: 8px; border-radius: 5px; margin: 2px 0; font-size: 0.8rem; }
    .news-link-btn { display: block; background-color: rgba(255, 255, 255, 0.1); color: #fff !important; 
                     padding: 8px; border-radius: 5px; font-size: 0.8rem; margin: 8px 0; border: 1px solid #777; 
                     text-decoration: none; text-align: left; }
    .key-indicator { font-size: 0.7rem; padding: 2px 6px; border-radius: 4px; background: #2d2d2d; 
                     border: 1px solid #444; display: inline-block; margin: 2px; }
    .key-active { background: #1e3a1e; border-color: #00FF00; color: #00FF00; }
    .key-exhausted { background: #3a1e1e; border-color: #ff4b4b; color: #ff4b4b; }
    </style>
""", unsafe_allow_html=True)

# =============================================================================
# CACHE & API MANAGER (unverändert)
# =============================================================================

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

class AlphaVantageManager:
    def __init__(self, keys):
        self.keys = [k for k in keys if k and "DEIN_" not in k]
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
            masked_key = f"{key[:4]}...{key[-4:]}" if len(key) > 8 else "N/A"
            status.append({
                'index': i, 'key': masked_key, 'active': i == self.current_index,
                'calls_today': limiter['calls_today'], 'exhausted': limiter['exhausted'],
                'can_call': self.can_call(i)
            })
        return status

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

finnhub_limiter = RateLimiter(60, 60)
alpha_manager = AlphaVantageManager(ALPHA_VANTAGE_KEYS)

# =============================================================================
# HILFSFUNKTIONEN (unverändert)
# =============================================================================

def simple_slope(x_list, y_list):
    n = len(x_list)
    if n < 2: 
        return 0
    x_mean = sum(x_list) / n
    y_mean = sum(y_list) / n
    numerator = sum((x_list[i] - x_mean) * (y_list[i] - y_mean) for i in range(n))
    denominator = sum((x_list[i] - x_mean) ** 2 for i in range(n))
    return numerator / denominator if denominator != 0 else 0

def is_market_open():
    try:
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)
        if now.weekday() >= 5: 
            return False
        market_open = now.replace(hour=9, minute=30, second=0)
        market_close = now.replace(hour=16, minute=0, second=0)
        return market_open <= now <= market_close
    except:
        return True 

def get_market_context():
    try:
        spy = yf.Ticker("SPY").history(period="20d")
        if spy.empty: 
            return None
        sma20 = spy['Close'].rolling(20).mean().iloc[-1]
        price = spy['Close'].iloc[-1]
        return {
            'spy_trend': "BULL" if price > sma20 else "BEAR",
            'spy_price': price, 'spy_sma20': sma20, 'risk_off': price < sma20 * 0.98
        }
    except: 
        return None

def analyze_structure(df, symbol=None):
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
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            hh = swing_highs[-1][1] > swing_highs[-2][1]
            hl = swing_lows[-1][1] > swing_lows[-2][1]
            slope = 0
            if len(swing_highs) >= 3:
                x = [swing_highs[-3][0], swing_highs[-2][0], swing_highs[-1][0]]
                y = [swing_highs[-3][1], swing_highs[-2][1], swing_highs[-1][1]]
                slope = simple_slope(x, y)
            result = {
                'higher_highs': hh, 'higher_lows': hl, 'trend_slope': slope,
                'structure_intact': hh and hl, 'last_swing_low': swing_lows[-1][1],
                'last_swing_high': swing_highs[-1][1]
            }
        else:
            result = {
                'structure_intact': False, 'last_swing_low': df['Low'].tail(5).min(),
                'last_swing_high': df['High'].tail(20).max()
            }
        
        if symbol:
            structure_cache.set(cache_key, result)
        return result
    except:
        return {
            'structure_intact': False, 'last_swing_low': df['Low'].tail(5).min(),
            'last_swing_high': df['High'].tail(20).max()
        }

def get_finnhub_news_smart(symbol):
    cache_key = f"fh_news_{symbol}"
    cached = news_cache.get(cache_key, 600)
    if cached is not None:
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['cache_hits'] = stats.get('cache_hits', 0) + 1
            st.session_state['api_stats'] = stats
        return cached, True
    
    if not finnhub_limiter.can_call():
        return [], False
    if "DEIN_" in FINNHUB_API_KEY or not FINNHUB_API_KEY:
        return [], False
    
    try:
        end = int(time.time())
        start = end - (3600 * 48)
        url = "https://finnhub.io/api/v1/company-news"
        params = {
            'symbol': symbol,
            'from': datetime.fromtimestamp(start).strftime('%Y-%m-%d'),
            'to': datetime.fromtimestamp(end).strftime('%Y-%m-%d'),
            'token': FINNHUB_API_KEY
        }
        response = requests.get(url, params=params, timeout=10)
        finnhub_limiter.record_call()
        stats = st.session_state.get('api_stats', {})
        if isinstance(stats, dict):
            stats['finnhub'] = stats.get('finnhub', 0) + 1
            st.session_state['api_stats'] = stats
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list):
                data.sort(key=lambda x: x.get('datetime', 0), reverse=True)
                result = data[:5]
                news_cache.set(cache_key, result)
                return result, False
    except:
        pass
    return [], False

def get_alpha_vantage_smart(symbol):
    cache_key = f"av_fund_{symbol}"
    cached = fundamentals_cache.get(cache_key, 3600)
    if cached is not None:
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
            try:
                url = "https://www.alphavantage.co/query"
                params = {'function': 'OVERVIEW', 'symbol': symbol, 'apikey': current_key}
                response = requests.get(url, params=params, timeout=10)
                alpha_manager.record_call()
                
                if response.status_code == 200:
                    data = response.json()
                    if 'Note' in data or 'Information' in data:
                        alpha_manager.limiters[alpha_manager.current_index]['exhausted'] = True
                        alpha_manager.rotate_key()
                        attempts += 1
                        time.sleep(0.5)
                        continue
                    
                    if 'Symbol' in data:
                        result = {
                            'pe_ratio': float(data.get('PERatio', 0)) if data.get('PERatio') else None,
                            'eps': float(data.get('EPS', 0)) if data.get('EPS') else None,
                            'sector': data.get('Sector', 'Unknown'),
                            'industry': data.get('Industry', 'Unknown'),
                            'market_cap': int(data.get('MarketCapitalization', 0)) if data.get('MarketCapitalization') else 0
                        }
                        fundamentals_cache.set(cache_key, result)
                        return result, False
            except:
                pass
        
        alpha_manager.rotate_key()
        attempts += 1
    
    return None, False

def get_yahoo_news_fallback(symbol):
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        if news:
            return [{'headline': n.get('title'), 'url': n.get('link'), 'source': 'Yahoo', 'datetime': 0} for n in news[:5]]
    except:
        pass
    return []

def analyze_news_tiered(symbol, tier, prelim_score):
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

def analyze_smart(symbol, tier, total_tickers, market_context=None):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period='3mo', interval='1h')
        
        if df.empty or len(df) < 50: 
            return None
        if df['Volume'].mean() < 50000: 
            return None
        
        hourly_returns = df['Close'].pct_change().abs()
        if hourly_returns.max() > 0.3:
            return None
        
        current_price = df['Close'].iloc[-1]
        lookback_period = min(70, len(df) - 10)
        recent_data = df.tail(lookback_period)
        recent_high = recent_data['High'].max()
        pullback_pct = (recent_high - current_price) / recent_high
        
        if pullback_pct < MIN_PULLBACK_PERCENT or pullback_pct > MAX_PULLBACK_PERCENT: 
            return None
        
        structure = analyze_structure(df, symbol)
        if not structure['structure_intact']: 
            return None
        if current_price < structure['last_swing_low'] * 0.98: 
            return None 
        
        prelim_score = 30
        if structure['trend_slope'] > 0.02:
            prelim_score += 15
        
        avg_vol = df['Volume'].tail(20).mean()
        current_vol = df['Volume'].iloc[-1]
        rvol = current_vol / avg_vol if avg_vol > 0 else 1.0
        
        if rvol > 2.0:
            prelim_score += 20
        elif rvol > 1.5:
            prelim_score += 10
        
        support_distance = (current_price - structure['last_swing_low']) / current_price
        if support_distance < 0.03:
            prelim_score += 15
        elif support_distance < 0.05:
            prelim_score += 8
        
        news_items, news_sources, news_cached = analyze_news_tiered(symbol, tier, prelim_score)
        if news_items:
            prelim_score += news_items[0]['score']
        
        fundamentals, fund_sources, fund_cached = None, [], False
        if tier <= 15 and prelim_score >= 65:
            fundamentals, fund_sources, fund_cached = get_alpha_vantage_smart(symbol)
        
        pe_ratio = None
        if fundamentals:
            pe_ratio = fundamentals.get('pe_ratio')
            if pe_ratio:
                if pe_ratio < 15:
                    prelim_score += 8
                elif pe_ratio > 100:
                    prelim_score -= 5
        
        atr = df['High'].rolling(14).max() - df['Low'].rolling(14).min()
        atr = atr.rolling(14).mean().iloc[-1]
        
        technical_stop = structure['last_swing_low'] * 0.985
        atr_stop = current_price - (2 * atr)
        stop_loss = max(technical_stop, atr_stop)
        
        target_1 = recent_high * 0.98 
        target_2 = current_price + (current_price - stop_loss) * 2
        target = min(target_1, target_2)
        
        risk = current_price - stop_loss
        reward = target - current_price
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < 1.5: 
            return None
        
        if market_context and market_context.get('risk_off'):
            prelim_score -= 10
        
        if prelim_score < 50: 
            return None
        
        reasons = [f"📉 -{pullback_pct:.1f}%"]
        if structure['trend_slope'] > 0.02:
            reasons.append("📈 Trend")
        if rvol > 1.5:
            reasons.append(f"⚡ Vol {rvol:.1f}x")
        if support_distance < 0.03:
            reasons.append("🎯 Support")
        if news_items:
            reasons.append(f"📰 {news_items[0]['source']}")
        if pe_ratio:
            reasons.append(f"{'💰' if pe_ratio < 15 else '📊'} PE {pe_ratio:.1f}")
        
        all_sources = news_sources + fund_sources
        unique_sources = list(set(all_sources))
        
        return {
            'symbol': symbol, 'tier': tier, 'score': min(100, int(prelim_score)), 
            'price': current_price, 'pullback_pct': pullback_pct, 'recent_high': recent_high, 
            'stop_loss': stop_loss, 'target': target, 'rr_ratio': rr_ratio, 'rvol': rvol, 
            'reasons': reasons, 'news': news_items, 'pe_ratio': pe_ratio,
            'fundamentals': fundamentals, 'api_sources': unique_sources,
            'from_cache': news_cached or fund_cached
        }
    except: 
        return None

def send_telegram_alert(symbol, price, pullback_pct, news_item, setup_type, pe_ratio=None, api_sources=None, tier=None):
    if "DEIN_" in TELEGRAM_BOT_TOKEN: 
        return
    news_text = news_item.get('title', 'News') if news_item else 'Keine News'
    news_url = news_item.get('url', f'https://finance.yahoo.com/quote/{symbol}') if news_item else f'https://finance.yahoo.com/quote/{symbol}'
    emoji = "🟣" if setup_type == "CATALYST" else "🏆" if setup_type == "GOLD" else "🐂"
    pe_info = f"\n📊 P/E: {pe_ratio:.1f}" if pe_ratio else ""
    api_info = f"\n📡 {','.join(api_sources)}" if api_sources else ""
    tier_info = f"\n🎯 Tier {tier}" if tier else ""
    
    msg = f"""{emoji} <b>{setup_type}: {symbol}</b> {emoji}
📉 Pullback: <b>-{pullback_pct:.1f}%</b>
💵 Preis: ${price:.2f}{pe_info}{api_info}{tier_info}
📰 {news_text[:60]}...
👉 <a href='{news_url}'>News</a> | <a href='https://www.tradingview.com/chart/?symbol={symbol}'>Chart</a>"""
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try: 
        requests.post(url, data=payload, timeout=5)
    except: 
        pass

def render_grid(result_data, container):
    if not result_data:
        container.warning('Keine starken Pullbacks gefunden.')
        return
    
    result_data.sort(key=lambda x: (x['score'], x['pullback_pct']), reverse=True)
    cols = container.columns(4)
    
    for i, item in enumerate(result_data[:16]):
        sym = item['symbol']
        score = item['score']
        price = item['price']
        pullback = item['pullback_pct']
        sl = item['stop_loss']
        target = item['target']
        rr = item['rr_ratio']
        pe = item.get('pe_ratio')
        tier = item.get('tier', '-')
        reasons_txt = ' | '.join(item['reasons'][:3])
        news_found = item.get('news', [])
        apis = item.get('api_sources', [])
        cached = item.get('from_cache', False)
        tv_url = f'https://www.tradingview.com/chart/?symbol={sym}'
        
        conf_color = '#9933ff' if score > 85 else '#FFD700' if score > 70 else '#00FF00'
        pullback_color = '#ff6b6b' if pullback > 0.15 else '#ffa502'
        tier_html = f'<div class="tier-badge">T{tier}</div>'
        api_html = ''.join([f'<div class="tier-badge">{a}</div>' for a in apis])
        cache_html = '<div class="cache-badge">CACHE</div>' if cached else ''
        
        with cols[i % 4]:
            if score > 80 and news_found and news_found[0]['tier'] == 1:
                n_i = news_found[0]
                st.markdown(f"""
<div class="purple-card">
    <h3>🔮 {sym}</h3>
    <div class="pullback-badge" style="background: {pullback_color};">
        -{pullback:.1%}
    </div>
    <div style="margin: 5px 0;">{tier_html}{api_html}{cache_html}</div>
    <div class="price">${price:.2f}</div>
    <div style="font-size: 0.8rem; color: #aaa; margin: 5px 0;">{reasons_txt}</div>
    <div style="margin: 8px 0;">
        <span class="stop-loss">SL: ${sl:.2f}</span>
        <span class="target">TP: ${target:.2f}</span>
    </div>
    <div style="font-size: 0.8rem; color: {conf_color}; margin: 5px 0;">Score: {score}/100</div>
    <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
    <div style="font-size: 0.75rem; color: #888; margin: 5px 0;">R:R {rr:.1f}x | Vol {item.get('rvol', 0):.1f}x</div>
    <a href="{n_i['url']}" target="_blank" class="news-link-btn">📰 {n_i['title'][:40]}...</a>
    <a href="{tv_url}" target="_blank" class="btn-link">📈 TradingView</a>
</div>
""", unsafe_allow_html=True)
            elif score > 70 and news_found:
                n_i = news_found[0]
                st.markdown(f"""
<div class="gold-card">
    <h3>🏆 {sym}</h3>
    <div class="pullback-badge" style="background: {pullback_color};">
        -{pullback:.1%}
    </div>
    <div style="margin: 5px 0;">{tier_html}{api_html}{cache_html}</div>
    <div class="price">${price:.2f}</div>
    <div style="font-size: 0.8rem; color: #aaa; margin: 5px 0;">{reasons_txt}</div>
    <div style="margin: 8px 0;">
        <span class="stop-loss">SL: ${sl:.2f}</span>
        <span class="target">TP: ${target:.2f}</span>
    </div>
    <div style="font-size: 0.8rem; color: {conf_color}; margin: 5px 0;">Score: {score}/100</div>
    <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
    <div style="font-size: 0.75rem; color: #888; margin: 5px 0;">R:R {rr:.1f}x | Vol {item.get('rvol', 0):.1f}x</div>
    <a href="{n_i['url']}" target="_blank" class="news-link-btn">📰 {n_i['title'][:40]}...</a>
    <a href="{tv_url}" target="_blank" class="btn-link">📈 TradingView</a>
</div>
""", unsafe_allow_html=True)
            else:
                st.markdown(f"""
<div class="bull-card">
    <h3>🐂 {sym}</h3>
    <div class="pullback-badge" style="background: {pullback_color};">
        -{pullback:.1%}
    </div>
    <div style="margin: 5px 0;">{tier_html}{api_html}{cache_html}</div>
    <div class="price">${price:.2f}</div>
    <div style="font-size: 0.8rem; color: #aaa; margin: 5px 0;">{reasons_txt}</div>
    <div style="margin: 8px 0;">
        <span class="stop-loss">SL: ${sl:.2f}</span>
        <span class="target">TP: ${target:.2f}</span>
    </div>
    <div style="font-size: 0.8rem; color: {conf_color}; margin: 5px 0;">Score: {score}/100</div>
    <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
    <div style="font-size: 0.75rem; color: #888; margin: 5px 0;">R:R {rr:.1f}x | Vol {item.get('rvol', 0):.1f}x</div>
    <a href="{tv_url}" target="_blank" class="btn-link">📈 TradingView</a>
</div>
""", unsafe_allow_html=True)

# =============================================================================
# HAUPTBEREICH - MIT MARKET CLOCK
# =============================================================================

# MARKET CLOCK - OBEN ANZEIGEN
clock = get_market_clock()
st.markdown(f"""
<div class="market-clock-container">
    <div class="market-time">{clock['time']}</div>
    <div style="margin: 10px 0;">
        <span class="market-status" style="background: {clock['color']};">
            {clock['status']}
        </span>
    </div>
    <div class="market-countdown">{clock['countdown']}</div>
    <div style="font-size: 0.9rem; color: #888; margin-top: 5px;">Next: {clock['next_event']}</div>
    {f'<div class="market-progress"><div class="market-progress-bar" style="width: {clock["progress"]*100}%;"></div></div>' if clock['is_open'] else ''}
</div>
""", unsafe_allow_html=True)

# Titel und Caption
st.title('🐂 Elite Bull Scanner Pro V5.5')
st.caption(f"3-Key Alpha Rotation | {len(alpha_manager.keys)} Keys aktiv | Market Clock | Real-time ET")

if st.session_state.get('auto_refresh', False):
    last_refresh = st.session_state.get('last_auto_refresh', 0)
    current_time = time.time()
    if current_time - last_refresh >= AUTO_REFRESH_INTERVAL:
        st.session_state['last_auto_refresh'] = current_time
        st.session_state['refresh_count'] = st.session_state.get('refresh_count', 0) + 1
        st.rerun()

# SIDEBAR
with st.sidebar:
    st.header("📡 Smart API Status")
    
    stats = st.session_state.get('api_stats', {
        'finnhub': 0, 'alpha_vantage': 0, 'cache_hits': 0, 'alpha_rotation_count': 0
    })
    
    fh_status = "🟢" if finnhub_limiter.can_call() else "🔴"
    alpha_status_list = alpha_manager.get_status()
    
    st.markdown(f"""
<div class="api-stat">
    <div style="display: flex; justify-content: space-between;">
        <span>Finnhub</span>
        <span>{fh_status} {finnhub_limiter.get_status()}/min</span>
    </div>
</div>
""", unsafe_allow_html=True)
    
    st.markdown("<div style='margin: 10px 0;'><b>Alpha Vantage Keys:</b></div>", unsafe_allow_html=True)
    
    for status in alpha_status_list:
        key_class = "key-active" if status['active'] else "key-exhausted" if status['exhausted'] else ""
        indicator = "▶️" if status['active'] else "✅" if not status['exhausted'] else "❌"
        st.markdown(f"""
<div class="key-indicator {key_class}">
    {indicator} Key {status['index']+1}: {status['calls_today']}/500
</div>
""", unsafe_allow_html=True)
    
    rotations = stats.get('alpha_rotation_count', 0) if isinstance(stats, dict) else 0
    st.markdown(f'<div class="rotation-badge">🔄 Rotationen: {rotations}</div>', unsafe_allow_html=True)
    
    cache_hits = stats.get('cache_hits', 0) if isinstance(stats, dict) else 0
    st.markdown(f"""
<div class="api-stat" style="margin-top: 10px;">
    <div style="display: flex; justify-content: space-between;">
        <span>Cache Hits</span>
        <span>🟢 {cache_hits}</span>
    </div>
</div>
""", unsafe_allow_html=True)
    
    # MARKET INFO IN SIDEBAR
    st.divider()
    st.header("🕐 Market Info")
    st.markdown(f"**Status:** <span style='color:{clock['color']}'>{clock['status']}</span>", unsafe_allow_html=True)
    st.markdown(f"**Next:** {clock['next_event']}")
    if clock['is_open']:
        st.progress(clock['progress'])
        st.caption(f"{int(clock['progress']*100)}% of trading day")
    
    # AUTO REFRESH
    st.divider()
    st.header("🔄 Auto Refresh")
    
    current_auto_refresh = st.session_state.get('auto_refresh', False)
    auto_refresh = st.toggle("Live-Modus aktivieren", value=current_auto_refresh)
    
    if auto_refresh != current_auto_refresh:
        st.session_state['auto_refresh'] = auto_refresh
        st.rerun()
    
    if auto_refresh:
        st.info(f"⏱️ Aktualisiert alle {AUTO_REFRESH_INTERVAL}s")
        last_refresh = st.session_state.get('last_auto_refresh', 0)
        time_since = time.time() - last_refresh
        progress = min(time_since / AUTO_REFRESH_INTERVAL, 1.0)
        st.progress(progress)
        st.caption(f"Nächster Scan in {int(AUTO_REFRESH_INTERVAL - time_since)}s")
    
    # WATCHLIST
    st.divider()
    st.header("📋 Watchlist Manager")
    
    tab1, tab2 = st.tabs(["Einzeln", "Massen-Import"])
    with tab1:
        new_ticker = st.text_input("Ticker:", placeholder="z.B. AAPL").upper()
        if st.button("➕ Hinzufügen") and new_ticker:
            current_list = st.session_state.get('watchlist', [])
            if new_ticker not in current_list:
                current_list.append(new_ticker)
                st.session_state['watchlist'] = sorted(current_list)
                st.success(f"{new_ticker} hinzugefügt!")
                time.sleep(0.5)
                st.rerun()

    with tab2:
        bulk_input = st.text_area("Ticker Liste:", height=150, placeholder="AAPL\nTSLA\nNVDA")
        if st.button("📥 Import"):
            if bulk_input:
                raw = bulk_input.replace(',', '\n').replace(';', '\n').split('\n')
                clean = [t.strip().upper() for t in raw if t.strip()]
                current_list = st.session_state.get('watchlist', [])
                added = 0
                for t in clean:
                    if t not in current_list:
                        current_list.append(t)
                        added += 1
                if added > 0:
                    st.session_state['watchlist'] = sorted(current_list)
                    st.success(f"✅ {added} Ticker importiert!")
                    time.sleep(1)
                    st.rerun()
    
    st.divider()
    current_watchlist = st.session_state.get('watchlist', [])
    updated = st.multiselect("Aktive Watchlist:", options=current_watchlist, default=current_watchlist)
    if len(updated) != len(current_watchlist):
        st.session_state['watchlist'] = updated
        st.rerun()
    
    st.metric("Anzahl", len(current_watchlist))

# SCAN LOGIK
scan_triggered = False
if st.button('🚀 Smart Scan Starten', type='primary'):
    scan_triggered = True

if scan_triggered:
    with st.spinner("🔍 Scanne..."):
        market_ctx = get_market_context()
        
        try:
            r = requests.get('https://finance.yahoo.com/gainers', headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            gainers = pd.read_html(StringIO(r.text))[0]['Symbol'].head(20).tolist()
        except: 
            gainers = []
        
        current_watchlist = st.session_state.get('watchlist', [])
        scan_list = [(s, '📋') for s in current_watchlist]
        seen = set(current_watchlist)
        for g in gainers:
            if g not in seen:
                scan_list.append((g, '🌍'))
                seen.add(g)
        
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, (sym, src) in enumerate(scan_list):
            tier = i + 1
            status_text.text(f"Tier {tier}/{len(scan_list)}: {sym}")
            res = analyze_smart(sym, tier, len(scan_list), market_ctx)
            if res:
                existing = [r for r in results if r['symbol'] == sym]
                if not existing or res['score'] > existing[0]['score']:
                    results = [r for r in results if r['symbol'] != sym]
                    results.append(res)
            progress_bar.progress((i + 1) / len(scan_list))
            if i % 10 == 0:
                time.sleep(0.5)
        
        progress_bar.empty()
        status_text.empty()
        st.session_state['scan_results'] = results
        st.session_state['last_scan_time'] = datetime.now()
        
        for item in results[:3]:
            if item['score'] > 75:
                alert_key = f"{item['symbol']}_{datetime.now().strftime('%H')}"
                sent_alerts = st.session_state.get('sent_alerts', set())
                if alert_key not in sent_alerts:
                    setup_type = "CATALYST" if (item.get('news') and item['news'][0]['tier'] == 1) else "GOLD"
                    send_telegram_alert(
                        item['symbol'], item['price'], item['pullback_pct'],
                        item.get('news', [{}])[0], setup_type,
                        item.get('pe_ratio'), item.get('api_sources'), item.get('tier')
                    )
                    sent_alerts.add(alert_key)
                    st.session_state['sent_alerts'] = sent_alerts
                    st.toast(f"🚨 T{item['tier']} {item['symbol']} Alert!")

# ERGEBNISSE ANZEIGEN
results = st.session_state.get('scan_results', [])

if results:
    col_title, col_status = st.columns([3, 1])
    with col_title:
        st.subheader(f"📊 Gefundene Setups: {len(results)}")
    with col_status:
        if st.session_state.get('auto_refresh', False):
            refresh_count = st.session_state.get('refresh_count', 0)
            st.markdown(f'<div style="background: #1a1a2e; padding: 10px; border-radius: 8px; border-left: 4px solid #00FF00;">🔴 LIVE #{refresh_count}</div>', unsafe_allow_html=True)
        else:
            last_scan = st.session_state.get('last_scan_time')
            if last_scan:
                st.caption(f"Letzter Scan: {last_scan.strftime('%H:%M:%S')}")
    
    stats = st.session_state.get('api_stats', {})
    api_summary = {}
    for r in results:
        for api in r.get('api_sources', []):
            api_summary[api] = api_summary.get(api, 0) + 1
    
    cache_count = sum(1 for r in results if r.get('from_cache'))
    
    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
    col_stat1.metric("Setups", len(results))
    
    fh_calls = stats.get('finnhub', 0) if isinstance(stats, dict) else 0
    av_calls = stats.get('alpha_vantage', 0) if isinstance(stats, dict) else 0
    cache_hits = stats.get('cache_hits', 0) if isinstance(stats, dict) else 0
    rotations = stats.get('alpha_rotation_count', 0) if isinstance(stats, dict) else 0
    
    col_stat2.metric("API Calls", f"FH:{fh_calls} AV:{av_calls}")
    col_stat3.metric("Cache Hits", cache_hits)
    col_stat4.metric("Rotations", rotations)
    
    alpha_detail = ", ".join([f"K{i+1}:{s['calls_today']}" for i, s in enumerate(alpha_manager.get_status())])
    st.success(f"✅ APIs: {api_summary} | Cache: {cache_count} | Alpha: {alpha_detail}")
    st.divider()
    
    render_grid(results, st)
    
    with st.expander("📡 API Details"):
        st.write(f"**Finnhub:** {finnhub_limiter.get_status()}/60 per minute")
        st.write("**Alpha Vantage Keys:**")
        for status in alpha_manager.get_status():
            active_mark = "▶️" if status['active'] else " "
            st.write(f"  {active_mark} Key {status['index']+1}: {status['calls_today']}/500")
        st.write(f"**Cache Hits:** {cache_hits}")
        st.write(f"**Rotations:** {rotations}")

elif not scan_triggered:
    st.info("👆 Klicke 'Smart Scan Starten' oder aktiviere 'Live-Modus' in der Sidebar.")
