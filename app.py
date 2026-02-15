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
        "DEIN_KEY_2_HIER",
        "DEIN_KEY_3_HIER"
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
        url = f"https://finnhub.io/api/v1/company-news"
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
        tier_html = f'<div class=\"tier-badge\">T{tier}</div>'
        api_html = ''.join([f'<div class=\"tier-badge\">{a}</div>' for a in apis])
        cache_html = '<div class=\"cache-badge\">CACHE</div>' if cached else ''
        
        with cols[i % 4]:
            if score > 80 and news_found and news_found[0]['tier'] == 1:
                n_i = news_found[0]
                st.markdown(f\"\"\"\n                <div class=\"purple-card\">\n                    <h3>🟣 {sym}</h3>\n                    <div class=\"pullback-badge\" style=\"background: {pullback_color};\">📉 -{pullback:.1%}</div>\n                    {tier_html}{api_html}{cache_html}\n                    <div class=\"confidence-bar\"><div class=\"confidence-fill\" style=\"width: {score}%; background: {conf_color};\"></div></div>\n                    <div class=\"meta\">SCORE: {score} | R/R {rr:.1f}:1</div>\n                    <div class=\"price\">${price:.2f}</div>\n                    <a href=\"{n_i['url']}\" target=\"_blank\" class=\"news-link-btn\">🔗 [{n_i['source']}] {n_i['title'][:50]}...</a>\n                    <div><span class=\"stop-loss\">SL: ${sl:.2f}</span><span class=\"target\">TP: ${target:.2f}</span></div>\n                    <div class=\"meta\" style=\"font-size: 0.7rem; margin-top:5px;\">{reasons_txt}</div>\n                    <a href=\"{tv_url}\" target=\"_blank\" class=\"btn-link\">📊 Chart</a>\n                </div>\"\"\", unsafe_allow_html=True)\n            elif score > 65:\n                n_html = f'<a href=\"{news_found[0][\"url\"]}\" target=\"_blank\" class=\"news-link-btn\">🔗 [{news_found[0][\"source\"]}] {news_found[0][\"title\"][:50]}...</a>' if news_found else \"\"\n                st.markdown(f\"\"\"\n                <div class=\"gold-card\">\n                    <h3>🏆 {sym}</h3>\n                    <div class=\"pullback-badge\" style=\"background: {pullback_color};\">📉 -{pullback:.1%}</div>\n                    {tier_html}{api_html}{cache_html}\n                    <div class=\"confidence-bar\"><div class=\"confidence-fill\" style=\"width: {score}%; background: {conf_color};\"></div></div>\n                    <div class=\"meta\">SCORE: {score} | R/R {rr:.1f}:1</div>\n                    <div class=\"price\">${price:.2f}</div>\n                    {n_html}\n                    <div><span class=\"stop-loss\">SL: ${sl:.2f}</span><span class=\"target\">TP: ${target:.2f}</span></div>\n                    <div class=\"meta\" style=\"font-size: 0.7rem; margin-top:5px;\">{reasons_txt}</div>\n                    <a href=\"{tv_url}\" target=\"_blank\" class=\"btn-link\">📈 Chart</a>\n                </div>\"\"\", unsafe_allow_html=True)\n            else:\n                st.markdown(f\"\"\"\n                <div class=\"bull-card\">\n                    <h3>🐂 {sym}</h3>\n                    <div class=\"pullback-badge\" style=\"background: {pullback_color};\">📉 -{pullback:.1%}</div>\n                    {tier_html}{api_html}{cache_html}\n                    <div class=\"confidence-bar\"><div class=\"confidence-fill\" style=\"width: {score}%; background: {conf_color};\"></div></div>\n                    <div class=\"meta\">Score: {score} | R/R {rr:.1f}:1</div>\n                    <div class=\"price\">${price:.2f}</div>\n                    <div><span class=\"stop-loss\">SL: ${sl:.2f}</span><span class=\"target\">TP: ${target:.2f}</span></div>\n                    <div class=\"meta\" style=\"font-size: 0.7rem; margin-top:5px;\">{reasons_txt}</div>\n                    <a href=\"{tv_url}\" target=\"_blank\" class=\"btn-link\">📈 Chart</a>\n                </div>\"\"\", unsafe_allow_html=True)

# =============================================================================
# HAUPTBEREICH - MIT MARKET CLOCK
# =============================================================================

# MARKET CLOCK - OBEN ANZEIGEN
clock = get_market_clock()

st.markdown(f\"\"\"\n    <div class=\"market-clock-container\">\n        <div class=\"market-time\">{clock['time']}</div>\n        <div style=\"margin: 10px 0;\">\n            <span class=\"market-status\" style=\"background-color: {clock['bg_color']}; color: {clock['color']}; border: 2px solid {clock['color']};\">\n                {clock['status']}\n            </span>\n        </div>\n        <div class=\"market-countdown\">{clock['countdown']}</div>\n        {f'<div class=\"market-progress\"><div class=\"market-progress-bar\" style=\"width: {clock[\"progress\"]*100}%\"></div></div>' if clock['is_open'] else ''}\n        {f'<div style=\"font-size: 0.8rem; color: #8b949e; margin-top: 5px;\">{int(clock[\"progress\"]*100)}% of session</div>' if clock['is_open'] else ''}\n    </div>\n\"\"\", unsafe_allow_html=True)

# Titel und Caption
st.title('🐂 Elite Bull Scanner Pro V5.5')
st.caption(f\"3-Key Alpha Rotation | {len(alpha_manager.keys)} Keys aktiv | Market Clock | Real-time ET\")\n\n# Warnung wenn Markt geschlossen
if not clock['is_open']:\n    st.warning(f\"⚠️ Market is **{clock['status']}**! Next session: {clock['next_event']} ET | Showing last available data.\")\n\n# AUTO REFRESH LOGIK
if st.session_state.get('auto_refresh', False):
    last_refresh = st.session_state.get('last_auto_refresh', 0)
    current_time = time.time()
    if current_time - last_refresh >= AUTO_REFRESH_INTERVAL:
        st.session_state['last_auto_refresh'] = current_time
        st.session_state['refresh_count'] = st.session_state.get('refresh_count', 0) + 1
        st.rerun()

# SIDEBAR
with st.sidebar:
    st.header(\"📡 Smart API Status\")\n    \n    stats = st.session_state.get('api_stats', {
        'finnhub': 0, 'alpha_vantage': 0, 'cache_hits': 0, 'alpha_rotation_count': 0
    })
    
    fh_status = \"🟢\" if finnhub_limiter.can_call() else \"🔴\"\n    alpha_status_list = alpha_manager.get_status()
n    \n    st.markdown(f\"\"\"\n        <div class=\"api-stat\">\n            <div style=\"display: flex; justify-content: space-between;\">\n                <span>Finnhub</span>\n                <span>{fh_status} {finnhub_limiter.get_status()}/min</span>\n            </div>\n        </div>\n    \"\"\", unsafe_allow_html=True)\n    \n    st.markdown(\"<div style='margin: 10px 0;'><b>Alpha Vantage Keys:</b></div>\", unsafe_allow_html=True)\n    \n    for status in alpha_status_list:\n        key_class = \"key-active\" if status['active'] else \"key-exhausted\" if status['exhausted'] else \"\"\n        indicator = \"▶️\" if status['active'] else \"✅\" if not status['exhausted'] else \"❌\"\n        st.markdown(f\"\"\"\n            <div class=\"key-indicator {key_class}\">\n                {indicator} Key {status['index']+1}: {status['calls_today']}/500\n            </div>\n        \"\"\", unsafe_allow_html=True)\n    \n    rotations = stats.get('alpha_rotation_count', 0) if isinstance(stats, dict) else 0\n    st.markdown(f'<div class=\"rotation-badge\">🔄 Rotationen: {rotations}</div>', unsafe_allow_html=True)\n    \n    cache_hits = stats.get('cache_hits', 0) if isinstance(stats, dict) else 0\n    st.markdown(f\"\"\"\n        <div class=\"api-stat\" style=\"margin-top: 10px;\">\n            <div style=\"display: flex; justify-content: space-between;\">\n                <span>Cache Hits</span>\n                <span>🟢 {cache_hits}</span>\n            </div>\n        </div>\n    \"\"\", unsafe_allow_html=True)
    
    # MARKET INFO IN SIDEBAR
    st.divider()
    st.header(\"🕐 Market Info\")\n    st.markdown(f\"**Status:** <span style='color:{clock['color']}'>{clock['status']}</span>\", unsafe_allow_html=True)
n    st.markdown(f\"**Next:** {clock['next_event']}\")\n    if clock['is_open']:\n        st.progress(clock['progress'])\n        st.caption(f\"{int(clock['progress']*100)}% of trading day\")\n    
    # AUTO REFRESH
    st.divider()
n    st.header(\"🔄 Auto Refresh\")\n    \n    current_auto_refresh = st.session_state.get('auto_refresh', False)\n    auto_refresh = st.toggle(\"Live-Modus aktivieren\", value=current_auto_refresh)\n    \n    if auto_refresh != current_auto_refresh:\n        st.session_state['auto_refresh'] = auto_refresh\n        st.rerun()
    
    if auto_refresh:
n        st.info(f\"⏱️ Aktualisiert alle {AUTO_REFRESH_INTERVAL}s\")\n        # Countdown bis zum nächsten Refresh\n        last_refresh = st.session_state.get('last_auto_refresh', 0)\n        time_since = time.time() - last_refresh\n        progress = min(time_since / AUTO_REFRESH_INTERVAL, 1.0)
n        st.progress(progress)\n        st.caption(f\"Nächster Scan in {int(AUTO_REFRESH_INTERVAL - time_since)}s\")\n    \n    # WATCHLIST
    st.divider()
    st.header(\"📋 Watchlist Manager\")\n    \n    tab1, tab2 = st.tabs([\"Einzeln\", \"Massen-Import\"])
n    with tab1:\n        new_ticker = st.text_input(\"Ticker:\", placeholder=\"z.B. AAPL\").upper()\n        if st.button(\"➕ Hinzufügen\") and new_ticker:\n            current_list = st.session_state.get('watchlist', [])\n            if new_ticker not in current_list:\n                current_list.append(new_ticker)\n                st.session_state['watchlist'] = sorted(current_list)\n                st.success(f\"{new_ticker} hinzugefügt!\")\n                time.sleep(0.5)\n                st.rerun()\n\n    with tab2:\n        bulk_input = st.text_area(\"Ticker Liste:\", height=150, placeholder=\"AAPL\\nTSLA\\nNVDA\")\n        if st.button(\"📥 Import\"):\n            if bulk_input:\n                raw = bulk_input.replace(',', '\\n').replace(';', '\\n').split('\\n')\n                clean = [t.strip().upper() for t in raw if t.strip()]\n                current_list = st.session_state.get('watchlist', [])\n                added = 0\n                for t in clean:\n                    if t not in current_list:\n                        current_list.append(t)\n                        added += 1\n                if added > 0:\n                    st.session_state['watchlist'] = sorted(current_list)\n                    st.success(f\"✅ {added} Ticker importiert!\")\n                    time.sleep(1)\n                    st.rerun()\n    \n    st.divider()\n    current_watchlist = st.session_state.get('watchlist', [])\n    updated = st.multiselect(\"Aktive Watchlist:\", options=current_watchlist, default=current_watchlist)\n    if len(updated) != len(current_watchlist):\n        st.session_state['watchlist'] = updated\n        st.rerun()\n    \n    st.metric(\"Anzahl\", len(current_watchlist))

# SCAN LOGIK
scan_triggered = False
if st.button('🚀 Smart Scan Starten', type='primary'):\n    scan_triggered = True

if scan_triggered:
    with st.spinner(\"🔍 Scanne...\"):\n        market_ctx = get_market_context()
n        
        try:\n            r = requests.get('https://finance.yahoo.com/gainers', headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)\n            gainers = pd.read_html(StringIO(r.text))[0]['Symbol'].head(20).tolist()\n        except: \n            gainers = []\n        \n        current_watchlist = st.session_state.get('watchlist', [])\n        scan_list = [(s, '📋') for s in current_watchlist]\n        seen = set(current_watchlist)\n        for g in gainers:\n            if g not in seen:\n                scan_list.append((g, '🌍'))\n                seen.add(g)\n        \n        results = []\n        progress_bar = st.progress(0)\n        status_text = st.empty()\n        \n        for i, (sym, src) in enumerate(scan_list):\n            tier = i + 1\n            status_text.text(f\"Tier {tier}/{len(scan_list)}: {sym}\")\n            res = analyze_smart(sym, tier, len(scan_list), market_ctx)\n            if res:\n                existing = [r for r in results if r['symbol'] == sym]\n                if not existing or res['score'] > existing[0]['score']:\n                    results = [r for r in results if r['symbol'] != sym]\n                    results.append(res)\n            progress_bar.progress((i + 1) / len(scan_list))\n            if i % 10 == 0:\n                time.sleep(0.5)\n        \n        progress_bar.empty()\n        status_text.empty()\n        st.session_state['scan_results'] = results\n        st.session_state['last_scan_time'] = datetime.now()\n        \n        for item in results[:3]:\n            if item['score'] > 75:\n                alert_key = f\"{item['symbol']}_{datetime.now().strftime('%H')}\"\n                sent_alerts = st.session_state.get('sent_alerts', set())\n                if alert_key not in sent_alerts:\n                    setup_type = \"CATALYST\" if (item.get('news') and item['news'][0]['tier'] == 1) else \"GOLD\"\n                    send_telegram_alert(\n                        item['symbol'], item['price'], item['pullback_pct'],\n                        item.get('news', [{}])[0], setup_type,\n                        item.get('pe_ratio'), item.get('api_sources'), item.get('tier')\n                    )\n                    sent_alerts.add(alert_key)\n                    st.session_state['sent_alerts'] = sent_alerts\n                    st.toast(f\"🚨 T{item['tier']} {item['symbol']} Alert!\")\n\n# ERGEBNISSE ANZEIGEN\nresults = st.session_state.get('scan_results', [])\n\nif results:\n    col_title, col_status = st.columns([3, 1])\n    with col_title:\n        st.subheader(f\"📊 Gefundene Setups: {len(results)}\")\n    with col_status:\n        if st.session_state.get('auto_refresh', False):\n            refresh_count = st.session_state.get('refresh_count', 0)\n            st.markdown(f'<div style=\"background: #1a1a2e; padding: 10px; border-radius: 8px; border-left: 4px solid #00FF00;\">🔴 LIVE #{refresh_count}</div>', unsafe_allow_html=True)\n        else:\n            last_scan = st.session_state.get('last_scan_time')\n            if last_scan:\n                st.caption(f\"Letzter Scan: {last_scan.strftime('%H:%M:%S')}\")\n    \n    stats = st.session_state.get('api_stats', {})\n    api_summary = {}\n    for r in results:\n        for api in r.get('api_sources', []):\n            api_summary[api] = api_summary.get(api, 0) + 1\n    \n    cache_count = sum(1 for r in results if r.get('from_cache'))\n    \n    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)\n    col_stat1.metric(\"Setups\", len(results))\n    \n    fh_calls = stats.get('finnhub', 0) if isinstance(stats, dict) else 0\n    av_calls = stats.get('alpha_vantage', 0) if isinstance(stats, dict) else 0\n    cache_hits = stats.get('cache_hits', 0) if isinstance(stats, dict) else 0\n    rotations = stats.get('alpha_rotation_count', 0) if isinstance(stats, dict) else 0\n    \n    col_stat2.metric(\"API Calls\", f\"FH:{fh_calls} AV:{av_calls}\")\n    col_stat3.metric(\"Cache Hits\", cache_hits)\n    col_stat4.metric(\"Rotations\", rotations)\n    \n    alpha_detail = \", \".join([f\"K{i+1}:{s['calls_today']}\" for i, s in enumerate(alpha_manager.get_status())])\n    st.success(f\"✅ APIs: {api_summary} | Cache: {cache_count} | Alpha: {alpha_detail}\")\n    st.divider()\n    \n    render_grid(results, st)\n    \n    with st.expander(\"📡 API Details\"):\n        st.write(f\"**Finnhub:** {finnhub_limiter.get_status()}/60 per minute\")\n        st.write(\"**Alpha Vantage Keys:**\")\n        for status in alpha_manager.get_status():\n            active_mark = \"▶️\" if status['active'] else \" \"\n            st.write(f\"  {active_mark} Key {status['index']+1}: {status['calls_today']}/500\")\n        st.write(f\"**Cache Hits:** {cache_hits}\")\n        st.write(f\"**Rotations:** {rotations}\")\n\nelif not scan_triggered:\n    st.info(\"👆 Klicke 'Smart Scan Starten' oder aktiviere 'Live-Modus' in der Sidebar.\")\n```

---

## 🕐 Was die Market Clock macht:

| Feature | Beschreibung |
|---------|-------------|
| **Echtzeit ET** | Zeigt aktuelle US Ostküstenzeit (New York) |
| **Markt-Status** | OPEN / CLOSED / PRE-MARKET / AFTER HOURS mit Farben |
| **Countdown** | Zeit bis Öffnung oder Schließung |
| **Progress Bar** | Wie viel % des Handelstags vergangen ist (nur bei OPEN) |
| **Sidebar Info** | Zusätzliche Markt-Infos in der Sidebar |

Die Uhr **aktualisiert sich automatisch** bei jedem Streamlit-Rerun (alle 30s im Live-Modus oder beim manuellen Scan).
