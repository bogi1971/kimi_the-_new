import streamlit as st
import yfinance as yf
import pandas as pd
import time
import requests
from datetime import datetime, timedelta
import numpy as np
from io import StringIO
import textwrap
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# --- 1. KONFIGURATION & SETUP ---
st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V4.0 - Institutional Grade", page_icon="🐂")

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# --- API KEYS & CONFIG ---
TELEGRAM_BOT_TOKEN = "8317204351:AAHRu-mYYU0_NRIxNGEQ5voneIQaDKeQuF8"
TELEGRAM_CHAT_ID = "5338135874"

FINNHUB_API_KEY = "d652vnpr01qqbln5m9cgd652vnpr01qqbln5m9d0"
ALPHA_VANTAGE_KEY = "N6PM9UCXL55JZTN9"
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# --- DEFAULT WATCHLIST ---
DEFAULT_WATCHLIST = sorted(list(set([
    "AQST", "IBRX", "MRNA", "ASND", "REGN", "DNLI", "VNDA", "ALDX", "RCKT", "LNTH", "NVDA", "TSLA", "BLTE",
    "CRSP", "EDIT", "NTLA", "BEAM", "FATE", "IOVA", "SRPT", "BIIB", "VRTX",
    "GILD", "AMGN", "LLY", "PFE", "MRK", "BMY", "JNJ", "ABBV", "NVS", "AZN", "SNY", "GSK", "TAK", "BNTX",
    "CURE", "NVAX", "INO", "VXRT", "OCGN", "SAVA", "AVXL", "LCTX", "ATOS", "SENS",
    "XERS", "EVFM", "TXMD", "CHRS", "RLAY", "MCRB", "VTYX", "PLRX", "BCRX",
    "ADMA", "GERN", "GOSS", "NKTR", "EXEL", "HALO", "JAZZ", "IONS", "ALNY",
    "AMRN", "MNKD", "RIGL", "VIVK", "RYTM", "APLS", "ZLAB",
    "HUT", "MARA", "RIOT", "COIN", "QGEN", "CRWD", "PLTR", "MSTR", "HOOD"
])))

# --- SESSION STATE ---
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = DEFAULT_WATCHLIST
if 'sent_alerts' not in st.session_state:
    st.session_state['sent_alerts'] = set()
if 'scan_history' not in st.session_state:
    st.session_state['scan_history'] = []

# --- CSS MAGIC (BULLISH, PURPLE & GOLD THEME) ---
st.markdown("""
    <style>
    .stMetric { background-color: #0E1117; padding: 10px; border-radius: 5px; }
    
    @keyframes greenPulse { 0% { box-shadow: 0 0 5px #00FF00; border-color: #00FF00; } 100% { box-shadow: 0 0 15px #00FF00; border-color: #FFF; } }
    @keyframes purplePulse { 0% { box-shadow: 0 0 5px #9933ff; border-color: #9933ff; } 100% { box-shadow: 0 0 30px #bf80ff; border-color: #FFF; } }
    @keyframes goldPulse { 0% { box-shadow: 0 0 5px #FFD700; border-color: #FFD700; } 100% { box-shadow: 0 0 25px #FFD700; border-color: #FFF; } }

    .bull-card { 
        background-color: #0d1f12; 
        border: 2px solid #00FF00; 
        border-radius: 10px; 
        padding: 15px; 
        text-align: center; 
        margin-bottom: 10px; 
        animation: greenPulse 2.0s infinite alternate; 
    }

    .gold-card { 
        background-color: #2b2b00; 
        border: 3px solid #FFD700; 
        border-radius: 10px; 
        padding: 15px; 
        text-align: center; 
        margin-bottom: 10px; 
        animation: goldPulse 1.5s infinite alternate; 
    }

    .purple-card { 
        background-color: #1a0033; 
        border: 3px solid #9933ff; 
        border-radius: 10px; 
        padding: 15px; 
        text-align: center; 
        margin-bottom: 10px; 
        animation: purplePulse 0.8s infinite alternate; 
    }
    
    .bull-card h3 { color: #00FF00 !important; margin: 0; padding-bottom: 5px; }
    .gold-card h3 { color: #FFD700 !important; margin: 0; padding-bottom: 5px; text-shadow: 0 0 10px #FFD700; }
    .purple-card h3 { color: #bf80ff !important; margin: 0; padding-bottom: 5px; text-shadow: 0 0 10px #9933ff; }
    
    .price { font-size: 1.8rem; font-weight: bold; color: white; margin: 10px 0; }
    .meta { font-size: 0.8rem; color: #bbb; margin-bottom: 10px; }
    
    .action-bull { font-weight: bold; font-style: italic; margin-bottom: 10px; display: block; color: #00FF00; }
    .action-gold { font-weight: bold; font-style: italic; margin-bottom: 10px; display: block; color: #FFD700; font-size: 1.1rem; }
    .action-purple { font-weight: bold; font-style: italic; margin-bottom: 10px; display: block; color: #bf80ff; font-size: 1.2rem; text-transform: uppercase; }
    
    .news-link-btn { 
        display: block;
        background-color: rgba(255, 255, 255, 0.1); 
        color: #fff !important; 
        padding: 8px; 
        border-radius: 5px; 
        font-size: 0.8rem; 
        margin: 8px 0; 
        border: 1px solid #777;
        text-decoration: none;
        transition: all 0.3s;
        text-align: left;
    }
    
    .purple-card .news-link-btn { border-color: #bf80ff; background-color: rgba(153, 51, 255, 0.15); }
    .purple-card .news-link-btn:hover { background-color: rgba(153, 51, 255, 0.4); border-color: white; }

    .gold-card .news-link-btn { border-color: #FFD700; background-color: rgba(255, 215, 0, 0.15); }
    .gold-card .news-link-btn:hover { background-color: rgba(255, 215, 0, 0.4); border-color: white; }
    
    .stop-loss { color: #ff9999; font-weight: bold; font-size: 0.9rem; margin-top: 5px; border: 1px solid #ff4b4b; border-radius: 4px; padding: 2px 8px; display: inline-block; }
    .target { color: #90EE90; font-weight: bold; font-size: 0.9rem; margin-top: 5px; border: 1px solid #00FF00; border-radius: 4px; padding: 2px 8px; display: inline-block; margin-left: 5px; }
    
    .btn-link { display: inline-block; background-color: #262730; padding: 5px 15px; border-radius: 5px; text-decoration: none; font-size: 0.9rem; transition: all 0.3s; border: 1px solid #555; color: white !important; }
    .btn-link:hover { background-color: #444; border-color: white; }
    
    .confidence-bar { width: 100%; height: 4px; background: #333; margin: 5px 0; border-radius: 2px; }
    .confidence-fill { height: 100%; border-radius: 2px; }
    </style>
    """, unsafe_allow_html=True)

# --- INTELLIGENT API STATUS CHECKER ---
@st.cache_resource
def check_api_status_deep():
    status = {"Yahoo Finance": 0, "Finnhub": 0, "Alpha Vantage": 0}
    try:
        if not yf.Ticker("SPY").history(period="1d").empty: 
            status["Yahoo Finance"] = 2
    except: pass

    if "DEIN_" not in FINNHUB_API_KEY:
        try:
            r = requests.get(f"https://finnhub.io/api/v1/news?category=general&token={FINNHUB_API_KEY}", timeout=5)
            if r.status_code == 200: status["Finnhub"] = 2
            elif r.status_code == 429: status["Finnhub"] = 1
        except: pass

    if "DEIN_" not in ALPHA_VANTAGE_KEY:
        try:
            r = requests.get(f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey={ALPHA_VANTAGE_KEY}", timeout=5)
            data = r.json()
            if "Global Quote" in data: status["Alpha Vantage"] = 2
            elif "Note" in data: status["Alpha Vantage"] = 1
        except: pass
    
    return status

# --- SIDEBAR ---
with st.sidebar:
    st.header("📡 System Status")
    api_stats = check_api_status_deep()
    
    for api, code in api_stats.items():
        if "DEIN_" in FINNHUB_API_KEY and api == "Finnhub": color = "#555"; label = "NOT CONFIG"
        elif "DEIN_" in ALPHA_VANTAGE_KEY and api == "Alpha Vantage": color = "#555"; label = "NOT CONFIG"
        elif code == 2: color = "#00ff00"; label = "ONLINE"
        elif code == 1: color = "#ffa500"; label = "LIMIT REACHED"
        else: color = "#ff0000"; label = "OFFLINE"

        st.markdown(f"""
            <div style="display: flex; align-items: center; margin-bottom: 8px; background-color: #1c1c1c; padding: 8px; border-radius: 5px;">
                <div style="width: 10px; height: 10px; border-radius: 50%; background-color: {color}; margin-right: 10px; box-shadow: 0 0 8px {color};"></div>
                <span style="font-size: 0.85rem; font-family: monospace;">{api}</span>
                <span style="margin-left: auto; font-size: 0.7rem; color: #888;">{label}</span>
            </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    
    st.header("📋 Watchlist Manager")
    tab1, tab2 = st.tabs(["Einzeln", "Massen-Import"])
    
    with tab1:
        new_ticker = st.text_input("Ticker einzeln:", placeholder="z.B. AAPL").upper()
        if st.button("➕ Hinzufügen") and new_ticker:
            if new_ticker not in st.session_state.watchlist:
                st.session_state.watchlist.append(new_ticker)
                st.session_state.watchlist = sorted(st.session_state.watchlist)
                st.success(f"{new_ticker} hinzugefügt!")
                time.sleep(0.5)
                st.rerun()

    with tab2:
        st.caption("Paste hier eine Spalte aus Excel oder eine Liste:")
        bulk_input = st.text_area("Ticker Liste:", height=150, placeholder="AAPL\nTSLA\nNVDA")
        if st.button("📥 Import starten"):
            if bulk_input:
                raw_tickers = bulk_input.replace(',', '\n').replace(';', '\n').replace(' ', '\n').split('\n')
                clean_tickers = [t.strip().upper() for t in raw_tickers if t.strip()]
                added_count = 0
                for t in clean_tickers:
                    if t not in st.session_state.watchlist:
                        st.session_state.watchlist.append(t)
                        added_count += 1
                
                if added_count > 0:
                    st.session_state.watchlist = sorted(st.session_state.watchlist)
                    st.success(f"✅ {added_count} Ticker importiert!")
                    time.sleep(1)
                    st.rerun()

    st.divider()
    updated_list = st.multiselect("Aktive Watchlist:", options=st.session_state.watchlist, default=st.session_state.watchlist)
    
    if len(updated_list) != len(st.session_state.watchlist):
        st.session_state.watchlist = updated_list
        st.rerun()
        
    st.metric("Gesamtanzahl", len(st.session_state.watchlist))
    st.divider()
    
    # Scanner Einstellungen
    st.header("⚙️ Scanner Config")
    min_score_threshold = st.slider("Min. Score", 30, 90, 50)
    max_weekly_decline = st.slider("Max. Wochen-Decline", 5, 30, 15) / 100
    
    st.markdown("**Scanning Mode:** 🐂 M-BULL | 📉 W-DIP | 🎯 I-GRADE")

# --- TELEGRAM ---
def send_telegram_alert(symbol, price, news_item, rvol, setup_type):
    if "DEIN_" in TELEGRAM_BOT_TOKEN: return 
    
    news_text = news_item.get('title', 'News')
    news_url = news_item.get('url', f'https://finance.yahoo.com/quote/{symbol}')
    
    emoji = "🟣" if setup_type == "CATALYST" else "🏆" if setup_type == "GOLD" else "🐂"
    
    msg = f"""{emoji} <b>{setup_type}: {symbol}</b> {emoji}

📰 <a href='{news_url}'>{news_text}</a>

💵 Preis: {price:.2f} $
📊 RVOL: {rvol:.1f}x
🎯 Setup: {setup_type}

👉 <a href='https://www.tradingview.com/chart/?symbol={symbol}'>Chart öffnen</a>"""
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = { "chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True }
    try: requests.post(url, data=payload, timeout=5)
    except: pass

# --- MARKET SENTIMENT & BREADTH ---
def get_market_context():
    try:
        # SPY Trend
        spy = yf.Ticker("SPY").history(period="20d")
        qqq = yf.Ticker("QQQ").history(period="20d")
        vix = yf.Ticker("^VIX").history(period="5d")
        
        if spy.empty or qqq.empty: return None
        
        spy_sma20 = spy['Close'].rolling(20).mean().iloc[-1]
        spy_price = spy['Close'].iloc[-1]
        spy_trend = "BULL" if spy_price > spy_sma20 else "BEAR"
        
        # VIX Level (Fear Index)
        vix_level = vix['Close'].iloc[-1] if not vix.empty else 20
        
        # QQQ Relative Strength
        qqq_return_5d = (qqq['Close'].iloc[-1] / qqq['Close'].iloc[-5] - 1) * 100
        
        return {
            'spy_trend': spy_trend,
            'spy_price': spy_price,
            'spy_sma20': spy_sma20,
            'vix': vix_level,
            'qqq_momentum': qqq_return_5d,
            'risk_on': vix_level < 25 and spy_trend == "BULL"
        }
    except:
        return None

# --- INSTITUTIONAL VOLUME ANALYSIS ---
def analyze_volume_profile(df, lookback=20):
    """Analysiert das Volumen-Profil für Support/Resistance Zonen"""
    try:
        recent = df.tail(lookback)
        
        # Volume Weighted Average Price (VWAP) Deviation
        typical_price = (recent['High'] + recent['Low'] + recent['Close']) / 3
        vwap = (typical_price * recent['Volume']).sum() / recent['Volume'].sum()
        
        # Point of Control (Preis mit höchstem Volumen)
        price_volume = {}
        for i in range(len(recent)):
            price = round(recent['Close'].iloc[i], 2)
            vol = recent['Volume'].iloc[i]
            price_volume[price] = price_volume.get(price, 0) + vol
        
        poc = max(price_volume.items(), key=lambda x: x[1])[0] if price_volume else vwap
        
        # Volume Delta (Käufer vs Verkäufer Druck)
        buy_vol = recent[recent['Close'] > recent['Open']]['Volume'].sum()
        sell_vol = recent[recent['Close'] < recent['Open']]['Volume'].sum()
        delta = (buy_vol - sell_vol) / (buy_vol + sell_vol) if (buy_vol + sell_vol) > 0 else 0
        
        return {
            'vwap': vwap,
            'poc': poc,
            'delta': delta,
            'buy_pressure': buy_vol > sell_vol * 1.2  # 20% mehr Käufervolumen
        }
    except:
        return None

# --- TECHNISCHE STRUKTUR ANALYSE ---
def analyze_structure(df):
    """Erkennt Higher Highs / Higher Lows Struktur"""
    try:
        # Finde lokale Hochs und Tiefs (Swing Points)
        highs = df['High'].values
        lows = df['Low'].values
        
        swing_highs = []
        swing_lows = []
        
        # Einfache Swing-Erkennung (3-Bar Pattern)
        for i in range(2, len(highs)-2):
            if highs[i] > highs[i-1] and highs[i] > highs[i-2] and highs[i] > highs[i+1] and highs[i] > highs[i+2]:
                swing_highs.append((i, highs[i]))
            if lows[i] < lows[i-1] and lows[i] < lows[i-2] and lows[i] < lows[i+1] and lows[i] < lows[i+2]:
                swing_lows.append((i, lows[i]))
        
        # Trendstruktur prüfen
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            # Higher Highs?
            hh = swing_highs[-1][1] > swing_highs[-2][1]
            # Higher Lows?
            hl = swing_lows[-1][1] > swing_lows[-2][1]
            
            # Trendstärke (Winkel der Trendlinien)
            if len(swing_highs) >= 3:
                x = np.array([swing_highs[-3][0], swing_highs[-2][0], swing_highs[-1][0]])
                y = np.array([swing_highs[-3][1], swing_highs[-2][1], swing_highs[-1][1]])
                slope_highs, _, _, _, _ = stats.linregress(x, y)
            else:
                slope_highs = 0
            
            return {
                'higher_highs': hh,
                'higher_lows': hl,
                'trend_slope': slope_highs,
                'structure_intact': hh and hl,
                'last_swing_low': swing_lows[-1][1] if swing_lows else df['Low'].tail(5).min()
            }
        
        return {'structure_intact': False, 'last_swing_low': df['Low'].tail(5).min()}
    except:
        return {'structure_intact': False, 'last_swing_low': df['Low'].tail(5).min()}

# --- NEWS CHECKER (INSTITUTIONAL GRADE) ---
@st.cache_data(ttl=300, show_spinner=False)
def check_relevant_news(symbol):
    tier1_keywords = [
        'fda approval', 'fda approved', 'phase 3 success', 'positive phase 3', 'merger agreement', 
        'acquisition', 'buyout', 'takeover', 'strategic review', 'partnership expansion',
        'breakthrough designation', 'fast track', 'orphan drug', 'priority review voucher'
    ]
    tier2_keywords = [
        'earnings beat', 'revenue beat', 'guidance raised', 'price target raised', 'upgrade',
        'insider buying', 'institutional accumulation', 'analyst day', 'data readout',
        'positive interim', 'enrollment completed', 'regulatory submission'
    ]
    tier3_keywords = [
        'initiated coverage', 'buy rating', 'outperform', 'presentation', 'conference',
        'pipeline update', 'new indication', 'label expansion'
    ]
    
    found = []
    
    # Yahoo Finance News (primär)
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        for n in news[:5]:
            title = n.get('title', '').lower()
            link = n.get('link', f"https://finance.yahoo.com/quote/{symbol}")
            publisher = n.get('publisher', '')
            
            # Gewichtung nach Publisher (Bloomberg, Reuters, etc. sind besser als "PennyStockWeekly")
            credibility = 1.0
            high_cred = ['reuters', 'bloomberg', 'cnbc', 'wsj', 'marketwatch', 'barrons', 'benzinga']
            low_cred = ['penny', 'stock', 'trader', 'alert']
            
            if any(h in publisher.lower() for h in high_cred): credibility = 1.3
            if any(l in publisher.lower() for l in low_cred): credibility = 0.7
            
            score = 0
            tier = 0
            if any(k in title for k in tier1_keywords):
                score = 40 * credibility
                tier = 1
            elif any(k in title for k in tier2_keywords):
                score = 25 * credibility
                tier = 2
            elif any(k in title for k in tier3_keywords):
                score = 15 * credibility
                tier = 3
            
            if score > 0:
                found.append({
                    'title': n.get('title', ''), 
                    'url': link, 
                    'score': score, 
                    'tier': tier,
                    'publisher': publisher,
                    'time': n.get('published', '')
                })
    except:
        pass
    
    # Sortiere nach Relevanz
    found.sort(key=lambda x: x['score'], reverse=True)
    return found[:3]  # Top 3 News

# --- CORE ANALYSE (INSTITUTIONAL GRADE) ---
def analyze_institutional_setup(symbol, source_tag='📋', market_context=None):
    """
    Institutioneller Setup-Scanner:
    1. Marktstruktur (Higher Highs/Lows)
    2. Volumen-Profil (POC, VWAP, Delta)
    3. Pullback-Qualität (Tiefe, Geschwindigkeit, Volumen)
    4. Entry-Zone Präzision
    """
    score = 0
    reasons = []
    setup_quality = {}
    
    try:
        ticker = yf.Ticker(symbol)
        
        # Daten holen: 3 Monate 1h für Struktur + 1 Woche 15m für Entry
        df_1h = ticker.history(period='3mo', interval='1h')
        df_15m = ticker.history(period='5d', interval='15m')
        
        if df_1h.empty or len(df_1h) < 50 or df_15m.empty:
            return None
        
        # --- FILTER 1: LIQUIDITÄT & DATENQUALITÄT ---
        avg_volume = df_1h['Volume'].mean()
        if avg_volume < 50000:  # Mindestens 50k pro Stunde
            return None
        
        # Check für Gaps/Splits (abnormale Returns)
        hourly_returns = df_1h['Close'].pct_change().abs()
        if hourly_returns.max() > 0.3:  # 30% in einer Stunde = Split oder Fehler
            return None
        
        current_price = df_1h['Close'].iloc[-1]
        
        # --- FILTER 2: MONATLICHE STRUKTUR (Trend) ---
        structure = analyze_structure(df_1h)
        
        if not structure['structure_intact']:
            return None  # Kein Higher High / Higher Low Pattern
        
        score += 25
        reasons.append("✅ HH/HL Struktur")
        
        # Trendstärke (Steigung der Hochs)
        if structure['trend_slope'] > 0.01:  # Steigende Trendlinie
            score += 10
            reasons.append("📈 Steigende Trendlinie")
        
        # --- FILTER 3: WOCHEN-PULLBACK ANALYSE ---
        week_data = df_1h.tail(35)  # ~1 Woche
        week_high = week_data['High'].max()
        week_low = week_data['Low'].min()
        week_range = week_high - week_low
        
        if week_range == 0:
            return None
        
        # Aktuelle Position im Wochen-Range (0 = Low, 1 = High)
        position_in_range = (current_price - week_low) / week_range
        
        # OPTIMAL: Wir wollen nahe am Low sein (0.0 - 0.3), aber nicht am absoluten Boden
        if position_in_range > 0.4:  # Zu hoch im Range = kein Dip
            return None
        
        if position_in_range < 0.05:  # Zu nah am Boden = Falling Knife
            return None
        
        score += 20
        reasons.append(f"📉 Dip-Zone {position_in_range:.0%}")
        
        # Pullback-Geschwindigkeit (sollte nicht zu schnell sein)
        recent_3d = df_1h.tail(21)  # 3 Tage
        decline_speed = (recent_3d['Close'].iloc[-1] / recent_3d['Close'].iloc[0] - 1)
        
        if decline_speed < -0.30:  # Mehr als 30% in 3 Tagen = zu heiß
            return None
        
        if decline_speed > -0.05:  # Weniger als 5% = kein echter Dip
            return None
        
        # --- FILTER 4: VOLUMEN-ANALYSE ---
        vol_profile = analyze_volume_profile(df_1h)
        if vol_profile:
            # Preis nahe am Point of Control (Institutionelle Akkumulationszone)
            poc_distance = abs(current_price - vol_profile['poc']) / current_price
            if poc_distance < 0.03:  # Innerhalb 3% vom POC
                score += 15
                reasons.append("🎯 POC-Zone")
            
            # Käuferdruck im letzten Push
            if vol_profile['buy_pressure']:
                score += 10
                reasons.append("📊 Käuferdruck")
            
            # Volumen-Delta positiv
            if vol_profile['delta'] > 0.1:
                score += 10
                reasons.append("⚡ Vol-Delta +")
        
        # Relative Volume
        avg_vol_20 = df_1h['Volume'].tail(20).mean()
        current_vol = df_1h['Volume'].iloc[-1]
        rvol = current_vol / avg_vol_20 if avg_vol_20 > 0 else 1.0
        
        if rvol > 2.0:  # Aussergewöhnliches Volumen
            score += 15
            reasons.append(f"🔥 RVOL {rvol:.1f}x")
        elif rvol > 1.5:
            score += 10
            reasons.append(f"⚡ RVOL {rvol:.1f}x")
        
        # --- FILTER 5: ENTRY-PRÄZISION (15m) ---
        # Prüfe auf 15m Chart für besseren Entry
        ema9_15m = df_15m['Close'].ewm(span=9).mean().iloc[-1]
        ema20_15m = df_15m['Close'].ewm(span=20).mean().iloc[-1]
        
        # Bullish Alignment auf 15m
        if current_price > ema9_15m > ema20_15m:
            score += 10
            reasons.append("🎯 15m Bull-Align")
        
        # --- FILTER 6: NEWS & CATALYST ---
        news_items = check_relevant_news(symbol)
        news_score = 0
        top_news = None
        
        if news_items:
            top_news = news_items[0]
            news_score = top_news['score']
            
            if top_news['tier'] == 1:
                score += min(35, news_score)
                reasons.append(f"🚨 {top_news['title'][:30]}...")
            elif top_news['tier'] == 2:
                score += min(25, news_score)
                reasons.append(f"📰 {top_news['title'][:30]}...")
        
        # --- STOP LOSS & TARGET BERECHNUNG ---
        atr = df_1h['High'].rolling(14).max() - df_1h['Low'].rolling(14).min()
        atr = atr.rolling(14).mean().iloc[-1]
        
        # Technischer Stop: Unter dem letzten Swing Low oder 2x ATR
        technical_stop = structure['last_swing_low'] * 0.99
        atr_stop = current_price - (2 * atr)
        stop_loss = max(technical_stop, atr_stop)
        
        # Target: 2:1 Reward/Risk minimum, oder nächstes Swing High
        risk = current_price - stop_loss
        target = current_price + (risk * 3)  # 3:1 R/R
        
        # Oder technisches Target (letztes Hoch)
        if structure.get('swing_highs'):
            last_high = structure['swing_highs'][-1][1]
            if last_high > current_price:
                target = min(target, last_high * 0.98)  # Leicht unter dem Hoch
        
        rr_ratio = (target - current_price) / risk if risk > 0 else 0
        
        if rr_ratio < 2:  # Zu schlechtes Risk/Reward
            return None
        
        # --- MARKT-KONTEXT FILTER ---
        if market_context:
            if not market_context['risk_on']:
                score -= 15  # Strafe in Bärenmärkten
                reasons.append("⚠️ VIX hoch")
            
            # Sektor-Check (einfache Heuristik)
            if symbol in ['MRNA', 'BNTX', 'NVAX', 'INO', 'VXRT']:
                # Biotech braucht extra Vorsicht wenn XBI fällt
                pass  # Hier könnte XBI Check rein
        
        # Finale Qualitätsprüfung
        if score < 50:  # Minimum Threshold
            return None
        
        setup_quality = {
            'structure_score': 35 if structure['structure_intact'] else 0,
            'pullback_quality': 20 if 0.05 < position_in_range < 0.4 else 0,
            'volume_score': min(25, (rvol - 1) * 10) if rvol > 1 else 0,
            'entry_precision': 15 if current_price > ema9_15m else 0,
            'news_score': news_score,
            'rr_ratio': rr_ratio
        }
        
        return {
            'symbol': symbol,
            'score': min(100, int(score)),
            'price': current_price,
            'stop_loss': stop_loss,
            'target': target,
            'rr_ratio': rr_ratio,
            'rvol': rvol,
            'reasons': reasons,
            'source': source_tag,
            'news': news_items,
            'setup_quality': setup_quality,
            'position_in_range': position_in_range,
            'structure': structure,
            'timestamp': datetime.now()
        }
        
    except Exception as e:
        return None

# --- RENDER ---
def render_grid(result_data, container):
    if not result_data:
        container.warning('Keine institutionellen Setups gefunden. Markt zu schwach oder keine Qualitäts-Dips.')
        return
    
    # Sortiere nach Score, dann nach R/R Ratio
    result_data.sort(key=lambda x: (x['score'], x['rr_ratio']), reverse=True)
    cols = container.columns(4)
    
    scan_time = datetime.now().strftime("%H:%M")
    
    for i, item in enumerate(result_data[:20]):  # Max 20 beste
        score = item['score']
        sym = item['symbol']
        price = item['price']
        sl = item['stop_loss']
        target = item['target']
        rr = item['rr_ratio']
        rvol = item['rvol']
        reasons_txt = ' | '.join(item['reasons'][:4])  # Max 4 Reasons
        news_found = item.get('news', [])
        tv_url = f'https://www.tradingview.com/chart/?symbol={sym}'
        
        # Confidence Bar Farbe
        conf_color = '#9933ff' if score > 85 else '#FFD700' if score > 70 else '#00FF00'
        
        with cols[i % 4]:
            
            # --- CARD SELECTION ---
            if score > 85 and news_found and news_found[0]['tier'] == 1:
                # PURPLE: High Score + Tier 1 News
                news_item = news_found[0]
                news_hl = textwrap.shorten(news_item['title'], width=80, placeholder="...")
                
                html_parts = [
                    f'<div class="purple-card">',
                    f'<h3>🟣 {sym}</h3>',
                    f'<div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>',
                    f'<div class="meta">SCORE: {score} | R/R {rr:.1f}:1</div>',
                    f'<div class="price">${price:.2f}</div>',
                    f'<div class="action-purple">INSTITUTIONAL CATALYST</div>',
                    f'<a href="{news_item["url"]}" target="_blank" class="news-link-btn">🔗 {news_hl}</a>',
                    f'<div style="margin: 5px 0;">',
                    f'<span class="stop-loss">SL: ${sl:.2f}</span>',
                    f'<span class="target">TP: ${target:.2f}</span>',
                    f'</div>',
                    f'<div class="meta" style="color: #bf80ff; font-size: 0.75rem;">⏰ {scan_time} | RVOL {rvol:.1f}x</div>',
                    f'<div class="meta" style="font-size: 0.7rem; margin-top: 5px;">{reasons_txt}</div>',
                    f'<a href="{tv_url}" target="_blank" class="btn-link">📊 Analyse</a>',
                    f'</div>'
                ]
                st.markdown("".join(html_parts), unsafe_allow_html=True)
                
            elif score > 70 or (score > 60 and rr > 2.5):
                # GOLD: High Score oder gutes R/R
                news_html = ""
                if news_found:
                    news_item = news_found[0]
                    news_hl = textwrap.shorten(news_item['title'], width=80, placeholder="...")
                    news_html = f'<a href="{news_item["url"]}" target="_blank" class="news-link-btn">🔗 {news_hl}</a>'
                
                html_parts = [
                    f'<div class="gold-card">',
                    f'<h3>🏆 {sym}</h3>',
                    f'<div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>',
                    f'<div class="meta">SCORE: {score} | R/R {rr:.1f}:1</div>',
                    f'<div class="price">${price:.2f}</div>',
                    f'<div class="action-gold">HIGH PROBABILITY SETUP</div>',
                    news_html,
                    f'<div style="margin: 5px 0;">',
                    f'<span class="stop-loss">SL: ${sl:.2f}</span>',
                    f'<span class="target">TP: ${target:.2f}</span>',
                    f'</div>',
                    f'<div class="meta" style="color: #FFD700; font-size: 0.75rem;">⏰ {scan_time} | RVOL {rvol:.1f}x</div>',
                    f'<div class="meta" style="font-size: 0.7rem; margin-top: 5px;">{reasons_txt}</div>',
                    f'<a href="{tv_url}" target="_blank" class="btn-link">📈 Chart</a>',
                    f'</div>'
                ]
                st.markdown("".join(html_parts), unsafe_allow_html=True)
                
            else:
                # GREEN: Standard Setup
                html_parts = [
                    f'<div class="bull-card">',
                    f'<h3>🐂 {sym}</h3>',
                    f'<div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>',
                    f'<div class="meta">Score: {score} | R/R {rr:.1f}:1</div>',
                    f'<div class="price">${price:.2f}</div>',
                    f'<div class="action-bull">PULLBACK SETUP</div>',
                    f'<div style="margin: 5px 0;">',
                    f'<span class="stop-loss">SL: ${sl:.2f}</span>',
                    f'<span class="target">TP: ${target:.2f}</span>',
                    f'</div>',
                    f'<div class="meta" style="font-size: 0.75rem;">RVOL: {rvol:.1f}x | {scan_time}</div>',
                    f'<div class="meta" style="font-size: 0.65rem;">{reasons_txt}</div>',
                    f'<a href="{tv_url}" target="_blank" class="btn-link">📈 Chart</a>',
                    f'</div>'
                ]
                st.markdown("".join(html_parts), unsafe_allow_html=True)

# --- MAIN ---
st.title('🐂 Elite Bull Scanner Pro V4.0 (Institutional Grade)')
st.caption("Struktur-basierte Dip-Buying Strategie | Higher Highs/Higher Lows | Volumen-Profil Analyse")

main_placeholder = st.empty()

if st.button('🚀 Start Institutional Scan', type='primary'):
    with main_placeholder.container():
        st.info("🔍 Analysiere Marktstruktur & Volumen-Profile...")
        
        # Market Context holen
        market_ctx = get_market_context()
        
        # Gainers holen für zusätzliche Universum
        try:
            r = requests.get('https://finance.yahoo.com/gainers', headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            html_data = StringIO(r.text)
            gainers = pd.read_html(html_data)[0]['Symbol'].head(20).tolist()
        except:
            gainers = []
        
        # Kombiniere Watchlist + Gainers (ohne Duplikate)
        scan_list = [(s, '📋') for s in st.session_state.watchlist]
        seen = set(st.session_state.watchlist)
        for g in gainers:
            if g not in seen:
                scan_list.append((g, '🌍'))
                seen.add(g)
        
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, (sym, src) in enumerate(scan_list):
            status_text.text(f"Analysiere {sym}... ({i+1}/{len(scan_list)})")
            
            res = analyze_institutional_setup(sym, src, market_ctx)
            
            if res and res['score'] >= min_score_threshold:
                # Duplikat-Check (nur beste Score pro Symbol behalten)
                existing = [r for r in results if r['symbol'] == sym]
                if not existing or res['score'] > existing[0]['score']:
                    results = [r for r in results if r['symbol'] != sym]
                    results.append(res)
            
            progress_bar.progress((i + 1) / len(scan_list))
            time.sleep(0.1)  # Rate limiting
        
        progress_bar.empty()
        status_text.empty()
        
        if not results:
            st.error("❌ Keine institutionellen Setups gefunden. Prüfe: 1) Markt-Trend 2) VIX Level 3) Watchlist Qualität")
            if market_ctx:
                st.json({
                    "SPY Trend": market_ctx['spy_trend'],
                    "VIX": f"{market_ctx['vix']:.1f}",
                    "QQQ 5D": f"{market_ctx['qqq_momentum']:.1f}%"
                })
        else:
            # Sortiere und speichere in History
            results.sort(key=lambda x: x['score'], reverse=True)
            st.session_state['scan_history'] = results[:10]
            
            # Live-Loop
            refresh_count = 0
            while True:
                try:
                    # Alle 5 Zyklen Market Context aktualisieren
                    if refresh_count % 5 == 0:
                        market_ctx = get_market_context()
                    
                    with main_placeholder.container():
                        # Header Stats
                        col1, col2, col3, col4 = st.columns(4)
                        
                        if market_ctx:
                            col1.metric('SPY Trend', 
                                       f"{'🟢' if market_ctx['spy_trend'] == 'BULL' else '🔴'} {market_ctx['spy_trend']}",
                                       f"{((market_ctx['spy_price']/market_ctx['spy_sma20']-1)*100):.2f}%")
                            col2.metric('VIX Fear Index', f"{market_ctx['vix']:.1f}", 
                                       "RISK ON" if market_ctx['vix'] < 20 else "CAUTION" if market_ctx['vix'] < 25 else "RISK OFF")
                        
                        col3.metric('Setup Candidates', len(results))
                        col4.metric('Last Update', datetime.now().strftime("%H:%M:%S"))
                        
                        st.divider()
                        
                        # Alerts für Top Setups
                        for item in results[:3]:  # Nur Top 3
                            if item['score'] > 80 and item.get('news'):
                                alert_key = f"{item['symbol']}_{datetime.now().strftime('%H')}"
                                if alert_key not in st.session_state['sent_alerts']:
                                    setup_type = "CATALYST" if item['score'] > 85 else "GOLD"
                                    send_telegram_alert(
                                        item['symbol'], 
                                        item['price'], 
                                        item['news'][0], 
                                        item['rvol'],
                                        setup_type
                                    )
                                    st.session_state['sent_alerts'].add(alert_key)
                                    st.toast(f"🚨 {setup_type} Alert: {item['symbol']}")
                        
                        # Render Grid
                        render_grid(results, st)
                        
                        # Detail-Analyse für Top Pick
                        if results:
                            top = results[0]
                            with st.expander(f"📊 Detail-Analyse: {top['symbol']} (Score: {top['score']})"):
                                c1, c2 = st.columns(2)
                                with c1:
                                    st.write("**Setup Qualität Breakdown:**")
                                    for key, val in top.get('setup_quality', {}).items():
                                        if isinstance(val, float):
                                            st.write(f"- {key}: {val:.2f}")
                                        else:
                                            st.write(f"- {key}: {val}")
                                with c2:
                                    st.write("**Struktur Daten:**")
                                    st.json(top.get('structure', {}))
                        
                    refresh_count += 1
                    time.sleep(30)
                    
                except Exception as e:
                    st.error(f"Live-Update Fehler: {e}")
                    time.sleep(30)
