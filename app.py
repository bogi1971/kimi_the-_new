import streamlit as st
import yfinance as yf
import pandas as pd
import time
import requests
from datetime import datetime
import warnings
import pytz
from io import StringIO
warnings.filterwarnings('ignore')

# --- 1. KONFIGURATION & SETUP ---
st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V4.3 - Hybrid Edition", page_icon="🐂")

# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
# --- API KEYS & CONFIG ---
TELEGRAM_BOT_TOKEN = "8317204351:AAHRu-mYYU0_NRIxNGEQ5voneIQaDKeQuF8"
TELEGRAM_CHAT_ID = "5338135874"

FINNHUB_API_KEY = "d652vnpr01qqbln5m9cgd652vnpr01qqbln5m9d0"
ALPHA_VANTAGE_KEY = "N6PM9UCXL55JZTN9"
# !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

# --- PULLBACK CONFIG ---
MIN_PULLBACK_PERCENT = 0.10  # Minimum 10%
MAX_PULLBACK_PERCENT = 0.25  # Maximum 25%

# --- DEFAULT WATCHLIST (Deine erweiterte Liste) ---
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

# --- SESSION STATE ---
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = DEFAULT_WATCHLIST
if 'sent_alerts' not in st.session_state:
    st.session_state['sent_alerts'] = set()

# --- CSS MAGIC ---
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
    
    .pullback-badge { background: linear-gradient(45deg, #ff6b6b, #ee5a24); color: white; padding: 4px 12px; border-radius: 12px; font-size: 0.9rem; font-weight: bold; display: inline-block; margin: 5px 0; }
    .pe-badge { background: linear-gradient(45deg, #4ecdc4, #44a3aa); color: white; padding: 2px 8px; border-radius: 8px; font-size: 0.75rem; display: inline-block; margin: 3px 0; }
    
    .news-link-btn { display: block; background-color: rgba(255, 255, 255, 0.1); color: #fff !important; padding: 8px; border-radius: 5px; font-size: 0.8rem; margin: 8px 0; border: 1px solid #777; text-decoration: none; transition: all 0.3s; text-align: left; }
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

# --- HILFSFUNKTIONEN ---
def simple_slope(x_list, y_list):
    n = len(x_list)
    if n < 2: return 0
    x_mean = sum(x_list) / n
    y_mean = sum(y_list) / n
    numerator = sum((x_list[i] - x_mean) * (y_list[i] - y_mean) for i in range(n))
    denominator = sum((x_list[i] - x_mean) ** 2 for i in range(n))
    return numerator / denominator if denominator != 0 else 0

def is_market_open():
    try:
        et = pytz.timezone('US/Eastern')
        now = datetime.now(et)
        if now.weekday() >= 5: return False
        market_open = now.replace(hour=9, minute=30, second=0)
        market_close = now.replace(hour=16, minute=0, second=0)
        return market_open <= now <= market_close
    except:
        return True 

@st.cache_resource
def check_api_status_deep():
    status = {"Yahoo Finance": 0, "Finnhub": 0, "Alpha Vantage": 0}
    try:
        if not yf.Ticker("SPY").history(period="1d").empty: 
            status["Yahoo Finance"] = 2
    except: pass
    return status

# --- SIDEBAR ---
with st.sidebar:
    st.header("📡 System Status")
    api_stats = check_api_status_deep()
    
    for api, code in api_stats.items():
        color = "#00ff00" if code == 2 else "#ff0000"
        label = "ONLINE" if code == 2 else "OFFLINE"
        st.markdown(f"""
            <div style="display: flex; align-items: center; margin-bottom: 8px; background-color: #1c1c1c; padding: 8px; border-radius: 5px;">
                <div style="width: 10px; height: 10px; border-radius: 50%; background-color: {color}; margin-right: 10px;"></div>
                <span style="font-size: 0.85rem;">{api}</span>
                <span style="margin-left: auto; font-size: 0.7rem; color: #888;">{label}</span>
            </div>
        """, unsafe_allow_html=True)
    
    st.divider()
    st.header("📋 Watchlist Manager")
    
    tab1, tab2 = st.tabs(["Einzeln", "Massen-Import"])
    with tab1:
        new_ticker = st.text_input("Ticker:", placeholder="z.B. AAPL").upper()
        if st.button("➕ Hinzufügen") and new_ticker:
            if new_ticker not in st.session_state.watchlist:
                st.session_state.watchlist.append(new_ticker)
                st.session_state.watchlist = sorted(st.session_state.watchlist)
                st.success(f"{new_ticker} hinzugefügt!")
                time.sleep(0.5)
                st.rerun()

    with tab2:
        bulk_input = st.text_area("Ticker Liste:", height=150, placeholder="AAPL\nTSLA\nNVDA")
        if st.button("📥 Import"):
            if bulk_input:
                raw = bulk_input.replace(',', '\n').replace(';', '\n').split('\n')
                clean = [t.strip().upper() for t in raw if t.strip()]
                added = 0
                for t in clean:
                    if t not in st.session_state.watchlist:
                        st.session_state.watchlist.append(t)
                        added += 1
                if added > 0:
                    st.session_state.watchlist = sorted(st.session_state.watchlist)
                    st.success(f"✅ {added} Ticker importiert!")
                    time.sleep(1)
                    st.rerun()
    
    st.divider()
    updated = st.multiselect("Aktive Watchlist:", options=st.session_state.watchlist, default=st.session_state.watchlist)
    if len(updated) != len(st.session_state.watchlist):
        st.session_state.watchlist = updated
        st.rerun()
    
    st.metric("Anzahl Watchlist", len(st.session_state.watchlist))
    
    # Pullback Range Anzeige
    st.divider()
    st.markdown(f"""
        <div style="background: #1c1c1c; padding: 10px; border-radius: 5px; border-left: 3px solid #FFD700;">
            <b>Pullback Range:</b><br>
            Min: {MIN_PULLBACK_PERCENT:.0%}<br>
            Max: {MAX_PULLBACK_PERCENT:.0%}<br>
            <small>Schwache Rücksetzer werden ignoriert</small>
        </div>
    """, unsafe_allow_html=True)

# --- TELEGRAM (KORRIGIERTE URLs) ---
def send_telegram_alert(symbol, price, pullback_pct, news_item, setup_type, pe_ratio=None):
    if "DEIN_" in TELEGRAM_BOT_TOKEN: 
        return
    
    news_text = news_item.get('title', 'News') if news_item else 'Keine News'
    news_url = news_item.get('url', f'https://finance.yahoo.com/quote/{symbol}') if news_item else f'https://finance.yahoo.com/quote/{symbol}'
    emoji = "🟣" if setup_type == "CATALYST" else "🏆" if setup_type == "GOLD" else "🐂"
    
    pe_info = f"\n📊 P/E: {pe_ratio:.1f}" if pe_ratio else ""
    
    msg = f"""{emoji} <b>{setup_type}: {symbol}</b> {emoji}

📉 Pullback: <b>-{pullback_pct:.1f}%</b>
💵 Preis: ${price:.2f}{pe_info}
📰 {news_text[:60]}...

👉 <a href='{news_url}'>News</a> | <a href='https://www.tradingview.com/chart/?symbol={symbol}'>Chart</a>"""
    
    # KORRIGIERT: Keine Leerzeichen in URL!
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try: 
        requests.post(url, data=payload, timeout=5)
    except: 
        pass

# --- MARKT KONTEXT ---
def get_market_context():
    try:
        spy = yf.Ticker("SPY").history(period="20d")
        if spy.empty: return None
        sma20 = spy['Close'].rolling(20).mean().iloc[-1]
        price = spy['Close'].iloc[-1]
        return {
            'spy_trend': "BULL" if price > sma20 else "BEAR",
            'spy_price': price,
            'spy_sma20': sma20,
            'risk_off': price < sma20 * 0.98 
        }
    except: 
        return None

# --- TECHNISCHE ANALYSE ---
def analyze_structure(df):
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
            
            return {
                'higher_highs': hh, 
                'higher_lows': hl, 
                'trend_slope': slope, 
                'structure_intact': hh and hl, 
                'last_swing_low': swing_lows[-1][1], 
                'last_swing_high': swing_highs[-1][1]
            }
        
        return {
            'structure_intact': False, 
            'last_swing_low': df['Low'].tail(5).min(), 
            'last_swing_high': df['High'].tail(20).max()
        }
    except:
        return {
            'structure_intact': False, 
            'last_swing_low': df['Low'].tail(5).min(), 
            'last_swing_high': df['High'].tail(20).max()
        }

# --- NEWS CHECKER (DEUTSCH + ENGLISCH) ---
@st.cache_data(ttl=300, show_spinner=False)
def check_relevant_news(symbol):
    """
    Bilinguale News-Analyse (Deutsch + Englisch)
    Gewichtung: Tier 1 (40 Pkt), Tier 2 (25 Pkt), Tier 3 (15 Pkt)
    """
    
    # TIER 1: Major Catalysts (Game Changer)
    tier1_keywords = [
        # Englisch
        'fda approval', 'fda approved', 'phase 3 success', 'positive phase 3', 
        'merger agreement', 'acquisition', 'buyout', 'takeover', 'acquired by',
        'breakthrough therapy', 'fast track designation', 'orphan drug status',
        'marketing authorization', 'regulatory approval', 'clinical trial success',
        'strategic review', 'exploring strategic alternatives', 'going private',
        'special dividend', 'spin-off', 'ipo', 'direct listing',
        # Deutsch
        'zulassung', 'fda zulassung', 'marktzulassung', 'übernahmeangebot', 
        'übernahme durch', 'fusion', 'phase 3 erfolg', 'durchbruchstherapie',
        'orphan drug', 'beschleunigtes verfahren', 'klinische studie erfolgreich',
        'strategische alternative', 'privatisierung', 'sonderdividende', 'börsengang'
    ]
    
    # TIER 2: Strong Positive (Momentum Driver)
    tier2_keywords = [
        # Englisch
        'earnings beat', 'revenue beat', 'eps beat', 'guidance raised', 
        'outperform', 'upgrade', 'price target raised', 'buy rating', 'strong buy',
        'partnership', 'collaboration agreement', 'licensing deal', 'co-development',
        'insider buying', 'institutional accumulation', '13f filing', 'activist investor',
        'share buyback', 'dividend increase', 'special dividend',
        'positive data readout', 'interim analysis positive', 'enrollment completed',
        'new indication', 'label expansion', 'pediatric indication',
        'analyst day', 'investor day', 'pipeline update', 'r&d day',
        # Deutsch
        'quartalszahlen übertroffen', 'umsatz über erwartung', 'prognose erhöht',
        'upgrade', 'kaufempfehlung', 'zielpreis erhöht', 'partnerschaft', 
        'kooperation', 'lizenzvereinbarung', 'insiderkäufe', 'rückkaufprogramm',
        'dividende erhöht', 'positive daten', 'studienabschluss', 'neue indikation',
        'analysten tag', 'pipeline update', 'forschungstag'
    ]
    
    # TIER 3: Moderate Positive (Context)
    tier3_keywords = [
        # Englisch
        'initiated coverage', 'overweight', 'market perform', 'neutral to buy',
        'conference presentation', 'abstract accepted', 'poster presentation',
        'patent granted', 'intellectual property', 'trade secret',
        'new contract', 'expansion', 'new market entry', 'geographic expansion',
        'ceo interview', 'cnbc', 'bloomberg', 'reuters', 'wsj',
        # Deutsch
        'neue coverage', 'marktgewicht', 'konferenz', 'kongress', 'patent erteilt',
        'geistiges eigentum', 'neuer vertrag', 'expansion', 'markteintritt',
        'ceo interview', 'handelsblatt', 'börsenzeitung', 'finanzen.net'
    ]
    
    found = []
    
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        
        for n in news[:8]:  # Prüfe top 8 News
            title = n.get('title', '').lower()
            publisher = n.get('publisher', '').lower()
            
            # Credibility Check
            credibility = 1.0
            high_cred = ['reuters', 'bloomberg', 'cnbc', 'wsj', 'marketwatch', 
                        'barrons', 'benzinga', 'handelsblatt', 'börsenzeitung', 
                        'finanzen.net', 'onvista', 'ariva']
            low_cred = ['penny', 'stock alert', 'trader', 'hot stock', 'pump']
            
            if any(h in publisher for h in high_cred): 
                credibility = 1.3
            if any(l in publisher for l in low_cred): 
                credibility = 0.7
            
            # Keyword Matching
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
                    'title': n.get('title'), 
                    'url': n.get('link'), 
                    'score': score, 
                    'tier': tier,
                    'publisher': n.get('publisher', 'Unknown')
                })
    except: 
        pass
    
    # Sort by score, return top 3
    found.sort(key=lambda x: x['score'], reverse=True)
    return found[:3]

# --- CORE ANALYSE (KORRIGIERT) ---
def analyze_strong_pullback(symbol, source_tag='📋', market_context=None):
    try:
        ticker = yf.Ticker(symbol)
        
        # Daten holen
        df = ticker.history(period='3mo', interval='1h')
        if df.empty or len(df) < 50: 
            return None
        if df['Volume'].mean() < 50000: 
            return None
        
        current_price = df['Close'].iloc[-1]
        
        # --- PULLBACK BERECHNUNG ---
        lookback_period = min(70, len(df) - 10)
        recent_data = df.tail(lookback_period)
        recent_high = recent_data['High'].max()
        pullback_pct = (recent_high - current_price) / recent_high
        
        if pullback_pct < MIN_PULLBACK_PERCENT or pullback_pct > MAX_PULLBACK_PERCENT: 
            return None
        
        # --- STRUKTUR PRÜFUNG ---
        structure = analyze_structure(df)
        if not structure['structure_intact']: 
            return None
        if current_price < structure['last_swing_low'] * 0.98: 
            return None 
        
        score = 30  
        reasons = [f"📉 -{pullback_pct:.1f}% Pullback"]
        
        if structure['trend_slope'] > 0.02:
            score += 15
            reasons.append("📈 Steiler Trend")
        
        # --- VOLUMEN ---
        avg_vol = df['Volume'].tail(20).mean()
        current_vol = df['Volume'].iloc[-1]
        rvol = current_vol / avg_vol if avg_vol > 0 else 1.0
        
        if rvol > 2.0:
            score += 20
            reasons.append(f"🔥 Kapitulation Vol {rvol:.1f}x")
        elif rvol > 1.5:
            score += 10
            reasons.append(f"⚡ RVOL {rvol:.1f}x")
        
        # --- SUPPORT ---
        support_distance = (current_price - structure['last_swing_low']) / current_price
        if support_distance < 0.03:
            score += 15
            reasons.append("🎯 Support-Touch")
        elif support_distance < 0.05:
            score += 8
            reasons.append("📍 Nahe Support")
        
        # --- NEWS (Bilingual) ---
        news_items = check_relevant_news(symbol)
        top_news = None
        if news_items:
            top_news = news_items[0]
            if top_news['tier'] == 1:
                score += min(35, int(top_news['score']))
                reasons.append(f"🚨 {top_news['title'][:25]}...")
            elif top_news['tier'] == 2:
                score += min(25, int(top_news['score']))
                reasons.append(f"📰 News")
        
        # --- P/E RATIO FILTER (KORRIGIERT) ---
        pe_ratio = None
        try:
            info = ticker.info
            pe_ratio = info.get('trailingPE') or info.get('forwardPE')
            
            if pe_ratio:
                if pe_ratio < 15:  # Value
                    score += 8
                    reasons.append(f"💰 Value (P/E {pe_ratio:.1f})")
                elif pe_ratio > 100:  # Spekulativ
                    score -= 5
                    reasons.append(f"⚠️ Hoch P/E {pe_ratio:.0f}")
                # 15-100 ist neutral (keine Änderung)
        except:
            pass
        
        # --- RISK MANAGEMENT ---
        stop_loss = structure['last_swing_low'] * 0.985
        target_1 = recent_high * 0.98 
        target_2 = current_price + (current_price - stop_loss) * 2
        target = min(target_1, target_2)
        
        risk = current_price - stop_loss
        reward = target - current_price
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < 1.5: 
            return None
        
        # --- MARKT KONTEXT ---
        if market_context and market_context.get('risk_off'):
            score -= 10
            reasons.append("⚠️ Schwacher Markt")
        
        if score < 50: 
            return None
        
        return {
            'symbol': symbol, 
            'score': min(100, int(score)), 
            'price': current_price,
            'pullback_pct': pullback_pct, 
            'recent_high': recent_high, 
            'stop_loss': stop_loss,
            'target': target, 
            'rr_ratio': rr_ratio, 
            'rvol': rvol, 
            'reasons': reasons,
            'source': source_tag, 
            'news': news_items,
            'structure': structure,
            'pe_ratio': pe_ratio
        }
        
    except Exception as e: 
        return None

# --- RENDER ---
def render_grid(result_data, container):
    if not result_data:
        container.warning(f'Keine starken Pullbacks ({MIN_PULLBACK_PERCENT:.0%}-{MAX_PULLBACK_PERCENT:.0%}) gefunden.')
        return
    
    result_data.sort(key=lambda x: (x['score'], x['pullback_pct']), reverse=True)
    cols = container.columns(4)
    scan_time = datetime.now().strftime("%H:%M")
    
    for i, item in enumerate(result_data[:16]):
        sym = item['symbol']
        score = item['score']
        price = item['price']
        pullback = item['pullback_pct']
        sl = item['stop_loss']
        target = item['target']
        rr = item['rr_ratio']
        rvol = item['rvol']
        pe = item.get('pe_ratio')
        reasons_txt = ' | '.join(item['reasons'][:3])
        news_found = item.get('news', [])
        tv_url = f'https://www.tradingview.com/chart/?symbol={sym}'
        
        conf_color = '#9933ff' if score > 85 else '#FFD700' if score > 70 else '#00FF00'
        pullback_color = '#ff6b6b' if pullback > 0.15 else '#ffa502'
        
        # P/E Badge
        pe_html = f'<div class="pe-badge">P/E: {pe:.1f}</div>' if pe else ''
        
        with cols[i % 4]:
            if score > 80 and news_found and news_found[0]['tier'] == 1:
                n_i = news_found[0]
                st.markdown(f"""
                <div class="purple-card">
                    <h3>🟣 {sym}</h3>
                    <div class="pullback-badge" style="background: {pullback_color};">📉 -{pullback:.1%}</div>
                    {pe_html}
                    <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
                    <div class="meta">SCORE: {score} | R/R {rr:.1f}:1</div>
                    <div class="price">${price:.2f}</div>
                    <div class="action-purple">STRONG PULLBACK PLAY</div>
                    <a href="{n_i['url']}" target="_blank" class="news-link-btn">🔗 {n_i['title'][:70]}...</a>
                    <div><span class="stop-loss">SL: ${sl:.2f}</span><span class="target">TP: ${target:.2f}</span></div>
                    <div class="meta" style="color: #bf80ff; font-size: 0.75rem; margin-top:5px;">⏰ {scan_time} | RVOL {rvol:.1f}x</div>
                    <div class="meta" style="font-size: 0.7rem;">{reasons_txt}</div>
                    <a href="{tv_url}" target="_blank" class="btn-link">📊 Chart</a>
                </div>""", unsafe_allow_html=True)
                
            elif score > 65:
                n_html = f'<a href="{news_found[0]["url"]}" target="_blank" class="news-link-btn">🔗 {news_found[0]["title"][:70]}...</a>' if news_found else ""
                st.markdown(f"""
                <div class="gold-card">
                    <h3>🏆 {sym}</h3>
                    <div class="pullback-badge" style="background: {pullback_color};">📉 -{pullback:.1%}</div>
                    {pe_html}
                    <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
                    <div class="meta">SCORE: {score} | R/R {rr:.1f}:1</div>
                    <div class="price">${price:.2f}</div>
                    <div class="action-gold">SOLID PULLBACK</div>
                    {n_html}
                    <div><span class="stop-loss">SL: ${sl:.2f}</span><span class="target">TP: ${target:.2f}</span></div>
                    <div class="meta" style="color: #FFD700; font-size: 0.75rem; margin-top:5px;">⏰ {scan_time} | RVOL {rvol:.1f}x</div>
                    <div class="meta" style="font-size: 0.7rem;">{reasons_txt}</div>
                    <a href="{tv_url}" target="_blank" class="btn-link">📈 Chart</a>
                </div>""", unsafe_allow_html=True)
                
            else:
                st.markdown(f"""
                <div class="bull-card">
                    <h3>🐂 {sym}</h3>
                    <div class="pullback-badge" style="background: {pullback_color};">📉 -{pullback:.1%}</div>
                    {pe_html}
                    <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
                    <div class="meta">Score: {score} | R/R {rr:.1f}:1</div>
                    <div class="price">${price:.2f}</div>
                    <div class="action-bull">PULLBACK ENTRY</div>
                    <div><span class="stop-loss">SL: ${sl:.2f}</span><span class="target">TP: ${target:.2f}</span></div>
                    <div class="meta" style="font-size: 0.75rem; margin-top:5px;">RVOL: {rvol:.1f}x | ⏰ {scan_time}</div>
                    <div class="meta" style="font-size: 0.65rem;">{reasons_txt}</div>
                    <a href="{tv_url}" target="_blank" class="btn-link">📈 Chart</a>
                </div>""", unsafe_allow_html=True)

# --- MAIN ---
st.title('🐂 Elite Bull Scanner Pro V4.3 - Hybrid Edition')
st.caption(f"Bilingual News (DE/EN) | P/E Filter | Starke Pullbacks: {MIN_PULLBACK_PERCENT:.0%}-{MAX_PULLBACK_PERCENT:.0%}")

if not is_market_open():
    st.warning("⚠️ **Markt geschlossen!** Zeige letzte verfügbare Daten.")

main_placeholder = st.empty()

if st.button('🚀 Scan Starten (Live)', type='primary'):
    with main_placeholder.container():
        st.info("🔍 Suche nach Setup-Kandidaten...")
        
        market_ctx = get_market_context()
        
        # Gainers holen
        try:
            r = requests.get('https://finance.yahoo.com/gainers', headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            gainers = pd.read_html(StringIO(r.text))[0]['Symbol'].head(20).tolist()
        except: 
            gainers = []
        
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
            status_text.text(f"Prüfe {sym}... ({i+1}/{len(scan_list)})")
            res = analyze_strong_pullback(sym, src, market_ctx)
            
            if res:
                existing = [r for r in results if r['symbol'] == sym]
                if not existing or res['score'] > existing[0]['score']:
                    results = [r for r in results if r['symbol'] != sym]
                    results.append(res)
            
            progress_bar.progress((i + 1) / len(scan_list))
        
        progress_bar.empty()
        status_text.empty()
        
        if not results:
            st.error("❌ Keine starken Pullbacks gefunden!")
            if market_ctx:
                st.json({
                    "SPY Trend": market_ctx['spy_trend'],
                    "Risk Off": "Ja" if market_ctx['risk_off'] else "Nein"
                })
        else:
            results.sort(key=lambda x: x['score'], reverse=True)
            
            # Live Loop (nicht blockierend)
            refresh_count = 0
            while True:
                try:
                    if refresh_count % 10 == 0:  # Alle 5 Minuten Market Context aktualisieren
                        market_ctx = get_market_context()
                    
                    with main_placeholder.container():
                        col1, col2, col3, col4 = st.columns(4)
                        
                        if market_ctx:
                            col1.metric('SPY Trend', market_ctx['spy_trend'])
                            col2.metric('Risk Status', 'OFF' if market_ctx['risk_off'] else 'ON')
                        
                        col3.metric('Strong Pullbacks', len(results))
                        col4.metric('Letztes Update', datetime.now().strftime("%H:%M:%S"))
                        
                        st.divider()
                        
                        # Alerts
                        for item in results[:2]:
                            if item['score'] > 75:
                                alert_key = f"{item['symbol']}_{datetime.now().strftime('%H')}"
                                if alert_key not in st.session_state['sent_alerts']:
                                    setup_type = "CATALYST" if (item.get('news') and item['news'][0]['tier'] == 1) else "GOLD"
                                    send_telegram_alert(
                                        item['symbol'], 
                                        item['price'], 
                                        item['pullback_pct'], 
                                        item.get('news', [{}])[0], 
                                        setup_type,
                                        item.get('pe_ratio')
                                    )
                                    st.session_state['sent_alerts'].add(alert_key)
                                    st.toast(f"🚨 {item['symbol']} Alert!")
                        
                        render_grid(results, st)
                        
                        # Statistik
                        with st.expander("📊 Scan Statistik"):
                            avg_pullback = sum(r['pullback_pct'] for r in results) / len(results)
                            avg_pe = sum(r['pe_ratio'] for r in results if r.get('pe_ratio')) / len([r for r in results if r.get('pe_ratio')]) if any(r.get('pe_ratio') for r in results) else 0
                            st.write(f"Durchschnittlicher Pullback: {avg_pullback:.1%}")
                            st.write(f"Durchschnittliches P/E: {avg_pe:.1f}")
                            st.write(f"Stärkster Pullback: {max(r['pullback_pct'] for r in results):.1%}")
                            st.write(f"Mit News: {len([r for r in results if r.get('news')])}/{len(results)}")
                    
                    refresh_count += 1
                    time.sleep(30)  # 30 Sekunden Update-Intervall
                    
                except Exception as e:
                    st.error(f"Live-Update Fehler: {e}")
                    time.sleep(30)
