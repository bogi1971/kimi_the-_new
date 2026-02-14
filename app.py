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

# --- KONFIGURATION ---
st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V4.3", page_icon="🐂")

# API KEYS (Aus Secrets oder direkt)
try:
    TELEGRAM_BOT_TOKEN = st.secrets["telegram"]["bot_token"]
    TELEGRAM_CHAT_ID = st.secrets["telegram"]["chat_id"]
    FINNHUB_API_KEY = st.secrets["api_keys"]["finnhub"]
    ALPHA_VANTAGE_KEY = st.secrets["api_keys"]["alpha_vantage"]
except:
    # Fallback - Direkteingabe (nicht empfohlen für Produktion)
    TELEGRAM_BOT_TOKEN = "8317204351:AAHRu-mYYU0_NRIxNGEQ5voneIQaDKeQuF8"
    TELEGRAM_CHAT_ID = "5338135874"
    FINNHUB_API_KEY = "d686cs1r01qobepk6ps0d686cs1r01qobepk6psg"
    ALPHA_VANTAGE_KEY = "N1LUO3XJH3B2197B"

# PULLBACK CONFIG
MIN_PULLBACK_PERCENT = 0.10
MAX_PULLBACK_PERCENT = 0.25

# WATCHLIST
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
if 'watchlist' not in st.session_state:
    st.session_state.watchlist = DEFAULT_WATCHLIST
if 'sent_alerts' not in st.session_state:
    st.session_state['sent_alerts'] = set()

# CSS
st.markdown("""
    <style>
    .stMetric { background-color: #0E1117; padding: 10px; border-radius: 5px; }
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
    .pe-badge { background: linear-gradient(45deg, #4ecdc4, #44a3aa); color: white; padding: 2px 8px; border-radius: 8px; font-size: 0.75rem; display: inline-block; margin: 3px 0; }
    .stop-loss { color: #ff9999; font-weight: bold; font-size: 0.9rem; border: 1px solid #ff4b4b; border-radius: 4px; padding: 2px 8px; display: inline-block; }
    .target { color: #90EE90; font-weight: bold; font-size: 0.9rem; border: 1px solid #00FF00; border-radius: 4px; padding: 2px 8px; display: inline-block; margin-left: 5px; }
    .btn-link { display: inline-block; background-color: #262730; padding: 5px 15px; border-radius: 5px; text-decoration: none; font-size: 0.9rem; border: 1px solid #555; color: white !important; }
    .confidence-bar { width: 100%; height: 4px; background: #333; margin: 5px 0; border-radius: 2px; }
    .confidence-fill { height: 100%; border-radius: 2px; }
    </style>
""", unsafe_allow_html=True)

# HILFSFUNKTIONEN
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

def get_market_context():
    try:
        spy = yf.Ticker("SPY").history(period="20d")
        if spy.empty: return None
        sma20 = spy['Close'].rolling(20).mean().iloc[-1]
        price = spy['Close'].iloc[-1]
        return {'spy_trend': "BULL" if price > sma20 else "BEAR", 'risk_off': price < sma20 * 0.98}
    except: 
        return None

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
            return {'higher_highs': hh, 'higher_lows': hl, 'trend_slope': slope, 'structure_intact': hh and hl, 'last_swing_low': swing_lows[-1][1], 'last_swing_high': swing_highs[-1][1]}
        return {'structure_intact': False, 'last_swing_low': df['Low'].tail(5).min(), 'last_swing_high': df['High'].tail(20).max()}
    except:
        return {'structure_intact': False, 'last_swing_low': df['Low'].tail(5).min(), 'last_swing_high': df['High'].tail(20).max()}

@st.cache_data(ttl=300, show_spinner=False)
def check_relevant_news(symbol):
    tier1 = ['fda approval', 'fda approved', 'phase 3 success', 'merger', 'acquisition', 'buyout', 'takeover', 'zulassung', 'übernahmeangebot', 'fusion', 'phase 3 erfolg']
    tier2 = ['earnings beat', 'guidance raised', 'upgrade', 'partnership', 'quartalszahlen übertroffen', 'prognose erhöht', 'kaufempfehlung', 'kooperation']
    found = []
    try:
        ticker = yf.Ticker(symbol)
        news = ticker.news
        for n in news[:5]:
            title = n.get('title', '').lower()
            if any(k in title for k in tier1):
                found.append({'title': n.get('title'), 'url': n.get('link'), 'tier': 1})
            elif any(k in title for k in tier2):
                found.append({'title': n.get('title'), 'url': n.get('link'), 'tier': 2})
    except: 
        pass
    return sorted(found, key=lambda x: x['tier'])[:2]

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
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
    try: 
        requests.post(url, data=payload, timeout=5)
    except: 
        pass

def analyze_strong_pullback(symbol, source_tag='📋', market_context=None):
    try:
        ticker = yf.Ticker(symbol)
        df = ticker.history(period='3mo', interval='1h')
        if df.empty or len(df) < 50: 
            return None
        if df['Volume'].mean() < 50000: 
            return None
        
        current_price = df['Close'].iloc[-1]
        lookback_period = min(70, len(df) - 10)
        recent_data = df.tail(lookback_period)
        recent_high = recent_data['High'].max()
        pullback_pct = (recent_high - current_price) / recent_high
        
        if pullback_pct < MIN_PULLBACK_PERCENT or pullback_pct > MAX_PULLBACK_PERCENT: 
            return None
        
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
        
        avg_vol = df['Volume'].tail(20).mean()
        current_vol = df['Volume'].iloc[-1]
        rvol = current_vol / avg_vol if avg_vol > 0 else 1.0
        
        if rvol > 2.0:
            score += 20
            reasons.append(f"🔥 Vol {rvol:.1f}x")
        elif rvol > 1.5:
            score += 10
            reasons.append(f"⚡ RVOL {rvol:.1f}x")
        
        support_distance = (current_price - structure['last_swing_low']) / current_price
        if support_distance < 0.03:
            score += 15
            reasons.append("🎯 Support")
        elif support_distance < 0.05:
            score += 8
            reasons.append("📍 Nahe Support")
        
        news_items = check_relevant_news(symbol)
        if news_items:
            if news_items[0]['tier'] == 1:
                score += 25
                reasons.append("🚨 Catalyst")
            else:
                score += 15
                reasons.append("📰 News")
        
        # P/E Filter
        pe_ratio = None
        try:
            info = ticker.info
            pe_ratio = info.get('trailingPE') or info.get('forwardPE')
            if pe_ratio:
                if pe_ratio < 15:
                    score += 8
                    reasons.append(f"💰 Value P/E {pe_ratio:.1f}")
                elif pe_ratio > 100:
                    score -= 5
                    reasons.append(f"⚠️ Hoch P/E {pe_ratio:.0f}")
        except:
            pass
        
        stop_loss = structure['last_swing_low'] * 0.985
        target_1 = recent_high * 0.98 
        target_2 = current_price + (current_price - stop_loss) * 2
        target = min(target_1, target_2)
        
        risk = current_price - stop_loss
        reward = target - current_price
        rr_ratio = reward / risk if risk > 0 else 0
        
        if rr_ratio < 1.5: 
            return None
        
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
            'pe_ratio': pe_ratio
        }
    except Exception as e: 
        return None

# SIDEBAR
with st.sidebar:
    st.header("📡 System Status")
    st.markdown("""
        <div style="display: flex; align-items: center; margin-bottom: 8px; background-color: #1c1c1c; padding: 8px; border-radius: 5px;">
            <div style="width: 10px; height: 10px; border-radius: 50%; background-color: #00ff00; margin-right: 10px;"></div>
            <span style="font-size: 0.85rem;">Yahoo Finance</span>
            <span style="margin-left: auto; font-size: 0.7rem; color: #888;">ONLINE</span>
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
    
    st.metric("Anzahl", len(st.session_state.watchlist))

# RENDER
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
                    <a href="{n_i['url']}" target="_blank" class="news-link-btn">🔗 {n_i['title'][:60]}...</a>
                    <div><span class="stop-loss">SL: ${sl:.2f}</span><span class="target">TP: ${target:.2f}</span></div>
                    <div class="meta" style="font-size: 0.7rem; margin-top:5px;">{reasons_txt}</div>
                    <a href="{tv_url}" target="_blank" class="btn-link">📊 Chart</a>
                </div>""", unsafe_allow_html=True)
            elif score > 65:
                n_html = f'<a href="{news_found[0]["url"]}" target="_blank" class="news-link-btn">🔗 {news_found[0]["title"][:60]}...</a>' if news_found else ""
                st.markdown(f"""
                <div class="gold-card">
                    <h3>🏆 {sym}</h3>
                    <div class="pullback-badge" style="background: {pullback_color};">📉 -{pullback:.1%}</div>
                    {pe_html}
                    <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
                    <div class="meta">SCORE: {score} | R/R {rr:.1f}:1</div>
                    <div class="price">${price:.2f}</div>
                    {n_html}
                    <div><span class="stop-loss">SL: ${sl:.2f}</span><span class="target">TP: ${target:.2f}</span></div>
                    <div class="meta" style="font-size: 0.7rem; margin-top:5px;">{reasons_txt}</div>
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
                    <div><span class="stop-loss">SL: ${sl:.2f}</span><span class="target">TP: ${target:.2f}</span></div>
                    <div class="meta" style="font-size: 0.7rem; margin-top:5px;">{reasons_txt}</div>
                    <a href="{tv_url}" target="_blank" class="btn-link">📈 Chart</a>
                </div>""", unsafe_allow_html=True)

# MAIN
st.title('🐂 Elite Bull Scanner Pro V4.3')
st.caption(f"Bilingual News | P/E Filter | Pullbacks: {MIN_PULLBACK_PERCENT:.0%}-{MAX_PULLBACK_PERCENT:.0%}")

if not is_market_open():
    st.warning("⚠️ Markt geschlossen! Zeige letzte verfügbare Daten.")

main_placeholder = st.empty()

if st.button('🚀 Scan Starten', type='primary'):
    with main_placeholder.container():
        st.info("🔍 Suche nach Setup-Kandidaten...")
        
        market_ctx = get_market_context()
        
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
        else:
            results.sort(key=lambda x: x['score'], reverse=True)
            
            refresh_count = 0
            while True:
                try:
                    if refresh_count % 10 == 0:
                        market_ctx = get_market_context()
                    
                    with main_placeholder.container():
                        col1, col2, col3, col4 = st.columns(4)
                        
                        if market_ctx:
                            col1.metric('SPY Trend', market_ctx['spy_trend'])
                            col2.metric('Risk', 'OFF' if market_ctx['risk_off'] else 'ON')
                        
                        col3.metric('Pullbacks', len(results))
                        col4.metric('Update', datetime.now().strftime("%H:%M:%S"))
                        
                        st.divider()
                        
                        for item in results[:2]:
                            if item['score'] > 75:
                                alert_key = f"{item['symbol']}_{datetime.now().strftime('%H')}"
                                if alert_key not in st.session_state['sent_alerts']:
                                    setup_type = "CATALYST" if (item.get('news') and item['news'][0]['tier'] == 1) else "GOLD"
                                    send_telegram_alert(item['symbol'], item['price'], item['pullback_pct'], item.get('news', [{}])[0], setup_type, item.get('pe_ratio'))
                                    st.session_state['sent_alerts'].add(alert_key)
                                    st.toast(f"🚨 {item['symbol']} Alert!")
                        
                        render_grid(results, st)
                    
                    refresh_count += 1
                    time.sleep(30)
                    
                except Exception as e:
                    st.error(f"Fehler: {e}")
                    time.sleep(30)
