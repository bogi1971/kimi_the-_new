Hier ist dein vollständiger, korrigierter Python-Code mit Reihennummern für eine bessere Übersicht. Ich habe die Zeilennummern in Kommentaren am Anfang jeder Zeile eingefügt:

```python
1  import streamlit as st
2  import yfinance as yf
3  import pandas as pd
4  import time
5  import requests
6  from datetime import datetime, timedelta
7  import numpy as np
8  from io import StringIO
9  import warnings
10 import pytz
11 import logging
12 import random
13 
14 warnings.filterwarnings('ignore')
15 logging.basicConfig(level=logging.INFO)
16 logger = logging.getLogger(__name__)
17 
18 # ============================== Helper Functions ==============================
19 def safe_requests_get(url, params=None, headers=None, timeout=10):
20     try:
21         response = requests.get(url, params=params, headers=headers, timeout=timeout)
22         response.raise_for_status()
23         return response
24     except requests.RequestException as e:
25         logger.error(f"API-Request Fehler: {e}")
26         return None
27 
28 def get_market_clock():
29     et = pytz.timezone('US/Eastern')
30     now = datetime.now(et)
31     market_open = now.replace(hour=9, minute=30, second=0, microsecond=0)
32     market_close = now.replace(hour=16, minute=0, second=0, microsecond=0)
33     pre_market = now.replace(hour=4, minute=0, second=0, microsecond=0)
34 
35     holidays_2026 = [(1, 1), (1, 19), (2, 16), (4, 3), (5, 25), (6, 19), (7, 3), (9, 7), (11, 26), (12, 25)]
36     is_holiday = (now.month, now.day) in holidays_2026
37 
38     if now.weekday() >= 5 or is_holiday:
39         status = "CLOSED" if now.weekday() >= 5 else "HOLIDAY"
40         color = "#ff4b4b"
41         countdown = "Weekend" if now.weekday() >= 5 else "Holiday"
42         next_event = "Tuesday 09:30 ET" if is_holiday and now.weekday() == 0 else "Monday 09:30 ET"
43         progress = 0
44     elif now < pre_market:
45         status = "CLOSED"
46         color = "#ff4b4b"
47         countdown = f"Pre-market in {str(pre_market - now)[:8]}"
48         next_event = "04:00 ET"
49         progress = 0
50     elif now < market_open:
51         status = "PRE-MARKET"
52         color = "#FFD700"
53         countdown = f"Opens in {str(market_open - now)[:8]}"
54         next_event = "09:30 ET"
55         progress = 0
56     elif market_open <= now <= market_close:
57         status = "OPEN"
58         color = "#00FF00"
59         countdown = f"Closes in {str(market_close - now)[:8]}"
60         next_event = "16:00 ET"
61         progress = (now - market_open) / (market_close - market_open)
62     else:
63         status = "CLOSED"
64         color = "#ff4b4b"
65         countdown = "Opens tomorrow"
66         next_event = "09:30 ET"
67         progress = 0
68 
69     return {
70         'time': now.strftime('%I:%M:%S %p'),
71         'status': status,
72         'color': color,
73         'countdown': countdown,
74         'next_event': next_event,
75         'progress': progress,
76         'is_open': status == "OPEN",
77         'is_holiday': is_holiday
78     }
79 
80 def get_market_context():
81     try:
82         spy = yf.Ticker("SPY")
83         spy_data = spy.history(period="5d")
84         if len(spy_data) < 2:
85             return {'risk_off': False, 'spy_change': 0, 'market_closed': True}
86         spy_change = (spy_data['Close'].iloc[-1] - spy_data['Close'].iloc[-2]) / spy_data['Close'].iloc[-2]
87         try:
88             vix = yf.Ticker("^VIX")
89             vix_data = vix.history(period="2d")
90             vix_level = vix_data['Close'].iloc[-1] if not vix_data.empty else 20
91         except:
92             vix_level = 20
93         risk_off = (spy_change < -0.02) or (vix_level > 30)
94         return {'risk_off': risk_off, 'spy_change': spy_change, 'vix_level': vix_level, 'market_closed': False}
95     except Exception as e:
96         logger.error(f"Fehler beim Marktkontext: {e}")
97         return {'risk_off': False, 'spy_change': 0, 'vix_level': 20, 'market_closed': True}
98 
99 # ============================== Konfiguration ==============================
100 st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V5.5", page_icon="🐂")
101 
102 # Filter Einstellungen
103 MIN_PULLBACK_PERCENT = 0.05
104 MAX_PULLBACK_PERCENT = 0.50
105 AUTO_REFRESH_INTERVAL = 1800
106 ALERT_COOLDOWN_MINUTES = 60
107 
108 # API Keys
109 TELEGRAM_BOT_TOKEN = "DEIN_TELEGRAM_TOKEN"
110 TELEGRAM_CHAT_ID = "DEINE_CHAT_ID"
111 FINNHUB_API_KEY = "DEIN_FINNHUB_API_KEY"
112 ALPHA_VANTAGE_KEYS = ["API_KEY_1", "API_KEY_2", "API_KEY_3"]
113 
114 DEFAULT_WATCHLIST = sorted(list(set([
115     "ABBV", "ACHV", "ACRS", "ADMA", "ALDX", "ALNY", "AMD", "AMGN", "AMRN", "APLS", "AQST", "ASND",
116     "ATOS", "ATRA", "AVXL", "AZN", "BCRX", "BEAM", "BIIB", "BLTE", "BMRN", "BMY", "BNTX", "CELC",
117     "CHRS", "CING", "COIN", "CRSP", "CRVS", "CRWD", "CURE", "DNLI", "EDIT", "ETON", "EVFM", "EXEL",
118     "FATE", "GERN", "GILD", "GOSS", "GSK", "HALO", "HOOD", "HUT", "IBRX", "INO", "IONS", "IOVA",
119     "JAZZ", "JNJ", "LCTX", "LLY", "LNTH", "MARA", "MCRB", "MNKD", "MRK", "MRNA", "MSTR", "NKTR",
120     "NTLA", "NVAX", "NVDA", "NVS", "OCGN", "PFE", "PLRX", "PLTR", "QGEN", "RAPT", "RCKT", "REGN",
121     "REPL", "RIGL", "RIOT", "RLAY", "ROG", "RYTM", "SAP", "SAVA", "SENS", "SNY", "SRPT", "TAK",
122     "TBPH", "TSLA", "TXMD", "UUUU", "VIVK", "VNDA", "VRTX", "VTYX", "VXRT", "XERS", "ZLAB"
123 ])))
124 
125 # ============================== Session State ==============================
126 def init_session_state():
127     defaults = {
128         'watchlist': DEFAULT_WATCHLIST,
129         'sent_alerts': {},
130         'api_stats': {'yahoo': 0, 'finnhub': 0, 'alpha_vantage': 0, 'cache_hits': 0, 'alpha_rotation_count': 0},
131         'scan_results': [],
132         'last_scan_time': None,
133         'auto_refresh': False,
134         'refresh_count': 0,
135         'last_auto_refresh': 0,
136         'alert_history': [],
137         'scan_debug': [],
138     }
139     for k, v in defaults.items():
140         if k not in st.session_state:
141             st.session_state[k] = v
142 
143 init_session_state()
144 
145 # ============================== Cache & API Manager ==============================
146 class SmartCache:
147     def __init__(self):
148         self.cache = {}
149         self.timestamps = {}
150     def get(self, key, ttl=600):
151         if key in self.cache:
152             age = time.time() - self.timestamps.get(key, 0)
153             if age < ttl:
154                 return self.cache[key]
155             else:
156                 del self.cache[key]
157                 del self.timestamps[key]
158         return None
159     def set(self, key, value):
160         self.cache[key] = value
161         self.timestamps[key] = time.time()
162 
163 news_cache = SmartCache()
164 fundamentals_cache = SmartCache()
165 structure_cache = SmartCache()
166 
167 class RateLimiter:
168     def __init__(self, max_calls, window_seconds):
169         self.max_calls = max_calls
170         self.window = window_seconds
171         self.calls = []
172     def can_call(self):
173         now = time.time()
174         self.calls = [c for c in self.calls if now - c < self.window]
175         return len(self.calls) < self.max_calls
176     def record_call(self):
177         self.calls.append(time.time())
178         return len(self.calls)
179     def get_status(self):
180         now = time.time()
181         self.calls = [c for c in self.calls if now - c < self.window]
182         return f"{len(self.calls)}/{self.max_calls}"
183 
184 class AlphaVantageManager:
185     def __init__(self, keys):
186         self.keys = [k for k in keys if k and len(k) > 10]
187         self.current_index = 0
188         self.limiters = {i: {'calls_today': 0, 'calls_per_min': [], 'key': k, 'exhausted': False} for i, k in enumerate(self.keys)}
189     def get_current_key(self):
190         return self.keys[self.current_index] if self.keys else None
191     def rotate_key(self):
192         if not self.keys:
193             return None
194         for _ in range(len(self.keys)):
195             self.current_index = (self.current_index + 1) % len(self.keys)
196             if not self.limiters[self.current_index]['exhausted']:
197                 stats = st.session_state.get('api_stats', {})
198                 if isinstance(stats, dict):
199                     stats['alpha_rotation_count'] = stats.get('alpha_rotation_count', 0) + 1
200                     st.session_state['api_stats'] = stats
201                 return self.get_current_key()
202         return None
203     def can_call(self, key_index=None):
204         if not self.keys:
205             return False
206         idx = key_index if key_index is not None else self.current_index
207         limiter = self.limiters[idx]
208         now = time.time()
209         limiter['calls_per_min'] = [c for c in limiter['calls_per_min'] if now - c < 60]
210         if len(limiter['calls_per_min']) >= 5:
211             return False
212         if limiter['calls_today'] >= 25:
213             limiter['exhausted'] = True
214             return False
215         return True
216     def record_call(self, key_index=None):
217         idx = key_index if key_index is not None else self.current_index
218         limiter = self.limiters[idx]
219         limiter['calls_per_min'].append(time.time())
220         limiter['calls_today'] += 1
221         stats = st.session_state.get('api_stats', {})
222         if isinstance(stats, dict):
223             stats['alpha_vantage'] = stats.get('alpha_vantage', 0) + 1
224             st.session_state['api_stats'] = stats
225         return limiter['calls_today']
226     def get_status(self):
227         return [{
228             'index': i,
229             'key': f"{k[:4]}...{k[-4:]}" if len(k)>8 else k,
230             'active': i == self.current_index,
231             'calls_today': self.limiters[i]['calls_today'],
232             'exhausted': self.limiters[i]['exhausted'],
233             'can_call': self.can_call(i)
234         } for i, k in enumerate(self.keys)]
235 
236 finnhub_limiter = RateLimiter(60, 60)
237 alpha_manager = AlphaVantageManager(ALPHA_VANTAGE_KEYS)
238 
239 # ============================== Analyse Funktionen ==============================
240 def analyze_structure(df, symbol=None):
241     if df is None or not isinstance(df, pd.DataFrame) or df.empty:
242         return _default_structure_result()
243     required_cols = ['High', 'Low', 'Close']
244     for col in required_cols:
245         if col not in df.columns:
246             return _default_structure_result()
247     if len(df) < 10:
248         return _default_structure_result()
249     df_clean = df[['High', 'Low', 'Close']].dropna()
250     if len(df_clean) < 10:
251         return _default_structure_result()
252     if symbol:
253         cache_key = f"structure_{symbol}"
254         cached = structure_cache.get(cache_key, 300)
255         if cached:
256             return cached
257     try:
258         highs = df_clean['High'].values
259         lows = df_clean['Low'].values
260         swing_highs = []
261         swing_lows = []
262         for i in range(2, len(highs)-2):
263             if not all(np.isfinite([highs[i], highs[i-1], highs[i-2], highs[i+1], highs[i+2]])):
264                 continue
265             if (highs[i] > highs[i-1] and highs[i] > highs[i-2] and 
266                 highs[i] > highs[i+1] and highs[i] > highs[i+2]):
267                 swing_highs.append((i, float(highs[i])))
268             if not all(np.isfinite([lows[i], lows[i-1], lows[i-2], lows[i+1], lows[i+2]])):
269                 continue
270             if (lows[i] < lows[i-1] and lows[i] < lows[i-2] and 
271                 lows[i] < lows[i+1] and lows[i] < lows[i+2]):
272                 swing_lows.append((i, float(lows[i])))
273         if len(swing_highs) >= 2 and len(swing_lows) >= 2:
274             hh = swing_highs[-1][1] > swing_highs[-2][1]
275             hl = swing_lows[-1][1] > swing_lows[-2][1]
276             slope = 0.0
277             if len(swing_highs) >= 3:
278                 x = [float(swing_highs[-3][0]), float(swing_highs[-2][0]), float(swing_highs[-1][0])]
279                 y = [float(swing_highs[-3][1]), float(swing_highs[-2][1]), float(swing_highs[-1][1])]
280                 if all(np.isfinite(val) for val in x + y):
281                     n = len(x)
282                     x_mean = sum(x) / n
283                     y_mean = sum(y) / n
284                     numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
285                     denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
286                     if denominator != 0 and np.isfinite(denominator):
287                         slope = numerator / denominator
288             result = {
289                 'higher_highs': bool(hh),
290                 'higher_lows': bool(hl),
291                 'trend_slope': float(slope) if np.isfinite(slope) else 0.0,
292                 'structure_intact': bool(hh and hl),
293                 'last_swing_low': float(swing_lows[-1][1]),
294                 'last_swing_high': float(swing_highs[-1][1])
295             }
296         else:
297             result = _default_structure_result(df_clean)
298         if symbol:
299             structure_cache.set(cache_key, result)
300         return result
301     except:
302         return _default_structure_result()
303 
304 def _default_structure_result(df=None):
305     if df is not None and not df.empty and 'Low' in df.columns and 'High' in df.columns:
306         try:
307             last_low = float(df['Low'].tail(5).min()) if len(df['Low']) > 0 else 0.0
308             last_high = float(df['High'].tail(20).max()) if len(df['High']) > 0 else 0.0
309             return {
310                 'structure_intact': False,
311                 'higher_highs': False,
312                 'higher_lows': False,
313                 'trend_slope': 0.0,
314                 'last_swing_low': last_low,
315                 'last_swing_high': last_high
316             }
317         except:
318             pass
319     return {
320         'structure_intact': False,
321         'higher_highs': False,
322         'higher_lows': False,
323         'trend_slope': 0.0,
324         'last_swing_low': 0.0,
325         'last_swing_high': 0.0
326     }
327 
328 # ============================== Alpha Vantage Smart Fetch ==============================
329 def get_alpha_vantage_smart(symbol):
330     cache_key = f"av_fund_{symbol}"
331     cached = fundamentals_cache.get(cache_key, 3600)
332     if cached:
333         stats = st.session_state.get('api_stats', {})
334         if isinstance(stats, dict):
335             stats['cache_hits'] = stats.get('cache_hits', 0) + 1
336             st.session_state['api_stats'] = stats
337         return cached, True
338     if not alpha_manager.keys:
339         return None, False
340     attempts = 0
341     max_attempts = len(alpha_manager.keys)
342     while attempts < max_attempts:
343         if alpha_manager.can_call():
344             current_key = alpha_manager.get_current_key()
345             if not current_key:
346                 break
347             url = "https://www.alphavantage.co/query"
348             params = {
349                 'function': 'OVERVIEW',
350                 'symbol': symbol,
351                 'apikey': current_key
352             }
353             try:
354                 response = safe_requests_get(url, params, timeout=15)
355                 if response:
356                     data = response.json()
357                     # Check API Limits/Notes
358                     if 'Note' in data or 'Information' in data:
359                         alpha_manager.limiters[alpha_manager.current_index]['exhausted'] = True
360                         alpha_manager.rotate_key()
361                         attempts += 1
362                         time.sleep(1)
363                         continue
364                     if 'Error Message' in data:
365                         return None, False
366                     if 'Symbol' in data and data['Symbol']:
367                         result = {
368                             'pe_ratio': float(data.get('PERatio', 0)) if data.get('PERatio') and data.get('PERatio') not in ['None', '0'] else None,
369                             'eps': float(data.get('EPS', 0)) if data.get('EPS') and data.get('EPS') not in ['None', '0'] else None,
370                             'sector': data.get('Sector', ''),
371                             'industry': data.get('Industry', ''),
372                             'market_cap': int(float(data.get('MarketCapitalization', 0))) if data.get('MarketCapitalization') and data.get('MarketCapitalization') not in ['None', '0'] else 0
373                         }
374                         fundamentals_cache.set(cache_key, result)
375                         alpha_manager.record_call()
376                         return result, False
377                     else:
378                         return None, False
379                 else:
380                     alpha_manager.rotate_key()
381                     attempts += 1
382                     time.sleep(0.5)
383             except:
384                 alpha_manager.rotate_key()
385                 attempts += 1
386                 time.sleep(0.5)
387         else:
388             alpha_manager.rotate_key()
389             attempts += 1
390     return None, False
391 
392 # ============================== Analyse Funktion ==============================
393 def analyze_smart(symbol, tier, total_tickers, market_ctx=None):
394     debug_info = {'symbol': symbol, 'tier': tier, 'errors': [], 'checks': {}}
395     # Yahoo Daten mit Retry-Logik
396     max_retries = 3
397     df = None
398     last_error = None
399     for attempt in range(max_retries):
400         try:
401             ticker = yf.Ticker(symbol)
402             df = ticker.history(period='3mo', interval='1d')
403             # Zähle erfolgreich
404             stats = st.session_state.get('api_stats', {})
405             stats['yahoo'] = stats.get('yahoo', 0) + 1
406             st.session_state['api_stats'] = stats
407             break
408         except Exception as e:
409             last_error = str(e)
410             if "Too Many Requests" in last_error or "Rate limit" in last_error:
411                 wait_time = (attempt + 1) * 2 + random.uniform(0, 1)
412                 time.sleep(wait_time)
413             else:
414                 time.sleep(0.5)
415     if df is None or df.empty:
416         debug_info['errors'].append(f"Yahoo Fehler: {last_error}")
417         _log_scan_debug(debug_info)
418         return None
419     if len(df) < 15:
420         debug_info['errors'].append(f"Zu wenig Daten: {len(df)}")
421         _log_scan_debug(debug_info)
422         return None
423     df_clean = df.dropna()
424     if len(df_clean) < 10:
425         debug_info['errors'].append("Zu wenig gültige Daten")
426         _log_scan_debug(debug_info)
427         return None
428     current_price = float(df_clean['Close'].iloc[-1])
429     if not np.isfinite(current_price) or current_price <= 0:
430         debug_info['errors'].append("Ungültiger Preis")
431         _log_scan_debug(debug_info)
432         return None
433     lookback = min(60, len(df_clean)-5)
434     recent = df_clean.tail(lookback)
435     recent_high = float(recent['High'].max())
436     if not np.isfinite(recent_high) or recent_high <= 0:
437         debug_info['errors'].append("Kein High")
438         _log_scan_debug(debug_info)
439         return None
440     pullback_pct = (recent_high - current_price) / recent_high
441     if pullback_pct < MIN_PULLBACK_PERCENT or pullback_pct > MAX_PULLBACK_PERCENT:
442         debug_info['errors'].append(f"Pullback {pullback_pct:.2%} außerhalb Grenzen")
443         _log_scan_debug(debug_info)
444         return None
445     # Struktur-Analyse
446     structure = analyze_structure(df_clean, symbol)
447     debug_info['checks']['structure'] = structure.get('structure_intact', False)
448     debug_info['checks']['higher_lows'] = structure.get('higher_lows', False)
449     # Akzeptiere nur höhere lows oder HH+HL
450     if not structure.get('structure_intact', False) and not structure.get('higher_lows', False):
451         debug_info['errors'].append("Kein bullischer Trend")
452         _log_scan_debug(debug_info)
453         return None
454     last_swing_low = structure.get('last_swing_low')
455     if last_swing_low is None or not np.isfinite(last_swing_low) or last_swing_low <= 0:
456         debug_info['errors'].append("Kein Swing Low")
457         _log_scan_debug(debug_info)
458         return None
459     if current_price < last_swing_low * 0.90:
460         debug_info['errors'].append("Preis zu weit unter Swing Low")
461         _log_scan_debug(debug_info)
462         return None
463     # Score Berechnung
464     score = 25
465     if structure.get('structure_intact', False):
466         score += 15
467     elif structure.get('higher_lows', False):
468         score += 10
469     trend_slope = structure.get('trend_slope', 0)
470     if trend_slope is not None and np.isfinite(trend_slope) and trend_slope > 0.005:
471         score += 5
472     # Volumen
473     avg_vol = df_clean['Volume'].mean()
474     current_vol = df_clean['Volume'].iloc[-1]
475     rvol = current_vol / avg_vol if avg_vol > 0 else 1.0
476     if rvol > 2:
477         score += 20
478     elif rvol > 1.0:
479         score += 10
480     # Support
481     support_dist = (current_price - last_swing_low) / current_price if current_price > 0 else 1.0
482     if support_dist < 0.03:
483         score += 15
484     elif support_dist < 0.08:
485         score += 8
486     # News & Fundamental
487     news, sources, cached_news = analyze_news_tiered(symbol, tier, score)
488     if news:
489         score += news[0]['score']
490     fundamentals, fund_cached = None, False
491     if score > 55 and tier <= 10:
492         fundamentals, fund_cached = get_alpha_vantage_smart(symbol)
493     pe_ratio = None
494     if fundamentals:
495         pe_ratio = fundamentals.get('pe_ratio')
496         if pe_ratio is not None and np.isfinite(pe_ratio):
497             if pe_ratio < 15:
498                 score += 8
499             elif pe_ratio > 100:
500                 score -= 5
501     # ATR & SL/TP
502     try:
503         atr = float((df_clean['High'].rolling(14).max() - df_clean['Low'].rolling(14).min()).mean())
504         if not np.isfinite(atr) or atr <= 0:
505             atr = current_price * 0.02
506     except:
507         atr = current_price * 0.02
508     stop_loss = max(last_swing_low * 0.97, current_price - (2*atr))
509     target = min(recent_high * 0.97, current_price + (current_price - stop_loss)*2)
510     if stop_loss <= 0 or stop_loss >= current_price or target <= current_price:
511         debug_info['errors'].append("Ungültige SL/TP")
512         _log_scan_debug(debug_info)
513         return None
514     rr_ratio = (target - current_price) / (current_price - stop_loss) if (current_price - stop_loss) > 0 else 0
515     if rr_ratio < 1.0:
516         debug_info['errors'].append(f"R:R {rr_ratio:.2f} zu niedrig")
517         _log_scan_debug(debug_info)
518         return None
519     if score < 35:
520         debug_info['errors'].append(f"Score {score} zu niedrig")
521         _log_scan_debug(debug_info)
522         return None
523     # Gründe & Zusammenfassung
524     reasons = [f"📉 -{pullback_pct:.1f}%"]
525     if structure.get('structure_intact', False):
526         reasons.append("📈 Trend stark")
527     elif structure.get('higher_lows', False):
528         reasons.append("📈 Trend schwach")
529     if rvol > 1.0:
530         reasons.append(f"⚡ Vol {rvol:.1f}x")
531     if support_dist < 0.03:
532         reasons.append("🎯 Support nah")
533     if news:
534         reasons.append(f"📰 {news[0]['source']}")
535     if pe_ratio is not None:
536         reasons.append(f"{'💰' if pe_ratio<15 else '📊'} PE {pe_ratio:.1f}")
537     # Ergebnis
538     return {
539         'symbol': symbol,
540         'tier': tier,
541         'score': min(100, int(score)),
542         'price': current_price,
543         'pullback_pct': pullback_pct,
544         'recent_high': recent_high,
545         'stop_loss': stop_loss,
546         'target': target,
547         'rr_ratio': rr_ratio,
548         'rvol': rvol,
549         'reasons': reasons,
550         'news': news,
551         'pe_ratio': pe_ratio,
552         'api_sources': list(set([s for s in sources] + (['AV'] if fundamentals else []))),
553         'from_cache': cached_news or fund_cached
554     }
555 
556 def _log_scan_debug(debug_info):
557     scan_debug = st.session_state.get('scan_debug', [])
558     scan_debug.append(debug_info)
559     st.session_state['scan_debug'] = scan_debug[-100:]
560 
561 # ============================== Alert Management ==============================
562 def should_send_alert(symbol, current_price, current_score):
563     sent_alerts = st.session_state.get('sent_alerts', {})
564     now = datetime.now()
565     if symbol not in sent_alerts:
566         return True
567     last_alert = sent_alerts[symbol]
568     time_diff = (now - last_alert['timestamp']).total_seconds() / 60
569     if time_diff < ALERT_COOLDOWN_MINUTES:
570         return False
571     price_change = abs(current_price - last_alert['price']) / last_alert['price']
572     score_change = current_score - last_alert['score']
573     if price_change < 0.02 and score_change < 10:
574         return False
575     return True
576 
577 def record_alert(symbol, price, score, setup_type):
578     st.session_state['sent_alerts'][symbol] = {
579         'timestamp': datetime.now(),
580         'price': price,
581         'score': score,
582         'setup_type': setup_type
583     }
584     st.session_state['alert_history'].append({
585         'timestamp': datetime.now(),
586         'symbol': symbol,
587         'price': price,
588         'score': score,
589         'setup_type': setup_type
590     })
591     st.session_state['alert_history'] = st.session_state['alert_history'][-20:]
592 
593 # ============================== Telegram Alert ==============================
594 def send_telegram_alert(symbol, price, pullback_pct, news_item, setup_type, pe_ratio=None, api_sources=None, tier=None):
595     if not TELEGRAM_BOT_TOKEN or len(TELEGRAM_BOT_TOKEN)<10:
596         return False
597     news_title = news_item.get('title','')[:40] + '...' if news_item else 'Keine News'
598     news_url = news_item.get('url','') if news_item else f'https://finance.yahoo.com/quote/{symbol}'
599     emoji = "🟣" if setup_type=="CATALYST" else "🏆" if setup_type=="GOLD" else "🐂"
600     pe_info = f"\n📊 P/E: {pe_ratio:.1f}" if pe_ratio else ""
601     api_info = f"\n📡 {','.join(api_sources)}" if api_sources else ""
602     tier_info = f"\n🎯 Tier {tier}" if tier else ""
603     msg = f"""{emoji} <b>{setup_type}: {symbol}</b> {emoji}
604 📉 Pullback: <b>-{pullback_pct:.1f}%</b>
605 💵 Preis: ${price:.2f}{pe_info}{api_info}{tier_info}
606 📰 {news_title}
607 👉 <a href='{news_url}'>News</a> | <a href='https://www.tradingview.com/chart/?symbol={symbol}'>Chart</a>"""
608     url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
609     payload = {"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML", "disable_web_page_preview": True}
610     try:
611         requests.post(url, data=payload, timeout=5).raise_for_status()
612         return True
613     except:
614         return False
615 
616 # ============================== Karten HTML ==============================
617 def render_card_html(sym, price, pullback, sl, target, rr, reasons, news_item, tier_html, api_html, cache_html, conf_color, tv_url, score, rvol, pullback_color):
618     news_title = news_item['title'][:40] + '...' if news_item else 'Keine News'
619     news_url = news_item['url'] if news_item else f'https://finance.yahoo.com/quote/{sym}'
620     html = f"""
621     <div class="bull-card">
622         <h3>🐂 {sym}</h3>
623         <div class="pullback-badge" style="background: {pullback_color};">-{pullback:.1%}</div>
624         <div style="margin: 5px 0;">{tier_html}{api_html}{cache_html}</div>
625         <div class="price">${price:.2f}</div>
626         <div style="font-size: 0.8rem; color: #aaa; margin: 5px 0;">{reasons}</div>
627         <div style="margin: 8px 0;">
628             <span class="stop-loss">SL: ${sl:.2f}</span>
629             <span class="target">TP: ${target:.2f}</span>
630         </div>
631         <div style="font-size: 0.8rem; color: {conf_color}; margin: 5px 0;">Score: {score}/100</div>
632         <div class="confidence-bar"><div class="confidence-fill" style="width: {score}%; background: {conf_color};"></div></div>
633         <div style="font-size: 0.75rem; color: #888; margin: 5px 0;">R:R {rr:.1f}x | Vol {rvol:.1f}x</div>
634         <a href="{news_url}" target="_blank" class="news-link-btn">📰 {news_title}</a>
635         <a href="{tv_url}" target="_blank" class="btn-link">📈 TradingView</a>
636     </div>
637     """
638     return html
639 
640 def render_card(item, container):
641     score = item['score']
642     sym = item['symbol']
643     price = item['price']
644     pullback = item['pullback_pct']
645     sl = item['stop_loss']
646     target = item['target']
647     rr = item['rr_ratio']
648     rvol = item['rvol']
649     pe = item.get('pe_ratio')
650     tier = item.get('tier', '-')
651     reasons_txt = ' | '.join(item['reasons'][:3])
652     news_found = item.get('news', [])
653     apis = item.get('api_sources', [])
654     cached = item.get('from_cache', False)
655     tv_url = f'https://www.tradingview.com/chart/?symbol={sym}'
656 
657     pullback_color = '#ff6b6b' if pullback > 0.15 else '#ffa502'
658     conf_color = '#9933ff' if score > 85 else '#FFD700' if score > 70 else '#00FF00'
659     tier_html = f'<div class="tier-badge">T{tier}</div>'
660     api_html = ''.join([f'<div class="tier-badge">{a}</div>' for a in apis])
661     cache_html = '<div class="cache-badge">CACHE</div>' if cached else ''
662     html = render_card_html(sym, price, pullback, sl, target, rr, reasons_txt, 
663                             news_found[0] if news_found else None, tier_html, api_html, cache_html, conf_color, tv_url, score, rvol, pullback_color)
664     with container:
665         st.markdown(html, unsafe_allow_html=True)
666 
667 # ============================== Main ==============================
668 clock = get_market_clock()
669 
670 # Markt-Holidays
671 if clock.get('is_holiday'):
672     st.markdown(f"""
673     <div class="holiday-banner">
674         🎌 US MARKET HOLIDAY - Presidents' Day 🎌<br>
675         <small>Markt ist geschlossen. Daten können unvollständig sein.</small>
676     </div>
677     """, unsafe_allow_html=True)
678 
679 # Marktzeit Anzeige
680 st.markdown(f"""
681 <div class="market-clock-container">
682     <div class="market-time">{clock['time']}</div>
683     <div style="margin: 10px 0;">
684         <span class="market-status" style="background: {clock['color']};">{clock['status']}</span>
685     </div>
686     <div class="market-countdown">{clock['countdown']}</div>
687     {f'<div class="market-progress"><div class="market-progress-bar" style="width: {clock["progress"]*100}%;"></div></div>' if clock['is_open'] else ''}
688 </div>
689 """, unsafe_allow_html=True)
690 
691 # ============================== Automatisches Refresh ==============================
692 if st.session_state.get('auto_refresh'):
693     last = st.session_state.get('last_auto_refresh', 0)
694     if time.time() - last >= AUTO_REFRESH_INTERVAL:
695         st.session_state['last_auto_refresh'] = time.time()
696         st.session_state['refresh_count'] = st.session_state.get('refresh_count', 0) + 1
697         st.rerun()
698 
699 # ============================== Sidebar & API Status ==============================
700 with st.sidebar:
701     st.header("📡 API Status")
702     stats = st.session_state.get('api_stats', {'yahoo':0,'finnhub':0,'alpha_vantage':0,'cache_hits':0,'alpha_rotation_count':0})
703     # Yahoo
704     yahoo_calls = stats.get('yahoo', 0)
705     st.markdown(f"""
706     <div class="info-box">
707     🟢 <b>Yahoo Finance</b><br>
708     Kursdaten: {yahoo_calls} Calls<br>
709     <small>Unbegrenzt kostenlos</small>
710     </div>
711     """, unsafe_allow_html=True)
712     # Finnhub
713     fh_status = "🟢" if finnhub_limiter.can_call() else "🔴"
714     st.markdown(f"""
715     <div class="api-stat">
716         <div style="display:flex; justify-content:space-between;">
717             <span>Finnhub News</span>
718             <span>{fh_status} {finnhub_limiter.get_status()}/60 pro Minute</span>
719         </div>
720     </div>
721     """, unsafe_allow_html=True)
722     # Alpha Vantage
723     alpha_status_list = alpha_manager.get_status()
724     all_alpha_exhausted = all(s['exhausted'] for s in alpha_status_list)
725     if all_alpha_exhausted:
726         st.markdown("""
727         <div class="error-box">
728         ⚠️ <b>Alpha Vantage erschöpft!</b><br>
729         Limit: 25/Tag pro Key<br>
730         Morgen wieder verfügbar
731         </div>
732         """, unsafe_allow_html=True)
733     st.markdown("<div style='margin:10px 0;'><b>Alpha Vantage (25/Tag):</b></div>", unsafe_allow_html=True)
734     for status in alpha_status_list:
735         cls = "key-active" if status['active'] else "key-exhausted" if status['exhausted'] else ""
736         indicator = "▶️" if status['active'] else "✅" if not status['exhausted'] else "❌"
737         st.markdown(f"""
738         <div class="key-indicator {cls}">
739             {indicator} Key {status['index']+1}: {status['calls_today']}/25
740         </div>
741         """, unsafe_allow_html=True)
742     rotations = stats.get('alpha_rotation_count',0)
743     cache_hits = stats.get('cache_hits',0)
744     st.markdown(f'<div style="font-size:0.8rem;margin:5px 0;">🔄 Rotationen: {rotations}</div>', unsafe_allow_html=True)
745     st.markdown(f'<div style="font-size:0.8rem;margin:5px 0;">📦 Cache Hits: {cache_hits}</div>', unsafe_allow_html=True)
746 
747     # API Tests
748     st.divider()
749     st.header("🧪 API Tests")
750     col1, col2 = st.columns(2)
751     with col1:
752         if st.button("Test Yahoo", use_container_width=True):
753             try:
754                 data = yf.Ticker("AAPL").history(period="5d")
755                 if not data.empty:
756                     st.success(f"✅ Yahoo OK! {len(data)} Tage")
757                     stats = st.session_state.get('api_stats', {})
758                     stats['yahoo'] = stats.get('yahoo', 0) + 1
759                     st.session_state['api_stats'] = stats
760                 else:
761                     st.error("❌ Keine Daten")
762             except:
763                 st.error("❌ Fehler")
764     with col2:
765         if st.button("Test Finnhub", use_container_width=True):
766             news, cached = get_finnhub_news_smart("TSLA")
767             if news:
768                 st.success(f"✅ Finnhub OK! {len(news)} News")
769             else:
770                 st.error("❌ Keine News")
771     # Manuelle Abfrage
772     st.divider()
773     st.header("🔍 Manuelle Abfrage")
774     manual_symbol = st.text_input("Symbol:", placeholder="z.B. NVDA", key="manual").upper()
775     if st.button("📊 Analyse starten") and manual_symbol:
776         with st.spinner(f"Analysiere {manual_symbol}..."):
777             result = analyze_smart(manual_symbol, 1, 1)
778             if result:
779                 st.success(f"✅ Setup gefunden für {manual_symbol}!")
780                 st.json({
781                     'Symbol': result['symbol'],
782                     'Score': result['score'],
783                     'Price': result['price'],
784                     'Pullback': f"{result['pullback_pct']:.2%}",
785                     'R:R': f"{result['rr_ratio']:.1f}x",
786                     'APIs': result['api_sources']
787                 })
788             else:
789                 st.error(f"❌ Kein Setup für {manual_symbol}")
790                 scan_debug = st.session_state.get('scan_debug', [])
791                 if scan_debug:
792                     last = scan_debug[-1]
793                     st.write("Checks:", last.get('checks', {}))
794                     st.write("Fehler:", last.get('errors', []))
795     if st.button("🔄 Stats zurücksetzen"):
796         st.session_state['api_stats'] = {'yahoo':0,'finnhub':0,'alpha_vantage':0,'cache_hits':0,'alpha_rotation_count':0}
797         st.session_state['scan_debug'] = []
798         for i in alpha_manager.limiters:
799             alpha_manager.limiters[i]['exhausted'] = False
800             alpha_manager.limiters[i]['calls_today'] = 0
801             alpha_manager.limiters[i]['calls_per_min'] = []
802         st.success("Zurückgesetzt!")
803         st.rerun()
804 
805 # ============================== Haupt-Scan Button ==============================
806 scan_triggered = False
807 if st.button('🚀 Smart Scan Starten'):
808     scan_triggered = True
809 
810 if scan_triggered:
811     with st.spinner("🔍 Scanne mit Yahoo Finance..."):
812         market_ctx = get_market_context()
813         if market_ctx.get('market_closed'):
814             st.warning("⚠️ Markt ist möglicherweise geschlossen.")
815         st.session_state['scan_debug'] = []
816         results = []
817         progress = st.progress(0)
818         status_text = st.empty()
819         error_count = 0
820         success_count = 0
821         scan_list = [(s, '📋') for s in st.session_state['watchlist']]
822         seen = set(st.session_state['watchlist'])
823 
824         # Gainers von Yahoo
825         try:
826             headers = {'User-Agent':'Mozilla/5.0'}
827             r = requests.get('https://finance.yahoo.com/gainers', headers=headers, timeout=10)
828             if r.status_code == 200:
829                 tables = pd.read_html(StringIO(r.text))
830                 if tables:
831                     gainers = tables[0]['Symbol'].head(20).tolist()
832                     for g in gainers:
833                         if g not in seen:
834                             scan_list.append((g, '🌍'))
835                             seen.add(g)
836         except:
837             pass
838 
839         for i, (sym, _) in enumerate(scan_list):
840             tier = i + 1
841             status_text.text(f"Analysiere: {sym} ({tier}/{len(scan_list)}) - OK:{success_count} Fehler:{error_count}")
842             try:
843                 res = analyze_smart(sym, tier, len(scan_list), market_ctx)
844                 if res:
845                     existing = [r for r in results if r['symbol'] == sym]
846                     if not existing or res['score'] > existing[0]['score']:
847                         results = [r for r in results if r['symbol'] != sym]
848                         results.append(res)
849                         success_count += 1
850                 else:
851                     error_count += 1
852             except:
853                 error_count += 1
854             progress.progress((i+1)/len(scan_list))
855             if i % 5 == 0 and i > 0:
856                 time.sleep(1.0)
857         progress.empty()
858         status_text.empty()
859         st.session_state['scan_results'] = results
860         st.session_state['last_scan_time'] = datetime.now()
861 
862         # Alerts
863         alerts_sent_this_scan = 0
864         for item in results:
865             if item['score'] > 75:
866                 symbol = item['symbol']
867                 price = item['price']
868                 score = item['score']
869                 if should_send_alert(symbol, price, score):
870                     setup_type = "CATALYST" if (item.get('news') and item['news'][0]['tier'] == 1) else "GOLD"
871                     success = send_telegram_alert(
872                         symbol, price, item['pullback_pct'],
873                         item['news'][0] if item.get('news') else None,
874                         setup_type, item.get('pe_ratio'),
875                         item.get('api_sources'), item.get('tier')
876                     )
877                     if success:
878                         record_alert(symbol, price, score, setup_type)
879                         alerts_sent_this_scan += 1
880                         if alerts_sent_this_scan <= 3:
881                             st.toast(f"🚨 {setup_type} Alert: {symbol} @ ${price:.2f} (Score: {score})")
882 
883 # ============================== Ergebnisse anzeigen ==============================
884 results = st.session_state.get('scan_results', [])
885 if results:
886     col1, col2 = st.columns([3,1])
887     with col1:
888         st.subheader(f"📊 Gefundene Setups: {len(results)}")
889     with col2:
890         if st.session_state.get('auto_refresh'):
891             count = st.session_state.get('refresh_count', 0)
892             st.markdown(f'<div style="background:#1a1a2e;padding:10px;border-radius:8px;border-left:4px solid #00FF00;">🔴 LIVE #{count}</div>', unsafe_allow_html=True)
893         else:
894             last_time = st.session_state.get('last_scan_time')
895             if last_time:
896                 st.caption(f"Letzter Scan: {last_time.strftime('%H:%M:%S')}")
897     sent_alerts = st.session_state.get('sent_alerts', {})
898     active_alerts = len([a for a in sent_alerts.values() if (datetime.now() - a['timestamp']).total_seconds() / 3600 < 24])
899     st.info(f"📱 Aktive Alerts (24h): {active_alerts} | In Cooldown: {len(sent_alerts) - active_alerts}")
900     stats = st.session_state.get('api_stats', {})
901     api_summary = {}
902     for r in results:
903         for s in r.get('api_sources', []):
904             api_summary[s] = api_summary.get(s, 0) + 1
905     cache_count = sum(1 for r in results if r.get('from_cache'))
906     st.metric("Setups", len(results))
907     st.metric("Yahoo Calls", stats.get('yahoo', 0))
908     st.metric("Finnhub", stats.get('finnhub', 0))
909     st.metric("Alpha Vantage", stats.get('alpha_vantage', 0))
910     st.success(f"✅ APIs in Ergebnissen: {api_summary} | Cache: {cache_count}")
911     # Ergebnisse grid
912     def render_results_grid(results):
913         results_sorted = sorted(results, key=lambda x: (x['score'], x['pullback_pct']), reverse=True)
914         cols = st.columns(4)
915         for i, r in enumerate(results_sorted[:16]):
916             with cols[i % 4]:
917                 render_card(r, st.container())
918     render_results_grid(results)
919     with st.expander("📡 API Details"):
920         st.write(f"**Yahoo Finance:** {stats.get('yahoo', 0)} Calls (unbegrenzt)")
921         st.write(f"**Finnhub:** {finnhub_limiter.get_status()}/60 pro Minute")
922         st.write(f"**Alpha Vantage:** {stats.get('alpha_vantage', 0)}/25 pro Tag")
923         ctx = get_market_context()
924         st.write(f"**Marktkontext:** {'Risk-Off' if ctx.get('risk_off') else 'Risk-On'}")
925         st.write("---")
926         st.write("**Letzte Alerts:**")
927         for symbol, alert in list(st.session_state.get('sent_alerts', {}).items())[:5]:
928             ago = int((datetime.now() - alert['timestamp']).total_seconds() / 60)
929             st.write(f"  • {symbol}: {alert['setup_type']} vor {ago}min @ ${alert['price']:.2f}")
930 alerts = st.session_state.get('sent_alerts', {})
931 if alerts:
932     # Code um Alerts anzuzeigen
933     st.write("**Letzte Alerts:**")
934     for symbol, alert in list(alerts.items())[:5]:
935         ago = int((datetime.now() - alert['timestamp']).total_seconds() / 60)
936         st.write(f"  • {symbol}: {alert['setup_type']} vor {ago}min @ ${alert['price']:.2f}")
937 else:
938     # Dieser Block ist eingerückt
939     st.info("👆 Klicke 'Smart Scan Starten' um die Watchlist zu analysieren!")
```

Wenn du noch spezielle Fragen hast oder weitere Anpassungen brauchst, helfe ich gern weiter!
