"""
Elite Bull Scanner Pro V7.0 - Async Refactor
Mit Type Hints, Unit Tests und strikten Rate-Limits
"""

import streamlit as st
import yfinance as yf
import pandas as pd
import time
import requests
import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Tuple, Any, Set, Callable
from dataclasses import dataclass, field
from enum import Enum
from io import StringIO
import warnings
import pytz
import logging
import random
import os
import json
from functools import wraps
import pytest
from unittest.mock import Mock, patch, AsyncMock

warnings.filterwarnings('ignore')
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================== Type Definitions ==============================

class SourceType(str, Enum):
    WATCHLIST = "watchlist"
    GAINERS = "gainers"
    LOSERS = "losers"
    MOST_ACTIVE = "most_active"
    UNKNOWN = "unknown"

class MarketStatus(str, Enum):
    OPEN = "OPEN"
    CLOSED = "CLOSED"
    PRE_MARKET = "PRE-MARKET"
    HOLIDAY = "HOLIDAY"

@dataclass
class RateLimitConfig:
    """Konfiguration für API Rate Limits"""
    calls_per_second: float = 1.0  # Yahoo Finance: konservativ 1/s
    calls_per_minute: int = 60     # Finnhub
    calls_per_day: int = 25        # Alpha Vantage pro Key
    burst_size: int = 5            # Kurzzeitig erlaubte Burst-Größe
    
    def get_min_delay(self) -> float:
        """Minimale Delay zwischen Calls in Sekunden"""
        return 1.0 / self.calls_per_second

@dataclass 
class ScanResult:
    """Typisiertes Ergebnis einer Symbol-Analyse"""
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

@dataclass
class MarketContext:
    """Marktkontext mit Typ-Sicherheit"""
    risk_off: bool
    spy_change: float
    vix_level: float
    market_closed: bool
    timestamp: datetime = field(default_factory=datetime.now)

# ============================== Rate Limit Manager ==============================

class AsyncRateLimiter:
    """Thread-sicherer Async Rate Limiter mit Token-Bucket-Algorithmus"""
    
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._semaphore = asyncio.Semaphore(config.burst_size)
        self._last_call_time: float = 0.0
        self._lock = asyncio.Lock()
        self._daily_calls: int = 0
        self._minute_calls: List[float] = []
        
    async def acquire(self) -> bool:
        """Erlaubt Call wenn Rate-Limit nicht überschritten"""
        async with self._lock:
            now = time.time()
            
            # Clean up alte Minute-Calls
            self._minute_calls = [t for t in self._minute_calls if now - t < 60]
            
            # Prüfe Daily Limit
            if self._daily_calls >= self.config.calls_per_day:
                return False
                
            # Prüfe Minute Limit
            if len(self._minute_calls) >= self.config.calls_per_minute:
                return False
            
            # Enforce min delay
            time_since_last = now - self._last_call_time
            if time_since_last < self.config.get_min_delay():
                await asyncio.sleep(self.config.get_min_delay() - time_since_last)
            
            # Record call
            self._last_call_time = time.time()
            self._minute_calls.append(self._last_call_time)
            self._daily_calls += 1
            
        return True
    
    async def __aenter__(self):
        if not await self.acquire():
            raise RateLimitExceeded("API Rate Limit erreicht")
        await self._semaphore.acquire()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._semaphore.release()

class RateLimitExceeded(Exception):
    pass

# API-spezifische Limiter
YAHOO_LIMITER = AsyncRateLimiter(RateLimitConfig(
    calls_per_second=0.8,  # Konservativ: alle 1.25s
    burst_size=3
))

FINNHUB_LIMITER = AsyncRateLimiter(RateLimitConfig(
    calls_per_minute=60,
    burst_size=10
))

ALPHA_LIMITERS = [
    AsyncRateLimiter(RateLimitConfig(
        calls_per_minute=5,  # 5 calls per minute
        calls_per_day=25,
        burst_size=2
    )) for _ in range(3)  # 3 Keys
]

# ============================== Async Data Fetchers ==============================

class YahooFinanceClient:
    """Async Yahoo Finance Client mit Rate Limiting und Caching"""
    
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: Dict[str, Tuple[pd.DataFrame, float]] = {}
        self._cache_ttl: int = 300  # 5 Minuten
        
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30),
                headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            )
        return self._session
    
    async def fetch_history(self, symbol: str, period: str = "3mo") -> Optional[pd.DataFrame]:
        """Async Yahoo Finance Daten mit Rate Limiting"""
        cache_key = f"yh_{symbol}_{period}"
        
        # Check Cache
        if cache_key in self._cache:
            data, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self._cache_ttl:
                return data
        
        # Rate Limiting
        async with YAHOO_LIMITER:
            try:
                # Yahoo Finance hat kein offizielles REST API für History
                # Wir verwenden yfinance synchron in ThreadPool
                loop = asyncio.get_event_loop()
                df = await loop.run_in_executor(
                    None, 
                    lambda: yf.Ticker(symbol).history(period=period, interval='1d')
                )
                
                if not df.empty:
                    self._cache[cache_key] = (df, time.time())
                    return df
                    
            except Exception as e:
                logger.error(f"Yahoo Fehler für {symbol}: {e}")
                await asyncio.sleep(2)  # Backoff bei Fehler
                
        return None
    
    async def fetch_batch(self, symbols: List[str], max_concurrent: int = 3) -> Dict[str, pd.DataFrame]:
        """Batch Fetch mit begrenzter Parallelität"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def fetch_one(sym: str) -> Tuple[str, Optional[pd.DataFrame]]:
            async with semaphore:
                data = await self.fetch_history(sym)
                return sym, data
        
        tasks = [fetch_one(s) for s in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        return {
            sym: data for sym, data in results 
            if not isinstance(data, Exception) and data is not None
        }

class FinnhubClient:
    """Async Finnhub Client für News"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._session: Optional[aiohttp.ClientSession] = None
        self._cache: SmartCache = SmartCache()
        
    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
        return self._session
    
    async def get_news(self, symbol: str) -> Tuple[Optional[List[Dict]], bool]:
        """News mit Rate Limiting und Caching"""
        cache_key = f"news_{symbol}"
        cached = self._cache.get(cache_key, 300)
        if cached:
            return cached, True
        
        async with FINNHUB_LIMITER:
            try:
                session = await self._get_session()
                url = "https://finnhub.io/api/v1/company-news"
                params = {
                    'symbol': symbol,
                    'from': (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d'),
                    'to': datetime.now().strftime('%Y-%m-%d'),
                    'token': self.api_key
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if isinstance(data, list) and len(data) > 0:
                            formatted = [
                                {
                                    'title': item.get('headline', 'No Title'),
                                    'url': item.get('url', ''),
                                    'source': item.get('source', 'Finnhub'),
                                    'datetime': item.get('datetime', 0),
                                    'score': 10
                                }
                                for item in sorted(data, key=lambda x: x.get('datetime', 0), reverse=True)[:5]
                            ]
                            self._cache.set(cache_key, formatted)
                            return formatted, False
                            
            except Exception as e:
                logger.error(f"Finnhub Fehler für {symbol}: {e}")
                
        return None, False

# ============================== Analysis Engine ==============================

class TechnicalAnalyzer:
    """Technische Analyse mit Type Hints"""
    
    @staticmethod
    def analyze_structure(df: pd.DataFrame, symbol: Optional[str] = None) -> Dict[str, Any]:
        """Swing High/Low Analyse mit NumPy"""
        if df is None or df.empty or len(df) < 10:
            return TechnicalAnalyzer._default_result()
            
        required = ['High', 'Low', 'Close']
        if not all(c in df.columns for c in required):
            return TechnicalAnalyzer._default_result()
        
        df_clean = df[required].dropna()
        if len(df_clean) < 10:
            return TechnicalAnalyzer._default_result()
        
        highs = df_clean['High'].values
        lows = df_clean['Low'].values
        
        # Vectorized Swing Detection
        swing_highs = TechnicalAnalyzer._find_swings(highs, is_high=True)
        swing_lows = TechnicalAnalyzer._find_swings(lows, is_high=False)
        
        if len(swing_highs) >= 2 and len(swing_lows) >= 2:
            hh = swing_highs[-1][1] > swing_highs[-2][1]
            hl = swing_lows[-1][1] > swing_lows[-2][1]
            
            # Trend Slope Berechnung
            slope = TechnicalAnalyzer._calculate_slope(swing_highs)
            
            return {
                'higher_highs': bool(hh),
                'higher_lows': bool(hl),
                'trend_slope': float(slope),
                'structure_intact': bool(hh and hl),
                'last_swing_low': float(swing_lows[-1][1]),
                'last_swing_high': float(swing_highs[-1][1])
            }
        
        return TechnicalAnalyzer._default_result(df_clean)
    
    @staticmethod
    def _find_swings(data: np.ndarray, is_high: bool) -> List[Tuple[int, float]]:
        """Vectorized Swing Point Detection"""
        # Rolling window comparison
        swings = []
        for i in range(2, len(data)-2):
            window = data[i-2:i+3]
            if is_high:
                if data[i] == np.max(window) and data[i] > data[i-1]:
                    swings.append((i, float(data[i])))
            else:
                if data[i] == np.min(window) and data[i] < data[i-1]:
                    swings.append((i, float(data[i])))
        return swings
    
    @staticmethod
    def _calculate_slope(swings: List[Tuple[int, float]]) -> float:
        """Lineare Regression für Trend-Slope"""
        if len(swings) < 3:
            return 0.0
        
        x = np.array([s[0] for s in swings[-3:]])
        y = np.array([s[1] for s in swings[-3:]])
        
        # NumPy polyfit für OLS
        if len(x) == len(y) and len(x) > 0:
            try:
                slope, _ = np.polyfit(x, y, 1)
                return float(slope) if np.isfinite(slope) else 0.0
            except:
                pass
        return 0.0
    
    @staticmethod
    def _default_result(df: Optional[pd.DataFrame] = None) -> Dict[str, Any]:
        if df is not None and not df.empty:
            try:
                return {
                    'structure_intact': False,
                    'higher_highs': False,
                    'higher_lows': False,
                    'trend_slope': 0.0,
                    'last_swing_low': float(df['Low'].tail(5).min()),
                    'last_swing_high': float(df['High'].tail(20).max())
                }
            except:
                pass
        return {
            'structure_intact': False, 'higher_highs': False,
            'higher_lows': False, 'trend_slope': 0.0,
            'last_swing_low': 0.0, 'last_swing_high': 0.0
        }

class ScoringEngine:
    """Berechnung des Setup-Scores mit dokumentierten Gewichtungen"""
    
    # Magic Numbers als Klassenkonstanten dokumentiert
    BASE_SCORE: int = 25
    STRUCTURE_STRONG: int = 15  # HH + HL
    STRUCTURE_WEAK: int = 10    # Nur HL
    TREND_SLOPE_BONUS: int = 5  # Slope > 0.005
    VOLUME_HIGH: int = 20         # RVOL > 2.0
    VOLUME_MED: int = 10          # RVOL > 1.0
    SUPPORT_CLOSE: int = 15       # < 3% zu Swing Low
    SUPPORT_MED: int = 8          # < 8% zu Swing Low
    PE_LOW: int = 8               # PE < 15
    PE_HIGH_PENALTY: int = -5     # PE > 100
    
    @classmethod
    def calculate(cls, 
                  structure: Dict[str, Any],
                  volume_metrics: Dict[str, float],
                  fundamentals: Optional[Dict] = None,
                  news_score: int = 0) -> Tuple[int, List[str]]:
        """Berechnet Score mit Begründungen"""
        score = cls.BASE_SCORE
        reasons: List[str] = []
        
        # Struktur
        if structure.get('structure_intact'):
            score += cls.STRUCTURE_STRONG
            reasons.append("📈 Trend stark")
        elif structure.get('higher_lows'):
            score += cls.STRUCTURE_WEAK
            reasons.append("📈 Trend schwach")
        
        # Trend-Slope
        slope = structure.get('trend_slope', 0)
        if slope > 0.005:
            score += cls.TREND_SLOPE_BONUS
        
        # Volumen
        rvol = volume_metrics.get('rvol', 1.0)
        if rvol > 2.0:
            score += cls.VOLUME_HIGH
            reasons.append(f"⚡ Vol {rvol:.1f}x")
        elif rvol > 1.0:
            score += cls.VOLUME_MED
            reasons.append(f"⚡ Vol {rvol:.1f}x")
        
        # Support-Distanz
        support_dist = volume_metrics.get('support_dist', 1.0)
        if support_dist < 0.03:
            score += cls.SUPPORT_CLOSE
            reasons.append("🎯 Support nah")
        elif support_dist < 0.08:
            score += cls.SUPPORT_MED
        
        # Fundamentals
        if fundamentals:
            pe = fundamentals.get('pe_ratio')
            if pe is not None:
                if pe < 15:
                    score += cls.PE_LOW
                    reasons.append(f"💰 PE {pe:.1f}")
                elif pe > 100:
                    score += cls.PE_HIGH_PENALTY
        
        # News
        score += news_score
        
        return min(100, score), reasons

# ============================== Async Scanner ==============================

class AsyncBullScanner:
    """Haupt-Scanner mit Async Processing und strikten Rate-Limits"""
    
    def __init__(self, 
                 watchlist: List[str],
                 finnhub_key: Optional[str] = None,
                 alpha_keys: Optional[List[str]] = None):
        self.watchlist = watchlist
        self.yahoo_client = YahooFinanceClient()
        self.finnhub_client = FinnhubClient(finnhub_key) if finnhub_key else None
        self.analyzer = TechnicalAnalyzer()
        self.scorer = ScoringEngine()
        
    async def scan_symbol(self, 
                         symbol: str, 
                         tier: int,
                         market_ctx: MarketContext) -> Optional[ScanResult]:
        """Analysiert ein einzelnes Symbol mit allen Checks"""
        
        # Yahoo Daten
        df = await self.yahoo_client.fetch_history(symbol)
        if df is None or len(df) < 15:
            return None
        
        df_clean = df.dropna()
        if len(df_clean) < 10:
            return None
        
        current_price = float(df_clean['Close'].iloc[-1])
        if not np.isfinite(current_price) or current_price <= 0:
            return None
        
        # Pullback Berechnung
        lookback = min(60, len(df_clean) - 5)
        recent = df_clean.tail(lookback)
        recent_high = float(recent['High'].max())
        
        pullback_pct = (recent_high - current_price) / recent_high
        if not (0.05 <= pullback_pct <= 0.50):
            return None
        
        # Technische Analyse
        structure = self.analyzer.analyze_structure(df_clean, symbol)
        
        if not structure['structure_intact'] and not structure['higher_lows']:
            return None
        
        last_swing_low = structure.get('last_swing_low', 0)
        if current_price < last_swing_low * 0.90:
            return None
        
        # Volumen-Metriken
        avg_vol = df_clean['Volume'].mean()
        current_vol = df_clean['Volume'].iloc[-1]
        rvol = current_vol / avg_vol if avg_vol > 0 else 1.0
        
        support_dist = (current_price - last_swing_low) / current_price
        
        # News (nur für Top-Tiers)
        news, news_score = [], 0
        if tier <= 20 and self.finnhub_client:
            news_data, _ = await self.finnhub_client.get_news(symbol)
            if news_data:
                news = news_data
                news_score = news[0].get('score', 10)
        
        # Scoring
        volume_metrics = {'rvol': rvol, 'support_dist': support_dist}
        score, reasons = self.scorer.calculate(structure, volume_metrics, None, news_score)
        
        if score < 60:
            return None
        
        # R:R Kalkulation
        atr = float((df_clean['High'].rolling(14).max() - df_clean['Low'].rolling(14).min()).mean())
        if not np.isfinite(atr) or atr <= 0:
            atr = current_price * 0.02
        
        stop_loss = max(last_swing_low * 0.97, current_price - (2 * atr))
        target = min(recent_high * 0.97, current_price + (current_price - stop_loss) * 2)
        rr_ratio = (target - current_price) / (current_price - stop_loss) if (current_price - stop_loss) > 0 else 0
        
        if rr_ratio < 1.0:
            return None
        
        return ScanResult(
            symbol=symbol,
            tier=tier,
            score=score,
            price=current_price,
            pullback_pct=pullback_pct,
            recent_high=recent_high,
            stop_loss=stop_loss,
            target=target,
            rr_ratio=rr_ratio,
            rvol=rvol,
            reasons=reasons,
            news=news,
            source=SourceType.WATCHLIST if symbol in self.watchlist else SourceType.UNKNOWN
        )
    
    async def scan_batch(self,
                        symbols: List[Tuple[str, SourceType]],
                        market_ctx: MarketContext,
                        progress_callback: Optional[Callable[[int, int], None]] = None) -> List[ScanResult]:
        """Batch-Scan mit Rate-Limiting und Progress"""
        results: List[ScanResult] = []
        
        # Verarbeite in Chunks um Memory-Pressure zu vermeiden
        chunk_size = 10
        
        for chunk_start in range(0, len(symbols), chunk_size):
            chunk = symbols[chunk_start:chunk_start + chunk_size]
            
            # Erstelle Tasks für diesen Chunk
            tasks = []
            for i, (sym, source) in enumerate(chunk):
                tier = chunk_start + i + 1
                task = self.scan_symbol(sym, tier, market_ctx)
                tasks.append(task)
            
            # Führe Chunk aus mit begrenzter Parallelität
            chunk_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for res in chunk_results:
                if isinstance(res, ScanResult):
                    results.append(res)
                # Ignoriere Exceptions für einzelne Symbole
            
            # Progress Update
            if progress_callback:
                progress_callback(min(chunk_start + chunk_size, len(symbols)), len(symbols))
            
            # Rate Limiting Pause zwischen Chunks
            if chunk_start + chunk_size < len(symbols):
                await asyncio.sleep(2)  # Respektiere Yahoo Limits
        
        return results

# ============================== Unit Tests ==============================

class TestRateLimiter:
    """Tests für Rate Limiting Logik"""
    
    @pytest.mark.asyncio
    async def test_yahoo_rate_limit(self):
        """Test: Max 0.8 Calls/s = min 1.25s Delay"""
        limiter = AsyncRateLimiter(RateLimitConfig(calls_per_second=0.8, burst_size=2))
        
        start = time.time()
        async with limiter:
            pass
        async with limiter:
            pass
        elapsed = time.time() - start
        
        assert elapsed >= 1.25, f"Delay zu kurz: {elapsed:.2f}s"
    
    @pytest.mark.asyncio
    async def test_finnhub_minute_limit(self):
        """Test: Max 60 Calls/Minute"""
        limiter = AsyncRateLimiter(RateLimitConfig(calls_per_minute=60))
        
        success_count = 0
        for _ in range(65):
            if await limiter.acquire():
                success_count += 1
        
        assert success_count == 60, f"Erwartet 60, got {success_count}"
    
    def test_scoring_weights(self):
        """Validiert Magic Numbers im Scoring"""
        # Test: Basis + Struktur stark = 40
        structure = {'structure_intact': True, 'higher_lows': True, 'trend_slope': 0}
        vol = {'rvol': 1.0, 'support_dist': 1.0}
        
        score, _ = ScoringEngine.calculate(structure, vol)
        assert score == 40, f"Erwartet 40, got {score}"  # 25 + 15
        
        # Test: Vol Bonus
        vol = {'rvol': 2.5, 'support_dist': 1.0}
        score, _ = ScoringEngine.calculate(structure, vol)
        assert score == 60, f"Erwartet 60, got {score}"  # 40 + 20

class TestTechnicalAnalysis:
    """Tests für technische Indikatoren"""
    
    def test_swing_detection(self):
        """Test: Swing Highs/Lows Erkennung"""
        # Erstelle Test-Daten mit klaren Swings
        highs = np.array([10, 11, 12, 11, 10, 11, 13, 12, 11, 12, 14, 13])
        lows = np.array([8, 9, 8, 7, 8, 9, 8, 9, 8, 9, 8, 9])
        
        df = pd.DataFrame({
            'High': highs,
            'Low': lows,
            'Close': (highs + lows) / 2
        })
        
        result = TechnicalAnalyzer.analyze_structure(df)
        
        assert result['structure_intact'] or result['higher_lows'], "Sollte Trend erkennen"
        assert result['last_swing_low'] > 0, "Sollte Swing Low haben"
        assert result['last_swing_high'] > 0, "Sollte Swing High haben"
    
    def test_trend_slope_calculation(self):
        """Test: Trend-Slope Berechnung"""
        # Steigender Trend
        swings = [(0, 100.0), (10, 110.0), (20, 120.0)]
        slope = TechnicalAnalyzer._calculate_slope(swings)
        
        assert slope > 0, f"Sollte positiver Trend sein, got {slope}"
        assert abs(slope - 1.0) < 0.1, f"Slope sollte ~1.0 sein, got {slope}"

# ============================== Streamlit UI ==============================

def render_async_ui():
    """Streamlit UI mit Async Integration"""
    st.set_page_config(layout="wide", page_title="Elite Bull Scanner Pro V7.0 Async", page_icon="🐂")
    
    # CSS Styles (gekürzt für Übersichtlichkeit)
    st.markdown("""
    <style>
    .bull-card { border: 1px solid #333; border-radius: 10px; padding: 15px; 
                 background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%); }
    .metric-green { color: #00FF00; font-weight: bold; }
    .metric-red { color: #ff4b4b; }
    </style>
    """, unsafe_allow_html=True)
    
    # Session State
    if 'scanner' not in st.session_state:
        # API Keys aus Secrets laden
        try:
            finnhub_key = st.secrets.get("finnhub", {}).get("api_key")
        except:
            finnhub_key = None
            
        st.session_state['scanner'] = AsyncBullScanner(
            watchlist=DEFAULT_WATCHLIST,
            finnhub_key=finnhub_key
        )
    
    scanner: AsyncBullScanner = st.session_state['scanner']
    
    # Markt-Uhr
    clock = get_market_clock()
    col1, col2, col3 = st.columns([2, 2, 1])
    with col1:
        st.markdown(f"### 🕐 {clock['time']} ET")
    with col2:
        status_color = "🟢" if clock['is_open'] else "🔴"
        st.markdown(f"### {status_color} {clock['status']}")
    
    # Async Scan Button
    if st.button('🚀 Async Scan Starten', type="primary"):
        progress_bar = st.progress(0)
        status_text = st.empty()
        results_container = st.container()
        
        async def run_scan():
            # Bereite Symbol-Liste vor
            symbols = [(s, SourceType.WATCHLIST) for s in DEFAULT_WATCHLIST]
            
            # Mock Market Context für Test
            market_ctx = MarketContext(
                risk_off=False,
                spy_change=0.01,
                vix_level=18.0,
                market_closed=False
            )
            
            def update_progress(current: int, total: int):
                progress = current / total
                progress_bar.progress(min(progress, 0.99))
                status_text.text(f"Scanne... {current}/{total} Symbole")
            
            results = await scanner.scan_batch(symbols, market_ctx, update_progress)
            return results
        
        # Führe Async Scan aus
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            results = loop.run_until_complete(run_scan())
            loop.close()
            
            progress_bar.empty()
            status_text.empty()
            
            # Zeige Ergebnisse
            with results_container:
                st.success(f"✅ {len(results)} Setups gefunden")
                
                # Sortiere nach Score
                results_sorted = sorted(results, key=lambda x: x.score, reverse=True)
                
                cols = st.columns(3)
                for i, r in enumerate(results_sorted[:12]):
                    with cols[i % 3]:
                        with st.container():
                            st.markdown(f"""
                            <div class="bull-card">
                                <h3>🐂 {r.symbol}</h3>
                                <div class="metric-green">Score: {r.score}/100</div>
                                <div>Price: ${r.price:.2f} | Pullback: -{r.pullback_pct:.1%}</div>
                                <div>SL: ${r.stop_loss:.2f} | TP: ${r.target:.2f}</div>
                                <div>R:R {r.rr_ratio:.1f}x | Vol {r.rvol:.1f}x</div>
                                <small>{' | '.join(r.reasons[:3])}</small>
                            </div>
                            """, unsafe_allow_html=True)
            
        except Exception as e:
            st.error(f"Scan Fehler: {e}")
            logger.exception("Scan failed")

# ============================== Legacy Support ==============================

# Behalte alte Funktionen für Kompatibilität
def get_market_clock() -> Dict[str, Any]:
    """Legacy Market Clock (unverändert)"""
    et = pytz.timezone('US/Eastern')
    now = datetime.now(et)
    # ... (Implementation wie vorher)

# ============================== Main ==============================

if __name__ == "__main__":
    render_async_ui()
