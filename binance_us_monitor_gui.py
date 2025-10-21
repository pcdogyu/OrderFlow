#!/usr/bin/env python3
"""Multi-symbol Tkinter GUI for Binance US order-flow monitoring.

Highlights:
    - Streams aggTrade + bookTicker for BTCUSDT and ETHUSDT (configurable).
    - Displays rolling Delta (30s), rolling CVD (60s), order-book imbalance,
      and divergence hints per symbol.
    - 2x2 Matplotlib layout: price+CVD (left column) and imbalance (right column)
      for each symbol (BTC row 1, ETH row 2 by default).

Defaults requested by user:
    * Refresh interval: 0.5s
    * Delta window:    30s
    * CVD window:      60s
    * Chart history:   15 minutes (900s)

Usage:
    python binance_us_monitor_gui.py
    python binance_us_monitor_gui.py --symbols btcusdt ethusdt solusdt

Requirements:
    pip install websocket-client pandas matplotlib
"""

from __future__ import annotations

import argparse
import json
import math
import queue
import signal
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

DISPLAY_TZ = ZoneInfo("Asia/Shanghai")
UTC_TZ = ZoneInfo("UTC")
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, Iterable, List, Optional, Set, Tuple

import pandas as pd
import websocket

import coinpair

try:
    import requests
except ImportError:  # pragma: no cover - optional dependency
    requests = None  # type: ignore

import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk
import matplotlib
from matplotlib import dates as mdates, font_manager
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

try:
    from influxdb_client import InfluxDBClient, Point, WritePrecision
except ImportError:  # pragma: no cover - optional dependency
    InfluxDBClient = Point = WritePrecision = None  # type: ignore

try:
    import config as app_config
except ImportError:
    app_config = None


@dataclass
class SymbolSource:
    exchange: str
    symbol: str

    def normalized(self) -> "SymbolSource":
        exchange, symbol = normalize_exchange_symbol(self.exchange, self.symbol)
        return SymbolSource(exchange, symbol)

    @property
    def display(self) -> str:
        return f"{self.exchange.upper()}:{self.symbol.upper()}"


@dataclass
class MonitorConfig:
    symbol_sources: List[SymbolSource] = field(
        default_factory=lambda: [
            SymbolSource("coinbase", "btcusdt"),
            SymbolSource("coinbase", "ethusdt"),
        ]
    )
    poll_interval: float = 0.5  # seconds
    history_seconds: int = 3600  # 60 minutes
    delta_window: int = 60
    cvd_window: int = 60
    imbalance_smooth_points: int = 5
    divergence_window: int = 300
    startup_backfill_seconds: int = 3600
    history_cache_path: Optional[str] = None
    price_tolerance: float = 0.0005
    cvd_tolerance: float = 0.25
    influx_url: Optional[str] = None
    influx_token: Optional[str] = None
    influx_org: Optional[str] = None
    influx_bucket: Optional[str] = None
    influx_measurement: str = "orderflow"

    # Strategy overlay defaults
    strategy_enabled: bool = True
    strategy_lookback_seconds: int = 120
    strategy_delta_threshold: float = 5.0
    strategy_long_imbalance: float = 0.20
    strategy_short_imbalance: float = -0.20
    strategy_cvd_slope_threshold: float = 0.0
    strategy_min_signal_score: float = 0.66


STRATEGY_SIGNAL_DISPLAY = {
    "LONG": "做多",
    "LEAN_LONG": "偏多",
    "SHORT": "做空",
    "LEAN_SHORT": "偏空",
    "HOLD": "观望",
    "DISABLED": "关闭",
}

STRATEGY_REASON_DISPLAY = {
    "OB Bid Bias": "盘口偏多",
    "OB Ask Bias": "盘口偏空",
    "Delta Accum": "Delta净买",
    "Delta Sell": "Delta净卖",
    "CVD Up": "CVD上行",
    "CVD Down": "CVD下行",
    "Await alignment": "等待共振",
    "Strategy overlay disabled": "策略功能关闭",
}

STRATEGY_REASON_SKIP = {"Strategy overlay disabled", "Await alignment"}




ALL_OPTION = "全部"
HOT_EMPTY_OPTION = "暂无热门"

class SearchableCombobox(ttk.Combobox):
    def __init__(self, master=None, *, values=None, **kwargs):
        values = tuple(values or ())
        state = kwargs.get('state')
        if state == 'readonly':
            kwargs['state'] = 'normal'
        elif state is None:
            kwargs['state'] = 'normal'
        super().__init__(master, values=values, **kwargs)
        self._all_values: tuple[str, ...] = tuple(values)
        self.bind('<KeyRelease>', self._on_key_release)
        self.bind('<<ComboboxSelected>>', self._restore_values)
        self.bind('<FocusIn>', self._restore_values)

    def set_values(self, values):
        self._all_values = tuple(values or ())
        self.configure(values=self._all_values)
        self._restore_values()

    def _restore_values(self, *_):
        self.configure(values=self._all_values)

    def _on_key_release(self, event):
        if event.keysym in {'Return', 'KP_Enter', 'Up', 'Down', 'Left', 'Right', 'Tab'}:
            return
        if event.keysym == 'Escape':
            self.set('')
            self.configure(values=self._all_values)
            return
        typed = self.get()
        if not typed:
            self.configure(values=self._all_values)
            return
        filtered = [value for value in self._all_values if typed.lower() in value.lower()]
        if not filtered:
            filtered = self._all_values
        self.configure(values=filtered)

def configure_chinese_font() -> Optional[str]:
    preferred_fonts = ["Microsoft YaHei", "SimHei", "Noto Sans CJK SC", "PingFang SC"]
    available = {f.name for f in font_manager.fontManager.ttflist}
    chosen = None
    for font_name in preferred_fonts:
        if font_name in available:
            matplotlib.rcParams["font.family"] = font_name
            matplotlib.rcParams["font.sans-serif"] = [font_name]
            chosen = font_name
            break
    if not chosen:
        matplotlib.rcParams["font.family"] = ["DejaVu Sans", "Arial"]
        matplotlib.rcParams["font.sans-serif"] = ["Arial", "DejaVu Sans"]
    matplotlib.rcParams["axes.unicode_minus"] = False
    return chosen


COINBASE_QUOTES = ("usdt", "usdc", "usd", "eur", "gbp", "btc", "eth", "ada", "sol")
OKX_SPOT_QUOTES = ("usdt", "usdc", "usd", "btc", "eth", "eur", "gbp", "jpy", "cnh")

KRAKEN_OI_BASE_MAP = {
    "BTC": "PI_XBTUSD",
    "ETH": "PI_ETHUSD",
}


def resolve_kraken_oi_symbol(symbol: str) -> Optional[str]:
    """Map normalized spot symbol (e.g., BTC-USDT) to Kraken Futures instrument."""
    if not symbol:
        return None
    clean = symbol.upper().replace("/", "-")
    if "-" in clean:
        base = clean.split("-", 1)[0]
    else:
        base = clean
        for suffix in ("USDT", "USD", "USDC", "EUR"):
            if base.endswith(suffix):
                base = base[: -len(suffix)]
                break
    return KRAKEN_OI_BASE_MAP.get(base)


def normalize_exchange_symbol(exchange: str, symbol: str) -> Tuple[str, str]:
    exchange_l = exchange.strip().lower() or "binanceus"
    symbol_l = symbol.strip().lower()
    symbol_l = symbol_l.replace(" ", "").replace("_", "-").replace("/", "-")
    if exchange_l == "coinbase":
        if "-" not in symbol_l:
            for quote in COINBASE_QUOTES:
                if symbol_l.endswith(quote):
                    base = symbol_l[: -len(quote)]
                    if base:
                        symbol_l = f"{base}-{quote}"
                        break
        if "-" not in symbol_l:
            raise ValueError(f"Unsupported Coinbase symbol format: {symbol}")
    elif exchange_l == "okx":
        if "-" not in symbol_l:
            for quote in OKX_SPOT_QUOTES:
                if symbol_l.endswith(quote):
                    base = symbol_l[: -len(quote)]
                    if base:
                        symbol_l = f"{base}-{quote}"
                        break
        symbol_l = symbol_l.upper()
        if symbol_l.count("-") == 1:
            symbol_l = symbol_l.upper()
        elif symbol_l.count("-") == 2:
            symbol_l = symbol_l.upper()
        else:
            raise ValueError(f"Unsupported OKX symbol format: {symbol}")
        symbol_l = symbol_l.upper()
        symbol_l = symbol_l.replace("--", "-")
        symbol_l = symbol_l.upper()
    return exchange_l, symbol_l


def parse_symbol_token(token: str) -> SymbolSource:
    if ":" in token:
        exchange, symbol = token.split(":", 1)
    else:
        exchange, symbol = "binanceus", token
    exchange_l, symbol_l = normalize_exchange_symbol(exchange, symbol)
    return SymbolSource(exchange_l, symbol_l)


def clip_records_to_window(
    records: List[Dict[str, Any]], seconds: int, *, assume_sorted: bool = False
) -> List[Dict[str, Any]]:
    if not records or seconds <= 0:
        return []
    ordered = list(records) if assume_sorted else sorted(
        records, key=lambda r: pd.Timestamp(r["timestamp"])
    )
    latest_ts = pd.Timestamp(ordered[-1]["timestamp"])
    cutoff = latest_ts - pd.Timedelta(seconds=seconds)
    filtered: List[Dict[str, Any]] = []
    for rec in ordered:
        ts = pd.Timestamp(rec["timestamp"])
        if ts >= cutoff:
            filtered.append(rec)
    return filtered


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        result = float(value)
        if math.isfinite(result):
            return result
    except (TypeError, ValueError):
        return None
    return None


class HistoryCache:
    """JSON-backed cache for recent aggregate records per symbol."""

    def __init__(self, path: Optional[str], max_seconds: int) -> None:
        self.max_seconds = max(60, int(max_seconds))
        self.lock = threading.Lock()
        self._data: Dict[str, List[Dict[str, Any]]] = {}
        self._loaded = False
        self.enabled = True

        if path is not None and not path.strip():
            self.enabled = False
            self.path: Optional[Path] = None
            return

        if path is None:
            default_dir = Path(__file__).resolve().parent / "output"
            default_dir.mkdir(parents=True, exist_ok=True)
            self.path = default_dir / "history_cache.json"
        else:
            candidate = Path(path).expanduser()
            if not candidate.is_absolute():
                candidate = (Path(__file__).resolve().parent / candidate).resolve()
            candidate.parent.mkdir(parents=True, exist_ok=True)
            self.path = candidate

    def _ensure_loaded(self) -> None:
        if not self.enabled or self._loaded or self.path is None:
            return
        with self.lock:
            if self._loaded:
                return
            try:
                if self.path.exists():
                    raw = json.loads(self.path.read_text(encoding="utf-8"))
                    if isinstance(raw, dict):
                        self._data = {
                            key: value
                            for key, value in raw.items()
                            if isinstance(key, str) and isinstance(value, list)
                        }
            except Exception as exc:
                print(f"[warn] failed to load history cache {self.path}: {exc}")
                self._data = {}
            finally:
                self._loaded = True

    def load_for_key(self, key: str) -> List[Dict[str, Any]]:
        if not self.enabled or self.path is None:
            return []
        self._ensure_loaded()
        with self.lock:
            raw_items = list(self._data.get(key, []))
        if not raw_items:
            return []
        cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(seconds=self.max_seconds)
        restored: List[Dict[str, Any]] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            ts_raw = item.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = pd.Timestamp(ts_raw)
            except Exception:
                continue
            if ts.tzinfo is not None:
                ts = ts.tz_convert(None)
            if ts < cutoff:
                continue
            record = {
                "timestamp": ts,
                "mid": _safe_float(item.get("mid")) or float("nan"),
                "delta": _safe_float(item.get("delta")) or 0.0,
                "delta_sum": _safe_float(item.get("delta_sum")) or 0.0,
                "cvd": _safe_float(item.get("cvd")) or 0.0,
                "ob_imbalance": _safe_float(item.get("ob_imbalance")) or 0.0,
                "buy_vol": _safe_float(item.get("buy_vol")) or 0.0,
                "sell_vol": _safe_float(item.get("sell_vol")) or 0.0,
            }
            oi_value = _safe_float(item.get("oi"))
            if oi_value is not None:
                record["oi"] = oi_value
            restored.append(record)
        restored.sort(key=lambda r: r["timestamp"])
        return restored

    def load_all(self, sources: Iterable[SymbolSource]) -> Dict[str, List[Dict[str, Any]]]:
        loaded: Dict[str, List[Dict[str, Any]]] = {}
        for source in sources:
            key = source.normalized().display
            records = self.load_for_key(key)
            if records:
                loaded[key] = records
        return loaded

    def save_records(self, key: str, records: Iterable[Dict[str, Any]]) -> None:
        if not self.enabled or self.path is None:
            return
        sanitized: List[Dict[str, Any]] = []
        cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(seconds=self.max_seconds)
        for rec in records:
            try:
                ts = pd.Timestamp(rec["timestamp"])
            except Exception:
                continue
            if ts.tzinfo is not None:
                ts = ts.tz_convert(None)
            if ts < cutoff:
                continue
            entry = {
                "timestamp": ts.isoformat(),
                "mid": _safe_float(rec.get("mid")),
                "delta": _safe_float(rec.get("delta")) or 0.0,
                "delta_sum": _safe_float(rec.get("delta_sum")) or 0.0,
                "cvd": _safe_float(rec.get("cvd")) or 0.0,
                "ob_imbalance": _safe_float(rec.get("ob_imbalance")) or 0.0,
                "buy_vol": _safe_float(rec.get("buy_vol")) or 0.0,
                "sell_vol": _safe_float(rec.get("sell_vol")) or 0.0,
            }
            oi_value = _safe_float(rec.get("oi"))
            if oi_value is not None:
                entry["oi"] = oi_value
            sanitized.append(entry)
        if not sanitized:
            return
        max_items = 5000
        if len(sanitized) > max_items:
            sanitized = sanitized[-max_items:]
        self._ensure_loaded()
        with self.lock:
            self._data[key] = sanitized
            try:
                payload = json.dumps(self._data, ensure_ascii=False, indent=2)
                self.path.write_text(payload, encoding="utf-8")
            except Exception as exc:
                print(f"[warn] failed to persist history cache {self.path}: {exc}")

    def flush(self) -> None:
        # No-op: writes happen eagerly inside save_records.
        return


REST_TIMEOUT = 10


class KrakenFuturesOI:
    """Simple cached fetcher for Kraken Futures open-interest values."""

    API_URL = "https://futures.kraken.com/derivatives/api/v3/openinterest"
    CACHE_TTL = 30  # seconds

    def __init__(self) -> None:
        self._cache: Dict[str, float] = {}
        self._last_fetch: float = 0.0
        self._lock = threading.Lock()
        self._missing_logged: set[str] = set()

    def _refresh_cache(self) -> None:
        now = time.time()
        if now - self._last_fetch < self.CACHE_TTL:
            return
        with self._lock:
            if now - self._last_fetch < self.CACHE_TTL:
                return
            try:
                resp = requests.get(self.API_URL, timeout=REST_TIMEOUT)
                resp.raise_for_status()
                payload = resp.json()
            except Exception as exc:
                print(f"[warn] Kraken OI fetch failed: {exc}")
                self._last_fetch = now
                return
            items = payload.get("openInterest")
            if not isinstance(items, list):
                self._last_fetch = now
                return
            refreshed: Dict[str, float] = {}
            for entry in items:
                if not isinstance(entry, dict):
                    continue
                symbol = entry.get("symbol")
                value = entry.get("openInterest")
                try:
                    if symbol and value is not None:
                        refreshed[str(symbol)] = float(value)
                except (TypeError, ValueError):
                    continue
            self._cache = refreshed
            self._last_fetch = now
            sample = list(refreshed.items())[:3]
            print(f"[info] Kraken OI refreshed ({len(refreshed)} instruments): {sample}")

    def get(self, instrument: Optional[str]) -> Optional[float]:
        if instrument is None:
            return None
        self._refresh_cache()
        with self._lock:
            value = self._cache.get(instrument)
            if value is None:
                if instrument not in self._missing_logged:
                    print(f"[warn] Kraken OI missing instrument '{instrument}'")
                    self._missing_logged.add(instrument)
                return None
            return float(value)


KRAKEN_OI_CLIENT = KrakenFuturesOI()


def _build_records_from_trades(
    trades: List[Dict[str, Any]],
    cfg: MonitorConfig,
    seconds: int,
    ob_mid: Optional[float],
    ob_imbalance: Optional[float],
    source: SymbolSource,
    oi_symbol: Optional[str] = None,
) -> List[Dict[str, Any]]:
    if not trades:
        return []
    trades_sorted = sorted(trades, key=lambda item: item["timestamp"])
    bucket_seconds = 1
    bucket_freq = f"{bucket_seconds}S"
    buckets: Dict[pd.Timestamp, Dict[str, Any]] = {}
    for trade in trades_sorted:
        ts = trade["timestamp"]
        bucket_ts = ts.floor(bucket_freq)
        bucket = buckets.setdefault(
            bucket_ts,
            {
                "timestamp": bucket_ts,
                "mid": float("nan"),
                "delta": 0.0,
                "delta_sum": 0.0,
                "cvd": 0.0,
                "ob_imbalance": ob_imbalance or 0.0,
                "buy_vol": 0.0,
                "sell_vol": 0.0,
            },
        )
        price = trade["price"]
        size = trade["size"]
        side = trade["side"]
        bucket["mid"] = price
        if side >= 0:
            bucket["buy_vol"] += size
        else:
            bucket["sell_vol"] += size
        bucket["delta"] += side * size
    cutoff = trades_sorted[-1]["timestamp"] - pd.Timedelta(seconds=seconds)
    delta_window = max(1, int(cfg.delta_window))
    delta_queue: Deque[Tuple[pd.Timestamp, float]] = deque()
    delta_sum = 0.0
    cvd = 0.0
    records: List[Dict[str, Any]] = []
    for ts in sorted(buckets.keys()):
        if ts < cutoff:
            continue
        entry = buckets[ts]
        mid_val = entry["mid"]
        if (
            not isinstance(mid_val, (int, float))
            or (isinstance(mid_val, float) and math.isnan(mid_val))
        ) and ob_mid is not None:
            entry["mid"] = ob_mid
        delta = entry["delta"]
        cvd += delta
        entry["cvd"] = cvd
        delta_queue.append((ts, delta))
        delta_sum += delta
        cutoff_ts = ts - pd.Timedelta(seconds=delta_window)
        while delta_queue and delta_queue[0][0] < cutoff_ts:
            _, old = delta_queue.popleft()
            delta_sum -= old
        entry["delta_sum"] = delta_sum
        entry["timestamp"] = ts
        entry["exchange"] = source.exchange.upper()
        entry["symbol"] = source.symbol.upper()
        records.append(entry)
    if not records and ob_mid is not None:
        now = pd.Timestamp.utcnow().tz_localize(None)
        records.append(
            {
                "timestamp": now,
                "mid": ob_mid,
                "delta": 0.0,
                "delta_sum": 0.0,
                "cvd": 0.0,
                "ob_imbalance": ob_imbalance or 0.0,
                "buy_vol": 0.0,
                "sell_vol": 0.0,
                "exchange": source.exchange.upper(),
                "symbol": source.symbol.upper(),
            }
        )
    records = records[-2000:]
    oi_value = KRAKEN_OI_CLIENT.get(oi_symbol)
    if oi_value is not None:
        for rec in records:
            rec["oi"] = oi_value
    return records


def _fetch_binance_rest_history(source: SymbolSource, seconds: int, cfg: MonitorConfig) -> List[Dict[str, Any]]:
    if requests is None:
        return []
    symbol = source.symbol.upper().replace("-", "")
    end_ms = int(pd.Timestamp.utcnow().timestamp() * 1000)
    start_ms = end_ms - seconds * 1000
    trades: List[Dict[str, Any]] = []
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms, "limit": 1000}
    while True:
        resp = requests.get("https://api.binance.us/api/v3/aggTrades", params=params, timeout=REST_TIMEOUT)
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or not payload:
            break
        trades.extend(payload)
        last_time = int(payload[-1]["T"])
        if last_time >= end_ms or last_time <= params["startTime"]:
            break
        params["startTime"] = last_time + 1
        if len(trades) >= 5000:
            break
    normalized: List[Dict[str, Any]] = []
    cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(seconds=seconds)
    for item in trades:
        try:
            ts = pd.Timestamp(item["T"], unit="ms").tz_localize(None)
        except Exception:
            continue
        if ts < cutoff:
            continue
        price = _safe_float(item.get("p"))
        qty = _safe_float(item.get("q"))
        if price is None or qty is None:
            continue
        side = -1 if item.get("m") else 1
        normalized.append({"timestamp": ts, "price": price, "size": qty, "side": side})
    ob_mid = None
    ob_imbalance = 0.0
    try:
        ob_resp = requests.get(
            "https://api.binance.us/api/v3/ticker/bookTicker", params={"symbol": symbol}, timeout=REST_TIMEOUT
        )
        ob_resp.raise_for_status()
        book = ob_resp.json()
        bid = _safe_float(book.get("bidPrice"))
        ask = _safe_float(book.get("askPrice"))
        bid_qty = _safe_float(book.get("bidQty")) or 0.0
        ask_qty = _safe_float(book.get("askQty")) or 0.0
        if bid is not None and ask is not None:
            ob_mid = (bid + ask) / 2
        denom = bid_qty + ask_qty
        if denom > 0:
            ob_imbalance = (bid_qty - ask_qty) / denom
    except Exception:
        pass
    oi_symbol = resolve_kraken_oi_symbol(source.symbol.upper().replace("/", "-"))
    return _build_records_from_trades(normalized, cfg, seconds, ob_mid, ob_imbalance, source, oi_symbol)

def _fetch_coinbase_candles(product_id: str, minutes: int) -> List[Dict[str, Any]]:
    if requests is None:
        return []
    minutes = max(1, minutes)
    end_time = pd.Timestamp.utcnow().ceil("T")
    start_time = end_time - pd.Timedelta(minutes=minutes)
    params = {
        "granularity": 60,
        "start": start_time.isoformat(),
        "end": end_time.isoformat(),
    }
    try:
        resp = requests.get(
            f"https://api.exchange.coinbase.com/products/{product_id}/candles",
            params=params,
            timeout=REST_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    candles = [
        item
        for item in payload
        if isinstance(item, (list, tuple)) and len(item) >= 6 and _safe_float(item[4]) is not None
    ]
    if not candles:
        return []
    candles.sort(key=lambda entry: entry[0])
    records: List[Dict[str, Any]] = []
    for entry in candles:
        ts = pd.Timestamp(entry[0], unit="s").tz_localize(None) + pd.Timedelta(seconds=60)
        close_price = float(entry[4])
        records.append(
            {
                "timestamp": ts,
                "mid": close_price,
                "delta": 0.0,
                "delta_sum": 0.0,
                "cvd": 0.0,
                "ob_imbalance": float("nan"),
                "buy_vol": 0.0,
                "sell_vol": 0.0,
            }
        )
    return records[-minutes:]

def _fetch_coinbase_rest_history(source: SymbolSource, seconds: int, cfg: MonitorConfig) -> List[Dict[str, Any]]:
    if requests is None:
        return []
    product_id = source.symbol.upper()
    minutes = max(1, int(math.ceil(seconds / 60)))
    candles = _fetch_coinbase_candles(product_id, minutes)
    if candles:
        ob_mid = None
        ob_imbalance = float("nan")
        try:
            ob_resp = requests.get(
                f"https://api.exchange.coinbase.com/products/{product_id}/book",
                params={"level": 1},
                timeout=REST_TIMEOUT,
            )
            ob_resp.raise_for_status()
            book = ob_resp.json()
            bids = book.get("bids") or []
            asks = book.get("asks") or []
            bid = _safe_float(bids[0][0]) if bids and isinstance(bids[0], (list, tuple)) else None
            ask = _safe_float(asks[0][0]) if asks and isinstance(asks[0], (list, tuple)) else None
            bid_qty = _safe_float(bids[0][1]) if bids and isinstance(bids[0], (list, tuple)) else 0.0
            ask_qty = _safe_float(asks[0][1]) if asks and isinstance(asks[0], (list, tuple)) else 0.0
            if bid is not None and ask is not None:
                ob_mid = (bid + ask) / 2
            denom = bid_qty + ask_qty
            if denom > 0:
                ob_imbalance = (bid_qty - ask_qty) / denom
        except Exception:
            pass
        if ob_mid is not None and candles:
            candles[-1]["mid"] = ob_mid
        if candles:
            candles[-1]["ob_imbalance"] = ob_imbalance
        for entry in candles:
            entry["exchange"] = source.exchange.upper()
            entry["symbol"] = source.symbol.upper()
        return candles
    cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(seconds=seconds)
    trades: List[Dict[str, Any]] = []
    params: Dict[str, Any] = {"limit": 100}
    attempts = 0
    reached_cutoff = False
    while attempts < 50 and not reached_cutoff:
        attempts += 1
        resp = requests.get(
            f"https://api.exchange.coinbase.com/products/{product_id}/trades", params=params, timeout=REST_TIMEOUT
        )
        resp.raise_for_status()
        payload = resp.json()
        if not isinstance(payload, list) or not payload:
            break
        for trade in payload:
            ts_raw = trade.get("time")
            if not ts_raw:
                continue
            try:
                ts = pd.Timestamp(ts_raw).tz_convert(None)
            except Exception:
                continue
            if ts < cutoff:
                reached_cutoff = True
                break
            price = _safe_float(trade.get("price"))
            size = _safe_float(trade.get("size"))
            if price is None or size is None:
                continue
            side_flag = str(trade.get("side") or "").lower()
            side = 1 if side_flag == "buy" else -1
            trades.append({"timestamp": ts, "price": price, "size": size, "side": side})
        if reached_cutoff:
            break
        last_trade = payload[-1]
        trade_id = last_trade.get("trade_id")
        if trade_id is None:
            break
        params["after"] = trade_id
    ob_mid = None
    ob_imbalance = 0.0
    try:
        ob_resp = requests.get(
            f"https://api.exchange.coinbase.com/products/{product_id}/book", params={"level": 1}, timeout=REST_TIMEOUT
        )
        ob_resp.raise_for_status()
        book = ob_resp.json()
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        bid = _safe_float(bids[0][0]) if bids and isinstance(bids[0], (list, tuple)) else None
        ask = _safe_float(asks[0][0]) if asks and isinstance(asks[0], (list, tuple)) else None
        bid_qty = _safe_float(bids[0][1]) if bids and isinstance(bids[0], (list, tuple)) else 0.0
        ask_qty = _safe_float(asks[0][1]) if asks and isinstance(asks[0], (list, tuple)) else 0.0
        if bid is not None and ask is not None:
            ob_mid = (bid + ask) / 2
        denom = bid_qty + ask_qty
        if denom > 0:
            ob_imbalance = (bid_qty - ask_qty) / denom
    except Exception:
        pass
    oi_symbol = resolve_kraken_oi_symbol(source.symbol.upper().replace("/", "-"))
    return _build_records_from_trades(trades, cfg, seconds, ob_mid, ob_imbalance, source, oi_symbol)


def fetch_rest_history(
    source: SymbolSource, cfg: MonitorConfig, seconds_override: Optional[int] = None
) -> List[Dict[str, Any]]:
    normalized = source.normalized()
    if seconds_override is not None:
        seconds = max(60, int(seconds_override))
    else:
        seconds = min(max(60, cfg.startup_backfill_seconds), cfg.history_seconds)
    if seconds <= 0:
        return []
    try:
        if normalized.exchange in {"binanceus", "binance"}:
            return _fetch_binance_rest_history(normalized, seconds, cfg)
        if normalized.exchange == "coinbase":
            return _fetch_coinbase_rest_history(normalized, seconds, cfg)
    except Exception as exc:
        print(f"[warn] REST backfill failed for {normalized.display}: {exc}")
    return []


class InfluxPublisher:
    """Thin wrapper around influxdb-client to handle concurrent writes."""

    def __init__(self, cfg: MonitorConfig) -> None:
        if not (cfg.influx_url and cfg.influx_token and cfg.influx_org and cfg.influx_bucket):
            raise ValueError("Influx configuration incomplete.")
        if InfluxDBClient is None:
            raise RuntimeError(
                "influxdb-client is required. Install via 'pip install influxdb-client'."
            )
        self.client = InfluxDBClient(url=cfg.influx_url, token=cfg.influx_token, org=cfg.influx_org)
        self.write_api = self.client.write_api()
        self.bucket = cfg.influx_bucket
        self.org = cfg.influx_org
        self.measurement = cfg.influx_measurement
        self.lock = threading.Lock()

    def close(self) -> None:
        try:
            self.write_api.close()
        except Exception:
            pass
        try:
            self.client.close()
        except Exception:
            pass

    def write(self, source: SymbolSource, record: Dict[str, Any]) -> None:
        timestamp = record.get("timestamp")
        if isinstance(timestamp, pd.Timestamp):
            ts = timestamp.to_pydatetime(warn=False)
        else:
            ts = pd.Timestamp.utcnow().to_pydatetime()

        point = (
            Point(self.measurement)
            .tag("exchange", source.exchange.upper())
            .tag("symbol", source.symbol.upper())
        )
        strategy_signal = record.get("strategy_signal")
        if strategy_signal:
            try:
                point = point.tag("strategy_signal", str(strategy_signal))
            except Exception:
                pass
        strategy_bias = record.get("strategy_bias")
        if strategy_bias:
            try:
                point = point.tag("strategy_bias", str(strategy_bias))
            except Exception:
                pass
        for field in (
            "mid",
            "delta",
            "delta_sum",
            "cvd",
            "ob_imbalance",
            "buy_vol",
            "sell_vol",
            "strategy_confidence",
            "strategy_long_confidence",
            "strategy_short_confidence",
            "strategy_ob_avg",
            "strategy_delta_sum",
            "strategy_cvd_slope",
        ):
            value = record.get(field)
            if value is None:
                continue
            if isinstance(value, float) and pd.isna(value):
                continue
            try:
                point = point.field(field, float(value))
            except (TypeError, ValueError):
                continue

        if "oi" in record and record["oi"] is not None:
            try:
                point = point.field("oi", float(record["oi"]))
            except (TypeError, ValueError):
                pass

        point = point.time(ts, WritePrecision.S)
        with self.lock:
            self.write_api.write(bucket=self.bucket, org=self.org, record=point)

    def fetch_recent(self, source: SymbolSource, seconds: int) -> List[Dict[str, Any]]:
        symbol = source.symbol.upper()
        exchange = source.exchange.upper()
        query = (
            f'from(bucket:"{self.bucket}") '
            f'|> range(start: -{int(max(seconds, 60))}s) '
            f'|> filter(fn:(r) => r["_measurement"] == "{self.measurement}" '
            f'and r["exchange"] == "{exchange}" and r["symbol"] == "{symbol}") '
            '|> pivot(rowKey:["_time"], columnKey:["_field"], valueColumn:"_value") '
            '|> sort(columns:["_time"])'
        )
        try:
            tables = self.client.query_api().query(query, org=self.org)
        except Exception as exc:
            print(f"[warn] failed to fetch history from InfluxDB: {exc}")
            return []

        records: Dict[pd.Timestamp, Dict[str, Any]] = {}
        for table in tables:
            for rec in table.records:
                ts = pd.Timestamp(rec["_time"]).tz_convert(None)
                entry = records.setdefault(ts, {"timestamp": ts})
                try:
                    field = rec.get_field()
                except KeyError:
                    field = None

                if field:
                    entry[field] = rec.get_value()
                    continue

                for key, value in rec.values.items():
                    if key in {
                        "result",
                        "table",
                        "_start",
                        "_stop",
                        "_measurement",
                        "exchange",
                        "symbol",
                        "_time",
                    } or key.startswith("_"):
                        continue
                    entry[key] = value

        ordered = [records[ts] for ts in sorted(records.keys())]
        return clip_records_to_window(ordered, seconds, assume_sorted=True)



class OrderFlowMonitor:
    """Order-flow monitor supporting multiple exchanges (Binance US, Bybit)."""

    def __init__(
        self,
        source: SymbolSource,
        config: MonitorConfig,
        record_queue: "queue.Queue[Tuple[str, Dict[str, Any]]]",
        publisher: Optional[InfluxPublisher] = None,
        history_cache: Optional[HistoryCache] = None,
    ) -> None:
        normalized = source.normalized()
        self.source = normalized
        self.exchange = normalized.exchange
        self.symbol = normalized.symbol
        self.exchange_upper = self.exchange.upper()
        self.symbol_upper = self.symbol.upper()
        self.symbol_key = normalized.display
        self.cfg = config
        self.record_queue = record_queue
        self.publisher = publisher
        self.history_cache = history_cache

        self.coinbase_product_id: Optional[str] = None
        if self.exchange == "coinbase":
            try:
                _, normalized_symbol = normalize_exchange_symbol(self.exchange, self.symbol)
            except ValueError as exc:
                raise ValueError(f"Invalid Coinbase symbol '{self.symbol}'") from exc
            self.coinbase_product_id = normalized_symbol.upper()
        self.okx_inst_id: Optional[str] = None
        if self.exchange == "okx":
            try:
                _, normalized_symbol = normalize_exchange_symbol(self.exchange, self.symbol)
            except ValueError as exc:
                raise ValueError(f"Invalid OKX symbol '{self.symbol}'") from exc
            self.okx_inst_id = normalized_symbol.upper()

        self.ticks: Deque[Tuple[pd.Timestamp, float, float, int]] = deque()
        self.book: Deque[Tuple[pd.Timestamp, float, float, float, float]] = deque()
        self.stats: Deque[Dict[str, Any]] = deque()

        self.delta_window_values: Deque[Tuple[pd.Timestamp, float]] = deque()
        self.cvd_window_values: Deque[Tuple[pd.Timestamp, float]] = deque()
        self.delta_sum_value: float = 0.0
        self.cvd_value: float = 0.0
        self.kraken_oi_symbol: Optional[str] = resolve_kraken_oi_symbol(self.symbol_upper.replace("_", "-"))

        self.ws_app: Optional[websocket.WebSocketApp] = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self._backfill_done = False
        self._last_cache_flush = 0.0

        self.strategy_state: Dict[str, Any] = {
            "last_signal": "HOLD",
            "last_change": None,
        }

    @staticmethod
    def _ensure_naive(ts: Any) -> pd.Timestamp:
        stamp = pd.Timestamp(ts)
        if stamp.tzinfo is not None and stamp.tzinfo.utcoffset(stamp) is not None:
            return stamp.tz_convert(None)
        return stamp

    def _binance_url(self) -> str:
        stream = f"{self.symbol}@aggTrade/{self.symbol}@bookTicker"
        return f"wss://stream.binance.us:9443/stream?streams={stream}"

    def _bybit_url(self) -> str:
        return "wss://stream.bybit.com/v5/public/linear"

    def _coinbase_url(self) -> str:
        return "wss://ws-feed.exchange.coinbase.com"

    def _okx_url(self) -> str:
        return "wss://ws.okx.com:8443/ws/v5/public"

    def _bybit_subscribe(self, ws: websocket.WebSocketApp) -> None:
        args = [
            f"trade.{self.symbol_upper}",
            f"orderbook.1.{self.symbol_upper}",
        ]
        ws.send(json.dumps({"op": "subscribe", "args": args}))

    def _okx_subscribe(self, ws: websocket.WebSocketApp) -> None:
        if not self.okx_inst_id:
            return
        payload = {"op": "subscribe", "args": [
            {"channel": "trades", "instId": self.okx_inst_id},
            {"channel": "books5", "instId": self.okx_inst_id},
        ]}
        ws.send(json.dumps(payload))

    def _coinbase_subscribe(self, ws: websocket.WebSocketApp) -> None:
        if not self.coinbase_product_id:
            return
        payload = {
            "type": "subscribe",
            "product_ids": [self.coinbase_product_id],
            "channels": [
                {"name": "ticker", "product_ids": [self.coinbase_product_id]},
                {"name": "matches", "product_ids": [self.coinbase_product_id]},
            ],
        }
        ws.send(json.dumps(payload))

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        payload = json.loads(message)
        if self.exchange == "bybit":
            self._handle_bybit_message(payload)
        elif self.exchange == "okx":
            self._handle_okx_message(payload)
        elif self.exchange == "coinbase":
            self._handle_coinbase_message(payload)
        else:
            self._handle_binance_message(payload)

    def _handle_binance_message(self, payload: Dict[str, Any]) -> None:
        data = payload.get("data", payload)
        event_type = data.get("e")
        now = self._ensure_naive(pd.Timestamp.utcnow())
        if event_type == "aggTrade":
            price = float(data["p"])
            qty = float(data["q"])
            side = -1 if data.get("m") else 1
            with self.lock:
                self.ticks.append((now, price, qty, side))
        elif data.get("s") == self.symbol_upper and all(k in data for k in ("b", "B", "a", "A")):
            bid_price = float(data["b"])
            bid_qty = float(data["B"])
            ask_price = float(data["a"])
            ask_qty = float(data["A"])
            with self.lock:
                self.book.append((now, bid_price, bid_qty, ask_price, ask_qty))

    def _handle_bybit_message(self, payload: Dict[str, Any]) -> None:
        if payload.get("op") == "subscribe" or payload.get("success") is not None:
            return
        topic = payload.get("topic")
        if not topic:
            return
        now = self._ensure_naive(pd.Timestamp.utcnow())
        data = payload.get("data")
        records = data if isinstance(data, list) else [data]
        if topic.startswith("trade."):
            for trade in records:
                if not trade:
                    continue
                try:
                    price = float(trade.get("p"))
                    qty = float(trade.get("v"))
                except (TypeError, ValueError):
                    continue
                ts = trade.get("T") or trade.get("ts")
                ts = self._ensure_naive(pd.Timestamp(ts, unit="ms")) if ts else now
                maker_flag = trade.get("m")
                side_flag = str(trade.get("S", "")).lower()
                if maker_flag is not None:
                    side = -1 if maker_flag else 1
                elif side_flag:
                    side = 1 if side_flag == "buy" else -1
                else:
                    side = 1
                with self.lock:
                    self.ticks.append((ts, price, qty, side))
        elif topic.startswith("orderbook.1."):
            for entry in records:
                if not entry:
                    continue
                bids = entry.get("b") or entry.get("bid") or []
                asks = entry.get("a") or entry.get("ask") or []
                if not bids and not asks:
                    continue
                try:
                    bid_price = float(bids[0][0]) if bids else float("nan")
                    bid_qty = float(bids[0][1]) if bids else 0.0
                    ask_price = float(asks[0][0]) if asks else float("nan")
                    ask_qty = float(asks[0][1]) if asks else 0.0
                except (TypeError, ValueError, IndexError):
                    continue
                ts = entry.get("ts")
                ts = self._ensure_naive(pd.Timestamp(ts, unit="ms")) if ts else now
                with self.lock:
                    self.book.append((ts, bid_price, bid_qty, ask_price, ask_qty))

    def _handle_okx_message(self, payload: Dict[str, Any]) -> None:
        if payload.get("event") in {"subscribe", "unsubscribe"}:
            return
        arg = payload.get("arg") or {}
        channel = arg.get("channel") or payload.get("channel")
        data = payload.get("data") or []
        if not channel or not data:
            return
        now = self._ensure_naive(pd.Timestamp.utcnow())
        if channel == "trades":
            for item in data:
                try:
                    price = float(item.get("px"))
                    qty = float(item.get("sz"))
                except (TypeError, ValueError):
                    continue
                ts_raw = item.get("ts")
                ts = self._parse_okx_timestamp(ts_raw, fallback=now)
                side = str(item.get("side") or "").lower()
                direction = 1 if side != "sell" else -1
                with self.lock:
                    self.ticks.append((ts, price, qty, direction))
        elif channel == "books5":
            item = data[0]
            bids = item.get("bids") or []
            asks = item.get("asks") or []
            try:
                bid_price = float(bids[0][0]) if bids else float('nan')
                bid_qty = float(bids[0][1]) if bids else 0.0
                ask_price = float(asks[0][0]) if asks else float('nan')
                ask_qty = float(asks[0][1]) if asks else 0.0
            except (TypeError, ValueError, IndexError):
                return
            ts_raw = item.get("ts")
            ts = self._parse_okx_timestamp(ts_raw, fallback=now)
            with self.lock:
                self.book.append((ts, bid_price, bid_qty, ask_price, ask_qty))

    def _parse_okx_timestamp(self, ts_raw: Any, fallback: pd.Timestamp) -> pd.Timestamp:
        if ts_raw is None:
            return fallback
        try:
            if isinstance(ts_raw, pd.Timestamp):
                return self._ensure_naive(ts_raw)
            if isinstance(ts_raw, (int, float)):
                value_int = int(ts_raw)
            else:
                ts_str = str(ts_raw).strip()
                if not ts_str:
                    return fallback
                if ts_str.isdigit():
                    value_int = int(ts_str)
                else:
                    parsed = pd.Timestamp(ts_str)
                    return self._ensure_naive(parsed)
            digits = len(str(abs(value_int))) if value_int != 0 else 1
            if digits >= 19:
                ts = pd.Timestamp(value_int, unit="ns")
            elif digits >= 16:
                ts = pd.Timestamp(value_int, unit="us")
            elif digits >= 13:
                ts = pd.Timestamp(value_int, unit="ms")
            else:
                ts = pd.Timestamp(value_int, unit="s")
            return self._ensure_naive(ts)
        except Exception as exc:
            print(f"[{self.symbol_key}] error: Parsing {ts_raw!r} to datetime failed ({exc})")
            return fallback

    def _handle_coinbase_message(self, payload: Dict[str, Any]) -> None:
        msg_type = payload.get("type")
        product_id = payload.get("product_id") or payload.get("productId")
        if product_id and self.coinbase_product_id and product_id.upper() != self.coinbase_product_id:
            return
        if msg_type in (None, "subscriptions", "heartbeat", "snapshot", "l2update"):
            return
        if msg_type == "error":
            print(f"[{self.symbol_key}] coinbase error: {payload}")
            return
        now = self._ensure_naive(pd.Timestamp.utcnow())
        if msg_type == "match":
            try:
                price = float(payload.get("price"))
                qty = float(payload.get("size") or payload.get("last_size") or 0.0)
            except (TypeError, ValueError):
                return
            ts_raw = payload.get("time")
            ts = self._ensure_naive(pd.Timestamp(ts_raw)) if ts_raw else now
            side = str(payload.get("side") or "").lower()
            direction = 1 if side != "sell" else -1
            with self.lock:
                self.ticks.append((ts, price, qty, direction))
        elif msg_type == "ticker":
            try:
                bid_price = float(payload.get("best_bid") or payload.get("bid"))
                ask_price = float(payload.get("best_ask") or payload.get("ask"))
            except (TypeError, ValueError):
                return
            try:
                bid_qty = float(payload.get("best_bid_size") or payload.get("bid_size") or 0.0)
            except (TypeError, ValueError):
                bid_qty = 0.0
            try:
                ask_qty = float(payload.get("best_ask_size") or payload.get("ask_size") or 0.0)
            except (TypeError, ValueError):
                ask_qty = 0.0
            ts_raw = payload.get("time")
            ts = self._ensure_naive(pd.Timestamp(ts_raw)) if ts_raw else now
            with self.lock:
                self.book.append((ts, bid_price, bid_qty, ask_price, ask_qty))

    def on_error(self, _: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[{self.symbol_key}] error: {error}")

    def on_close(self, *_: object) -> None:
        print(f"[{self.symbol_key}] websocket closed")

    def run_ws(self) -> None:
        while not self.stop_event.is_set():
            if self.exchange == "bybit":
                url = self._bybit_url()
                self.ws_app = websocket.WebSocketApp(
                    url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws_app.on_open = self._bybit_subscribe
            elif self.exchange == "okx":
                if not self.okx_inst_id:
                    raise ValueError(f"OKX instrument not resolved for {self.symbol_key}")
                url = self._okx_url()
                self.ws_app = websocket.WebSocketApp(
                    url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws_app.on_open = self._okx_subscribe
            elif self.exchange == "coinbase":
                if not self.coinbase_product_id:
                    raise ValueError(f"Coinbase product id not resolved for {self.symbol_key}")
                url = self._coinbase_url()
                self.ws_app = websocket.WebSocketApp(
                    url,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                )
                self.ws_app.on_open = self._coinbase_subscribe
            else:
                raise ValueError(f"Unsupported exchange: {self.exchange}")

            try:
                self.ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                if self.stop_event.is_set():
                    break
                print(f"[{self.symbol_key}] reconnecting websocket ({exc})")
                time.sleep(2)
                continue

            if self.stop_event.is_set():
                break

            print(f"[{self.symbol_key}] websocket disconnected, retrying in 2s")
            time.sleep(2)

    def seed_history(self, records: List[Dict[str, Any]]) -> None:
        if not records:
            return
        ordered = sorted(records, key=lambda r: self._ensure_naive(r["timestamp"]))
        clipped = clip_records_to_window(ordered, self.cfg.history_seconds, assume_sorted=True)
        self.stats = deque(clipped)
        self.delta_window_values.clear()
        self.cvd_window_values.clear()
        self.delta_sum_value = 0.0
        self.cvd_value = 0.0
        self.strategy_state = {
            "last_signal": "HOLD",
            "last_change": None,
        }
        if not self.stats:
            return
        last_ts = self._ensure_naive(self.stats[-1]["timestamp"])
        cutoff_delta = last_ts - pd.Timedelta(seconds=self.cfg.delta_window)
        cutoff_cvd = last_ts - pd.Timedelta(seconds=self.cfg.cvd_window)
        for rec in self.stats:
            ts = self._ensure_naive(rec["timestamp"])
            delta_val = float(rec.get("delta", 0.0) or 0.0)
            if ts >= cutoff_delta:
                self.delta_window_values.append((ts, delta_val))
                self.delta_sum_value += delta_val
            if ts >= cutoff_cvd:
                self.cvd_window_values.append((ts, delta_val))
                self.cvd_value += delta_val
        if self.stats:
            self._backfill_done = True
            self._persist_history_snapshot(force=True)
            last = self.stats[-1]
            info = self._evaluate_strategy(last, self._ensure_naive(last["timestamp"]))
            last.update(info)

    def _update_window(
        self,
        buffer: Deque[Tuple[pd.Timestamp, float]],
        value: float,
        now: pd.Timestamp,
        window_seconds: int,
        current_sum: float,
    ) -> float:
        buffer.append((now, value))
        current_sum += value
        cutoff = now - pd.Timedelta(seconds=window_seconds)
        while buffer and buffer[0][0] < cutoff:
            _, old_value = buffer.popleft()
            current_sum -= old_value
        return current_sum

    def _ensure_startup_backfill(self, record: Dict[str, Any]) -> None:
        if self._backfill_done or self.stats:
            return
        seconds = min(max(60, self.cfg.startup_backfill_seconds), self.cfg.history_seconds)
        if seconds <= 0:
            self._backfill_done = True
            return
        now_ts = self._ensure_naive(record["timestamp"])
        start_ts = now_ts - pd.Timedelta(seconds=seconds)
        step_seconds = max(1.0, self.cfg.poll_interval)
        step_delta = pd.Timedelta(seconds=step_seconds)
        template_mid = _safe_float(record.get("mid"))
        template_ob = _safe_float(record.get("ob_imbalance")) or 0.0
        backfill: List[Dict[str, Any]] = []
        ts_cursor = start_ts
        while ts_cursor < now_ts:
            backfill.append(
                {
                    "timestamp": ts_cursor,
                    "mid": template_mid if template_mid is not None else float("nan"),
                    "delta": 0.0,
                    "delta_sum": 0.0,
                    "cvd": 0.0,
                    "ob_imbalance": float(template_ob),
                    "buy_vol": 0.0,
                    "sell_vol": 0.0,
                }
            )
            ts_cursor += step_delta
        if backfill:
            self.seed_history(backfill)
        self._backfill_done = True

    def _persist_history_snapshot(self, force: bool = False) -> None:
        if self.history_cache is None or not self.stats:
            return
        now_time = time.time()
        if not force and now_time - self._last_cache_flush < 5.0:
            return
        self._last_cache_flush = now_time
        try:
            self.history_cache.save_records(self.symbol_key, list(self.stats))
        except Exception as exc:
            print(f"[warn] failed to cache history for {self.symbol_key}: {exc}")

    def detect_divergence(self) -> str:
        if len(self.stats) < 3:
            return ""
        now = self._ensure_naive(self.stats[-1]["timestamp"])
        cutoff = now - pd.Timedelta(seconds=self.cfg.divergence_window)
        window = [item for item in self.stats if self._ensure_naive(item["timestamp"]) >= cutoff]
        if len(window) < 3:
            return ""
        price_high = max(item["mid"] for item in window)
        price_low = min(item["mid"] for item in window)
        cvd_high = max(item["cvd"] for item in window)
        cvd_low = min(item["cvd"] for item in window)
        price = window[-1]["mid"]
        cvd = window[-1]["cvd"]
        price_range = max(price_high - price_low, 1e-9)
        cvd_range = max(cvd_high - cvd_low, 1e-9)
        if price >= price_high - price_range * self.cfg.price_tolerance:
            if cvd <= cvd_high - cvd_range * self.cfg.cvd_tolerance:
                return "看跌背离 / Bearish"
        if price <= price_low + price_range * self.cfg.price_tolerance:
            if cvd >= cvd_low + cvd_range * self.cfg.cvd_tolerance:
                return "看涨背离 / Bullish"
        return ""

    def _evaluate_strategy(self, record: Dict[str, Any], now: pd.Timestamp) -> Dict[str, Any]:
        if not self.cfg.strategy_enabled:
            return {
                "strategy_signal": "DISABLED",
                "strategy_bias": "NEUTRAL",
                "strategy_confidence": 0.0,
                "strategy_long_confidence": 0.0,
                "strategy_short_confidence": 0.0,
                "strategy_reason": "Strategy overlay disabled",
                "strategy_ob_avg": record.get("ob_imbalance"),
                "strategy_delta_sum": record.get("delta_sum"),
                "strategy_cvd_slope": 0.0,
                "strategy_last_change": self.strategy_state.get("last_change"),
            }

        lookback_seconds = max(30, int(self.cfg.strategy_lookback_seconds))
        lookback = pd.Timedelta(seconds=lookback_seconds)
        relevant: List[Dict[str, Any]] = []
        for rec in reversed(self.stats):
            ts = self._ensure_naive(rec["timestamp"])
            if now - ts <= lookback:
                relevant.append(rec)
            else:
                break

        if not relevant:
            relevant = [record]
        relevant = list(reversed(relevant))

        delta_values = [float(r.get("delta", 0.0) or 0.0) for r in relevant]
        delta_sum = float(sum(delta_values))

        ob_values: List[float] = []
        for r in relevant:
            value = r.get("ob_imbalance")
            if value is None:
                continue
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            if not pd.isna(value):
                ob_values.append(float(value))
        if ob_values:
            ob_avg = float(sum(ob_values) / len(ob_values))
        else:
            ob_avg = float("nan")

        cvd_start = float(relevant[0].get("cvd", 0.0) or 0.0)
        cvd_end = float(relevant[-1].get("cvd", 0.0) or 0.0)
        start_ts = self._ensure_naive(relevant[0]["timestamp"])
        end_ts = self._ensure_naive(relevant[-1]["timestamp"])
        elapsed = max((end_ts - start_ts).total_seconds(), 1.0)
        cvd_slope = (cvd_end - cvd_start) / elapsed

        long_reasons: List[str] = []
        short_reasons: List[str] = []

        metrics_available = 3
        if math.isnan(ob_avg):
            metrics_available -= 1

        if not math.isnan(ob_avg):
            if ob_avg >= self.cfg.strategy_long_imbalance:
                long_reasons.append("OB Bid Bias")
            if ob_avg <= self.cfg.strategy_short_imbalance:
                short_reasons.append("OB Ask Bias")

        if delta_sum >= self.cfg.strategy_delta_threshold:
            long_reasons.append("Delta Accum")
        if delta_sum <= -self.cfg.strategy_delta_threshold:
            short_reasons.append("Delta Sell")

        slope_threshold = abs(self.cfg.strategy_cvd_slope_threshold)
        if slope_threshold == 0.0:
            slope_threshold = 1e-6
        if cvd_slope >= slope_threshold:
            long_reasons.append("CVD Up")
        if cvd_slope <= -slope_threshold:
            short_reasons.append("CVD Down")

        available = max(metrics_available, 1)
        long_confidence = min(1.0, len(long_reasons) / available)
        short_confidence = min(1.0, len(short_reasons) / available)
        bias = "NEUTRAL"
        if long_confidence > short_confidence:
            bias = "LONG"
        elif short_confidence > long_confidence:
            bias = "SHORT"

        min_score = min(max(self.cfg.strategy_min_signal_score, 0.0), 1.0)
        signal = "HOLD"
        active_reasons: List[str] = []
        confidence = max(long_confidence, short_confidence)

        if long_confidence >= min_score and long_confidence >= short_confidence:
            signal = "LONG"
            active_reasons = long_reasons
            confidence = long_confidence
        elif short_confidence >= min_score and short_confidence >= long_confidence:
            signal = "SHORT"
            active_reasons = short_reasons
            confidence = short_confidence
        else:
            lean_threshold = max(0.4, min_score * 0.75)
            if long_confidence >= lean_threshold and long_confidence > short_confidence:
                signal = "LEAN_LONG"
                active_reasons = long_reasons
                confidence = long_confidence
            elif short_confidence >= lean_threshold and short_confidence > long_confidence:
                signal = "LEAN_SHORT"
                active_reasons = short_reasons
                confidence = short_confidence
            else:
                signal = "HOLD"
                active_reasons = long_reasons if bias == "LONG" else short_reasons if bias == "SHORT" else []

        last_signal = self.strategy_state.get("last_signal")
        if signal != last_signal:
            self.strategy_state["last_signal"] = signal
            self.strategy_state["last_change"] = now
        elif self.strategy_state.get("last_change") is None:
            self.strategy_state["last_change"] = now

        reason = ", ".join(active_reasons) if active_reasons else "Await alignment"
        info = {
            "strategy_signal": signal,
            "strategy_bias": bias,
            "strategy_confidence": float(round(confidence, 4)),
            "strategy_long_confidence": float(round(long_confidence, 4)),
            "strategy_short_confidence": float(round(short_confidence, 4)),
            "strategy_reason": reason,
            "strategy_ob_avg": ob_avg,
            "strategy_delta_sum": delta_sum,
            "strategy_cvd_slope": cvd_slope,
            "strategy_last_change": self.strategy_state.get("last_change"),
        }
        return info

    def aggregate_once(self) -> Optional[Dict[str, Any]]:
        now = self._ensure_naive(pd.Timestamp.utcnow())
        window_start = now - pd.Timedelta(seconds=self.cfg.history_seconds)
        with self.lock:
            while self.ticks and self.ticks[0][0] < window_start:
                self.ticks.popleft()
            while self.book and self.book[0][0] < window_start:
                self.book.popleft()
            recent_ticks = [t for t in self.ticks if t[0] >= now - pd.Timedelta(seconds=1.5)]
            recent_book = [b for b in self.book if b[0] >= now - pd.Timedelta(seconds=2)]
        if not recent_ticks and not recent_book:
            return None
        buy_vol = sum(q for _, _, q, s in recent_ticks if s > 0)
        sell_vol = sum(q for _, _, q, s in recent_ticks if s < 0)
        delta = buy_vol - sell_vol
        self.delta_sum_value = self._update_window(
            self.delta_window_values, delta, now, self.cfg.delta_window, self.delta_sum_value
        )
        self.cvd_value = self._update_window(
            self.cvd_window_values, delta, now, self.cfg.cvd_window, self.cvd_value
        )
        mid = float("nan")
        ob_imbalance = float("nan")
        if recent_book:
            last = recent_book[-1]
            bid_price, bid_qty = last[1], last[2]
            ask_price, ask_qty = last[3], last[4]
            if not pd.isna(bid_price) and not pd.isna(ask_price):
                mid = (bid_price + ask_price) / 2
            denom = bid_qty + ask_qty
            if denom:
                ob_imbalance = (bid_qty - ask_qty) / (denom + 1e-9)
        elif recent_ticks:
            prices = [p for _, p, _, _ in recent_ticks]
            mid = sum(prices) / len(prices)
        record = {
            "timestamp": now,
            "exchange": self.exchange_upper,
            "symbol": self.symbol_upper,
            "mid": mid,
            "delta": delta,
            "delta_sum": self.delta_sum_value,
            "cvd": self.cvd_value,
            "ob_imbalance": ob_imbalance,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
        }
        oi_val = KRAKEN_OI_CLIENT.get(self.kraken_oi_symbol)
        if oi_val is not None:
            record["oi"] = oi_val
        if not self.stats:
            self._ensure_startup_backfill(record)
        self.stats.append(record)
        while self.stats and (
            now - self._ensure_naive(self.stats[0]["timestamp"])
        ).total_seconds() > self.cfg.history_seconds:
            self.stats.popleft()
        strategy_info = self._evaluate_strategy(record, now)
        record.update(strategy_info)
        record["divergence"] = self.detect_divergence()
        if record["divergence"]:
            record["display_divergence"] = record["divergence"]
            record["divergence_until"] = now + pd.Timedelta(seconds=60)
        else:
            record["display_divergence"] = ""
            record["divergence_until"] = None
        if self.publisher is not None:
            try:
                self.publisher.write(self.source, record)
            except Exception as exc:
                print(f"[warn] failed to write to InfluxDB for {self.symbol_key}: {exc}")
        try:
            self.record_queue.put_nowait((self.symbol_key, record))
        except queue.Full:
            try:
                self.record_queue.get_nowait()
            except queue.Empty:
                pass
            self.record_queue.put_nowait((self.symbol_key, record))
        self._persist_history_snapshot()
        return record

    def run_aggregation_loop(self) -> None:
        while not self.stop_event.is_set():
            self.aggregate_once()
            time.sleep(self.cfg.poll_interval)

    def start(self) -> None:
        ws_thread = threading.Thread(target=self.run_ws, name=f"ws-{self.symbol_key}", daemon=True)
        agg_thread = threading.Thread(
            target=self.run_aggregation_loop, name=f"agg-{self.symbol_key}", daemon=True
        )
        ws_thread.start()
        agg_thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.ws_app is not None:
            try:
                self.ws_app.close()
            except Exception:
                pass
        self._persist_history_snapshot(force=True)
class MonitorGUI:
    """Tkinter GUI handling multi-symbol layout."""

    def __init__(
        self,
        cfg: MonitorConfig,
        record_queue: "queue.Queue[Tuple[str, Dict[str, Any]]]",
        stop_callback: Optional[Callable[[], None]] = None,
        preferred_font: Optional[str] = None,
        initial_history: Optional[Dict[str, List[Dict[str, Any]]]] = None,
    ) -> None:
        self.cfg = cfg
        self.record_queue = record_queue
        self.stop_callback = stop_callback
        self._is_closing = False

        self.display_tz = DISPLAY_TZ
        self.utc_tz = UTC_TZ

        self.records_by_symbol: Dict[str, List[Dict[str, Any]]] = {
            source.normalized().display: [] for source in cfg.symbol_sources
        }
        if initial_history:
            for key, items in initial_history.items():
                clipped = clip_records_to_window(items, self.cfg.history_seconds)
                if clipped:
                    self.records_by_symbol[key] = clipped

        self.popular_pairs: Dict[str, List[str]] = {}
        self.popular_pairs_timestamp: Optional[datetime] = None
        self.selected_pair_key: Optional[str] = None
        self.pair_choice_map: Dict[str, str] = {}
        self._reload_popular_pairs(initial=True)

        self.root = tk.Tk()
        self.preferred_font = preferred_font
        if preferred_font:
            for name in ("TkDefaultFont", "TkTextFont", "TkMenuFont", "TkHeadingFont"):
                try:
                    tkfont.nametofont(name).configure(family=preferred_font)
                except tk.TclError:
                    pass
        self.root.title("Binance US Order Flow 鐩戞帶闈㈡澘")
        self.root.geometry("1200x720")

        base_font = tkfont.nametofont("TkDefaultFont")
        self.ui_font_family = preferred_font or base_font.cget("family")
        self.heading_font = tkfont.Font(family=self.ui_font_family, size=9, weight="bold")
        self.value_font = tkfont.Font(family=self.ui_font_family, size=12)
        self.strategy_font = tkfont.Font(family=self.ui_font_family, size=10)
        self.timeframe_presets: List[Tuple[str, int]] = [
            ("5M", 5),
            ("15M", 15),
            ("30M", 30),
            ("1H", 60),
            ("4H", 240),
        ]
        self.timeframe_minutes_map: Dict[str, int] = {
            label: minutes for label, minutes in self.timeframe_presets
        }
        default_timeframe = "1H" if "1H" in self.timeframe_minutes_map else self.timeframe_presets[0][0]
        self.timeframe_var = tk.StringVar(value=default_timeframe)
        self.selected_timeframe_minutes = self.timeframe_minutes_map.get(default_timeframe, 60)
        self._price_fill_thread: Optional[threading.Thread] = None

        self._build_widgets()
        self._setup_plot()
        self._schedule_popular_pairs_refresh()

        if initial_history:
            for key, records in initial_history.items():
                if key in self.records_by_symbol and records:
                    last = records[-1]
                    self._update_metrics(key, last)
                    self._update_charts(key)

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.update_ui()

    # ------------------------------------------------------------------ UI layout


    def _build_widgets(self) -> None:
        mainframe = ttk.Frame(self.root, padding="8 8 8 8")
        mainframe.pack(fill=tk.BOTH, expand=True)

        top_container = ttk.Frame(mainframe)
        top_container.pack(fill=tk.X, pady=(0, 6))
        top_container.columnconfigure(0, weight=0)
        top_container.columnconfigure(1, weight=1)
        top_container.rowconfigure(0, weight=1)

        control_frame = ttk.LabelFrame(top_container, text="�������� Parameters", padding="6")
        control_frame.grid(row=0, column=0, sticky="nw")

        self.poll_interval_var = tk.DoubleVar(value=self.cfg.poll_interval)
        self.delta_window_var = tk.IntVar(value=self.cfg.delta_window)
        self.cvd_window_var = tk.IntVar(value=self.cfg.cvd_window)
        self.history_seconds_var = tk.IntVar(value=self.cfg.history_seconds)
        self.imbalance_smooth_var = tk.IntVar(value=self.cfg.imbalance_smooth_points)

        def add_control(col: int, text: str, var: Any, from_, to, increment, fmt: str) -> ttk.Spinbox:
            frame = ttk.Frame(control_frame)
            frame.grid(row=0, column=col, padx=4, pady=2, sticky="w")
            ttk.Label(frame, text=text).pack(anchor="w")
            spin = ttk.Spinbox(
                frame,
                from_=from_,
                to=to,
                increment=increment,
                textvariable=var,
                width=8,
                format=fmt,
            )
            spin.pack(anchor="w")
            return spin

        for col in range(7):
            control_frame.columnconfigure(col, weight=0)

        add_control(0, "刷新周期(s)", self.poll_interval_var, 0.2, 5.0, 0.1, "%.2f")
        add_control(1, "Delta窗口(s)", self.delta_window_var, 5, 600, 1, "%d")
        add_control(2, "CVD窗口(s)", self.cvd_window_var, 10, 900, 1, "%d")
        add_control(3, "图表秒数", self.history_seconds_var, 60, 3600, 30, "%d")
        add_control(4, "OB平滑点数", self.imbalance_smooth_var, 1, 200, 1, "%d")

        ttk.Button(control_frame, text="应用 Apply", command=self.apply_parameters).grid(
            row=0, column=5, padx=8, pady=2
        )
        self.price_fill_button = ttk.Button(
            control_frame, text="Price Fill", command=self._on_price_fill_clicked
        )
        self.price_fill_button.grid(row=0, column=6, padx=8, pady=2)

        timeframe_frame = ttk.Frame(control_frame)
        timeframe_frame.grid(row=1, column=0, columnspan=7, sticky="w", pady=(4, 0))
        ttk.Label(timeframe_frame, text="Price Span:").pack(side=tk.LEFT, padx=(0, 6))
        for label, _ in self.timeframe_presets:
            ttk.Radiobutton(
                timeframe_frame,
                text=label,
                value=label,
                variable=self.timeframe_var,
                command=self._on_timeframe_change,
            ).pack(side=tk.LEFT, padx=2)

        metrics_frame = ttk.LabelFrame(top_container, text="ʵʱָ�� Live Metrics", padding="6")
        metrics_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0))

        self.metric_labels: Dict[str, Dict[str, tk.StringVar]] = {}
        self.metric_widgets: Dict[str, Dict[str, tk.Label]] = {}
        self.heading_labels: Dict[str, Dict[str, ttk.Label]] = {}

        selector_frame = ttk.Frame(metrics_frame)
        selector_frame.pack(fill=tk.X, pady=(0, 6))
        self.symbol_filter_var = tk.StringVar(value=ALL_OPTION)
        ttk.Label(selector_frame, text="币对").grid(row=0, column=0, padx=(0, 4))
        pair_choices = self._build_pair_choices()
        self.pair_selector = SearchableCombobox(
            selector_frame,
            textvariable=self.symbol_filter_var,
            values=pair_choices,
            width=28,
        )
        self.pair_selector.grid(row=0, column=1, padx=(0, 12))
        self.pair_selector.bind("<<ComboboxSelected>>", lambda _: self._on_pair_selected())
        ttk.Label(selector_frame, text="").grid(row=0, column=2, padx=(0, 4))
        self.hot_pairs_update_label = ttk.Label(selector_frame, text="")
        self.hot_pairs_update_label.grid(row=0, column=3, sticky="w")
        selector_frame.columnconfigure(4, weight=1)
        container = ttk.Frame(metrics_frame)
        container.pack(fill=tk.X)
        self.metrics_container = container
        total = len(self.cfg.symbol_sources)
        for col in range(total):
            container.columnconfigure(col, weight=1)

        self.symbol_frames: Dict[str, ttk.LabelFrame] = {}

        for idx, source in enumerate(self.cfg.symbol_sources):
            key = source.normalized().display
            display_symbol = self._extract_display_parts(key)[1]
            block = ttk.LabelFrame(container, text=display_symbol, padding="4")
            block.grid(row=0, column=idx, sticky="nsew", padx=4, pady=2)
            self.symbol_frames[key] = block

            grid = ttk.Frame(block)
            grid.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)
            for col in range(8):
                grid.columnconfigure(col, weight=1)

            vars_map = {
                "mid": tk.StringVar(value="--"),
                "delta": tk.StringVar(value="--"),
                "delta_sum": tk.StringVar(value="--"),
                "cvd": tk.StringVar(value="--"),
                "oi": tk.StringVar(value="--"),
                "ob": tk.StringVar(value="--"),
                "divergence": tk.StringVar(value="None"),
                "strategy": tk.StringVar(value="HOLD"),
            }
            self.metric_labels[key] = vars_map
            self.heading_labels[key] = {}

            label_widgets: Dict[str, tk.Label] = {}

            def add_item(col: int, title: str, subtitle: str, label_key: str) -> None:
                cell = ttk.Frame(grid, padding="2")
                cell.grid(row=0, column=col, sticky="w", padx=4)
                heading = ttk.Label(cell, text=f"{title}\n{subtitle}", font=self.heading_font)
                heading.pack(anchor=tk.W)
                self.heading_labels[key][label_key] = heading
                value_font = self.strategy_font if label_key == "strategy" else self.value_font
                lbl = tk.Label(cell, textvariable=vars_map[label_key], font=value_font)
                lbl.pack(anchor=tk.W)
                label_widgets[label_key] = lbl

                if label_key == "cvd":
                    heading.configure(foreground="#000000")
                    lbl.configure(fg="#000000")

            add_item(0, "Price", "价格", "mid")
            add_item(1, "Delta", "净成交量", "delta")
            add_item(2, f"Δ Sum ({self.cfg.delta_window}s)", "Delta累积", "delta_sum")
            add_item(3, f"CVD ({self.cfg.cvd_window}s)", "累积Delta", "cvd")
            add_item(4, "Open Interest", "未平仓量", "oi")
            add_item(5, "OB Imbalance", "盘口不平衡", "ob")
            add_item(6, "Divergence", "背离信号", "divergence")
            add_item(7, "策略", "策略建议", "strategy")

            self.metric_widgets[key] = label_widgets

        self._update_pair_selector()

        chart_frame = ttk.LabelFrame(mainframe, text="60分钟滚动图 Realtime Charts", padding="6")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=6)
        self.chart_frame = chart_frame

    def _reload_popular_pairs(self, initial: bool = False) -> None:
        try:
            pairs_map, fetched_at = coinpair.get_pairs_map(limit=50)
        except Exception as exc:
            prefix = "初始化" if initial else "定时"
            print(f"[warn] {prefix}热门币对加载失败：{exc}")
            return
        self.popular_pairs = pairs_map
        self.popular_pairs_timestamp = fetched_at
        if hasattr(self, "pair_selector"):
            self._update_pair_selector()
            self._update_hot_pairs_label()

    def _schedule_popular_pairs_refresh(self) -> None:
        interval_seconds = max(3600, int(coinpair.CACHE_MAX_AGE.total_seconds()))
        self.root.after(interval_seconds * 1000, self._refresh_popular_pairs_periodic)

    def _refresh_popular_pairs_periodic(self) -> None:
        self._reload_popular_pairs(initial=False)
        self._schedule_popular_pairs_refresh()

    @staticmethod
    def _extract_display_parts(key: str) -> tuple[str, str]:
        if ':' in key:
            exchange, symbol = key.split(':', 1)
        else:
            exchange, symbol = '', key
        return exchange.upper(), symbol.replace('-', '').upper()

    def _build_pair_choices(self) -> list[str]:
        choice_map: dict[str, str] = {}
        choices: list[str] = [ALL_OPTION]
        for key in sorted(self.records_by_symbol.keys()):
            _, symbol_display = self._extract_display_parts(key)
            if symbol_display not in choice_map:
                choice_map[symbol_display] = key
                choices.append(symbol_display)
        hot_pairs = sorted(self.popular_pairs.get('COINBASE', []))
        for symbol in hot_pairs:
            symbol_display = symbol.replace('-', '').upper()
            if symbol_display in choice_map:
                continue
            choice_map[symbol_display] = f"COINBASE:{symbol.upper()}"
            choices.append(symbol_display)
        self.pair_choice_map = choice_map
        return choices

    def _update_pair_selector(self) -> None:
        choices = self._build_pair_choices()
        self.pair_selector.set_values(choices)
        display = self.symbol_filter_var.get()
        if display not in choices:
            display = ALL_OPTION
        self.symbol_filter_var.set(display)
        self.selected_pair_key = None if display == ALL_OPTION else self.pair_choice_map.get(display)
        self.pair_selector.set(display)
        self._apply_symbol_filter()

    def _on_pair_selected(self) -> None:
        display = self.symbol_filter_var.get()
        if display == ALL_OPTION:
            self.selected_pair_key = None
        else:
            self.selected_pair_key = self.pair_choice_map.get(display)
        self._apply_symbol_filter()

    def _apply_symbol_filter(self) -> None:
        target = getattr(self, "selected_pair_key", None)
        keys = list(self.symbol_frames.keys())
        if target:
            filtered = [key for key in keys if key == target]
        else:
            filtered = keys[:]
        if not filtered:
            filtered = keys

        max_cols = len(keys) + 1
        for col in range(max_cols):
            self.metrics_container.columnconfigure(col, weight=0)

        for idx, key in enumerate(filtered):
            frame = self.symbol_frames[key]
            frame.grid(row=0, column=idx, sticky="nsew", padx=4, pady=2)
            self.metrics_container.columnconfigure(idx, weight=1)
        for key, frame in self.symbol_frames.items():
            if key not in filtered:
                frame.grid_remove()

        self._update_chart_visibility()
        self._refresh_all_charts()

    def _on_timeframe_change(self) -> None:
        selection = self.timeframe_var.get()
        minutes = self.timeframe_minutes_map.get(selection, 60)
        self.selected_timeframe_minutes = minutes
        if minutes * 60 > self.cfg.history_seconds:
            self.cfg.history_seconds = minutes * 60
            self.history_seconds_var.set(self.cfg.history_seconds)
        self._refresh_all_charts()

    def _refresh_all_charts(self) -> None:
        if not hasattr(self, "axes_price"):
            return
        for key, records in list(self.records_by_symbol.items()):
            if records:
                self._update_charts(key)

    def _on_price_fill_clicked(self) -> None:
        if self._price_fill_thread and self._price_fill_thread.is_alive():
            return
        try:
            self.price_fill_button.state(["disabled"])
        except Exception:
            pass
        thread = threading.Thread(target=self._price_fill_worker, daemon=True)
        self._price_fill_thread = thread
        thread.start()

    def _price_fill_worker(self) -> None:
        selection = self.timeframe_var.get()
        minutes = self.timeframe_minutes_map.get(selection, 60)
        seconds = max(60, minutes * 60)
        updates: Dict[str, List[Dict[str, Any]]] = {}
        for source in self.cfg.symbol_sources:
            try:
                records = fetch_rest_history(source, self.cfg, seconds_override=seconds)
            except Exception as exc:
                print(f"[warn] price fill failed for {source.normalized().display}: {exc}")
                continue
            if not records:
                continue
            updates[source.normalized().display] = records

        def _apply() -> None:
            self._apply_price_fill_updates(updates, seconds)

        try:
            self.root.after(0, _apply)
        except tk.TclError:
            pass

    def _apply_price_fill_updates(self, updates: Dict[str, List[Dict[str, Any]]], seconds: int) -> None:
        try:
            self.price_fill_button.state(["!disabled"])
        except Exception:
            pass
        self._price_fill_thread = None
        if not updates:
            return
        history_limit = max(self.cfg.history_seconds, seconds)
        if history_limit > self.cfg.history_seconds:
            self.cfg.history_seconds = history_limit
            self.history_seconds_var.set(self.cfg.history_seconds)
        for key, new_records in updates.items():
            existing = list(self.records_by_symbol.get(key, []))
            merged_map: Dict[pd.Timestamp, Dict[str, Any]] = {}
            for rec in existing:
                ts = OrderFlowMonitor._ensure_naive(rec["timestamp"])
                entry = dict(rec)
                entry["timestamp"] = ts
                merged_map[ts] = entry
            for rec in new_records:
                ts = OrderFlowMonitor._ensure_naive(rec.get("timestamp"))
                entry = merged_map.get(ts)
                if entry is None:
                    entry = dict(rec)
                else:
                    for field in ("mid", "delta", "delta_sum", "cvd", "ob_imbalance", "buy_vol", "sell_vol", "oi"):
                        if field not in rec:
                            continue
                        value = rec.get(field)
                        if value is None:
                            continue
                        if isinstance(value, float) and pd.isna(value):
                            continue
                        entry[field] = value
                entry["timestamp"] = ts
                merged_map[ts] = entry
            merged_records = [merged_map[ts] for ts in sorted(merged_map.keys())]
            clipped = clip_records_to_window(merged_records, history_limit, assume_sorted=True)
            self.records_by_symbol[key] = clipped
            if clipped:
                self._update_metrics(key, clipped[-1])
                self._update_charts(key)
        print(f"[info] price fill completed for timeframe {self.timeframe_var.get()} ({seconds}s)")

    def _update_chart_visibility(self) -> None:
        if not hasattr(self, "axes_price") or not hasattr(self, "canvas"):
            return

        target = getattr(self, "selected_pair_key", None)
        axes_keys = list(self.axes_price.keys())
        if target not in axes_keys:
            target = None

        for key in axes_keys:
            visible = target is None or key == target
            ax_price = self.axes_price[key]
            ax_cvd = self.axes_cvd[key]
            ax_imbal = self.axes_imbalance[key]
            ax_oi = self.axes_oi.get(key)

            for ax in (ax_price, ax_cvd, ax_imbal, ax_oi):
                if ax is None:
                    continue
                ax.set_visible(visible)
                ax.get_xaxis().set_visible(visible)
                ax.get_yaxis().set_visible(visible)
                ax.patch.set_visible(visible)
                for spine in ax.spines.values():
                    spine.set_visible(visible)

        self.canvas.draw_idle()

    def _update_hot_pairs_label(self) -> None:
        if not self.popular_pairs_timestamp:
            self.hot_pairs_update_label.configure(text="")
            return
        dt_local = self.popular_pairs_timestamp.astimezone(self.display_tz)
        self.hot_pairs_update_label.configure(text=f"{dt_local.strftime('%Y-%m-%d %H:%M')}")

    def _setup_plot(self) -> None:
        ncols = max(1, len(self.cfg.symbol_sources))
        figure = Figure(figsize=(11, 12), dpi=100)
        self.figure = figure
        self.axes_price: Dict[str, Any] = {}
        self.axes_cvd: Dict[str, Any] = {}
        self.axes_imbalance: Dict[str, Any] = {}
        self.axes_oi: Dict[str, Any] = {}

        time_formatter = mdates.DateFormatter("%H:%M:%S", tz=self.display_tz)
        for col, source in enumerate(self.cfg.symbol_sources):
            key = source.normalized().display
            _, symbol_display = self._extract_display_parts(key)

            ax_price = figure.add_subplot(4, ncols, col + 1)
            ax_price.set_title(f"{symbol_display} Price")
            ax_price.set_ylabel("Price")
            ax_price.grid(True, linestyle="--", alpha=0.3)
            ax_price.xaxis.set_major_formatter(time_formatter)
            ax_price.tick_params(axis='x', rotation=20)
            ax_price.tick_params(axis='x', labelbottom=False)
            self.axes_price[key] = ax_price

            ax_cvd = figure.add_subplot(4, ncols, ncols + col + 1, sharex=ax_price)
            ax_cvd.set_title(f"{symbol_display} CVD", color="#ef6c00")
            ax_cvd.set_ylabel("CVD", color="#ef6c00")
            ax_cvd.grid(True, linestyle="--", alpha=0.3)
            ax_cvd.xaxis.set_major_formatter(time_formatter)
            ax_cvd.tick_params(axis='x', rotation=20)
            ax_cvd.tick_params(axis='x', labelbottom=False)
            ax_cvd.tick_params(axis='y', colors="#ef6c00")
            ax_cvd.spines["left"].set_color("#ef6c00")
            self.axes_cvd[key] = ax_cvd

            ax_imbal = figure.add_subplot(4, ncols, (2 * ncols) + col + 1, sharex=ax_price)
            ax_imbal.set_title(f"{symbol_display} OB Imbalance", color="#8e24aa")
            ax_imbal.set_ylabel("盘口不平衡", color="#8e24aa", labelpad=14)
            ax_imbal.yaxis.set_label_position("left")
            ax_imbal.yaxis.set_ticks_position("left")
            ax_imbal.spines["left"].set_color("#8e24aa")
            ax_imbal.spines["right"].set_visible(True)
            ax_imbal.spines["right"].set_color("#212121")
            ax_imbal.spines["right"].set_linewidth(ax_imbal.spines["left"].get_linewidth())
            ax_imbal.grid(True, linestyle="--", alpha=0.3)
            ax_imbal.xaxis.set_major_formatter(time_formatter)
            ax_imbal.tick_params(axis='x', rotation=20, labelbottom=False)
            ax_imbal.tick_params(axis='y', colors="#8e24aa")
            self.axes_imbalance[key] = ax_imbal

            ax_oi = figure.add_subplot(4, ncols, (3 * ncols) + col + 1, sharex=ax_price)
            ax_oi.set_title(f"{symbol_display} Open Interest", color="#3949ab")
            ax_oi.set_ylabel("未平仓量", color="#3949ab", labelpad=14)
            ax_oi.grid(True, linestyle="--", alpha=0.3)
            ax_oi.xaxis.set_major_formatter(time_formatter)
            ax_oi.tick_params(axis='x', rotation=20)
            ax_oi.tick_params(axis='y', colors="#3949ab")
            ax_oi.spines["left"].set_color("#3949ab")
            self.axes_oi[key] = ax_oi

        self.canvas = FigureCanvasTkAgg(figure, master=self.chart_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.time_formatter = time_formatter
        self._update_chart_visibility()
    def apply_parameters(self) -> None:
        try:
            poll_interval = max(0.2, float(self.poll_interval_var.get()))
            delta_window = max(1, int(self.delta_window_var.get()))
            cvd_window = max(1, int(self.cvd_window_var.get()))
            history_seconds = max(60, int(self.history_seconds_var.get()))
            imbalance_smooth = max(1, int(self.imbalance_smooth_var.get()))
        except (TypeError, ValueError):
            print('[warn] 参数格式错误，保持原值')
            return

        self.cfg.poll_interval = poll_interval
        self.cfg.delta_window = delta_window
        self.cfg.cvd_window = cvd_window
        self.cfg.history_seconds = history_seconds
        self.cfg.imbalance_smooth_points = imbalance_smooth

        for key, headings in self.heading_labels.items():
            if 'delta_sum' in headings:
                headings['delta_sum'].config(
                    text=f'Δ Sum ({self.cfg.delta_window}s)\nDelta累积'
                )
            if 'cvd' in headings:
                headings['cvd'].config(
                    text=f'CVD ({self.cfg.cvd_window}s)\n累积Delta'
                )

        for key, history in self.records_by_symbol.items():
            clipped = clip_records_to_window(history, self.cfg.history_seconds, assume_sorted=True)
            self.records_by_symbol[key] = clipped
            if clipped:
                latest = clipped[-1]
                self._update_metrics(key, latest)
                self._update_charts(key)
            else:
                for var in self.metric_labels.get(key, {}).values():
                    var.set('--')

        cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(seconds=self.cfg.history_seconds)
        for key in list(self.records_by_symbol.keys()):
            self.records_by_symbol[key] = [
                r
                for r in self.records_by_symbol[key]
                if OrderFlowMonitor._ensure_naive(r['timestamp']) >= cutoff
            ]
            if self.records_by_symbol[key]:
                self._update_metrics(key, self.records_by_symbol[key][-1])
                self._update_charts(key)

        self._update_pair_selector()
        print(
            f"[info] 参数已更新: poll={self.cfg.poll_interval}s, "
            f"delta_window={self.cfg.delta_window}s, cvd_window={self.cfg.cvd_window}s, "
            f"history={self.cfg.history_seconds}s, ob_smooth={self.cfg.imbalance_smooth_points}, "
            f"strategy={'on' if self.cfg.strategy_enabled else 'off'}({self.cfg.strategy_lookback_seconds}s)"
        )

    def update_ui(self) -> None:
        if self._is_closing:
            return
        try:
            if not self.root.winfo_exists():
                return
        except tk.TclError:
            return
        updated_symbols: set[str] = set()
        try:
            while True:
                symbol_key, record = self.record_queue.get_nowait()
                self.records_by_symbol.setdefault(symbol_key, [])
                records = self.records_by_symbol[symbol_key]
                records.append(record)
    
                cutoff = OrderFlowMonitor._ensure_naive(record['timestamp']) - pd.Timedelta(
                    seconds=self.cfg.history_seconds
                )
                self.records_by_symbol[symbol_key] = [
                    r
                    for r in records
                    if OrderFlowMonitor._ensure_naive(r['timestamp']) >= cutoff
                ]
                updated_symbols.add(symbol_key)
        except queue.Empty:
            pass
    
        for symbol_key in updated_symbols:
            history = self.records_by_symbol[symbol_key]
            latest = history[-1]
    
            if latest.get('display_divergence') and latest.get('divergence_until'):
                if OrderFlowMonitor._ensure_naive(latest['timestamp']) > latest['divergence_until']:
                    latest['display_divergence'] = 'None'
            else:
                display = 'None'
                for rec in reversed(history):
                    if rec.get('divergence_until') and OrderFlowMonitor._ensure_naive(rec['timestamp']) <= rec['divergence_until']:
                        display = rec.get('divergence') or 'None'
                        break
                latest['display_divergence'] = display
    
            self._update_metrics(symbol_key, latest)
            self._update_charts(symbol_key)
    
        self.root.after(int(self.cfg.poll_interval * 1000), self.update_ui)
    
    def _update_metrics(self, symbol_key: str, record: Dict[str, Any]) -> None:
        labels = self.metric_labels.get(symbol_key)
        widgets = self.metric_widgets.get(symbol_key)
        if not labels or not widgets:
            return

        try:
            mid = record.get('mid')
            labels['mid'].set(f"{mid:.2f}" if isinstance(mid, (int, float)) else '--')

            delta = float(record.get('delta', 0.0) or 0.0)
            delta_sum = float(record.get('delta_sum', 0.0) or 0.0)
            cvd = float(record.get('cvd', 0.0) or 0.0)
            labels['delta'].set(f"{delta:.3f}")
            labels['delta_sum'].set(f"{delta_sum:.3f}")
            labels['cvd'].set(f"{cvd:.3f}")

            oi_value_raw = record.get('oi')
            if oi_value_raw is None and 'open_interest' in record:
                oi_value_raw = record.get('open_interest')
            oi_value = _safe_float(oi_value_raw)
            if oi_value is None:
                labels['oi'].set('--')
                if widgets.get('oi') and widgets['oi'].winfo_exists():
                    widgets['oi'].configure(fg='#37474f')
            else:
                labels['oi'].set(f"{oi_value:,.0f}")
                if widgets.get('oi') and widgets['oi'].winfo_exists():
                    widgets['oi'].configure(fg='#3949ab')

            ob_raw = record.get('ob_imbalance')
            ob = ob_raw
            smooth_window = max(1, self.cfg.imbalance_smooth_points)
            if smooth_window > 1:
                history = self.records_by_symbol.get(symbol_key, [])
                if history:
                    recent_values = [
                        float(rec.get('ob_imbalance', float('nan')))
                        for rec in history[-smooth_window:]
                    ]
                    series = pd.Series(recent_values, dtype="float64")
                    if not series.dropna().empty:
                        ob = float(series.mean(skipna=True))

            if ob is None or (isinstance(ob, float) and pd.isna(ob)):
                labels['ob'].set('NaN')
                if widgets['ob'].winfo_exists():
                    widgets['ob'].configure(fg='#607d8b')
            else:
                labels['ob'].set(f"{ob:.2f}")
                if widgets['ob'].winfo_exists():
                    if ob > 0.2:
                        widgets['ob'].configure(fg='#2e7d32')
                    elif ob < -0.2:
                        widgets['ob'].configure(fg='#c62828')
                    else:
                        widgets['ob'].configure(fg='#37474f')

            div = record.get('display_divergence') or record.get('divergence') or 'None'
            labels['divergence'].set(div)
            if widgets['divergence'].winfo_exists():
                if 'Bullish' in div:
                    widgets['divergence'].configure(fg='#2e7d32')
                elif 'Bearish' in div:
                    widgets['divergence'].configure(fg='#c62828')
                else:
                    widgets['divergence'].configure(fg='#37474f')

            strategy_signal = str(record.get('strategy_signal') or 'HOLD')
            signal_display = STRATEGY_SIGNAL_DISPLAY.get(strategy_signal, strategy_signal)
            strategy_conf = record.get('strategy_confidence')
            if isinstance(strategy_conf, (int, float)):
                display_strategy = f"{signal_display} ({strategy_conf * 100:.0f}%)"
            else:
                display_strategy = signal_display

            strategy_reason_raw = str(record.get('strategy_reason') or '').strip()
            reason_parts = [part.strip() for part in strategy_reason_raw.split(",") if part.strip()]
            translated_parts = [STRATEGY_REASON_DISPLAY.get(part, part) for part in reason_parts]
            if not translated_parts and strategy_reason_raw:
                translated_reason = STRATEGY_REASON_DISPLAY.get(strategy_reason_raw, strategy_reason_raw)
            else:
                translated_reason = "、".join(translated_parts)

            if (
                strategy_reason_raw
                and strategy_reason_raw not in STRATEGY_REASON_SKIP
                and translated_reason
            ):
                display_strategy = f"{display_strategy}\n{translated_reason}"

            labels['strategy'].set(display_strategy)
            if widgets.get('strategy') and widgets['strategy'].winfo_exists():
                color = '#37474f'
                if strategy_signal in {'LONG', 'LEAN_LONG'}:
                    color = '#1b5e20' if strategy_signal == 'LONG' else '#2e7d32'
                elif strategy_signal in {'SHORT', 'LEAN_SHORT'}:
                    color = '#b71c1c' if strategy_signal == 'SHORT' else '#c62828'
                elif strategy_signal == 'DISABLED':
                    color = '#78909c'
                widgets['strategy'].configure(fg=color)

            if widgets['delta'].winfo_exists():
                if delta > 0:
                    widgets['delta'].configure(fg='#2e7d32')
                elif delta < 0:
                    widgets['delta'].configure(fg='#c62828')
                else:
                    widgets['delta'].configure(fg='#37474f')

        except tk.TclError:
            return
    
    def _update_charts(self, symbol_key: str) -> None:
        records = self.records_by_symbol.get(symbol_key, [])
        if not records:
            return

        exchange_upper, symbol_display = self._extract_display_parts(symbol_key)

        times = [self._to_display_timestamp(r['timestamp']) for r in records]
        price = [float(r.get('mid', float('nan'))) for r in records]
        cvd = [float(r.get('cvd', 0.0) or 0.0) for r in records]
        imbalance = [
            0.0 if pd.isna(r.get('ob_imbalance')) else float(r.get('ob_imbalance', 0.0))
            for r in records
        ]
        oi_values: List[float] = []
        for r in records:
            oi_raw = r.get('oi')
            if oi_raw is None and 'open_interest' in r:
                oi_raw = r.get('open_interest')
            oi_val = _safe_float(oi_raw)
            oi_values.append(oi_val if oi_val is not None else float('nan'))
        smooth_window = max(1, self.cfg.imbalance_smooth_points)
        if smooth_window > 1:
            series = pd.Series(imbalance, dtype="float64")
            imbalance = series.rolling(window=smooth_window, min_periods=1).mean().tolist()

        window_start: Optional[pd.Timestamp] = None
        if times:
            window_end = times[-1]
            span_minutes = max(1, self.selected_timeframe_minutes)
            candidate_start = window_end - pd.Timedelta(minutes=span_minutes)
            filtered_samples = [
                (ts, pr, cv, im, oi)
                for ts, pr, cv, im, oi in zip(times, price, cvd, imbalance, oi_values)
                if ts >= candidate_start
            ]
            if filtered_samples:
                filtered_times, filtered_price, filtered_cvd, filtered_imbal, filtered_oi = zip(
                    *filtered_samples
                )
                times = list(filtered_times)
                price = list(filtered_price)
                cvd = list(filtered_cvd)
                imbalance = list(filtered_imbal)
                oi_values = list(filtered_oi)
                window_start = candidate_start

        ax_price = self.axes_price[symbol_key]
        ax_cvd = self.axes_cvd[symbol_key]
        ax_imbal = self.axes_imbalance[symbol_key]
        ax_oi = self.axes_oi[symbol_key]

        for axis in (ax_price, ax_cvd, ax_imbal, ax_oi):
            axis.clear()

        ax_price.plot(times, price, color='#1976d2', label='Price')
        ax_price.set_title(f"{symbol_display} Price")
        ax_price.set_ylabel('Price')
        ax_price.grid(True, linestyle='--', alpha=0.3)
        ax_price.xaxis.set_major_formatter(self.time_formatter)
        ax_price.tick_params(axis='x', rotation=20, labelbottom=False)
        ax_price.ticklabel_format(style='plain', useOffset=False, axis='y')
        ax_price.get_yaxis().get_major_formatter().set_scientific(False)

        ax_cvd.plot(times, cvd, color='#ef6c00', label='CVD', linewidth=1.6, zorder=5)
        ax_cvd.set_ylabel('CVD', color='#ef6c00')
        ax_cvd.grid(True, linestyle='--', alpha=0.3)
        ax_cvd.xaxis.set_major_formatter(self.time_formatter)
        ax_cvd.tick_params(axis='x', rotation=20, labelbottom=False)
        ax_cvd.tick_params(axis='y', colors='#ef6c00')
        ax_cvd.spines['left'].set_color('#ef6c00')
        ax_cvd.axhline(0, color='#ef6c00', linewidth=1, linestyle='--', alpha=0.4, zorder=4)

        ax_imbal.plot(
            times,
            imbalance,
            color='#8e24aa',
            label='OB Imbalance',
            linewidth=1.3,
            alpha=0.85,
            zorder=3,
        )
        ax_imbal.axhline(0, color='#8e24aa', linewidth=1, linestyle='--', alpha=0.5, zorder=2)
        ax_imbal.set_ylabel('盘口不平衡', color='#8e24aa', labelpad=14)
        ax_imbal.yaxis.set_label_position('left')
        ax_imbal.yaxis.set_ticks_position('left')
        ax_imbal.spines['left'].set_color('#8e24aa')
        ax_imbal.spines['right'].set_visible(True)
        ax_imbal.spines['right'].set_color('#212121')
        ax_imbal.spines['right'].set_linewidth(ax_imbal.spines['left'].get_linewidth())
        ax_imbal.grid(True, linestyle='--', alpha=0.3)
        ax_imbal.xaxis.set_major_formatter(self.time_formatter)
        ax_imbal.tick_params(axis='x', rotation=20, labelbottom=False)
        ax_imbal.tick_params(axis='y', colors='#8e24aa')

        ax_oi.set_ylabel('未平仓量', color='#3949ab', labelpad=14)
        ax_oi.yaxis.set_label_position('left')
        ax_oi.yaxis.set_ticks_position('left')
        ax_oi.spines['left'].set_color('#3949ab')
        ax_oi.grid(True, linestyle='--', alpha=0.3)
        ax_oi.xaxis.set_major_formatter(self.time_formatter)
        ax_oi.tick_params(axis='x', rotation=20)
        ax_oi.tick_params(axis='y', colors='#3949ab')

        oi_series = [val for val in oi_values if isinstance(val, (int, float)) and not pd.isna(val)]
        if oi_series:
            ax_oi.plot(
                times,
                oi_values,
                color='#3949ab',
                label='Open Interest',
                linewidth=1.4,
                alpha=0.9,
                zorder=3,
            )
            ax_oi.ticklabel_format(style='plain', useOffset=False, axis='y')
        else:
            ax_oi.set_ylim(0, 1)
            ax_oi.set_yticks([])
            ax_oi.text(
                0.5,
                0.5,
                "暂无未平仓量数据",
                transform=ax_oi.transAxes,
                ha='center',
                va='center',
                color='#3949ab',
                alpha=0.6,
            )
        ax_oi.set_ylabel('未平仓量', color='#3949ab', labelpad=14)
        ax_oi.yaxis.set_label_position('left')
        ax_oi.yaxis.set_ticks_position('left')
        ax_oi.spines['left'].set_color('#3949ab')
        ax_oi.grid(True, linestyle='--', alpha=0.3)
        ax_oi.xaxis.set_major_formatter(self.time_formatter)
        ax_oi.tick_params(axis='x', rotation=20)
        ax_oi.tick_params(axis='y', colors='#3949ab')
        ax_oi.ticklabel_format(style='plain', useOffset=False, axis='y')
        price_values = [p for p in price if not pd.isna(p)]
        price_range = max(price_values) - min(price_values) if price_values else 0.0
        base_price = price_values[-1] if price_values else 1.0
        offset = max(price_range * 0.02, abs(base_price) * 0.0005, 0.5)
        min_offset = max(abs(offset), 1e-6)

        span_styles: Dict[str, Tuple[str, float]] = {
            "LONG": ("#c8e6c9", 0.35),
            "LEAN_LONG": ("#e8f5e9", 0.25),
            "SHORT": ("#ffcdd2", 0.35),
            "LEAN_SHORT": ("#ffebee", 0.25),
        }
        latest_signal = records[-1].get('strategy_signal') if records else None
        if latest_signal in span_styles and times:
            fill_color, fill_alpha = span_styles[latest_signal]
            latest_ts = OrderFlowMonitor._ensure_naive(records[-1]['timestamp'])
            fill_start_ts = latest_ts - pd.Timedelta(seconds=self.cfg.strategy_lookback_seconds)
            fill_start_dt = self._to_display_timestamp(fill_start_ts)
            fill_end_dt = self._to_display_timestamp(latest_ts)
            fill_start_dt = max(fill_start_dt, times[0])
            fill_end_dt = max(fill_end_dt, fill_start_dt)
            for axis in (ax_price, ax_cvd, ax_imbal, ax_oi):
                axis.axvspan(fill_start_dt, fill_end_dt, color=fill_color, alpha=fill_alpha, zorder=0)
    
        cutoff_time = (
            times[-1] - pd.Timedelta(seconds=60) if times else self._to_display_timestamp(pd.Timestamp.utcnow())
        )
        drawn_minutes: Set[pd.Timestamp] = set()
        for rec in records:
            div_label = rec.get('display_divergence') or rec.get('divergence')
            if not div_label or div_label == 'None':
                continue
            ts = self._to_display_timestamp(rec['timestamp'])
            if window_start is not None and ts < window_start:
                continue
            if ts < cutoff_time:
                continue
            until = rec.get('divergence_until')
            until_ts = self._to_display_timestamp(until) if until else None
            if until_ts and ts > until_ts:
                continue
            price_y = rec.get('mid')
            if price_y is None or pd.isna(price_y):
                continue

            minute_key = ts.floor('min')
            if minute_key in drawn_minutes:
                continue

            if 'Bullish' in div_label:
                ax_price.scatter(
                    ts,
                    float(price_y) - min_offset * 1.1,
                    marker='^',
                    s=120,
                    color='#2e7d32',
                    edgecolors='white',
                    linewidths=0.6,
                    zorder=6,
                )
                drawn_minutes.add(minute_key)
            elif 'Bearish' in div_label:
                ax_price.scatter(
                    ts,
                    float(price_y) + min_offset * 1.1,
                    marker='v',
                    s=120,
                    color='#d32f2f',
                    edgecolors='white',
                    linewidths=0.6,
                    zorder=6,
                )
                drawn_minutes.add(minute_key)

        if len(times) > 1:
            ax_price.set_xlim(times[0], times[-1])
            ax_cvd.set_xlim(times[0], times[-1])
            ax_imbal.set_xlim(times[0], times[-1])
            ax_oi.set_xlim(times[0], times[-1])
        self.figure.tight_layout()
        self.canvas.draw_idle()

    def _align_zero_axes(self, ax_primary: Any, ax_secondary: Any, secondary_values: List[float]) -> None:
        try:
            lower_primary, upper_primary = ax_primary.get_ylim()
        except Exception:
            return
        span_primary = upper_primary - lower_primary
        if span_primary <= 0 or not (lower_primary < 0 < upper_primary):
            return
        zero_ratio = (0 - lower_primary) / span_primary
        if zero_ratio <= 1e-3 or zero_ratio >= 1 - 1e-3:
            # Avoid extreme ratios that would explode scaling
            return

        ratio = zero_ratio / (1 - zero_ratio)

        try:
            sec_lower, sec_upper = ax_secondary.get_ylim()
        except Exception:
            return

        values = [sec_lower, sec_upper, 0.0]
        for val in secondary_values:
            if isinstance(val, (int, float)):
                values.append(float(val))
        required_lower = min(values)
        required_upper = max(values)

        if required_lower >= 0:
            required_lower = -1e-6
        if required_upper <= 0:
            required_upper = 1e-6

        upper_candidate = max(required_upper, (-required_lower) / ratio)
        upper_candidate = max(upper_candidate, 1e-6)
        lower_candidate = -ratio * upper_candidate
        if lower_candidate > required_lower:
            needed = -required_lower
            upper_candidate = max(upper_candidate, needed / ratio)
            lower_candidate = -ratio * upper_candidate

        ax_secondary.set_ylim(lower_candidate, upper_candidate)

    def _to_display_timestamp(self, value: Any) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(UTC_TZ)
        return ts.tz_convert(self.display_tz)

    def _format_display_time(self, value: Any) -> str:
        return self._to_display_timestamp(value).strftime('%H:%M:%S')

    # ------------------------------------------------------------------ Shutdown
    def on_close(self) -> None:
        self._is_closing = True
        if self.stop_callback is not None:
            try:
                self.stop_callback()
            except Exception:
                pass
        self.root.quit()
        self.root.destroy()

    def run(self) -> None:
        self.root.mainloop()



def parse_args() -> MonitorConfig:
    parser = argparse.ArgumentParser(description="Binance US / Bybit order-flow GUI")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Symbols as exchange:symbol (e.g. binanceus:btcusdt, bybit:btcusdt)."
             " Default: binanceus:btcusdt binanceus:ethusdt",
    )
    parser.add_argument("--poll-interval", type=float, default=None, help="Refresh interval in seconds")
    parser.add_argument("--history-seconds", type=int, default=None, help="Chart history in seconds")
    parser.add_argument("--delta-window", type=int, default=None, help="Delta rolling window (seconds)")
    parser.add_argument("--cvd-window", type=int, default=None, help="CVD rolling window (seconds)")
    parser.add_argument(
        "--imbalance-smooth",
        type=int,
        default=None,
        help="Order-book imbalance smoothing window (points, 1=off)",
    )
    parser.add_argument("--divergence-window", type=int, default=None, help="Divergence window (seconds)")
    parser.add_argument("--strategy-disabled", action="store_true", help="Disable strategy overlay integration")
    parser.add_argument(
        "--strategy-lookback",
        type=int,
        default=None,
        help="Strategy evaluation window in seconds (default: 120)",
    )
    parser.add_argument(
        "--strategy-delta-threshold",
        type=float,
        default=None,
        help="Delta accumulation threshold for strategy bias",
    )
    parser.add_argument(
        "--strategy-long-imbalance",
        type=float,
        default=None,
        help="Minimum average order-book imbalance to favour long bias",
    )
    parser.add_argument(
        "--strategy-short-imbalance",
        type=float,
        default=None,
        help="Maximum average order-book imbalance to favour short bias",
    )
    parser.add_argument(
        "--strategy-cvd-slope",
        type=float,
        default=None,
        help="Minimum CVD slope (per second) for strategy confirmation",
    )
    parser.add_argument(
        "--strategy-min-score",
        type=float,
        default=None,
        help="Minimum confidence score (0-1) required before issuing LONG/SHORT",
    )
    parser.add_argument("--influx-url", type=str, default=None, help="InfluxDB base URL (e.g. http://localhost:8086)")
    parser.add_argument("--influx-token", type=str, default=None, help="InfluxDB API token")
    parser.add_argument("--influx-org", type=str, default=None, help="InfluxDB organisation")
    parser.add_argument("--influx-bucket", type=str, default=None, help="InfluxDB bucket")
    parser.add_argument(
        "--influx-measurement", type=str, default=None, help="Measurement name (default: orderflow)"
    )
    parser.add_argument(
        "--startup-backfill",
        type=int,
        default=None,
        help="Seconds of synthetic backfill when no history exists (default: 900)",
    )
    parser.add_argument(
        "--history-cache",
        type=str,
        default=None,
        help="Path to local JSON cache for aggregated history (default: output/history_cache.json)",
    )
    args = parser.parse_args()

    cfg = MonitorConfig()
    if app_config is not None:
        url = getattr(app_config, "INFLUX_URL", None)
        token = getattr(app_config, "INFLUX_TOKEN", None)
        org = getattr(app_config, "INFLUX_ORG", None)
        bucket = getattr(app_config, "INFLUX_BUCKET", None)
        measurement = getattr(app_config, "INFLUX_MEASUREMENT", None)
        history_default = getattr(app_config, "DEFAULT_HISTORY_SECONDS", None)
        imbalance_default = getattr(app_config, "DEFAULT_IMBALANCE_SMOOTH_POINTS", None)

        if url:
            cfg.influx_url = url
        if token:
            cfg.influx_token = token
        if org:
            cfg.influx_org = org
        if bucket:
            cfg.influx_bucket = bucket
        if measurement:
            cfg.influx_measurement = measurement
        if history_default:
            try:
                cfg.history_seconds = max(60, int(history_default))
            except (TypeError, ValueError):
                pass
        if imbalance_default:
            try:
                cfg.imbalance_smooth_points = max(1, int(imbalance_default))
            except (TypeError, ValueError):
                pass
        strategy_enabled_default = getattr(app_config, "DEFAULT_STRATEGY_ENABLED", None)
        if strategy_enabled_default is not None:
            cfg.strategy_enabled = bool(strategy_enabled_default)
        strategy_lookback_default = getattr(app_config, "DEFAULT_STRATEGY_LOOKBACK_SECONDS", None)
        if strategy_lookback_default is not None:
            try:
                cfg.strategy_lookback_seconds = max(30, int(strategy_lookback_default))
            except (TypeError, ValueError):
                pass
        strategy_delta_default = getattr(app_config, "DEFAULT_STRATEGY_DELTA_THRESHOLD", None)
        if strategy_delta_default is not None:
            try:
                cfg.strategy_delta_threshold = float(strategy_delta_default)
            except (TypeError, ValueError):
                pass
        strategy_long_imbalance_default = getattr(app_config, "DEFAULT_STRATEGY_LONG_IMBALANCE", None)
        if strategy_long_imbalance_default is not None:
            try:
                cfg.strategy_long_imbalance = float(strategy_long_imbalance_default)
            except (TypeError, ValueError):
                pass
        strategy_short_imbalance_default = getattr(app_config, "DEFAULT_STRATEGY_SHORT_IMBALANCE", None)
        if strategy_short_imbalance_default is not None:
            try:
                cfg.strategy_short_imbalance = float(strategy_short_imbalance_default)
            except (TypeError, ValueError):
                pass
        strategy_slope_default = getattr(app_config, "DEFAULT_STRATEGY_CVD_SLOPE_THRESHOLD", None)
        if strategy_slope_default is not None:
            try:
                cfg.strategy_cvd_slope_threshold = float(strategy_slope_default)
            except (TypeError, ValueError):
                pass
        strategy_min_score_default = getattr(app_config, "DEFAULT_STRATEGY_MIN_SIGNAL_SCORE", None)
        if strategy_min_score_default is not None:
            try:
                cfg.strategy_min_signal_score = min(max(float(strategy_min_score_default), 0.0), 1.0)
            except (TypeError, ValueError):
                pass
        poll_default = getattr(app_config, "DEFAULT_POLL_INTERVAL", None)
        if poll_default is not None:
            try:
                cfg.poll_interval = max(0.2, float(poll_default))
            except (TypeError, ValueError):
                pass
        delta_default = getattr(app_config, "DEFAULT_DELTA_WINDOW", None)
        if delta_default is not None:
            try:
                cfg.delta_window = max(1, int(delta_default))
            except (TypeError, ValueError):
                pass
        cvd_default = getattr(app_config, "DEFAULT_CVD_WINDOW", None)
        if cvd_default is not None:
            try:
                cfg.cvd_window = max(1, int(cvd_default))
            except (TypeError, ValueError):
                pass
        divergence_default = getattr(app_config, "DEFAULT_DIVERGENCE_WINDOW", None)
        if divergence_default is not None:
            try:
                cfg.divergence_window = max(10, int(divergence_default))
            except (TypeError, ValueError):
                pass
        backfill_default = getattr(app_config, "DEFAULT_STARTUP_BACKFILL_SECONDS", None)
        if backfill_default is not None:
            try:
                cfg.startup_backfill_seconds = max(0, int(backfill_default))
            except (TypeError, ValueError):
                pass
        history_cache_default = getattr(app_config, "HISTORY_CACHE_PATH", None)
        if history_cache_default is not None:
            cfg.history_cache_path = str(history_cache_default)
        symbols_default = getattr(app_config, "DEFAULT_SYMBOLS", None)
        if symbols_default:
            loaded_sources: List[SymbolSource] = []
            for token in symbols_default:
                try:
                    loaded_sources.append(parse_symbol_token(token))
                except ValueError as exc:
                    print(f"[warn] 无法解析配置中的交易对 {token!r}: {exc}")
            if loaded_sources:
                cfg.symbol_sources = loaded_sources
    if args.symbols:
        cli_sources: List[SymbolSource] = []
        for token in args.symbols:
            try:
                cli_sources.append(parse_symbol_token(token))
            except ValueError as exc:
                print(f"[warn] 无法解析命令行交易对 {token!r}: {exc}")
        if cli_sources:
            cfg.symbol_sources = cli_sources
    if args.poll_interval is not None:
        cfg.poll_interval = max(0.2, args.poll_interval)
    if args.history_seconds is not None:
        cfg.history_seconds = max(120, args.history_seconds)
    if args.delta_window is not None:
        cfg.delta_window = max(1, args.delta_window)
    if args.cvd_window is not None:
        cfg.cvd_window = max(1, args.cvd_window)
    if args.imbalance_smooth is not None:
        cfg.imbalance_smooth_points = max(1, args.imbalance_smooth)
    if args.divergence_window is not None:
        cfg.divergence_window = max(10, args.divergence_window)
    if args.strategy_disabled:
        cfg.strategy_enabled = False
    if args.strategy_lookback is not None:
        cfg.strategy_lookback_seconds = max(30, args.strategy_lookback)
    if args.strategy_delta_threshold is not None:
        cfg.strategy_delta_threshold = float(args.strategy_delta_threshold)
    if args.strategy_long_imbalance is not None:
        cfg.strategy_long_imbalance = float(args.strategy_long_imbalance)
    if args.strategy_short_imbalance is not None:
        cfg.strategy_short_imbalance = float(args.strategy_short_imbalance)
    if args.strategy_cvd_slope is not None:
        cfg.strategy_cvd_slope_threshold = float(args.strategy_cvd_slope)
    if args.strategy_min_score is not None:
        cfg.strategy_min_signal_score = min(max(float(args.strategy_min_score), 0.0), 1.0)
    if args.startup_backfill is not None:
        cfg.startup_backfill_seconds = max(0, args.startup_backfill)
    if args.history_cache is not None:
        cfg.history_cache_path = args.history_cache
    if args.influx_url is not None:
        cfg.influx_url = args.influx_url
    if args.influx_token is not None:
        cfg.influx_token = args.influx_token
    if args.influx_org is not None:
        cfg.influx_org = args.influx_org
    if args.influx_bucket is not None:
        cfg.influx_bucket = args.influx_bucket
    if args.influx_measurement is not None:
        cfg.influx_measurement = args.influx_measurement
    return cfg



def main() -> None:
    cfg = parse_args()
    chosen_font = configure_chinese_font()
    record_queue: "queue.Queue[Tuple[str, Dict[str, Any]]]" = queue.Queue(maxsize=5000)

    influx_publisher: Optional[InfluxPublisher] = None
    influx_params = [cfg.influx_url, cfg.influx_token, cfg.influx_org, cfg.influx_bucket]
    if any(influx_params):
        if all(influx_params):
            try:
                influx_publisher = InfluxPublisher(cfg)
                print(
                    f"[info] InfluxDB writer enabled -> {cfg.influx_url} "
                    f"bucket={cfg.influx_bucket}, org={cfg.influx_org}"
                )
            except Exception as exc:
                print(f"[warn] failed to initialise InfluxDB writer: {exc}")
                influx_publisher = None
        else:
            print("[warn] InfluxDB parameters incomplete; skipping export.")

    history_cache = HistoryCache(
        cfg.history_cache_path,
        max(cfg.history_seconds, cfg.startup_backfill_seconds),
    )

    initial_history: Dict[str, List[Dict[str, Any]]] = {}
    if influx_publisher is not None:
        fetch_seconds = max(cfg.history_seconds, 3600)
        for source in cfg.symbol_sources:
            history = influx_publisher.fetch_recent(source, fetch_seconds)
            if history:
                initial_history[source.normalized().display] = history
    local_history = history_cache.load_all(cfg.symbol_sources)
    for key, records in local_history.items():
        initial_history.setdefault(key, records)
    for source in cfg.symbol_sources:
        key = source.normalized().display
        if key in initial_history:
            continue
        rest_records = fetch_rest_history(source, cfg)
        if rest_records:
            initial_history[key] = rest_records

    monitors: List[OrderFlowMonitor] = []
    for source in cfg.symbol_sources:
        monitor = OrderFlowMonitor(
            source,
            cfg,
            record_queue,
            publisher=influx_publisher,
            history_cache=history_cache,
        )
        key = source.normalized().display
        if key in initial_history:
            monitor.seed_history(initial_history[key])
        monitor.start()
        monitors.append(monitor)

    def stop_monitors() -> None:
        for m in monitors:
            m.stop()
        if influx_publisher is not None:
            influx_publisher.close()
        history_cache.flush()

    gui = MonitorGUI(
        cfg,
        record_queue,
        stop_callback=stop_monitors,
        preferred_font=chosen_font,
        initial_history=initial_history,
    )

    def handle_exit(_signum: int, _frame) -> None:
        gui.on_close()

    signal.signal(signal.SIGINT, handle_exit)
    signal.signal(signal.SIGTERM, handle_exit)

    try:
        gui.run()
    finally:
        stop_monitors()
        time.sleep(0.5)


if __name__ == "__main__":
    main()
