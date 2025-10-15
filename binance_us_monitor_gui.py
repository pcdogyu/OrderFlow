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
import queue
import signal
import threading
import time
from zoneinfo import ZoneInfo

DISPLAY_TZ = ZoneInfo("Asia/Shanghai")
UTC_TZ = ZoneInfo("UTC")
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

import pandas as pd
import websocket

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
            SymbolSource("binanceus", "btcusdt"),
            SymbolSource("binanceus", "ethusdt"),
        ]
    )
    poll_interval: float = 0.5  # seconds
    history_seconds: int = 3600  # 60 minutes
    delta_window: int = 60
    cvd_window: int = 60
    imbalance_smooth_points: int = 5
    divergence_window: int = 300
    price_tolerance: float = 0.0005
    cvd_tolerance: float = 0.25
    influx_url: Optional[str] = None
    influx_token: Optional[str] = None
    influx_org: Optional[str] = None
    influx_bucket: Optional[str] = None
    influx_measurement: str = "orderflow"


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
        for field in ("mid", "delta", "delta_sum", "cvd", "ob_imbalance", "buy_vol", "sell_vol"):
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

        self.coinbase_product_id: Optional[str] = None
        if self.exchange == "coinbase":
            try:
                _, normalized_symbol = normalize_exchange_symbol(self.exchange, self.symbol)
            except ValueError as exc:
                raise ValueError(f"Invalid Coinbase symbol '{self.symbol}'") from exc
            self.coinbase_product_id = normalized_symbol.upper()

        self.ticks: Deque[Tuple[pd.Timestamp, float, float, int]] = deque()
        self.book: Deque[Tuple[pd.Timestamp, float, float, float, float]] = deque()
        self.stats: Deque[Dict[str, Any]] = deque()

        self.delta_window_values: Deque[Tuple[pd.Timestamp, float]] = deque()
        self.cvd_window_values: Deque[Tuple[pd.Timestamp, float]] = deque()
        self.delta_sum_value: float = 0.0
        self.cvd_value: float = 0.0

        self.ws_app: Optional[websocket.WebSocketApp] = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()

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
        return "wss://advanced-trade-ws.coinbase.com"

    def _bybit_subscribe(self, ws: websocket.WebSocketApp) -> None:
        args = [
            f"trade.{self.symbol_upper}",
            f"orderbook.1.{self.symbol_upper}",
        ]
        ws.send(json.dumps({"op": "subscribe", "args": args}))

    def _coinbase_subscribe(self, ws: websocket.WebSocketApp) -> None:
        if not self.coinbase_product_id:
            return
        payload = {
            "type": "subscribe",
            "channels": [
                {"name": "market_trades", "product_ids": [self.coinbase_product_id]},
                {"name": "ticker", "product_ids": [self.coinbase_product_id]},
            ],
        }
        ws.send(json.dumps(payload))

    def on_message(self, ws: websocket.WebSocketApp, message: str) -> None:
        payload = json.loads(message)
        if self.exchange == "bybit":
            self._handle_bybit_message(payload)
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

    def _handle_coinbase_message(self, payload: Dict[str, Any]) -> None:
        msg_type = payload.get("type")
        product_id = payload.get("product_id") or payload.get("productId")
        if product_id and self.coinbase_product_id and product_id.upper() != self.coinbase_product_id:
            return
        if msg_type in (None, "subscriptions", "heartbeat"):
            return
        if msg_type == "error":
            print(f"[{self.symbol_key}] coinbase error: {payload}")
            return
        now = self._ensure_naive(pd.Timestamp.utcnow())
        if msg_type == "market_trades":
            trades = payload.get("trades") or []
            for trade in trades:
                if not trade:
                    continue
                try:
                    price = float(trade.get("price"))
                    qty = float(trade.get("size") or trade.get("quantity"))
                except (TypeError, ValueError):
                    continue
                ts_raw = (
                    trade.get("trade_time")
                    or trade.get("time")
                    or trade.get("ts")
                    or payload.get("time")
                )
                ts = self._ensure_naive(pd.Timestamp(ts_raw)) if ts_raw else now
                side = str(trade.get("side") or trade.get("taker_side") or "").lower()
                direction = 1 if side != "sell" else -1
                with self.lock:
                    self.ticks.append((ts, price, qty, direction))
        elif msg_type == "ticker":
            try:
                bid_price = float(payload.get("best_bid") or payload.get("bid_price"))
                ask_price = float(payload.get("best_ask") or payload.get("ask_price"))
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
            ts_raw = payload.get("time") or payload.get("ts")
            ts = self._ensure_naive(pd.Timestamp(ts_raw)) if ts_raw else now
            with self.lock:
                self.book.append((ts, bid_price, bid_qty, ask_price, ask_qty))

    def on_error(self, _: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[{self.symbol_key}] error: {error}")

    def on_close(self, *_: object) -> None:
        print(f"[{self.symbol_key}] websocket closed")

    def run_ws(self) -> None:
        if self.exchange == "bybit":
            url = self._bybit_url()
            self.ws_app = websocket.WebSocketApp(
                url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            self.ws_app.on_open = self._bybit_subscribe
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
            url = self._binance_url()
            self.ws_app = websocket.WebSocketApp(
                url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
        while not self.stop_event.is_set():
            try:
                self.ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as exc:
                print(f"[{self.symbol_key}] reconnecting websocket ({exc})")
                time.sleep(2)
            else:
                break

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
        self.stats.append(record)
        while self.stats and (
            now - self._ensure_naive(self.stats[0]["timestamp"])
        ).total_seconds() > self.cfg.history_seconds:
            self.stats.popleft()
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

        self._build_widgets()
        self._setup_plot()

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

        control_frame = ttk.LabelFrame(mainframe, text="参数设置 Parameters", padding="6")
        control_frame.pack(fill=tk.X, pady=(0, 6))

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

        for col in range(5):
            control_frame.columnconfigure(col, weight=0)

        add_control(0, "刷新周期(s)", self.poll_interval_var, 0.2, 5.0, 0.1, "%.2f")
        add_control(1, "Delta窗口(s)", self.delta_window_var, 5, 600, 1, "%d")
        add_control(2, "CVD窗口(s)", self.cvd_window_var, 10, 900, 1, "%d")
        add_control(3, "图表秒数", self.history_seconds_var, 60, 3600, 30, "%d")
        add_control(4, "OB平滑点数", self.imbalance_smooth_var, 1, 200, 1, "%d")

        ttk.Button(control_frame, text="应用 Apply", command=self.apply_parameters).grid(
            row=0, column=5, padx=8, pady=2
        )

        metrics_frame = ttk.LabelFrame(mainframe, text="实时指标 Live Metrics", padding="6")
        metrics_frame.pack(fill=tk.X)

        self.metric_labels: Dict[str, Dict[str, tk.StringVar]] = {}
        self.metric_widgets: Dict[str, Dict[str, tk.Label]] = {}
        self.heading_labels: Dict[str, Dict[str, ttk.Label]] = {}

        selector_frame = ttk.Frame(metrics_frame)
        selector_frame.pack(fill=tk.X, pady=(0, 6))
        self.exchange_filter_var = tk.StringVar(value="全部")
        self.symbol_filter_var = tk.StringVar(value="全部")
        ttk.Label(selector_frame, text="交易所").grid(row=0, column=0, padx=(0, 4))
        self.exchange_selector = ttk.Combobox(
            selector_frame,
            textvariable=self.exchange_filter_var,
            state="readonly",
            width=12,
        )
        self.exchange_selector.grid(row=0, column=1, padx=(0, 12))
        self.exchange_selector.bind("<<ComboboxSelected>>", lambda _: self._on_exchange_filter_change())
        ttk.Label(selector_frame, text="币对").grid(row=0, column=2, padx=(0, 4))
        self.symbol_selector = ttk.Combobox(
            selector_frame,
            textvariable=self.symbol_filter_var,
            state="readonly",
            width=14,
        )
        self.symbol_selector.grid(row=0, column=3, padx=(0, 12))
        self.symbol_selector.bind("<<ComboboxSelected>>", lambda _: self._on_symbol_filter_change())
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
            block = ttk.LabelFrame(container, text=key, padding="4")
            block.grid(row=0, column=idx, sticky="nsew", padx=4, pady=2)
            self.symbol_frames[key] = block

            grid = ttk.Frame(block)
            grid.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)
            for col in range(7):
                grid.columnconfigure(col, weight=1)

            vars_map = {
                "time": tk.StringVar(value="--:--:--"),
                "mid": tk.StringVar(value="--"),
                "delta": tk.StringVar(value="--"),
                "delta_sum": tk.StringVar(value="--"),
                "cvd": tk.StringVar(value="--"),
                "ob": tk.StringVar(value="--"),
                "divergence": tk.StringVar(value="None"),
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
                lbl = tk.Label(cell, textvariable=vars_map[label_key], font=self.value_font)
                lbl.pack(anchor=tk.W)
                label_widgets[label_key] = lbl

            add_item(0, "Time", "时间", "time")
            add_item(1, "Price", "价格", "mid")
            add_item(2, "Delta", "净成交量", "delta")
            add_item(3, f"Δ Sum ({self.cfg.delta_window}s)", "Delta累积", "delta_sum")
            add_item(4, f"CVD ({self.cfg.cvd_window}s)", "累积Delta", "cvd")
            add_item(5, "OB Imbalance", "盘口不平衡", "ob")
            add_item(6, "Divergence", "背离信号", "divergence")

            self.metric_widgets[key] = label_widgets

        self._refresh_symbol_selector()

        chart_frame = ttk.LabelFrame(mainframe, text="15分钟滚动图 Realtime Charts", padding="6")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=6)
        self.chart_frame = chart_frame

    def _refresh_symbol_selector(self) -> None:
        keys = [source.normalized().display for source in self.cfg.symbol_sources]
        exchanges = sorted({key.split(":", 1)[0] for key in keys})
        exchange_values = ["全部"] + exchanges
        self.exchange_selector["values"] = exchange_values
        if self.exchange_filter_var.get() not in exchange_values:
            self.exchange_filter_var.set(exchange_values[0] if exchange_values else "全部")
        self._update_symbol_options()

    def _on_exchange_filter_change(self) -> None:
        self._update_symbol_options()

    def _update_symbol_options(self) -> None:
        exchange = self.exchange_filter_var.get()
        keys = list(self.symbol_frames.keys())
        if exchange not in ("全部", "", None):
            keys = [key for key in keys if key.split(":", 1)[0] == exchange]
        symbols = sorted({key.split(":", 1)[1] for key in keys})
        symbol_values = ["全部"] + symbols
        self.symbol_selector["values"] = symbol_values
        if self.symbol_filter_var.get() not in symbol_values:
            self.symbol_filter_var.set(symbol_values[0] if symbol_values else "全部")
        self._apply_symbol_filter()

    def _on_symbol_filter_change(self) -> None:
        self._apply_symbol_filter()

    def _apply_symbol_filter(self) -> None:
        exchange = self.exchange_filter_var.get()
        symbol = self.symbol_filter_var.get()
        keys = list(self.symbol_frames.keys())
        filtered: List[str] = []
        for key in keys:
            key_exchange, key_symbol = key.split(":", 1)
            if exchange not in ("全部", "", None) and key_exchange != exchange:
                continue
            if symbol not in ("全部", "", None) and key_symbol != symbol:
                continue
            filtered.append(key)
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

    def _setup_plot(self) -> None:
        ncols = max(1, len(self.cfg.symbol_sources))
        figure = Figure(figsize=(11, 7.8), dpi=100)
        self.figure = figure
        self.axes_price: Dict[str, Any] = {}
        self.axes_cvd: Dict[str, Any] = {}
        self.axes_imbalance: Dict[str, Any] = {}
    
        time_formatter = mdates.DateFormatter("%H:%M:%S", tz=self.display_tz)
        for col, source in enumerate(self.cfg.symbol_sources):
            key = source.normalized().display
    
            ax_price = figure.add_subplot(3, ncols, col + 1)
            ax_price.set_title(f"{key} Price")
            ax_price.set_ylabel("Price / 价格")
            ax_price.grid(True, linestyle="--", alpha=0.3)
            ax_price.xaxis.set_major_formatter(time_formatter)
            self.axes_price[key] = ax_price
    
            ax_cvd = figure.add_subplot(3, ncols, ncols + col + 1)
            ax_cvd.set_title(f"{key} CVD")
            ax_cvd.set_ylabel("CVD")
            ax_cvd.grid(True, linestyle="--", alpha=0.3)
            ax_cvd.xaxis.set_major_formatter(time_formatter)
            self.axes_cvd[key] = ax_cvd
    
            ax_imbal = figure.add_subplot(3, ncols, 2 * ncols + col + 1)
            ax_imbal.set_title(f"{key} OB Imbalance")
            ax_imbal.set_ylabel("盘口不平衡")
            ax_imbal.set_ylim(-1, 1)
            ax_imbal.grid(True, linestyle="--", alpha=0.3)
            ax_imbal.xaxis.set_major_formatter(time_formatter)
            self.axes_imbalance[key] = ax_imbal
    
        self.canvas = FigureCanvasTkAgg(figure, master=self.chart_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.time_formatter = time_formatter
    
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

        print(
            f"[info] 参数已更新：poll={self.cfg.poll_interval}s, "
            f"delta_window={self.cfg.delta_window}s, cvd_window={self.cfg.cvd_window}s, "
            f"history={self.cfg.history_seconds}s, "
            f"ob_smooth={self.cfg.imbalance_smooth_points}"
        )
    # ------------------------------------------------------------------ Updates

    def update_ui(self) -> None:
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
        labels = self.metric_labels[symbol_key]
        widgets = self.metric_widgets[symbol_key]
    
        timestamp = record.get('timestamp') or pd.Timestamp.utcnow().tz_localize("UTC")
        labels['time'].set(self._format_display_time(timestamp))
    
        mid = record.get('mid')
        labels['mid'].set(f"{mid:.2f}" if isinstance(mid, (int, float)) else '--')
    
        delta = float(record.get('delta', 0.0) or 0.0)
        delta_sum = float(record.get('delta_sum', 0.0) or 0.0)
        cvd = float(record.get('cvd', 0.0) or 0.0)
        labels['delta'].set(f"{delta:.3f}")
        labels['delta_sum'].set(f"{delta_sum:.3f}")
        labels['cvd'].set(f"{cvd:.3f}")
    
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
            widgets['ob'].configure(fg='#607d8b')
        else:
            labels['ob'].set(f"{ob:.2f}")
            if ob > 0.2:
                widgets['ob'].configure(fg='#2e7d32')
            elif ob < -0.2:
                widgets['ob'].configure(fg='#c62828')
            else:
                widgets['ob'].configure(fg='#37474f')
    
        div = record.get('display_divergence') or record.get('divergence') or 'None'
        labels['divergence'].set(div)
        if 'Bullish' in div:
            widgets['divergence'].configure(fg='#2e7d32')
        elif 'Bearish' in div:
            widgets['divergence'].configure(fg='#c62828')
        else:
            widgets['divergence'].configure(fg='#37474f')
    
        if delta > 0:
            widgets['delta'].configure(fg='#2e7d32')
        elif delta < 0:
            widgets['delta'].configure(fg='#c62828')
        else:
            widgets['delta'].configure(fg='#37474f')
    
    def _update_charts(self, symbol_key: str) -> None:
        records = self.records_by_symbol.get(symbol_key, [])
        if not records:
            return
    
        times = [self._to_display_timestamp(r['timestamp']) for r in records]
        price = [float(r.get('mid', float('nan'))) for r in records]
        cvd = [float(r.get('cvd', 0.0) or 0.0) for r in records]
        imbalance = [
            0.0 if pd.isna(r.get('ob_imbalance')) else float(r.get('ob_imbalance', 0.0))
            for r in records
        ]
        smooth_window = max(1, self.cfg.imbalance_smooth_points)
        if smooth_window > 1:
            series = pd.Series(imbalance, dtype="float64")
            imbalance = series.rolling(window=smooth_window, min_periods=1).mean().tolist()
    
        ax_price = self.axes_price[symbol_key]
        ax_cvd = self.axes_cvd[symbol_key]
        ax_imbal = self.axes_imbalance[symbol_key]
    
        ax_price.clear()
        ax_cvd.clear()
        ax_imbal.clear()
    
        ax_price.plot(times, price, color='#1976d2', label='Price')
        ax_price.set_ylabel('Price / 价格')
        ax_price.grid(True, linestyle='--', alpha=0.3)
        ax_price.xaxis.set_major_formatter(self.time_formatter)
        ax_price.tick_params(axis='x', rotation=20)
        ax_price.ticklabel_format(style='plain', useOffset=False, axis='y')
        ax_price.get_yaxis().get_major_formatter().set_scientific(False)
    
        ax_cvd.plot(times, cvd, color='#6a1b9a', label='CVD', linewidth=1.2)
        ax_cvd.set_ylabel('CVD')
        ax_cvd.grid(True, linestyle='--', alpha=0.3)
        ax_cvd.xaxis.set_major_formatter(self.time_formatter)
        ax_cvd.tick_params(axis='x', rotation=20)
    
        ax_imbal.axhline(0, color='#9e9e9e', linewidth=1, linestyle='--')
        ax_imbal.plot(times, imbalance, color='#ff8f00')
        ax_imbal.set_ylabel('盘口不平衡')
        ax_imbal.set_ylim(-1, 1)
        ax_imbal.grid(True, linestyle='--', alpha=0.3)
        ax_imbal.xaxis.set_major_formatter(self.time_formatter)
        ax_imbal.tick_params(axis='x', rotation=20)
    
        price_values = [p for p in price if not pd.isna(p)]
        price_range = max(price_values) - min(price_values) if price_values else 0.0
        base_price = price_values[-1] if price_values else 1.0
        offset = max(price_range * 0.02, abs(base_price) * 0.0005, 0.5)
        min_offset = max(abs(offset), 1e-6)
    
        cutoff_time = (
            times[-1] - pd.Timedelta(seconds=60) if times else self._to_display_timestamp(pd.Timestamp.utcnow())
        )
        for rec in records:
            div_label = rec.get('display_divergence') or rec.get('divergence')
            if not div_label or div_label == 'None':
                continue
            ts = self._to_display_timestamp(rec['timestamp'])
            if ts < cutoff_time:
                continue
            until = rec.get('divergence_until')
            until_ts = self._to_display_timestamp(until) if until else None
            if until_ts and ts > until_ts:
                continue
            price_y = rec.get('mid')
            if price_y is None or pd.isna(price_y):
                continue
    
            if 'Bullish' in div_label:
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
            elif 'Bearish' in div_label:
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
    
        if len(times) > 1:
            ax_price.set_xlim(times[0], times[-1])
            ax_cvd.set_xlim(times[0], times[-1])
            ax_imbal.set_xlim(times[0], times[-1])
        self.figure.tight_layout()
        self.canvas.draw_idle()

    def _to_display_timestamp(self, value: Any) -> pd.Timestamp:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize(UTC_TZ)
        return ts.tz_convert(self.display_tz)

    def _format_display_time(self, value: Any) -> str:
        return self._to_display_timestamp(value).strftime('%H:%M:%S')

    # ------------------------------------------------------------------ Shutdown
    def on_close(self) -> None:
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
    parser.add_argument("--influx-url", type=str, default=None, help="InfluxDB base URL (e.g. http://localhost:8086)")
    parser.add_argument("--influx-token", type=str, default=None, help="InfluxDB API token")
    parser.add_argument("--influx-org", type=str, default=None, help="InfluxDB organisation")
    parser.add_argument("--influx-bucket", type=str, default=None, help="InfluxDB bucket")
    parser.add_argument(
        "--influx-measurement", type=str, default=None, help="Measurement name (default: orderflow)"
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

    initial_history: Dict[str, List[Dict[str, Any]]] = {}
    if influx_publisher is not None:
        initial_fetch_seconds = max(
            cfg.history_seconds,
            getattr(app_config, "INITIAL_FETCH_SECONDS", 3600) if app_config else 3600,
        )
        for source in cfg.symbol_sources:
            history = influx_publisher.fetch_recent(source, initial_fetch_seconds)
            if history:
                initial_history[source.normalized().display] = history

    monitors: List[OrderFlowMonitor] = []
    for source in cfg.symbol_sources:
        monitor = OrderFlowMonitor(source, cfg, record_queue, publisher=influx_publisher)
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
