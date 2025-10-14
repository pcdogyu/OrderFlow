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


@dataclass
class MonitorConfig:
    symbols: List[str] = field(default_factory=lambda: ["btcusdt", "ethusdt"])
    poll_interval: float = 0.5  # seconds
    history_seconds: int = 900  # 15 minutes
    delta_window: int = 30
    cvd_window: int = 60
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

    def write(self, symbol: str, record: Dict[str, Any]) -> None:
        timestamp = record.get("timestamp")
        if isinstance(timestamp, pd.Timestamp):
            ts = timestamp.to_pydatetime(warn=False)
        else:
            ts = pd.Timestamp.utcnow().to_pydatetime()

        point = Point(self.measurement).tag("symbol", symbol)
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


class OrderFlowMonitor:
    """Background worker for a single trading pair."""

    def __init__(
        self,
        symbol: str,
        config: MonitorConfig,
        record_queue: "queue.Queue[Tuple[str, Dict[str, float]]]",
        publisher: Optional[InfluxPublisher] = None,
    ) -> None:
        self.symbol = symbol.lower()
        self.symbol_upper = symbol.upper()
        self.cfg = config
        self.record_queue = record_queue
        self.publisher = publisher

        self.ticks: Deque[Tuple[pd.Timestamp, float, float, int]] = deque()
        self.book: Deque[Tuple[pd.Timestamp, float, float, float, float]] = deque()
        self.stats: Deque[Dict[str, float]] = deque()

        self.delta_window_values: Deque[Tuple[pd.Timestamp, float]] = deque()
        self.cvd_window_values: Deque[Tuple[pd.Timestamp, float]] = deque()
        self.delta_sum_value: float = 0.0
        self.cvd_value: float = 0.0

        self.ws_app: Optional[websocket.WebSocketApp] = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()

    # ------------------------------------------------------------------ WebSocket
    def on_message(self, _: websocket.WebSocketApp, message: str) -> None:
        payload = json.loads(message)
        data = payload.get("data", payload)
        now = pd.Timestamp.now()

        event_type = data.get("e")
        if event_type == "aggTrade":
            price = float(data["p"])
            qty = float(data["q"])
            side = -1 if data.get("m") else 1
            with self.lock:
                self.ticks.append((now, price, qty, side))
            return

        if data.get("s") == self.symbol_upper and all(k in data for k in ("b", "B", "a", "A")):
            bid_price = float(data["b"])
            bid_qty = float(data["B"])
            ask_price = float(data["a"])
            ask_qty = float(data["A"])
            with self.lock:
                self.book.append((now, bid_price, bid_qty, ask_price, ask_qty))

    def on_error(self, _: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[{self.symbol_upper}] error: {error}")

    def on_close(self, *_: object) -> None:
        print(f"[{self.symbol_upper}] websocket closed")

    def run_ws(self) -> None:
        stream = f"{self.symbol}@aggTrade/{self.symbol}@bookTicker"
        url = f"wss://stream.binance.us:9443/stream?streams={stream}"
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
                print(f"[{self.symbol_upper}] reconnecting websocket ({exc})")
                time.sleep(2)
            else:
                break

    # ---------------------------------------------------------------- Aggregation
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
        now = self.stats[-1]["timestamp"]
        cutoff = now - pd.Timedelta(seconds=self.cfg.divergence_window)
        window = [item for item in self.stats if item["timestamp"] >= cutoff]
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

    def aggregate_once(self) -> Optional[Dict[str, float]]:
        now = pd.Timestamp.now()
        window_start = now - pd.Timedelta(seconds=self.cfg.history_seconds)

        with self.lock:
            # prune old samples
            while self.ticks and self.ticks[0][0] < window_start:
                self.ticks.popleft()
            while self.book and self.book[0][0] < window_start:
                self.book.popleft()

            recent_ticks = [t for t in self.ticks if t[0] >= now - pd.Timedelta(seconds=self.cfg.poll_interval + 0.1)]
            recent_book = [b for b in self.book if b[0] >= now - pd.Timedelta(seconds=2)]

        buy_vol = sum(q for _, _, q, s in recent_ticks if s > 0)
        sell_vol = sum(q for _, _, q, s in recent_ticks if s < 0)
        delta = buy_vol - sell_vol
        self.delta_sum_value = self._update_window(
            self.delta_window_values, delta, now, self.cfg.delta_window, self.delta_sum_value
        )
        self.cvd_value = self._update_window(
            self.cvd_window_values, delta, now, self.cfg.cvd_window, self.cvd_value
        )

        if recent_book:
            last = recent_book[-1]
            bid_price, bid_qty = last[1], last[2]
            ask_price, ask_qty = last[3], last[4]
            mid = (bid_price + ask_price) / 2
            ob_imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty + 1e-9)
        elif recent_ticks:
            # fall back to trade price if no book update
            mid = sum(p for _, p, _, _ in recent_ticks) / len(recent_ticks)
            ob_imbalance = float("nan")
        else:
            return None

        record = {
            "timestamp": now,
            "mid": mid,
            "delta": delta,
            "delta_sum": self.delta_sum_value,
            "cvd": self.cvd_value,
            "ob_imbalance": ob_imbalance,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
        }

        # maintain history deque
        self.stats.append(record)
        while self.stats and (now - self.stats[0]["timestamp"]).total_seconds() > self.cfg.history_seconds:
            self.stats.popleft()

        divergence = self.detect_divergence()
        record["divergence"] = divergence
        if divergence:
            record["display_divergence"] = divergence
            record["divergence_until"] = now + pd.Timedelta(seconds=60)
        else:
            record["display_divergence"] = ""
            record["divergence_until"] = None

        if self.publisher is not None:
            try:
                self.publisher.write(self.symbol_upper, record)
            except Exception as exc:
                print(f"[warn] failed to write to InfluxDB for {self.symbol_upper}: {exc}")

        try:
            self.record_queue.put_nowait((self.symbol_upper, record))
        except queue.Full:
            try:
                self.record_queue.get_nowait()
            except queue.Empty:
                pass
            self.record_queue.put_nowait((self.symbol_upper, record))

        return record

    def run_aggregation_loop(self) -> None:
        while not self.stop_event.is_set():
            self.aggregate_once()
            time.sleep(self.cfg.poll_interval)

    def start(self) -> None:
        ws_thread = threading.Thread(target=self.run_ws, name=f"ws-{self.symbol}", daemon=True)
        agg_thread = threading.Thread(
            target=self.run_aggregation_loop, name=f"agg-{self.symbol}", daemon=True
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
        record_queue: "queue.Queue[Tuple[str, Dict[str, float]]]",
        stop_callback: Optional[Callable[[], None]] = None,
        preferred_font: Optional[str] = None,
    ) -> None:
        self.cfg = cfg
        self.record_queue = record_queue
        self.stop_callback = stop_callback

        self.records_by_symbol: Dict[str, List[Dict[str, float]]] = {
            symbol.upper(): [] for symbol in cfg.symbols
        }

        self.root = tk.Tk()
        self.preferred_font = preferred_font
        if preferred_font:
            for name in ("TkDefaultFont", "TkTextFont", "TkMenuFont", "TkHeadingFont"):
                try:
                    tkfont.nametofont(name).configure(family=preferred_font)
                except tk.TclError:
                    pass
        self.root.title("Binance US Order Flow 监控面板")
        self.root.geometry("1200x720")

        base_font = tkfont.nametofont("TkDefaultFont")
        self.ui_font_family = preferred_font or base_font.cget("family")
        self.heading_font = tkfont.Font(family=self.ui_font_family, size=9, weight="bold")
        self.value_font = tkfont.Font(family=self.ui_font_family, size=12)

        self._build_widgets()
        self._setup_plot()

        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        self.update_ui()

    # ------------------------------------------------------------------ UI layout
    def _build_widgets(self) -> None:
        mainframe = ttk.Frame(self.root, padding="8 8 8 8")
        mainframe.pack(fill=tk.BOTH, expand=True)

        metrics_frame = ttk.LabelFrame(mainframe, text="实时指标 Live Metrics", padding="6")
        metrics_frame.pack(fill=tk.X)

        self.metric_labels: Dict[str, Dict[str, tk.StringVar]] = {}
        self.metric_widgets: Dict[str, Dict[str, tk.Label]] = {}

        container = ttk.Frame(metrics_frame)
        container.pack(fill=tk.X)
        total_symbols = len(self.cfg.symbols)
        for col in range(total_symbols):
            container.columnconfigure(col, weight=1)

        for idx, symbol in enumerate(self.cfg.symbols):
            symbol_upper = symbol.upper()
            block = ttk.LabelFrame(container, text=f"{symbol_upper}", padding="4")
            block.grid(row=0, column=idx, sticky="nsew", padx=4, pady=2)

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
            self.metric_labels[symbol_upper] = vars_map

            label_widgets: Dict[str, tk.Label] = {}

            def add_item(col: int, title: str, subtitle: str, key: str) -> None:
                cell = ttk.Frame(grid, padding="2")
                cell.grid(row=0, column=col, sticky="w", padx=4)
                ttk.Label(cell, text=f"{title}\n{subtitle}", font=self.heading_font).pack(
                    anchor=tk.W
                )
                lbl = tk.Label(cell, textvariable=vars_map[key], font=self.value_font)
                lbl.pack(anchor=tk.W)
                label_widgets[key] = lbl

            add_item(0, "Time", "时间", "time")
            add_item(1, "Price", "价格", "mid")
            add_item(2, "Delta", "净成交量", "delta")
            add_item(3, f"Δ Sum ({self.cfg.delta_window}s)", "Delta累积", "delta_sum")
            add_item(4, f"CVD ({self.cfg.cvd_window}s)", "累积Delta", "cvd")
            add_item(5, "OB Imbalance", "盘口不平衡", "ob")
            add_item(6, "Divergence", "背离信号", "divergence")

            self.metric_widgets[symbol_upper] = label_widgets

        chart_frame = ttk.LabelFrame(mainframe, text="15分钟滚动图 Realtime Charts", padding="6")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=6)
        self.chart_frame = chart_frame

    def _setup_plot(self) -> None:
        ncols = max(1, len(self.cfg.symbols))
        figure = Figure(figsize=(11, 6.2), dpi=100)
        self.figure = figure
        self.axes_price: Dict[str, Tuple] = {}
        self.axes_imbalance: Dict[str, any] = {}

        time_formatter = mdates.DateFormatter("%H:%M:%S")
        symbol_list = [s.upper() for s in self.cfg.symbols]

        for col, symbol in enumerate(symbol_list):
            ax_price = figure.add_subplot(2, ncols, col + 1)
            ax_cvd = ax_price.twinx()
            ax_price.set_title(f"{symbol} Price / CVD")
            ax_price.set_ylabel("Price / 价格")
            ax_cvd.set_ylabel("CVD")
            ax_price.grid(True, linestyle="--", alpha=0.3)
            ax_price.xaxis.set_major_formatter(time_formatter)

            ax_imbal = figure.add_subplot(2, ncols, ncols + col + 1)
            ax_imbal.set_title(f"{symbol} OB Imbalance")
            ax_imbal.set_ylabel("不平衡 / OB")
            ax_imbal.set_ylim(-1, 1)
            ax_imbal.grid(True, linestyle="--", alpha=0.3)
            ax_imbal.xaxis.set_major_formatter(time_formatter)

            self.axes_price[symbol] = (ax_price, ax_cvd)
            self.axes_imbalance[symbol] = ax_imbal

        self.canvas = FigureCanvasTkAgg(figure, master=self.chart_frame)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.time_formatter = time_formatter

    # ------------------------------------------------------------------ Updates
    def update_ui(self) -> None:
        updated_symbols: set[str] = set()
        try:
            while True:
                symbol, record = self.record_queue.get_nowait()
                symbol = symbol.upper()
                self.records_by_symbol.setdefault(symbol, [])
                records = self.records_by_symbol[symbol]
                records.append(record)

                cutoff = record["timestamp"] - pd.Timedelta(seconds=self.cfg.history_seconds)
                self.records_by_symbol[symbol] = [r for r in records if r["timestamp"] >= cutoff]
                updated_symbols.add(symbol)
        except queue.Empty:
            pass

        for symbol in updated_symbols:
            history = self.records_by_symbol[symbol]
            latest = history[-1]

            if latest.get("display_divergence") and latest.get("divergence_until"):
                if latest["timestamp"] > latest["divergence_until"]:
                    latest["display_divergence"] = "None"
            else:
                display = "None"
                for rec in reversed(history):
                    if rec.get("divergence_until") and rec["timestamp"] <= rec["divergence_until"]:
                        display = rec.get("divergence") or "None"
                        break
                latest["display_divergence"] = display

            self._update_metrics(symbol, latest)
            self._update_charts(symbol)

        self.root.after(int(self.cfg.poll_interval * 1000), self.update_ui)

    def _update_metrics(self, symbol: str, record: Dict[str, float]) -> None:
        labels = self.metric_labels[symbol]
        widgets = self.metric_widgets[symbol]

        labels["time"].set(record["timestamp"].strftime("%H:%M:%S"))
        labels["mid"].set(f"{record['mid']:.2f}")
        labels["delta"].set(f"{record['delta']:.3f}")
        labels["delta_sum"].set(f"{record['delta_sum']:.3f}")
        labels["cvd"].set(f"{record['cvd']:.3f}")

        ob = record["ob_imbalance"]
        if pd.isna(ob):
            labels["ob"].set("NaN")
            widgets["ob"].configure(fg="#607d8b")
        else:
            labels["ob"].set(f"{ob:.2f}")
            if ob > 0.2:
                widgets["ob"].configure(fg="#2e7d32")
            elif ob < -0.2:
                widgets["ob"].configure(fg="#c62828")
            else:
                widgets["ob"].configure(fg="#37474f")

        div = record.get("display_divergence") or record.get("divergence") or "None"
        labels["divergence"].set(div)
        if "Bullish" in div:
            widgets["divergence"].configure(fg="#2e7d32")
        elif "Bearish" in div:
            widgets["divergence"].configure(fg="#c62828")
        else:
            widgets["divergence"].configure(fg="#37474f")

        delta = record["delta"]
        if delta > 0:
            widgets["delta"].configure(fg="#2e7d32")
        elif delta < 0:
            widgets["delta"].configure(fg="#c62828")
        else:
            widgets["delta"].configure(fg="#37474f")

    def _update_charts(self, symbol: str) -> None:
        records = self.records_by_symbol.get(symbol, [])
        if not records:
            return

        times = [r["timestamp"] for r in records]
        price = [r["mid"] for r in records]
        cvd = [r["cvd"] for r in records]
        imbalance = [0.0 if pd.isna(r["ob_imbalance"]) else r["ob_imbalance"] for r in records]

        ax_price, ax_cvd = self.axes_price[symbol]
        ax_imbal = self.axes_imbalance[symbol]

        ax_price.clear()
        ax_cvd.clear()
        ax_imbal.clear()

        ax_price.plot(times, price, color="#1976d2", label="Price")
        ax_price.set_ylabel("Price / 价格")
        ax_price.grid(True, linestyle="--", alpha=0.3)
        ax_price.xaxis.set_major_formatter(self.time_formatter)
        ax_price.tick_params(axis="x", rotation=20)
        ax_price.ticklabel_format(style="plain", useOffset=False, axis="y")
        ax_price.get_yaxis().get_major_formatter().set_scientific(False)

        ax_cvd.plot(times, cvd, color="#d32f2f", label="CVD", alpha=0.7)
        ax_cvd.set_ylabel("CVD")

        ax_imbal.axhline(0, color="#9e9e9e", linewidth=1, linestyle="--")
        ax_imbal.plot(times, imbalance, color="#ff8f00")
        ax_imbal.set_ylabel("盘口不平衡")
        ax_imbal.set_ylim(-1, 1)
        ax_imbal.grid(True, linestyle="--", alpha=0.3)
        ax_imbal.xaxis.set_major_formatter(self.time_formatter)
        ax_imbal.tick_params(axis="x", rotation=20)

        price_range = max(price) - min(price) if price else 0.0
        base_price = price[-1] if price else 1.0
        offset = max(price_range * 0.02, abs(base_price) * 0.0005, 0.5)
        min_offset = max(abs(offset), 1e-6)

        cutoff_time = times[-1] - pd.Timedelta(seconds=60)
        for rec in records:
            div_label = rec.get("display_divergence") or rec.get("divergence")
            if not div_label or div_label == "None":
                continue
            ts = rec["timestamp"]
            if ts < cutoff_time:
                continue
            until = rec.get("divergence_until")
            if until and ts > until:
                continue
            price_y = rec.get("mid")
            if price_y is None:
                continue

            if "Bullish" in div_label:
                ax_price.annotate(
                    "",
                    xy=(ts, price_y + min_offset * 0.5),
                    xytext=(ts, price_y + min_offset * 1.6),
                    arrowprops=dict(arrowstyle="-|>", color="#d32f2f", linewidth=1.6),
                )
            elif "Bearish" in div_label:
                ax_price.annotate(
                    "",
                    xy=(ts, price_y - min_offset * 0.5),
                    xytext=(ts, price_y - min_offset * 1.6),
                    arrowprops=dict(arrowstyle="-|>", color="#2e7d32", linewidth=1.6),
                )

        if len(times) > 1:
            ax_price.set_xlim(times[0], times[-1])
            ax_imbal.set_xlim(times[0], times[-1])
        self.figure.tight_layout()
        self.canvas.draw_idle()

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
    parser = argparse.ArgumentParser(description="Binance US multi-symbol order-flow GUI")
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=None,
        help="Trading pairs to monitor (default: btcusdt ethusdt)",
    )
    parser.add_argument("--poll-interval", type=float, default=None, help="Refresh interval in seconds")
    parser.add_argument("--history-seconds", type=int, default=None, help="Chart history in seconds")
    parser.add_argument("--delta-window", type=int, default=None, help="Delta rolling window (seconds)")
    parser.add_argument("--cvd-window", type=int, default=None, help="CVD rolling window (seconds)")
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
    if args.symbols:
        cfg.symbols = [s.lower() for s in args.symbols]
    if args.poll_interval is not None:
        cfg.poll_interval = max(0.2, args.poll_interval)
    if args.history_seconds is not None:
        cfg.history_seconds = max(120, args.history_seconds)
    if args.delta_window is not None:
        cfg.delta_window = max(1, args.delta_window)
    if args.cvd_window is not None:
        cfg.cvd_window = max(1, args.cvd_window)
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
    record_queue: "queue.Queue[Tuple[str, Dict[str, float]]]" = queue.Queue(maxsize=5000)

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

    monitors: List[OrderFlowMonitor] = []
    for symbol in cfg.symbols:
        monitor = OrderFlowMonitor(symbol, cfg, record_queue, publisher=influx_publisher)
        monitor.start()
        monitors.append(monitor)

    def stop_monitors() -> None:
        for m in monitors:
            m.stop()
        if influx_publisher is not None:
            influx_publisher.close()

    gui = MonitorGUI(cfg, record_queue, stop_callback=stop_monitors, preferred_font=chosen_font)

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
