#!/usr/bin/env python3
"""Tkinter GUI for monitoring ETHUSDT order-flow via Binance streams."""

from __future__ import annotations

import argparse
import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Deque, List, Optional, Sequence, Tuple

import tkinter as tk
from tkinter import ttk

import matplotlib
from matplotlib import dates as mdates, font_manager
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import websocket

matplotlib.rcParams["axes.unicode_minus"] = False


def configure_chinese_font() -> Optional[str]:
    preferred_fonts = ["Microsoft YaHei", "SimHei", "PingFang SC", "Noto Sans CJK SC"]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for font_name in preferred_fonts:
        if font_name in available:
            matplotlib.rcParams["font.family"] = [font_name]
            matplotlib.rcParams["font.sans-serif"] = [font_name]
            return font_name
    matplotlib.rcParams.setdefault("font.family", ["DejaVu Sans"])
    return None


configure_chinese_font()


@dataclass
class StreamConfig:
    symbol: str = "ethusdt"
    depth_levels: int = 10
    trade_window: int = 60
    update_interval_ms: int = 200
    depth_speed: str = "100ms"
    chart_history_seconds: int = 300

    @property
    def stream_url(self) -> str:
        depth_token = f"{self.symbol}@depth{self.depth_levels}@{self.depth_speed}"
        trade_token = f"{self.symbol}@aggTrade"
        return f"wss://stream.binance.com:9443/stream?streams={depth_token}/{trade_token}"


@dataclass
class Trade:
    ts: float
    price: float
    qty: float
    side: str  # "buy" or "sell"


@dataclass
class MarketSnapshot:
    timestamp: float
    bids: List[Tuple[float, float]]
    asks: List[Tuple[float, float]]
    best_bid: Optional[float]
    best_ask: Optional[float]
    mid: Optional[float]
    spread: Optional[float]
    spread_pct: Optional[float]
    total_bid_volume: float
    total_ask_volume: float
    imbalance: Optional[float]
    buy_volume: float
    sell_volume: float
    delta: float
    cvd: float
    trades: List[Trade]
    price_points: List[Tuple[float, float]]
    delta_points: List[Tuple[float, float]]
    cvd_points: List[Tuple[float, float]]


class MarketState:
    """Thread-safe container for order-book and trade data."""

    def __init__(self, cfg: StreamConfig) -> None:
        self.cfg = cfg
        self.bids: List[Tuple[float, float]] = []
        self.asks: List[Tuple[float, float]] = []
        self.trades: Deque[Trade] = deque()
        self.cvd: float = 0.0
        self.price_history: Deque[Tuple[float, float]] = deque(
            maxlen=max(240, cfg.chart_history_seconds * 5)
        )
        self.stats_history: Deque[Tuple[float, float, float]] = deque(
            maxlen=max(240, cfg.chart_history_seconds * 5)
        )
        self.lock = threading.Lock()

    def handle_depth(self, payload: dict) -> None:
        bids_raw = payload.get("b") or payload.get("bids") or []
        asks_raw = payload.get("a") or payload.get("asks") or []

        bids: List[Tuple[float, float]] = []
        for price_raw, qty_raw in bids_raw:
            price = float(price_raw)
            qty = float(qty_raw)
            if qty <= 0:
                continue
            bids.append((price, qty))
        bids.sort(key=lambda item: item[0], reverse=True)

        asks: List[Tuple[float, float]] = []
        for price_raw, qty_raw in asks_raw:
            price = float(price_raw)
            qty = float(qty_raw)
            if qty <= 0:
                continue
            asks.append((price, qty))
        asks.sort(key=lambda item: item[0])

        with self.lock:
            self.bids = bids[: self.cfg.depth_levels]
            self.asks = asks[: self.cfg.depth_levels]

    def handle_trade(self, payload: dict) -> None:
        price = float(payload["p"])
        qty = float(payload["q"])
        side = "sell" if payload.get("m") else "buy"
        ts_ms = payload.get("T") or payload.get("E") or int(time.time() * 1000)
        ts = ts_ms / 1000.0

        delta = qty if side == "buy" else -qty

        trade = Trade(ts=ts, price=price, qty=qty, side=side)
        cutoff = ts - self.cfg.trade_window

        with self.lock:
            self.trades.append(trade)
            self.cvd += delta
            while self.trades and self.trades[0].ts < cutoff:
                self.trades.popleft()

    def snapshot(self) -> MarketSnapshot:
        now = time.time()
        cutoff = now - self.cfg.trade_window

        with self.lock:
            while self.trades and self.trades[0].ts < cutoff:
                self.trades.popleft()
            bids = list(self.bids)
            asks = list(self.asks)
            trades = list(self.trades)[-20:]
            cvd = self.cvd
            best_bid = bids[0][0] if bids else None
            best_ask = asks[0][0] if asks else None
            mid = None
            spread = None
            spread_pct = None
            if best_bid is not None and best_ask is not None:
                mid = (best_bid + best_ask) / 2
                spread = best_ask - best_bid
                spread_pct = spread / mid if mid else None

            if mid is not None:
                self.price_history.append((now, mid))
            history_cutoff = now - self.cfg.chart_history_seconds
            while self.price_history and self.price_history[0][0] < history_cutoff:
                self.price_history.popleft()
            price_points = list(self.price_history)

        total_bid_volume = sum(qty for _, qty in bids)
        total_ask_volume = sum(qty for _, qty in asks)
        imbalance = None
        denom = total_bid_volume + total_ask_volume
        if denom:
            imbalance = (total_bid_volume - total_ask_volume) / denom

        buy_volume = sum(t.qty for t in trades if t.side == "buy")
        sell_volume = sum(t.qty for t in trades if t.side == "sell")
        delta = buy_volume - sell_volume

        return MarketSnapshot(
            timestamp=now,
            bids=bids,
            asks=asks,
            best_bid=best_bid,
            best_ask=best_ask,
            mid=mid,
            spread=spread,
            spread_pct=spread_pct,
            total_bid_volume=total_bid_volume,
            total_ask_volume=total_ask_volume,
            imbalance=imbalance,
            buy_volume=buy_volume,
            sell_volume=sell_volume,
            delta=delta,
            cvd=cvd,
            trades=trades,
            price_points=price_points,
        )


class WebSocketWorker(threading.Thread):
    """Background thread that maintains the Binance stream connection."""

    def __init__(self, state: MarketState) -> None:
        super().__init__(daemon=True)
        self.state = state
        self.stop_event = threading.Event()
        self.ws_app: Optional[websocket.WebSocketApp] = None

    def run(self) -> None:
        url = self.state.cfg.stream_url
        while not self.stop_event.is_set():
            self.ws_app = websocket.WebSocketApp(
                url,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            try:
                self.ws_app.run_forever(ping_interval=20, ping_timeout=10)
            except Exception:
                if self.stop_event.is_set():
                    break
                time.sleep(3)
            else:
                if self.stop_event.is_set():
                    break
                time.sleep(1)

    def stop(self) -> None:
        self.stop_event.set()
        if self.ws_app is not None:
            try:
                self.ws_app.close()
            except Exception:
                pass

    def on_message(self, _: websocket.WebSocketApp, message: str) -> None:
        payload = json.loads(message)
        data = payload.get("data", payload)
        event = data.get("e")

        if event == "depthUpdate" or ("b" in data and "a" in data) or ("bids" in data and "asks" in data):
            self.state.handle_depth(data)
        elif event == "aggTrade":
            self.state.handle_trade(data)

    def on_error(self, _: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[error] websocket: {error}")

    def on_close(self, *_: object) -> None:
        print("[info] websocket connection closed")


class MonitorGUI:
    def __init__(self, cfg: StreamConfig) -> None:
        self.cfg = cfg
        self.state = MarketState(cfg)
        self.worker = WebSocketWorker(self.state)
        self._closing = False

        self.root = tk.Tk()
        self.root.title(f"{cfg.symbol.upper()} Order-Flow Monitor")
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self._make_widgets()

    def _make_widgets(self) -> None:
        main = ttk.Frame(self.root, padding=10)
        main.pack(fill=tk.BOTH, expand=True)

        title = ttk.Label(
            main,
            text=f"{self.cfg.symbol.upper()} 实时订单流",
            font=("Segoe UI", 16, "bold"),
        )
        title.pack(anchor=tk.W, pady=(0, 8))

        stats_frame = ttk.Frame(main)
        stats_frame.pack(fill=tk.X, pady=(0, 10))

        self.price_var = tk.StringVar(value="--")
        self.spread_var = tk.StringVar(value="--")
        self.imbalance_var = tk.StringVar(value="--")
        self.delta_var = tk.StringVar(value="--")
        self.cvd_var = tk.StringVar(value="--")

        ttk.Label(stats_frame, text="最新价格").grid(row=0, column=0, sticky=tk.W, padx=4)
        ttk.Label(stats_frame, textvariable=self.price_var, font=("Segoe UI", 12, "bold")).grid(
            row=0, column=1, sticky=tk.W
        )

        ttk.Label(stats_frame, text="价差 / 百分比").grid(row=0, column=2, sticky=tk.W, padx=12)
        ttk.Label(stats_frame, textvariable=self.spread_var).grid(row=0, column=3, sticky=tk.W)

        ttk.Label(stats_frame, text="盘口失衡").grid(row=1, column=0, sticky=tk.W, padx=4, pady=4)
        ttk.Label(stats_frame, textvariable=self.imbalance_var).grid(row=1, column=1, sticky=tk.W)

        ttk.Label(stats_frame, text="成交Delta").grid(row=1, column=2, sticky=tk.W, padx=12)
        ttk.Label(stats_frame, textvariable=self.delta_var).grid(row=1, column=3, sticky=tk.W)

        ttk.Label(stats_frame, text=f"CVD ({self.cfg.trade_window}s)").grid(
            row=2, column=0, sticky=tk.W, padx=4
        )
        ttk.Label(stats_frame, textvariable=self.cvd_var).grid(row=2, column=1, sticky=tk.W)

        chart_frame = ttk.LabelFrame(main, text="价格走势")
        chart_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))

        self.figure = Figure(figsize=(6, 2.5), dpi=100)
        self.ax = self.figure.add_subplot(111)
        self.ax.set_title("中间价 (Mid)")  # price mid
        self.ax.set_xlabel("时间")
        self.ax.set_ylabel("价格")
        self.ax.grid(True, alpha=0.25, linestyle="--")
        self.ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
        self.chart_line, = self.ax.plot([], [], color="#1f77b4", linewidth=1.2)
        self.canvas = FigureCanvasTkAgg(self.figure, master=chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.figure.autofmt_xdate(rotation=25)

        book_frame = ttk.Frame(main)
        book_frame.pack(fill=tk.BOTH, expand=True)

        self.bids_tree = self._create_book_tree(
            book_frame, heading="买盘 (Bid)", tag_name="bid", foreground="#0c7d55"
        )
        self.asks_tree = self._create_book_tree(
            book_frame, heading="卖盘 (Ask)", tag_name="ask", foreground="#c74343"
        )
        self.bids_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        self.asks_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(5, 0))

        trades_frame = ttk.LabelFrame(main, text="最近成交")
        trades_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))

        columns = ("time", "side", "price", "qty")
        self.trades_tree = ttk.Treeview(trades_frame, columns=columns, show="headings", height=8)
        self.trades_tree.heading("time", text="时间")
        self.trades_tree.heading("side", text="方向")
        self.trades_tree.heading("price", text="价格")
        self.trades_tree.heading("qty", text="数量")
        self.trades_tree.column("time", width=100, anchor=tk.CENTER)
        self.trades_tree.column("side", width=60, anchor=tk.CENTER)
        self.trades_tree.column("price", width=110, anchor=tk.E)
        self.trades_tree.column("qty", width=110, anchor=tk.E)
        self.trades_tree.tag_configure("buy", foreground="#0c7d55")
        self.trades_tree.tag_configure("sell", foreground="#c74343")
        self.trades_tree.pack(fill=tk.BOTH, expand=True)

    @staticmethod
    def _create_book_tree(parent: tk.Widget, heading: str, tag_name: str, foreground: str) -> ttk.Treeview:
        columns = ("price", "qty")
        tree = ttk.Treeview(parent, columns=columns, show="headings", height=12)
        tree.heading("price", text=f"{heading} 价格", anchor=tk.CENTER)
        tree.heading("qty", text="数量", anchor=tk.CENTER)
        tree.column("price", anchor=tk.CENTER, width=120)
        tree.column("qty", anchor=tk.CENTER, width=120)
        tree.tag_configure(tag_name, foreground=foreground)
        return tree

    def start(self) -> None:
        self.worker.start()
        self._schedule_update()
        try:
            self.root.mainloop()
        finally:
            self._on_close()

    def _schedule_update(self) -> None:
        self._refresh_view()
        self.root.after(self.cfg.update_interval_ms, self._schedule_update)

    def _refresh_view(self) -> None:
        snapshot = self.state.snapshot()

        if snapshot.mid is not None:
            self.price_var.set(f"{snapshot.mid:.2f}")
        else:
            self.price_var.set("--")

        if snapshot.spread is not None and snapshot.spread_pct is not None:
            pct = snapshot.spread_pct * 100
            self.spread_var.set(f"{snapshot.spread:.2f} / {pct:.3f}%")
        else:
            self.spread_var.set("--")

        if snapshot.imbalance is not None:
            self.imbalance_var.set(f"{snapshot.imbalance:+.2%}")
        else:
            self.imbalance_var.set("--")

        self.delta_var.set(f"{snapshot.delta:+.4f}")
        self.cvd_var.set(f"{snapshot.cvd:+.4f}")

        self._update_book_tree(self.bids_tree, snapshot.bids, tag_name="bid")
        self._update_book_tree(self.asks_tree, snapshot.asks, tag_name="ask")
        self._update_trades(snapshot.trades)
        self._update_chart(snapshot)

    @staticmethod
    def _update_book_tree(tree: ttk.Treeview, rows: Sequence[Tuple[float, float]], tag_name: str) -> None:
        existing = tree.get_children()
        for item in existing:
            tree.delete(item)
        for price, qty in rows:
            tree.insert("", tk.END, values=(f"{price:.2f}", f"{qty:.4f}"), tags=(tag_name,))

    def _update_trades(self, trades: Sequence[Trade]) -> None:
        existing = self.trades_tree.get_children()
        for item in existing:
            self.trades_tree.delete(item)
        for trade in reversed(trades):
            ts = datetime.fromtimestamp(trade.ts).strftime("%H:%M:%S")
            side_label = "买入" if trade.side == "buy" else "卖出"
            self.trades_tree.insert(
                "",
                tk.END,
                values=(ts, side_label, f"{trade.price:.2f}", f"{trade.qty:.4f}"),
                tags=(trade.side,),
            )

    def _update_chart(self, snapshot: MarketSnapshot) -> None:
        points = snapshot.price_points
        if not points:
            self.chart_line.set_data([], [])
            self.canvas.draw_idle()
            return
        times, prices = zip(*points)
        dates = [mdates.date2num(datetime.fromtimestamp(ts)) for ts in times]
        self.chart_line.set_data(dates, prices)
        if dates[-1] == dates[0]:
            margin = 1.0 / (24 * 60 * 60)  # one second in days
            self.ax.set_xlim(dates[0] - margin, dates[-1] + margin)
        else:
            pad = (dates[-1] - dates[0]) * 0.05
            self.ax.set_xlim(dates[0] - pad, dates[-1] + pad)
        self.ax.relim()
        self.ax.autoscale_view(scalex=False, scaley=True)
        self.canvas.draw_idle()

    def _on_close(self) -> None:
        if self._closing:
            return
        self._closing = True
        self.worker.stop()
        self.worker.join(timeout=2)
        try:
            if self.root.winfo_exists():
                self.root.destroy()
        except tk.TclError:
            pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETHUSDT order-flow monitor with GUI.")
    parser.add_argument("--symbol", default="ethusdt", help="交易对 (默认: ethusdt)")
    parser.add_argument(
        "--depth-levels", type=int, default=10, help="盘口深度级别 (默认: 10)"
    )
    parser.add_argument(
        "--trade-window",
        type=int,
        default=60,
        help="滚动成交窗口秒数，用于Delta/CVD (默认: 60)",
    )
    parser.add_argument(
        "--update-interval",
        type=int,
        default=200,
        help="界面刷新间隔 (毫秒, 默认: 200)",
    )
    parser.add_argument(
        "--chart-history",
        type=int,
        default=300,
        help="价格曲线历史窗口秒数 (默认: 300)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbol = args.symbol.lower().replace("/", "").replace("-", "")
    cfg = StreamConfig(
        symbol=symbol,
        depth_levels=max(1, args.depth_levels),
        trade_window=max(5, args.trade_window),
        update_interval_ms=max(100, args.update_interval),
        chart_history_seconds=max(60, args.chart_history),
    )
    app = MonitorGUI(cfg)
    app.start()


if __name__ == "__main__":
    main()
