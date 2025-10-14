#!/usr/bin/env python3
"""Realtime Binance US order-flow monitor: price divergence, CVD/Delta, imbalance.

Example:
    python binance_us_order_monitor.py --symbol btcusdt

Dependencies:
    pip install websocket-client pandas
"""

from __future__ import annotations

import argparse
import json
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Deque, Dict, Optional, Tuple

import pandas as pd
import websocket


@dataclass
class MonitorConfig:
    symbol: str = "btcusdt"
    history_seconds: int = 3600
    delta_window: int = 30
    divergence_window: int = 180
    price_tolerance: float = 0.0005
    cvd_tolerance: float = 0.25
    poll_interval: float = 1.0


class OrderFlowMonitor:
    def __init__(self, config: MonitorConfig) -> None:
        self.cfg = config
        self.symbol_upper = config.symbol.upper()
        self.symbol_lower = config.symbol.lower()

        self.ticks: Deque[Tuple[pd.Timestamp, float, float, int]] = deque(
            maxlen=config.history_seconds * 5
        )
        self.book: Deque[Tuple[pd.Timestamp, float, float, float, float]] = deque(
            maxlen=config.history_seconds * 5
        )
        self.stats: Deque[Dict[str, float]] = deque(maxlen=config.history_seconds)

        self.cvd: float = 0.0
        self.ws_app: Optional[websocket.WebSocketApp] = None
        self.stop_event = threading.Event()
        self.lock = threading.Lock()

    # WebSocket handlers -------------------------------------------------
    def on_message(self, _: websocket.WebSocketApp, message: str) -> None:
        payload = json.loads(message)
        data = payload.get("data", payload)
        now = pd.Timestamp.utcnow()

        event_type = data.get("e")
        if event_type == "aggTrade":
            price = float(data["p"])
            qty = float(data["q"])
            side = -1 if data.get("m") else 1
            with self.lock:
                self.ticks.append((now, price, qty, side))
            return

        if data.get("s") == self.symbol_upper and all(
            key in data for key in ("b", "B", "a", "A")
        ):
            bid_price = float(data["b"])
            bid_qty = float(data["B"])
            ask_price = float(data["a"])
            ask_qty = float(data["A"])
            with self.lock:
                self.book.append((now, bid_price, bid_qty, ask_price, ask_qty))

    def on_error(self, _: websocket.WebSocketApp, error: Exception) -> None:
        print(f"[error] {error}")

    def on_close(self, *_: object) -> None:
        print("[info] websocket connection closed")

    def run_ws(self) -> None:
        url = (
            f"wss://stream.binance.us:9443/stream?"
            f"streams={self.symbol_lower}@aggTrade/{self.symbol_lower}@bookTicker"
        )
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
                print(f"[warn] websocket reconnecting due to: {exc}")
                time.sleep(3)
            else:
                break

    # Aggregation --------------------------------------------------------
    def aggregate_once(self) -> Optional[Dict[str, float]]:
        now = pd.Timestamp.utcnow().floor("s")
        start = now - pd.Timedelta(seconds=1)
        with self.lock:
            recent_ticks = [t for t in self.ticks if start <= t[0] < now]
            recent_book = [b for b in self.book if start <= b[0] < now]

        if not recent_ticks and not recent_book:
            return None

        buy_vol = sum(q for _, _, q, s in recent_ticks if s > 0)
        sell_vol = sum(q for _, _, q, s in recent_ticks if s < 0)
        delta = buy_vol - sell_vol
        self.cvd += delta

        if recent_book:
            last_bid_price, last_bid_qty = recent_book[-1][1], recent_book[-1][2]
            last_ask_price, last_ask_qty = recent_book[-1][3], recent_book[-1][4]
            mid = (last_bid_price + last_ask_price) / 2
            ob_imbalance = (last_bid_qty - last_ask_qty) / (
                last_bid_qty + last_ask_qty + 1e-9
            )
        else:
            if recent_ticks:
                mid = sum(p for _, p, _, _ in recent_ticks) / len(recent_ticks)
            else:
                return None
            ob_imbalance = float("nan")

        record = {
            "timestamp": now,
            "mid": mid,
            "delta": delta,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "cvd": self.cvd,
            "ob_imbalance": ob_imbalance,
        }
        self.stats.append(record)
        return record

    def delta_sum(self) -> float:
        window = list(self.stats)[-self.cfg.delta_window :]
        return float(sum(item["delta"] for item in window))

    def detect_divergence(self) -> str:
        window = list(self.stats)[-self.cfg.divergence_window :]
        if len(window) < 3:
            return ""

        current = window[-1]
        price_high = max(item["mid"] for item in window)
        price_low = min(item["mid"] for item in window)
        cvd_high = max(item["cvd"] for item in window)
        cvd_low = min(item["cvd"] for item in window)
        price = current["mid"]
        cvd = current["cvd"]

        price_range = max(price_high - price_low, 1e-9)
        cvd_range = max(cvd_high - cvd_low, 1e-9)

        if price >= price_high - price_range * self.cfg.price_tolerance:
            if cvd <= cvd_high - cvd_range * self.cfg.cvd_tolerance:
                return "Bearish divergence"

        if price <= price_low + price_range * self.cfg.price_tolerance:
            if cvd >= cvd_low + cvd_range * self.cfg.cvd_tolerance:
                return "Bullish divergence"

        return ""

    def display(self, record: Dict[str, float]) -> None:
        delta_window_sum = self.delta_sum()
        divergence = self.detect_divergence()
        msg = (
            f"{record['timestamp'].strftime('%H:%M:%S')} | "
            f"mid: {record['mid']:.2f} | "
            f"delta: {record['delta']:.3f} | "
            f"sumDelta({self.cfg.delta_window}s): {delta_window_sum:.3f} | "
            f"CVD: {record['cvd']:.3f} | "
            f"OB: {record['ob_imbalance']:.2f}"
        )
        if divergence:
            msg += f" | {divergence}"
        print(msg, flush=True)

    def run_aggregation_loop(self) -> None:
        while not self.stop_event.is_set():
            record = self.aggregate_once()
            if record is not None:
                self.display(record)
            time.sleep(self.cfg.poll_interval)

    # Control ------------------------------------------------------------
    def start(self) -> None:
        ws_thread = threading.Thread(target=self.run_ws, name="ws-thread", daemon=True)
        agg_thread = threading.Thread(
            target=self.run_aggregation_loop, name="agg-thread", daemon=True
        )
        ws_thread.start()
        agg_thread.start()

        def shutdown_handler(_signum: int, _frame: object) -> None:
            print("\n[info] shutting down...")
            self.stop_event.set()
            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception:
                    pass

        signal.signal(signal.SIGINT, shutdown_handler)
        signal.signal(signal.SIGTERM, shutdown_handler)

        try:
            while not self.stop_event.is_set():
                time.sleep(0.5)
        finally:
            self.stop_event.set()
            if self.ws_app is not None:
                try:
                    self.ws_app.close()
                except Exception:
                    pass
            ws_thread.join(timeout=2)
            agg_thread.join(timeout=2)


def parse_args() -> MonitorConfig:
    parser = argparse.ArgumentParser(description="Binance US order-flow monitor")
    parser.add_argument("--symbol", default="btcusdt", help="Symbol to subscribe, e.g. btcusdt")
    parser.add_argument("--delta-window", type=int, default=30, help="Delta accumulation window (seconds)")
    parser.add_argument("--divergence-window", type=int, default=180, help="Divergence detection window (seconds)")
    parser.add_argument("--price-tolerance", type=float, default=0.0005, help="Price tolerance used in divergence detection")
    parser.add_argument("--cvd-tolerance", type=float, default=0.25, help="CVD tolerance used in divergence detection")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Aggregation refresh interval (seconds)")
    args = parser.parse_args()

    cfg = MonitorConfig(
        symbol=args.symbol,
        delta_window=max(1, args.delta_window),
        divergence_window=max(10, args.divergence_window),
        price_tolerance=max(0.0, args.price_tolerance),
        cvd_tolerance=max(0.0, args.cvd_tolerance),
        poll_interval=max(0.2, args.poll_interval),
    )
    return cfg


def main() -> None:
    cfg = parse_args()
    monitor = OrderFlowMonitor(cfg)
    print(
        f"[info] starting Binance US order-flow monitor for {cfg.symbol.upper()} "
        f"(delta window={cfg.delta_window}s, divergence window={cfg.divergence_window}s)"
    )
    monitor.start()


if __name__ == "__main__":
    main()
