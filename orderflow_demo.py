#!/usr/bin/env python3
"""Synthetic order-flow generator with common microstructure indicators.

The script produces a toy data set that mimics the behaviour of order-flow
metrics (Delta, CVD, order-book imbalance), exports the results to CSV and
renders a couple of visual diagnostics that are handy when teaching the
concepts.

Outputs (created under ./output):
    - orderflow_demo.csv: second-level aggregates of the simulated stream.
    - price_cvd_divergence.png: price vs CVD with divergence markers.
    - delta_cvd_panels.png: stacked view of Delta and CVD.
    - ob_imbalance_heatmap.png: heat map of the order-book imbalance.
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


plt.style.use("ggplot")


@dataclass
class SimulationConfig:
    seed: int = 7
    seconds: int = 45 * 60  # 45 minutes of data
    start_price: float = 26_500.0
    volatility: float = 0.10  # annualised volatility proxy for drift simulation
    base_volume: float = 80.0  # average contracts traded per second
    imbalance_strength: float = 1.5
    divergence_window: int = 120  # seconds


def ensure_output_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def simulate_price(cfg: SimulationConfig, timestamps: pd.DatetimeIndex) -> pd.Series:
    """Simulate a microstructure-aware price path with short-term momentum."""
    np.random.seed(cfg.seed)
    seconds_per_year = 365 * 24 * 60 * 60
    dt = 1 / seconds_per_year

    # Random walk with a mild mean-reverting component to avoid drift to infinity.
    shocks = np.random.normal(0.0, cfg.volatility * np.sqrt(dt), size=len(timestamps))
    price = np.empty_like(shocks)
    price[0] = cfg.start_price
    for i in range(1, len(price)):
        momentum = 0.25 * (price[i - 1] - cfg.start_price) / cfg.start_price
        price[i] = price[i - 1] * (1 + shocks[i] - 0.05 * momentum)

    return pd.Series(price, index=timestamps, name="mid")


def simulate_orderflow(
    cfg: SimulationConfig, price: pd.Series
) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """Generate Delta, buy volume, sell volume that co-move with price changes."""
    np.random.seed(cfg.seed + 1)
    price_returns = price.pct_change().fillna(0.0)

    gross_volume = np.random.lognormal(
        mean=np.log(cfg.base_volume), sigma=0.35, size=len(price_returns)
    )

    # Introduce correlation between price direction and imbalance.
    imbalance_signal = (
        0.6 * np.sign(price_returns.ewm(span=20).mean()) + 0.4 * np.random.randn(len(price_returns))
    )
    delta = imbalance_signal * gross_volume * 0.5

    buy_volume = np.clip((gross_volume + delta) / 2.0, a_min=0.0, a_max=None)
    sell_volume = np.clip((gross_volume - delta) / 2.0, a_min=0.0, a_max=None)

    delta_series = pd.Series(delta, index=price.index, name="delta")
    buy_series = pd.Series(buy_volume, index=price.index, name="buy_vol")
    sell_series = pd.Series(sell_volume, index=price.index, name="sell_vol")
    return delta_series, buy_series, sell_series


def simulate_order_book_imbalance(
    cfg: SimulationConfig, delta: pd.Series
) -> pd.Series:
    """Produce a smoothed top-of-book imbalance signal."""
    np.random.seed(cfg.seed + 2)
    delta_norm = (delta.rolling(window=30, min_periods=1).mean()) / (
        delta.rolling(window=30, min_periods=1).std().replace(0, np.nan)
    )
    delta_norm = delta_norm.fillna(0.0)
    noise = 0.35 * np.random.randn(len(delta))
    imbalance = np.tanh(cfg.imbalance_strength * delta_norm + noise)
    return pd.Series(imbalance, index=delta.index, name="ob_imbalance")


def detect_divergences(
    price: pd.Series, cvd: pd.Series, window: int
) -> pd.DataFrame:
    """Flag bullish/bearish divergences between price and CVD."""
    price_high = price.rolling(window=window, min_periods=1).max()
    price_low = price.rolling(window=window, min_periods=1).min()
    cvd_high = cvd.rolling(window=window, min_periods=1).max()
    cvd_low = cvd.rolling(window=window, min_periods=1).min()

    bearish = (price >= price_high) & (cvd < cvd_high.shift(1))
    bullish = (price <= price_low) & (cvd > cvd_low.shift(1))

    return pd.DataFrame(
        {"bearish_div": bearish.fillna(False), "bullish_div": bullish.fillna(False)}
    )


def render_price_cvd(
    df: pd.DataFrame, div_flags: pd.DataFrame, output_dir: Path
) -> None:
    fig, ax_price = plt.subplots(figsize=(12, 6))
    ax_cvd = ax_price.twinx()

    ax_price.plot(df.index, df["mid"], color="#1976d2", label="Mid Price")
    ax_cvd.plot(df.index, df["cvd"], color="#d32f2f", label="CVD", alpha=0.7)

    ax_price.set_ylabel("Price")
    ax_cvd.set_ylabel("CVD")

    # Divergence markers
    bearish_idx = df.index[div_flags["bearish_div"]]
    bullish_idx = df.index[div_flags["bullish_div"]]

    ax_price.scatter(
        bearish_idx,
        df.loc[bearish_idx, "mid"],
        color="#e53935",
        marker="x",
        s=60,
        label="Bearish divergence",
    )
    ax_price.scatter(
        bullish_idx,
        df.loc[bullish_idx, "mid"],
        color="#43a047",
        marker="o",
        s=45,
        label="Bullish divergence",
    )

    ax_price.set_title("Price vs. CVD (divergence markers)")
    lines, labels = ax_price.get_legend_handles_labels()
    lines2, labels2 = ax_cvd.get_legend_handles_labels()
    ax_price.legend(lines + lines2, labels + labels2, loc="upper left")

    fig.tight_layout()
    fig.savefig(output_dir / "price_cvd_divergence.png", dpi=150)
    plt.close(fig)


def render_delta_panels(df: pd.DataFrame, output_dir: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(12, 7), sharex=True)

    axes[0].plot(df.index, df["delta"], color="#ff8f00")
    axes[0].set_ylabel("Delta")
    axes[0].set_title("Second-level Delta")

    axes[1].plot(df.index, df["cvd"], color="#6a1b9a")
    axes[1].set_ylabel("CVD")
    axes[1].set_title("Cumulative Volume Delta")

    axes[1].set_xlabel("Timestamp")
    fig.tight_layout()
    fig.savefig(output_dir / "delta_cvd_panels.png", dpi=150)
    plt.close(fig)


def render_imbalance_heatmap(df: pd.DataFrame, output_dir: Path) -> None:
    reshaped = df["ob_imbalance"].to_numpy().reshape(-1, 60)
    fig, ax = plt.subplots(figsize=(12, 4))
    cmap = plt.get_cmap("RdYlGn")
    mesh = ax.imshow(
        reshaped,
        aspect="auto",
        cmap=cmap,
        vmin=-1,
        vmax=1,
        interpolation="nearest",
        origin="lower",
    )
    ax.set_title("Order Book Imbalance Heatmap (1 row = 1 minute)")
    ax.set_ylabel("Minute index")
    ax.set_xlabel("Second within minute")
    fig.colorbar(mesh, ax=ax, label="OB Imbalance")
    fig.tight_layout()
    fig.savefig(output_dir / "ob_imbalance_heatmap.png", dpi=150)
    plt.close(fig)


def run_simulation(cfg: SimulationConfig, output_dir: Path) -> Path:
    timestamps = pd.date_range(
        start=pd.Timestamp.utcnow().floor("min"),
        periods=cfg.seconds,
        freq="s",
        tz="UTC",
    )
    price = simulate_price(cfg, timestamps)
    delta, buy_vol, sell_vol = simulate_orderflow(cfg, price)
    cvd = delta.cumsum()
    ob_imb = simulate_order_book_imbalance(cfg, delta)

    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "mid": price,
            "buy_vol": buy_vol,
            "sell_vol": sell_vol,
            "delta": delta,
            "cvd": cvd,
            "ob_imbalance": ob_imb,
        }
    ).set_index("timestamp")

    csv_path = output_dir / "orderflow_demo.csv"
    df.to_csv(csv_path, float_format="%.6f")
    return csv_path


def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic order-flow demo.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help="Directory where CSV and figures will be stored.",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=None,
        help="Override the number of simulated seconds.",
    )
    args = parser.parse_args()

    cfg = SimulationConfig()
    if args.seconds is not None:
        cfg.seconds = max(120, args.seconds)  # guard against degenerate heatmaps

    output_dir = ensure_output_dir(args.output_dir)
    csv_path = run_simulation(cfg, output_dir)
    df = pd.read_csv(csv_path, parse_dates=["timestamp"], index_col="timestamp")

    div_flags = detect_divergences(df["mid"], df["cvd"], cfg.divergence_window)
    render_price_cvd(df, div_flags, output_dir)
    render_delta_panels(df, output_dir)
    render_imbalance_heatmap(df, output_dir)

    print(f"Wrote synthetic order-flow data to {csv_path.as_posix()}")
    print("Saved figures:")
    for name in [
        "price_cvd_divergence.png",
        "delta_cvd_panels.png",
        "ob_imbalance_heatmap.png",
    ]:
        print(f"  - {(output_dir / name).as_posix()}")


if __name__ == "__main__":
    main()
