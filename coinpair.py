"""
Helpers for fetching and caching popular trading pairs.

The module downloads volume-ranked symbols for Coinbase,
persists them to a local cache, and exposes helpers for the GUI to
populate "hot pair" selectors. The cache is refreshed automatically
when missing or older than 100 hours.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Dict, List, Tuple

import requests

CACHE_MAX_AGE = timedelta(hours=100)
DEFAULT_LIMIT = 50
COINBASE_PRODUCTS_STATS_URL = "https://api.exchange.coinbase.com/products/stats"
COINBASE_PRODUCTS_URL = "https://api.exchange.coinbase.com/products"

CACHE_FILE = Path(__file__).with_name("coinpair_cache.json")


@dataclass
class PairCache:
    fetched_at: datetime
    pairs: Dict[str, List[str]]


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _load_cache() -> PairCache | None:
    if not CACHE_FILE.exists():
        return None
    try:
        data = json.loads(CACHE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    fetched_at_raw = data.get("fetched_at")
    if not fetched_at_raw:
        return None
    try:
        fetched_at = datetime.fromisoformat(fetched_at_raw)
    except ValueError:
        return None
    if fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)
    pairs = data.get("pairs")
    if not isinstance(pairs, dict):
        return None
    normalized: Dict[str, List[str]] = {}
    for exchange, symbols in pairs.items():
        if not isinstance(symbols, list):
            continue
        normalized[exchange.upper()] = [str(sym).upper() for sym in symbols]
    if not normalized:
        return None
    return PairCache(fetched_at=fetched_at, pairs=normalized)


def _save_cache(cache: PairCache) -> None:
    payload = {
        "fetched_at": cache.fetched_at.isoformat(),
        "pairs": cache.pairs,
    }
    CACHE_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _is_cache_valid(cache: PairCache | None) -> bool:
    if cache is None:
        return False
    return _utcnow() - cache.fetched_at < CACHE_MAX_AGE


def _fetch_coinbase_pairs(limit: int) -> List[str]:
    stats_resp = requests.get(COINBASE_PRODUCTS_STATS_URL, timeout=15)
    stats_resp.raise_for_status()
    stats_data = stats_resp.json()
    if not isinstance(stats_data, dict):
        raise ValueError("Unexpected Coinbase stats response")

    products_resp = requests.get(COINBASE_PRODUCTS_URL, timeout=15)
    products_resp.raise_for_status()
    products = products_resp.json()
    active_products = {
        str(item.get("id")).upper()
        for item in products
        if isinstance(item, dict)
        and not item.get("trading_disabled")
        and item.get("status") == "online"
    }

    def _volume(entry: dict) -> float:
        stats_24h = entry.get("stats_24hour") or entry.get("stats_24Hour") or {}
        try:
            return float(stats_24h.get("volume") or 0.0)
        except (TypeError, ValueError):
            return 0.0

    pairs: List[Tuple[str, float]] = []
    for product_id, entry in stats_data.items():
        if entry is None:
            continue
        product_upper = str(product_id).upper()
        if product_upper not in active_products:
            continue
        vol = _volume(entry)
        pairs.append((product_upper, vol))

    pairs.sort(key=lambda item: item[1], reverse=True)
    top = [product for product, _ in pairs[:limit]]
    return sorted(top)


def _download_pairs(limit: int) -> PairCache:
    fetched_at = _utcnow()
    pairs = {
        "COINBASE": _fetch_coinbase_pairs(limit),
    }
    cache = PairCache(fetched_at=fetched_at, pairs=pairs)
    _save_cache(cache)
    return cache


def ensure_pairs(limit: int = DEFAULT_LIMIT) -> PairCache:
    cache = _load_cache()
    if _is_cache_valid(cache):
        return cache
    try:
        return _download_pairs(limit)
    except Exception:
        if cache is not None:
            return cache
        raise


def get_pairs_map(limit: int = DEFAULT_LIMIT) -> Tuple[Dict[str, List[str]], datetime | None]:
    cache = ensure_pairs(limit)
    pairs_map = {}
    for exchange, symbols in cache.pairs.items():
        pairs_map[exchange.upper()] = [sym.upper() for sym in symbols[:limit]]
    return pairs_map, cache.fetched_at


def get_pairs_for_exchange(exchange: str, limit: int = DEFAULT_LIMIT) -> List[str]:
    exchange_key = str(exchange).upper()
    cache = ensure_pairs(limit)
    return [sym.upper() for sym in cache.pairs.get(exchange_key, [])[:limit]]


def pairs_to_choices(exchange: str, limit: int = DEFAULT_LIMIT) -> List[str]:
    return [f"{exchange.lower()}:{symbol.lower()}" for symbol in get_pairs_for_exchange(exchange, limit)]
