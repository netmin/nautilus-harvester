#!/usr/bin/env python3
# ---------------------------------------------------------------
# Binance Monthly Loader
# Download 1-minute OHLCV bars from Binance (spot or USD-M perp)
# and store them in a Nautilus-Trader Parquet catalog directory
# grouped by year/month.
#
# Author: Your Name <you@example.com>
# License: MIT
# ---------------------------------------------------------------
from __future__ import annotations

import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import List, Tuple
from rich.logging import RichHandler

import ccxt
import pandas as pd
from tqdm import tqdm

from nautilus_trader.model import BarType
from nautilus_trader.model.currencies import USDT, BTC  # predefined Currency objects
from nautilus_trader.model.enums import CurrencyType
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CurrencyPair, CryptoPerpetual
from nautilus_trader.model.objects import Currency, Money, Price, Quantity
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.persistence.catalog.types import CatalogWriteMode
from nautilus_trader.persistence.wranglers import BarDataWrangler

# ------------- configuration -------------------------------------------------

logging.basicConfig(
    level="INFO",
    format="%(message)s",         # Rich печатает сам
    datefmt="[%H:%M:%S]",
    handlers=[RichHandler(rich_tracebacks=True, markup=True)],
)
LOGGER = logging.getLogger("loader")

CAT_LOCK = Lock()  # ensure catalog writes are thread-safe
# -----------------------------------------------------------------------------


# ---------- helper functions -------------------------------------------------
def month_range(start: str, end: str) -> List[Tuple[int, int]]:
    """
    Return list of (year, month) tuples between YYYY-MM bounds inclusive.
    """
    cur = datetime.strptime(start, "%Y-%m").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m").replace(tzinfo=timezone.utc)
    out: List[Tuple[int, int]] = []
    while cur <= end_dt:
        out.append((cur.year, cur.month))
        month = cur.month % 12 + 1
        year = cur.year + (cur.month // 12)
        cur = cur.replace(year=year, month=month, day=1)
    return out


# --- dynamic Currency fallback ----------------------------------------------
import nautilus_trader.model.currencies as nt_cur


def get_currency(code: str) -> Currency:
    """
    Return a Currency instance: built-in constant if available,
    otherwise create a generic crypto Currency (8-digit precision).
    """
    if hasattr(nt_cur, code):
        return getattr(nt_cur, code)
    return Currency(code, 8, 0, code, CurrencyType.CRYPTO)


# ---------- instrument builders ---------------------------------------------
def build_instrument(symbol: str, venue: str, market: str):
    """
    Build a minimal Instrument object for Nautilus-Trader.
    Supports spot pairs (*USDT quote*) and USD-M perpetuals.
    """
    iid = InstrumentId(Symbol(symbol.replace("/", "")), Venue(venue))
    price_inc = Price(0.01, 2)
    lot = Quantity(1, 0)

    if market == "spot":
        base, quote = symbol.split("/")
        return CurrencyPair(
            instrument_id=iid,
            base=get_currency(base),
            quote=get_currency(quote),
            price_precision=2,
            price_increment=price_inc,
            lot_size=lot,
            min_notional=Money("10", get_currency(quote)),
            ts_event=0,
            ts_init=0,
        )

    base_code = symbol.replace("USDT", "")
    # Handle both new/old CryptoPerpetual signatures
    try:
        return CryptoPerpetual(
            instrument_id=iid,
            raw_symbol=Symbol(symbol),
            base_currency=get_currency(base_code),
            quote_currency=USDT,
            settlement_currency=USDT,
            is_inverse=False,
            price_precision=2,
            size_precision=3,
            price_increment=price_inc,
            size_increment=Quantity(0.001, 3),
            ts_event=0,
            ts_init=0,
            margin_init=Decimal("0.05"),
            margin_maint=Decimal("0.025"),
            maker_fee=Decimal("0.0002"),
            taker_fee=Decimal("0.0005"),
        )
    except TypeError:  # legacy constructor
        return CryptoPerpetual(
            instrument_id=iid,
            base=get_currency(base_code),
            quote=USDT,
            price_precision=2,
            price_increment=price_inc,
            lot_size=lot,
            tick_value=Money("1", USDT),
            contract_value=Decimal("1"),
            ts_event=0,
            ts_init=0,
        )


# ---------- data acquisition -------------------------------------------------
def fetch_month(exchange: ccxt.Exchange, symbol: str, year: int, month: int) -> pd.DataFrame | None:
    """
    Fetch one calendar month of 1-minute klines via CCXT.
    Returns DataFrame indexed by UTC close-time.
    """
    since = exchange.parse8601(f"{year}-{month:02d}-01T00:00:00Z")
    until = (
        exchange.parse8601(f"{year+1}-01-01T00:00:00Z")
        if month == 12
        else exchange.parse8601(f"{year}-{month+1:02d}-01T00:00:00Z")
    )
    limit, tf, out = 1000, "1m", []
    t = since
    while t < until:
        ohlcv = exchange.fetch_ohlcv(symbol, tf, since=t, limit=limit)
        if not ohlcv:
            break
        out.extend([r for r in ohlcv if r[0] < until])
        t = ohlcv[-1][0] + 60_000
    if not out:
        return None
    df = pd.DataFrame(out, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.set_index("ts", inplace=True)
    return df[["open", "high", "low", "close", "volume"]]


def fetch_save(
    exchange: ccxt.Exchange,
    symbol: str,
    year: int,
    month: int,
    wrangler: BarDataWrangler,
    catalog: ParquetDataCatalog,
):
    """
    Download a single month and write to catalog (thread-safe).
    """
    df = fetch_month(exchange, symbol, year, month)
    if df is None or df.empty:
        LOGGER.warning("No data for %s %04d-%02d", symbol, year, month)
        return

    bars = wrangler.process(df)
    bar_dir = str(wrangler.bar_type).lower()  # bar/<bar_type>/
    basename_template = f"{year}/{month:02d}/{{i}}"  # yyyy/mm/0.parquet

    with CAT_LOCK:
        target_dir = Path(catalog.path) / "data" / "bar" / bar_dir / f"{year}" / f"{month:02d}"
        target_dir.mkdir(parents=True, exist_ok=True)

        catalog.write_data(
            bars,
            write_mode=CatalogWriteMode.NEWFILE,
            basename_template=basename_template,
        )
    LOGGER.info("Saved %s %04d-%02d (%d bars)", symbol, year, month, len(bars))


# ---------- CLI entry-point --------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Download monthly 1m OHLCV bars into a Nautilus catalog")
    parser.add_argument("--market", choices=["spot", "futures"], required=True, help="spot or USD-M perp")
    parser.add_argument("--symbols", nargs="+", required=True, help="space-separated list e.g. BTCUSDT ETHUSDT")
    parser.add_argument("--start", required=True, help="first month YYYY-MM")
    parser.add_argument("--end", required=True, help="last month YYYY-MM")
    parser.add_argument("--catalog", default="./catalog", help="path to Parquet catalog")
    parser.add_argument("--workers", type=int, default=4, help="download threads")
    args = parser.parse_args()

    catalog = ParquetDataCatalog(Path(args.catalog))
    months = month_range(args.start, args.end)
    LOGGER.info("Selected %d symbols, %d months", len(args.symbols), len(months))

    tasks = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        for sym in args.symbols:
            instr = build_instrument(sym, "BINANCE", args.market)
            bar_type = BarType.from_str(f"{instr.id}-1-MINUTE-LAST-EXTERNAL")
            wrangler = BarDataWrangler(bar_type, instr)

            exchange = ccxt.binance({"enableRateLimit": True})
            for year, month in months:
                tasks.append(
                    pool.submit(fetch_save, exchange, sym, year, month, wrangler, catalog)
                )

        for fut in tqdm(as_completed(tasks), total=len(tasks)):
            fut.result()  # propagate exceptions

    LOGGER.info("All done ✅")


if __name__ == "__main__":
    main()
