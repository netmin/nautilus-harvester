#!/usr/bin/env python3
"""Nautilus‑Harvester
====================
Universal market‑data downloader that mirrors OHLCV bars into a Nautilus‑Trader
Parquet catalog. Focuses on Binance spot & USD‑M perpetual at **1‑minute**
resolution.

Key features
------------
* **Incremental** – never overwrites existing Parquet files, writes only the
  missing month/day slices.
* **Dual source** – tries Binance public *archive* first, falls back to REST
  *API* transparently.
* **Parallel** – thread‑pool fetcher (one client per worker) ⇒ fast bulk sync.
* **Colour logs** – friendly structured output via *rich*.
* **ALL keyword** – expands to the complete list of active USDT pairs.

Usage example
-------------
```bash
python nautilus_harvester.py \
  --exchange binance --market futures \
  --symbols ALL \
  --start 2024-01 --end 2024-12 \
  --period daily                 # or --daily for short \
  --catalog ./catalog \
  --workers 8
```
"""
from __future__ import annotations

import argparse
import calendar
import concurrent.futures as cf
import datetime as dt
import io
import logging
import os
import pathlib
import sys
import time
from typing import Iterator, List, Tuple
from zipfile import ZipFile

import pandas as pd
import requests
from rich.logging import RichHandler
from tqdm import tqdm

# --------------------------------------------------------------------------- #
# Logging setup                                                               #
# --------------------------------------------------------------------------- #

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s\n%(filename)s:%(lineno)d"
logging.basicConfig(
    format=LOG_FORMAT,
    datefmt="%H:%M:%S",
    level=logging.INFO,
    handlers=[RichHandler(rich_tracebacks=True, markup=False)],
)
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# CLI                                                                         #
# --------------------------------------------------------------------------- #


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download Binance OHLCV 1‑minute bars into a Nautilus‑Trader catalog",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument("--exchange", required=True, help="CCXT exchange id (only 'binance' supported)")
    parser.add_argument(
        "--market",
        required=True,
        choices=["spot", "futures"],
        help="'futures' → USD‑M perpetual",
    )
    parser.add_argument(
        "--symbols",
        nargs="+",
        required=True,
        help="space‑separated list of symbols or the keyword ALL",
    )
    parser.add_argument("--start", required=True, help="First month YYYY‑MM inclusive")
    parser.add_argument("--end", required=True, help="Last month YYYY‑MM inclusive")
    parser.add_argument("--interval", default="1m", help="kline timeframe")
    parser.add_argument("--catalog", default="./catalog", help="Parquet catalog root path")
    parser.add_argument("--workers", type=int, default=os.cpu_count() or 4, help="Thread count")

    period_grp = parser.add_mutually_exclusive_group()
    period_grp.add_argument(
        "--period",
        choices=["monthly", "daily"],
        default="monthly",
        help="Archive granularity; 'daily' downloads one zip per day",
    )
    period_grp.add_argument(
        "--daily",
        action="store_true",
        help="Shortcut for --period daily (kept for backward compatibility)",
    )

    parser.add_argument(
        "--fail-on-400",
        action="store_true",
        help="Abort on single HTTP‑400 (default: skip & continue)",
    )

    ns = parser.parse_args()

    # Harmonise legacy flag
    if ns.daily:
        ns.period = "daily"

    ns.symbols = resolve_symbols(ns.symbols, ns.exchange, ns.market)
    return ns


# --------------------------------------------------------------------------- #
# Helpers                                                                     #
# --------------------------------------------------------------------------- #


def resolve_symbols(symbols_arg: List[str], exchange: str, market: str) -> List[str]:
    """Expand the single keyword 'ALL' to the full Binance USDT list."""
    if len(symbols_arg) == 1 and symbols_arg[0].upper() == "ALL":
        return fetch_all_symbols(exchange, market)
    return [s.upper() for s in symbols_arg]


def fetch_all_symbols(exchange: str, market: str) -> List[str]:
    if exchange != "binance":
        raise ValueError("'ALL' expansion is only implemented for Binance")

    url = (
        "https://api.binance.com/api/v3/exchangeInfo"
        if market == "spot"
        else "https://fapi.binance.com/fapi/v1/exchangeInfo"
    )

    resp = requests.get(url, timeout=10)
    resp.raise_for_status()

    out: list[str] = []
    for s in resp.json()["symbols"]:
        if s.get("status") != "TRADING":
            continue
        quote = s.get("quoteAsset", "USDT")
        if quote != "USDT":
            continue
        if market == "futures" and s.get("contractType") != "PERPETUAL":
            continue
        out.append(s["symbol"].upper())

    logger.info(f"Expanded ALL → {len(out)} symbols")
    return out


def month_iter(start: dt.date, end: dt.date) -> Iterator[Tuple[int, int]]:
    """Yield (year, month) pairs from *start* to *end* inclusive."""
    curr = dt.date(start.year, start.month, 1)
    while curr <= end:
        yield curr.year, curr.month
        y_inc, m_zero_based = divmod(curr.month, 12)
        curr = dt.date(curr.year + y_inc, m_zero_based + 1, 1)


def day_iter(start: dt.date, end: dt.date) -> Iterator[dt.date]:
    delta = end - start
    for i in range(delta.days + 1):
        yield start + dt.timedelta(days=i)


def archive_url(exchange: str, market: str, symbol: str, interval: str, date_obj: dt.date, period: str) -> str:
    if exchange != "binance":
        raise ValueError("Only Binance supported for archives")

    root = "futures/um" if market == "futures" else "spot"
    sub = "daily" if period == "daily" else "monthly"
    date_part = date_obj.strftime("%Y-%m-%d" if period == "daily" else "%Y-%m")

    return (
        f"https://data.binance.vision/data/{root}/{sub}/klines/{symbol}/{interval}/"
        f"{symbol}-{interval}-{date_part}.zip"
    )


def archive_exists(url: str) -> bool:
    try:
        return requests.head(url, timeout=10, allow_redirects=True).status_code == 200
    except requests.RequestException as exc:
        logger.warning(f"HEAD failed for {url}: {exc}")
        return False


def read_csv_bytes(b: bytes) -> pd.DataFrame:
    """Parse a raw Binance klines CSV (11 or 12 cols, with/without header)."""
    buf = io.BytesIO(b)

    # Try *no header* first – fastest path for archive files
    df = pd.read_csv(buf, header=None)

    # Detect accidental header row (starts with a string)
    if isinstance(df.iloc[0, 0], str) and df.iloc[0, 0].lower().startswith("open_time"):
        buf.seek(0)
        df = pd.read_csv(buf)  # header=0 implied

    n_cols = df.shape[1]
    if n_cols not in (11, 12):
        raise ValueError(f"Unexpected column count {n_cols}")

    cols_11 = [
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "trades",
        "taker_buy_base_vol",
        "taker_buy_quote_vol",
    ]
    cols_12 = cols_11 + ["ignore"]

    df.columns = cols_12 if n_cols == 12 else cols_11

    # Drop optional column if present
    if "ignore" in df.columns:
        df.drop(columns=["ignore"], inplace=True)

    float_cols = [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "quote_asset_volume",
        "taker_buy_base_vol",
        "taker_buy_quote_vol",
    ]
    df[float_cols] = df[float_cols].astype(float)
    df["trades"] = df["trades"].astype(int)
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    return df


def fetch_via_api(symbol: str, market: str, interval: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    base = (
        "https://api.binance.com/api/v3/klines"
        if market == "spot"
        else "https://fapi.binance.com/fapi/v1/klines"
    )

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": 1000,
        "startTime": start_ms,
        "endTime": end_ms,
    }

    data: list[list] = []
    while True:
        r = requests.get(base, params=params, timeout=15)
        if r.status_code == 400:
            raise requests.HTTPError("400 Bad Request", response=r)
        r.raise_for_status()
        batch: list[list] = r.json()
        if not batch:
            break
        data.extend(batch)
        params["startTime"] = batch[-1][0] + 60_000  # +1 minute
        if params["startTime"] >= end_ms:
            break
        time.sleep(0.05)

    if not data:
        raise ValueError("Empty API payload – check symbol/date range")

    csv_bytes = pd.DataFrame(data).to_csv(index=False).encode()
    return read_csv_bytes(csv_bytes)


def fetch_archive(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    with ZipFile(io.BytesIO(r.content)) as z:
        first_file = z.namelist()[0]
        return read_csv_bytes(z.read(first_file))


def save_dataframe(
    df: pd.DataFrame,
    catalog_root: pathlib.Path,
    exchange: str,
    market: str,
    symbol: str,
    interval: str,
    date_obj: dt.date,
    period: str,
) -> None:
    root = f"futures/um" if market == "futures" else "spot"
    sub = "daily" if period == "daily" else "monthly"
    dest_dir = catalog_root / root / sub / "klines" / symbol / interval
    dest_dir.mkdir(parents=True, exist_ok=True)

    date_str = date_obj.strftime("%Y-%m-%d") if period == "daily" else date_obj.strftime("%Y-%m")
    filename = f"{symbol}-{interval}-{date_str}.parquet"
    dest_path = dest_dir / filename

    if dest_path.exists():
        logger.debug(f"Skip existing {dest_path}")
        return

    df.to_parquet(dest_path, index=False)
    logger.info(f"Saved {dest_path} ({len(df)} rows)")


# --------------------------------------------------------------------------- #
# Task processors                                                             #
# --------------------------------------------------------------------------- #


def process_period(task: Tuple[str, dt.date, argparse.Namespace]):
    symbol, date_obj, ns = task

    url = archive_url(ns.exchange, ns.market, symbol, ns.interval, date_obj, ns.period)
    df: pd.DataFrame | None = None

    # 1) Archive
    if archive_exists(url):
        try:
            df = fetch_archive(url)
        except Exception as exc:  # noqa: BLE001
            logger.warning(f"Parse failed for {url} ({exc}) – fallback to API")

    # 2) API fallback
    if df is None:
        if ns.period == "daily":
            start_ms = int(dt.datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=dt.timezone.utc).timestamp() * 1000)
            end_ms = start_ms + 86_400_000 - 1
        else:  # monthly
            start_ms = int(dt.datetime(date_obj.year, date_obj.month, 1, tzinfo=dt.timezone.utc).timestamp() * 1000)
            last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
            end_ms = int(dt.datetime(date_obj.year, date_obj.month, last_day, 23, 59, 59, tzinfo=dt.timezone.utc).timestamp() * 1000)

        try:
            df = fetch_via_api(symbol, ns.market, ns.interval, start_ms, end_ms)
        except requests.HTTPError as http_exc:
            if http_exc.response.status_code == 400 and not ns.fail_on_400:
                logger.error(f"Task failed for {symbol} ({date_obj}): {http_exc}")
                return
            raise

    save_dataframe(
        df,
        pathlib.Path(ns.catalog),
        ns.exchange,
        ns.market,
        symbol,
        ns.interval,
        date_obj,
        ns.period,
    )


# --------------------------------------------------------------------------- #
# Main                                                                         #
# --------------------------------------------------------------------------- #


def main() -> None:
    ns = parse_args()

    start_date = dt.datetime.strptime(ns.start, "%Y-%m").date().replace(day=1)
    end_tmp = dt.datetime.strptime(ns.end, "%Y-%m").date().replace(day=1)
    last_day_of_end_month = calendar.monthrange(end_tmp.year, end_tmp.month)[1]
    end_date = end_tmp.replace(day=last_day_of_end_month)

    dates: List[dt.date] = []
    if ns.period == "monthly":
        for y, m in month_iter(start_date, end_date):
            dates.append(dt.date(y, m, 1))
    else:
        dates.extend(day_iter(start_date, end_date))

    tasks: List[Tuple[str, dt.date, argparse.Namespace]] = [
        (sym, d, ns) for sym in ns.symbols for d in dates
    ]

    logger.info(f"Starting {len(tasks)} tasks ({len(ns.symbols)} symbols × {len(dates)} {'months' if ns.period=='monthly' else 'dates'}) …")

    with cf.ThreadPoolExecutor(max_workers=ns.workers) as ex:
        for _ in tqdm(
            ex.map(process_period, tasks),
            total=len(tasks),
            desc="Total",
            ncols=80,
        ):
            pass

    logger.info("Done.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Interrupted by user (Ctrl+C)")
        sys.exit(130)
