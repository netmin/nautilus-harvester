# nautilus‑harvester

*Universal market‑data downloader that mirrors Binance OHLCV bars into a Nautilus‑Trader Parquet catalog*

---

## Features

| ✔                        | Description                                                                                               |
| ------------------------ | --------------------------------------------------------------------------------------------------------- |
| **Multi‑exchange**       | Binance **spot** & **USD‑M perpetual** supported out‑of‑the‑box; other CCXT venues pluggable              |
| **Daily *and* monthly**  | Pass `--period daily` or `--period monthly` (or use `--daily`) – folder structure is **identical to Binance archives** |
| **Incremental**          | Writes only when a partition is missing – never overwrites existing Parquet files                         |
| **API fallback**         | If a ZIP archive is not published yet (or 404), the loader seamlessly pulls the same range via REST API   |
| **Fully NT‑compatible**  | Arrow schema & directory layout recognised by `ParquetDataCatalog`                                        |
| **Parallel**             | Thread‑pool fetcher (one REST client per thread) ⇒ lightning‑fast bulk sync                               |
| **Zero global installs** | Powered by [`uv`](https://github.com/astral-sh/uv) for ultra‑fast isolated venvs                          |
| **Modern DX**            | Typed code, `ruff` + `black` pre‑commit, colour logs via `rich`                                           |

---

## Quick Start (≈15 sec)

```bash
# clone and enter the repo
git clone https://github.com/netmin/nautilus-harvester.git
cd nautilus-harvester

# create & activate an isolated env via *uv* (ultra‑fast)
uv venv && source .venv/bin/activate

# install runtime deps (cached wheels → seconds)
uv pip install -r requirements.txt

# download one year of BTC & ETH USD‑M perp *monthly* 1‑minute bars
python nautilus_harvester.py \
  --exchange  binance   \
  --market    futures   \
  --symbols   BTCUSDT ETHUSDT \
  --period    monthly   \
  --start     2024-01   \
  --end       2024-12   \
  --interval  1m        \
  --catalog   ./catalog \
  --workers   6
```

### Daily example

```bash
python nautilus_harvester.py \
  --exchange  binance      \
  --market    spot         \
  --symbols   BTCUSDT      \
  --period    daily        \
  --start     2024-05-15   \
  --end       2024-05-20   \
  --interval  1m           \
  --catalog   ./catalog    \
  --workers   4
```

Resulting tree (daily layout example):

```text
catalog/
└─ futures/um/daily/klines/BTCUSDT/1m/
   ├─ BTCUSDT-1m-2024-05-15.parquet
   ├─ BTCUSDT-1m-2024-05-16.parquet
   └─ …
```

Monthly layout looks like:

```text
catalog/
└─ futures/um/monthly/klines/BTCUSDT/1m/
   └─ BTCUSDT-1m-2024-05.parquet
```

Both layouts are automatically discovered by **Nautilus‑Trader**:

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
catalog = ParquetDataCatalog("./catalog")
```

---

## CLI Reference

| flag            | required | example                   | description                                   |
| --------------- | -------- | ------------------------- | --------------------------------------------- |
| `--exchange`    | ✔        | `binance`                 | CCXT exchange id                              |
| `--market`      | ✔        | `spot` / `futures`        | `futures` ⇒ USD‑M perpetual                   |
| `--futures_sub` | ✖        | `um` / `cm`               | futures sub‑folder (`um`=USDT‑M, `cm`=COIN‑M) |
| `--symbols`     | ✔        | `BTCUSDT ETHUSDT`         | space‑separated list or `ALL`                 |
| `--period`      | ✖        | `daily` / `monthly`       | archive granularity (use `--daily` for short; default: `monthly`) |
| `--daily`       | ✖        |                           | shortcut for `--period daily` (kept for backward compatibility)   |
| `--start`       | ✔        | `2023-01` or `2023-01-15` | first month/day (inclusive)                                       |
| `--end`         | ✔        | `2025-01` or `2023-03-01` | last month/day (inclusive)                                        |
| `--interval`    | ✖        | `1m`                      | kline timeframe (default `1m`)                                    |
| `--catalog`     | ✖        | `./catalog`               | Parquet catalog root                                              |
| `--workers`     | ✖        | `6`                       | download threads                                                  |

---

## Project layout

```
nautilus-harvester/
├── LICENSE                # MIT
├── README.md              # you are here
├── requirements.txt       # runtime deps
├── dev-requirements.txt   # ruff, black, mypy
└── nautilus_harvester.py  # single‑file CLI
```

> **Tip:** the code is fully typed & linted. Run `ruff` and `black` pre‑commit hooks locally:
>
> ```bash
> uv pip install pre-commit
> pre-commit install   # auto‑fix on each commit
> ```

---

## Cron example (daily update, 03:00 UTC)

```cron
0 3 * * * cd /data/harvester && \
  source .venv/bin/activate && \
  python nautilus_harvester.py \
    --exchange  binance         \
    --market    futures         \
    --symbols   BTCUSDT ETHUSDT \
    --period    daily           \
    --start     2024-01-01      \
    --end       $(date +\%Y-\%m-\%d) \
    --catalog   /data/nt_catalog \
    --workers   6 >> harvest.log 2>&1
```

---

## Development / Linting

```bash
uv pip install -r dev-requirements.txt
ruff . --fix   # lint + auto‑fix
black .        # code style
```

---

## Contributing / Bug Reports

Found a bug, have a feature request or new exchange adapter idea?

1. **Open a GitHub Issue**
   • Click the *Issues* tab ↗ and choose **Bug report** or **Improvement** template.
   • Please include: steps to reproduce, full CLI command, log output and your OS / Python version.
   • For feature ideas add a short rationale (why it helps other users) and a minimal sketch of expected CLI flags.

2. **Pull‑requests welcome**
   • Fork → feature branch → `uv pip install -r dev-requirements.txt` → run `ruff`, `black`, `mypy` → submit PR.
   • Keep PRs focused; one logical change per PR.
   • CI (ruff+black+mypy) must pass before review.

3. **Security issues**
   • Please **do not** open a public issue; instead e‑mail `netmin@pm.me`.

---

## License

MIT © 2025
