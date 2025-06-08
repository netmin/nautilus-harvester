# nautilus-harvester

*Universal market‑data downloader that mirrors OHLCV bars into a Nautilus‑Trader Parquet catalog*

## Features

| ✔                        | Description                                                                    |
| ------------------------ |--------------------------------------------------------------------------------|
| **Multi‑exchange**       | Binance spot & USD‑M perpetual out‑of‑the‑box; other CCXT venues pluggable     |
| **Incremental**          | Writes `year/month/0.parquet` only when a month is missing – never overwrites  |
| **Fully NT‑compatible**  | Arrow schema & folder layout recognised by `ParquetDataCatalog`                |
| **Parallel**             | Thread‑pool fetcher (one REST client per thread) ⇒ fast bulk sync              |
| **Zero global installs** | Uses [`uv`](https://github.com/astral-sh/uv) for lightning‑fast isolated venvs |
| **Modern DX**            | `ruff` + `black` pre‑commit, typed code, colour logs via `rich`                |

---

## Quick Start (15 sec)

```bash
# clone and enter the repo
git clone https://github.com/netmin/nautilus-harvester.git
cd nautilus-harvester

# create & activate an isolated env via *uv* (ultra‑fast)
uv venv && source .venv/bin/activate

# install runtime deps (cached wheels → seconds)
uv pip install -r requirements.txt

# download one year of BTC & ETH USD‑M perp minutes
python nautilus_harvester.py \
  --exchange binance   \
  --market   futures   \
  --symbols  BTCUSDT ETHUSDT \
  --start    2024-01   \
  --end      2024-12   \
  --catalog  ./catalog \
  --workers  6
```

Resulting tree (example):

```text
catalog/
└─ data/
   └─ bar/
      ├─ btcusdt.binance-1-minute-last-external/
      │   └─ 2024/06/0.parquet
      └─ ethusdt.binance-1-minute-last-external/
          └─ 2024/06/0.parquet
```

Use in backtests / live NT nodes:

```python
from nautilus_trader.persistence.catalog import ParquetDataCatalog
from nautilus_trader.backtest.engine import BacktestEngine

catalog = ParquetDataCatalog("./catalog")
engine  = BacktestEngine(data_catalog_path="./catalog")
# engine.request_bars(...) etc.
```

---

## CLI Reference

| flag         | required | example            | description                 |
| ------------ | -------- | ------------------ |-----------------------------|
| `--exchange` | ✔        | `binance`          | CCXT exchange id            |
| `--market`   | ✔        | `spot` / `futures` | `futures` ⇒ USD‑M perpetual |
| `--symbols`  | ✔        | `BTCUSDT ETHUSDT`  | space‑separated list        |
| `--start`    | ✔        | `2023-01`          | first month (inclusive)     |
| `--end`      | ✔        | `2025-01`          | last month (inclusive)      |
| `--catalog`  | ✖        | `./catalog`        | Parquet catalog root        |
| `--workers`  | ✖        | `6`                | download threads            |

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

> **Tip:** the code is 100 % typed & linted. Run `ruff` and `black` pre‑commit hooks locally:
>
> ```bash
> uv pip install pre-commit
> pre-commit install   # auto‑fix on each commit
> ```

---

## Cron example (daily update, 03:00 UTC)

```cron
0 3 * * * cd /data/harvester && \
  source .venv/bin/activate && \
  python nautilus_harvester.py \
    --exchange binance --market futures \
    --symbols BTCUSDT ETHUSDT \
    --start 2024-01 --end $(date +\%Y-\%m) \
    --catalog /data/nt_catalog \
    --workers 6 >> harvest.log 2>&1
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
