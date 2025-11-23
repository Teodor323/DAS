# filter3.py
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pandas as pd  # type: ignore
import yfinance as yf  # type: ignore
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv  # type: ignore

from db_utils import insert_daily_history

load_dotenv()

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "32"))

BULK_BATCH_SIZE = int(os.getenv("BULK_BATCH_SIZE", "25"))
BULK_WORKERS = int(os.getenv("BULK_WORKERS", "4"))
BULK_MAX_RETRIES = int(os.getenv("BULK_MAX_RETRIES", "3"))


def safe_float(x: Any) -> float:
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def _parse_batch_df(
    df: pd.DataFrame,
    batch_symbols: List[str],
) -> List[List[Any]]:
    rows: List[List[Any]] = []

    if isinstance(df.columns, pd.MultiIndex):
        for sym in batch_symbols:
            if sym not in df.columns.levels[0]:
                continue
            sym_df = df[sym].dropna(how="all")
            for idx, row in sym_df.iterrows():
                date_str = idx.date().isoformat()
                o = safe_float(row.get("Open"))
                h = safe_float(row.get("High"))
                l = safe_float(row.get("Low"))
                c = safe_float(row.get("Close"))
                v = safe_float(row.get("Volume"))
                rows.append([sym, date_str, o, h, l, c, v])
    else:
        sym = batch_symbols[0]
        sym_df = df.dropna(how="all")
        for idx, row in sym_df.iterrows():
            date_str = idx.date().isoformat()
            o = safe_float(row.get("Open"))
            h = safe_float(row.get("High"))
            l = safe_float(row.get("Low"))
            c = safe_float(row.get("Close"))
            v = safe_float(row.get("Volume"))
            rows.append([sym, date_str, o, h, l, c, v])

    return rows


def bulk_download(conn, plan: List[Dict[str, Any]]) -> None:
    if not plan:
        return

    start_date = plan[0]["start_date"]
    end_date = plan[0]["end_date"]

    try:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        end_dt = datetime.utcnow()
    end_plus_one = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    symbols = [item["symbol"] for item in plan]
    batches = [
        symbols[i : i + BULK_BATCH_SIZE]
        for i in range(0, len(symbols), BULK_BATCH_SIZE)
    ]
    total_batches = len(batches)

    print(
        f"[INFO] FILTER 3 BULK: {len(symbols)} symbols, "
        f"{total_batches} batches of up to {BULK_BATCH_SIZE}, "
        f"workers={BULK_WORKERS}"
    )

    def fetch_batch_rows(batch_index: int, batch_symbols: List[str]) -> List[List[Any]]:
        attempt = 0
        while attempt < BULK_MAX_RETRIES:
            attempt += 1
            try:
                df = yf.download(
                    tickers=batch_symbols,
                    start=start_date,
                    end=end_plus_one,
                    interval="1d",
                    auto_adjust=False,
                    group_by="ticker",
                    threads=False,
                    progress=False,
                )
            except Exception as e:
                wait = min(5.0, 0.5 * attempt)
                print(
                    f"[ERR] BULK batch {batch_index+1}/{total_batches} "
                    f"attempt {attempt}/{BULK_MAX_RETRIES} "
                    f"symbols={len(batch_symbols)} error: {e!r}, "
                    f"retrying in {wait:.1f}s"
                )
                time.sleep(wait)
                continue

            if df.empty:
                print(
                    f"[WARN] BULK batch {batch_index+1}/{total_batches} "
                    f"attempt {attempt}/{BULK_MAX_RETRIES} returned empty DataFrame"
                )
                return []

            rows = _parse_batch_df(df, batch_symbols)
            print(
                f"[OK] BULK batch {batch_index+1}/{total_batches} "
                f"attempt {attempt} symbols={len(batch_symbols)} rows={len(rows)}"
            )
            return rows

        print(
            f"[ERR] BULK batch {batch_index+1}/{total_batches} "
            f"FAILED after {BULK_MAX_RETRIES} attempts; skipping"
        )
        return []

    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=BULK_WORKERS) as executor:
        futures = {
            executor.submit(fetch_batch_rows, i, batch): i
            for i, batch in enumerate(batches)
        }

        done_batches = 0
        for fut in as_completed(futures):
            rows = fut.result()
            if rows:
                insert_daily_history(conn, rows)
            done_batches += 1
            elapsed = time.perf_counter() - start_time
            print(
                f"[PROGRESS] BULK: {done_batches}/{total_batches} batches done "
                f"({elapsed:.1f}s elapsed)"
            )


def run_history_fetch(conn, plan: List[Dict[str, Any]]) -> None:
    total = len(plan)
    if total == 0:
        print("[INFO] FILTER 3: No updates needed; DB already up to date.")
        return

    ranges = {(p["start_date"], p["end_date"]) for p in plan}
    if len(ranges) == 1 and total > 1:
        print(
            f"[INFO] FILTER 3: Uniform date range {next(iter(ranges))} "
            f"-> bulk mode"
        )
        bulk_download(conn, plan)
        return

    print(
        f"[INFO] FILTER 3: Mixed date ranges -> per-symbol mode with "
        f"MAX_WORKERS={MAX_WORKERS}"
    )

    def fetch_symbol_rows(item: Dict[str, Any]) -> List[List[Any]]:
        """
        Worker: fetch one symbol's history and return rows.
        NO DB access here.
        """
        symbol = item["symbol"]
        start_date = item["start_date"]
        end_date = item["end_date"]

        try:
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            end_dt = datetime.utcnow()
        end_plus_one = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d")

        try:
            t = yf.Ticker(symbol)
            df = t.history(
                start=start_date,
                end=end_plus_one,
                interval="1d",
                auto_adjust=False,
                timeout=REQUEST_TIMEOUT,
            )
        except TypeError:
            t = yf.Ticker(symbol)
            df = t.history(
                start=start_date,
                end=end_plus_one,
                interval="1d",
                auto_adjust=False,
            )
        except Exception as e:
            print(f"[ERR] {symbol:12s} history error: {e}")
            return []

        if df.empty:
            print(f"[WARN] {symbol:12s} per-symbol mode: empty DataFrame")
            return []

        rows: List[List[Any]] = []
        for idx, row in df.iterrows():
            date_str = idx.date().isoformat()
            o = safe_float(row.get("Open"))
            h = safe_float(row.get("High"))
            l = safe_float(row.get("Low"))
            c = safe_float(row.get("Close"))
            v = safe_float(row.get("Volume"))
            rows.append([symbol, date_str, o, h, l, c, v])

        print(f"[OK] {symbol:12s} rows={len(rows)}")
        return rows

    start_time = time.perf_counter()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_symbol_rows, item) for item in plan]

        done = 0
        for fut in as_completed(futures):
            rows = fut.result()
            if rows:
                insert_daily_history(conn, rows)
            done += 1
            if done % 20 == 0 or done == total:
                elapsed = time.perf_counter() - start_time
                print(
                    f"[PROGRESS] FILTER 3: {done}/{total} symbols fetched "
                    f"({elapsed:.1f}s elapsed)"
                )
