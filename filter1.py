import os
import time
from typing import Any, Dict, List, Set

import requests
from dotenv import load_dotenv  # type: ignore

from db_utils import insert_top_coins

load_dotenv()

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

YF_SCREENER_URLS = [
    "https://query1.finance.yahoo.com/v1/finance/screener/predefined/saved",
    "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved",
]

SCR_ID = "all_cryptocurrencies_us"


def _fetch_page_json(start: int, count: int) -> List[Dict[str, Any]]:
    last_err: Exception | None = None

    params = {
        "formatted": "false",
        "lang": "en-US",
        "region": "US",
        "scrIds": SCR_ID,
        "start": str(start),
        "count": str(count),
    }

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    }

    for base in YF_SCREENER_URLS:
        try:
            resp = requests.get(
                base,
                params=params,
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            last_err = e
            continue

        try:
            finance = data.get("finance", {})
            results = finance.get("result", [])
            if not results:
                continue
            quotes = results[0].get("quotes", [])
            if not isinstance(quotes, list):
                continue
            return quotes
        except Exception as e:
            last_err = e
            continue

    if last_err is not None:
        raise RuntimeError(
            f"Failed to fetch screener page start={start}, "
            f"count={count}: {last_err}"
        )

    return []


def _extract_market_cap(q: Dict[str, Any]) -> float:
    raw_mcap = q.get("marketCap")
    if isinstance(raw_mcap, dict):
        raw_mcap = raw_mcap.get("raw")

    try:
        return float(raw_mcap)
    except (TypeError, ValueError):
        return 0.0


def run_filter1(conn) -> List[Dict[str, Any]]:
    target_count = 1000
    page_size = 100
    max_pages = 20 

    print("[INFO] FILTER 1: Fetching Yahoo screener JSON (cryptos)...")
    collected: List[Dict[str, Any]] = []
    seen_symbols: Set[str] = set()

    start = 0
    for page in range(max_pages):
        t0 = time.perf_counter()
        try:
            quotes = _fetch_page_json(start=start, count=page_size)
        except Exception as e:
            print(
                f"[ERROR] FILTER 1: page {page+1}/{max_pages} "
                f"start={start} failed: {e}"
            )
            break

        elapsed = time.perf_counter() - t0

        new_count = 0
        for q in quotes:
            symbol = str(q.get("symbol"))
            if not symbol or symbol in seen_symbols:
                continue

            name = q.get("shortName") or q.get("longName") or symbol
            mcap_val = _extract_market_cap(q)

            seen_symbols.add(symbol)
            collected.append(
                {
                    "symbol": symbol,
                    "name": name,
                    "market_cap": mcap_val,
                    "rank": None,
                }
            )
            new_count += 1

        print(
            f"[INFO] FILTER 1: Page {page+1}/{max_pages} "
            f"start={start}, quotes={len(quotes)}, "
            f"new symbols={new_count}, total={len(collected)} "
            f"({elapsed:.2f}s)"
        )

        start += page_size

        if not quotes:
            break
        if new_count == 0:
            break
        if len(collected) >= target_count:
            break

        time.sleep(0.2)

    collected.sort(key=lambda c: c["market_cap"], reverse=True)
    collected = collected[:target_count]
    for i, c in enumerate(collected, start=1):
        c["rank"] = i

    print(f"[INFO] FILTER 1: Final coin count: {len(collected)}")

    insert_top_coins(conn, collected, source="yahoo_screener_api")
    print("[INFO] FILTER 1: Wrote coins into DB 'coins' table")

    return collected
