import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from dotenv import load_dotenv  # type: ignore
from db_utils import get_last_dates

load_dotenv()

HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "3650"))


def _today_utc_date_str() -> str:
    return datetime.utcnow().date().isoformat()


def _default_start_date_str() -> str:
    today = datetime.utcnow().date()
    start = today - timedelta(days=HISTORY_DAYS)
    return start.isoformat()


def build_update_plan(
    conn,
    top_coins: List[Dict[str, Any]],
) -> List[Dict[str, str]]:
    print("[INFO] FILTER 2: Building update plan from DB...")

    last_dates = get_last_dates(conn)
    today_str = _today_utc_date_str()
    default_start = _default_start_date_str()

    plan: List[Dict[str, str]] = []
    for coin in top_coins:
        symbol = str(coin["symbol"])
        last_date = last_dates.get(symbol)

        if last_date is None:
            start_date = default_start
        else:
            try:
                dt = datetime.strptime(last_date, "%Y-%m-%d").date()
            except ValueError:
                dt = datetime.utcnow().date()
            start_date = (dt + timedelta(days=1)).isoformat()

        if start_date > today_str:
            continue

        plan.append(
            {
                "symbol": symbol,
                "start_date": start_date,
                "end_date": today_str,
            }
        )

    print(f"[INFO] FILTER 2: Plan size = {len(plan)} symbols needing updates")
    return plan
