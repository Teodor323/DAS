import os
import time
from dotenv import load_dotenv  # type: ignore

from db_utils import get_connection, init_db, DB_WRITE_TIME
from filter1 import run_filter1
from filter2 import build_update_plan
from filter3 import run_history_fetch

load_dotenv()

DB_PATH = os.getenv("DB_PATH", "crypto_history.db")
HISTORY_DAYS = int(os.getenv("HISTORY_DAYS", "3650"))


def main() -> None:
    global_start = time.perf_counter()
    print(
        f"[INFO] Top 1000 crypto daily history → SQLite\n"
        f"       DB: {DB_PATH}\n"
        f"       HISTORY_DAYS: {HISTORY_DAYS}\n"
    )

    conn = get_connection(DB_PATH)
    init_db(conn)
    top_coins = run_filter1(conn)
    print(f"[INFO] FILTER 1: Got {len(top_coins)} coins")
    plan = build_update_plan(conn, top_coins)

    run_history_fetch(conn, plan)

    total_time = time.perf_counter() - global_start
    db_write_time = DB_WRITE_TIME
    non_db_time = total_time - db_write_time

    print()
    print(f"[TIMER] Total program time      : {total_time:.2f} seconds")
    print(f"[TIMER] Time spent in DB writes : {db_write_time:.2f} seconds")
    print(
        f"[TIMER] Time without DB writes  : {non_db_time:.2f} seconds "
        f"(network + parsing + CPU only)"
    )


if __name__ == "__main__":
    main()
