import sqlite3
import time
from typing import Dict, Iterable, List, Tuple, Any

DB_WRITE_TIME: float = 0.0


def get_connection(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=OFF;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA cache_size=-64000;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS coins (
            symbol       TEXT PRIMARY KEY,
            name         TEXT,
            market_cap   REAL,
            rank         INTEGER,
            source       TEXT,
            last_updated TEXT
        );

        CREATE TABLE IF NOT EXISTS daily_history (
            symbol TEXT NOT NULL,
            date   TEXT NOT NULL,  -- YYYY-MM-DD
            open   REAL,
            high   REAL,
            low    REAL,
            close  REAL,
            volume REAL,
            PRIMARY KEY (symbol, date)
        );

        CREATE INDEX IF NOT EXISTS idx_daily_history_symbol
            ON daily_history(symbol);
        """
    )
    conn.commit()


def get_last_dates(conn: sqlite3.Connection) -> Dict[str, str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, MAX(date) AS last_date
        FROM daily_history
        GROUP BY symbol
        """
    )
    result: Dict[str, str] = {}
    for symbol, last_date in cur.fetchall():
        if last_date:
            result[str(symbol)] = str(last_date)
    return result


def insert_daily_history(
    conn: sqlite3.Connection,
    rows: Iterable[List[Any]],
) -> None:
    rows = list(rows)
    if not rows:
        return

    global DB_WRITE_TIME
    t0 = time.perf_counter()

    with conn: 
        conn.executemany(
            """
            INSERT OR REPLACE INTO daily_history
            (symbol, date, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

    DB_WRITE_TIME += time.perf_counter() - t0


def insert_top_coins(
    conn: sqlite3.Connection,
    coins: Iterable[Dict[str, Any]],
    source: str,
) -> None:
    coins = list(coins)
    if not coins:
        return

    global DB_WRITE_TIME
    t0 = time.perf_counter()

    with conn:
        conn.execute("DELETE FROM coins")

        conn.executemany(
            """
            INSERT OR REPLACE INTO coins
            (symbol, name, market_cap, rank, source, last_updated)
            VALUES (?, ?, ?, ?, ?, datetime('now'))
            """,
            [
                (
                    c.get("symbol"),
                    c.get("name"),
                    float(c.get("market_cap") or 0.0),
                    int(c.get("rank") or 0),
                    source,
                )
                for c in coins
            ],
        )

    DB_WRITE_TIME += time.perf_counter() - t0
