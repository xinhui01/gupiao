from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "stock_store.sqlite3"
_DB_WRITE_LOCK = threading.RLock()


def _ensure_dir() -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_dir()
    conn = sqlite3.connect(_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    _init_schema(conn)
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS universe (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            exchange TEXT NOT NULL DEFAULT '',
            board TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS history (
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            open REAL,
            close REAL,
            high REAL,
            low REAL,
            volume REAL,
            amount REAL,
            amplitude REAL,
            change_pct REAL,
            change_amount REAL,
            turnover_rate REAL,
            updated_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (code, trade_date)
        );

        CREATE TABLE IF NOT EXISTS scan_snapshots (
            signature TEXT PRIMARY KEY,
            scan_date TEXT NOT NULL,
            complete INTEGER NOT NULL DEFAULT 1,
            row_count INTEGER NOT NULL DEFAULT 0,
            saved_at TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL
        );
        """
    )


def db_path() -> Path:
    return _DB_PATH


def ensure_store_ready() -> None:
    """Ensure the SQLite file and schema exist before the app starts."""
    with _connect():
        pass


def _retry_locked(fn, retries: int = 8, base_delay: float = 0.15):
    last_exc = None
    for attempt in range(retries):
        try:
            return fn()
        except sqlite3.OperationalError as exc:
            last_exc = exc
            msg = str(exc).lower()
            if "locked" not in msg and "busy" not in msg:
                raise
            time.sleep(base_delay * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise sqlite3.OperationalError("database is locked")


def clear_universe() -> None:
    with _DB_WRITE_LOCK:
        _retry_locked(lambda: _clear_table("universe"))


def clear_history() -> None:
    with _DB_WRITE_LOCK:
        _retry_locked(lambda: _clear_table("history"))


def clear_scan_snapshots() -> None:
    with _DB_WRITE_LOCK:
        _retry_locked(lambda: _clear_table("scan_snapshots"))


def save_universe(df: pd.DataFrame) -> None:
    if df is None or df.empty or "code" not in df.columns:
        return
    out = df.copy()
    for col in ["name", "exchange", "board"]:
        if col not in out.columns:
            out[col] = ""
    out["code"] = out["code"].astype(str).str.strip().str.zfill(6)
    out["name"] = out["name"].astype(str).fillna("")
    out["exchange"] = out["exchange"].astype(str).fillna("")
    out["board"] = out["board"].astype(str).fillna("")
    rows = [
        (
            str(row["code"]),
            str(row.get("name", "") or ""),
            str(row.get("exchange", "") or ""),
            str(row.get("board", "") or ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        for _, row in out.iterrows()
    ]
    def _write():
        with _connect() as conn:
            conn.executemany(
                """
                INSERT INTO universe(code, name, exchange, board, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    name=excluded.name,
                    exchange=excluded.exchange,
                    board=excluded.board,
                    updated_at=excluded.updated_at
                """,
                rows,
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_universe() -> Optional[pd.DataFrame]:
    if not _DB_PATH.is_file():
        return None
    def _read():
        with _connect() as conn:
            return pd.read_sql_query(
                "SELECT code, name, exchange, board FROM universe ORDER BY code",
                conn,
                dtype={"code": str},
            )

    df = _retry_locked(_read)
    if df.empty:
        return None
    df["code"] = df["code"].astype(str).str.strip().str.zfill(6)
    return df


def save_history(stock_code: str, df: pd.DataFrame) -> None:
    if df is None or df.empty or "date" not in df.columns:
        return
    code = str(stock_code).strip().zfill(6)
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    for col in [
        "open",
        "close",
        "high",
        "low",
        "volume",
        "amount",
        "amplitude",
        "change_pct",
        "change_amount",
        "turnover_rate",
    ]:
        if col not in out.columns:
            out[col] = None
    out = out.sort_values("date").reset_index(drop=True)
    rows = [
        (
            code,
            str(row["date"]),
            _to_float(row.get("open")),
            _to_float(row.get("close")),
            _to_float(row.get("high")),
            _to_float(row.get("low")),
            _to_float(row.get("volume")),
            _to_float(row.get("amount")),
            _to_float(row.get("amplitude")),
            _to_float(row.get("change_pct")),
            _to_float(row.get("change_amount")),
            _to_float(row.get("turnover_rate")),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        for _, row in out.iterrows()
    ]
    def _write():
        with _connect() as conn:
            conn.executemany(
                """
                INSERT INTO history(
                    code, trade_date, open, close, high, low, volume, amount,
                    amplitude, change_pct, change_amount, turnover_rate, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, trade_date) DO UPDATE SET
                    open=excluded.open,
                    close=excluded.close,
                    high=excluded.high,
                    low=excluded.low,
                    volume=excluded.volume,
                    amount=excluded.amount,
                    amplitude=excluded.amplitude,
                    change_pct=excluded.change_pct,
                    change_amount=excluded.change_amount,
                    turnover_rate=excluded.turnover_rate,
                    updated_at=excluded.updated_at
                """,
                rows,
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_history(stock_code: str, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
    if not _DB_PATH.is_file():
        return None
    code = str(stock_code).strip().zfill(6)
    sql = """
        SELECT trade_date AS date, open, close, high, low, volume, amount,
               amplitude, change_pct, change_amount, turnover_rate
        FROM history
        WHERE code = ?
    """
    params: List[Any] = [code]
    if limit is not None and limit > 0:
        sql += " ORDER BY trade_date DESC LIMIT ?"
        params.append(int(limit))
    else:
        sql += " ORDER BY trade_date"
    def _read():
        with _connect() as conn:
            return pd.read_sql_query(sql, conn, params=params)

    df = _retry_locked(_read)
    if df.empty:
        return None
    df["date"] = df["date"].astype(str).str.strip()
    if limit is not None and limit > 0:
        df = df.sort_values("date").reset_index(drop=True)
    return df.reset_index(drop=True)


def save_scan_snapshot(signature: str, payload: Dict[str, Any]) -> None:
    def _write():
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO scan_snapshots(signature, scan_date, complete, row_count, saved_at, payload_json)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(signature) DO UPDATE SET
                    scan_date=excluded.scan_date,
                    complete=excluded.complete,
                    row_count=excluded.row_count,
                    saved_at=excluded.saved_at,
                    payload_json=excluded.payload_json
                """,
                (
                    signature,
                    payload.get("scan_date", ""),
                    1 if payload.get("complete", True) else 0,
                    int(payload.get("row_count", 0)),
                    payload.get("saved_at", ""),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_scan_snapshot(signature: str, scan_date: Optional[str] = None) -> Optional[Dict[str, Any]]:
    if not _DB_PATH.is_file():
        return None
    sql = "SELECT payload_json, scan_date, complete FROM scan_snapshots WHERE signature = ?"
    params: List[Any] = [signature]
    def _read():
        with _connect() as conn:
            return conn.execute(sql, params).fetchone()

    row = _retry_locked(_read)
    if row is None:
        return None
    if scan_date is not None and str(row["scan_date"]) != str(scan_date):
        return None
    if int(row["complete"]) != 1:
        return None
    try:
        return json.loads(row["payload_json"])
    except Exception:
        return None


def _to_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        if isinstance(value, str) and value.strip() == "":
            return None
        v = float(value)
        return v
    except Exception:
        return None


def _clear_table(table_name: str) -> None:
    with _connect() as conn:
        conn.execute(f"DELETE FROM {table_name}")
