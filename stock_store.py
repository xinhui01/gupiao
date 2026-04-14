from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from stock_logger import get_logger

logger = get_logger(__name__)


_DATA_DIR = Path(__file__).resolve().parent / "data"
_DB_PATH = _DATA_DIR / "stock_store.sqlite3"
_DB_WRITE_LOCK = threading.RLock()
_SCHEMA_INITIALIZED = False
_SCHEMA_INITIALIZED_PATH = ""
_SCHEMA_LOCK = threading.Lock()
_THREAD_LOCAL = threading.local()


def _ensure_dir() -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_dir()
    # 线程本地连接复用：同一线程内复用连接，避免每次操作都创建新连接
    conn = getattr(_THREAD_LOCAL, "conn", None)
    conn_path = getattr(_THREAD_LOCAL, "conn_path", None)
    if conn is not None and conn_path == str(_DB_PATH):
        try:
            conn.execute("SELECT 1")
            return conn
        except (sqlite3.ProgrammingError, sqlite3.OperationalError):
            _THREAD_LOCAL.conn = None
    db_path_str = str(_DB_PATH)
    conn = sqlite3.connect(_DB_PATH, timeout=30.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=30000;")
    global _SCHEMA_INITIALIZED, _SCHEMA_INITIALIZED_PATH
    need_schema = False
    if not _SCHEMA_INITIALIZED or _SCHEMA_INITIALIZED_PATH != db_path_str:
        with _SCHEMA_LOCK:
            if not _SCHEMA_INITIALIZED or _SCHEMA_INITIALIZED_PATH != db_path_str:
                need_schema = True
                _SCHEMA_INITIALIZED = True
                _SCHEMA_INITIALIZED_PATH = db_path_str
    if need_schema:
        _init_schema(conn)
    _THREAD_LOCAL.conn = conn
    _THREAD_LOCAL.conn_path = db_path_str
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS universe (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            exchange TEXT NOT NULL DEFAULT '',
            board TEXT NOT NULL DEFAULT '',
            concepts TEXT NOT NULL DEFAULT '',
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

        CREATE TABLE IF NOT EXISTS fund_flow (
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            close REAL,
            change_pct REAL,
            main_force_amount REAL,
            main_force_ratio REAL,
            big_order_amount REAL,
            big_order_ratio REAL,
            super_big_order_amount REAL,
            super_big_order_ratio REAL,
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

        CREATE TABLE IF NOT EXISTS history_meta (
            code TEXT PRIMARY KEY,
            latest_trade_date TEXT NOT NULL DEFAULT '',
            row_count INTEGER NOT NULL DEFAULT 0,
            refreshed_at TEXT NOT NULL DEFAULT '',
            source TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS app_config (
            key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT ''
        );

        CREATE TABLE IF NOT EXISTS intraday_cache (
            code TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            data_json TEXT NOT NULL,
            auction_json TEXT NOT NULL DEFAULT '',
            row_count INTEGER NOT NULL DEFAULT 0,
            saved_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (code, trade_date)
        );

        CREATE TABLE IF NOT EXISTS limit_up_pool (
            trade_date TEXT NOT NULL,
            pool_type TEXT NOT NULL DEFAULT 'today',
            data_json TEXT NOT NULL DEFAULT '[]',
            row_count INTEGER NOT NULL DEFAULT 0,
            saved_at TEXT NOT NULL DEFAULT '',
            PRIMARY KEY (trade_date, pool_type)
        );

        CREATE TABLE IF NOT EXISTS watchlist (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT '',
            note TEXT NOT NULL DEFAULT '',
            board TEXT NOT NULL DEFAULT '',
            latest_close REAL,
            score REAL,
            score_breakdown TEXT NOT NULL DEFAULT '',
            added_at TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT ''
        );
        """
    )
    existing_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(universe)").fetchall()
    }
    if "concepts" not in existing_columns:
        conn.execute("ALTER TABLE universe ADD COLUMN concepts TEXT NOT NULL DEFAULT ''")


def db_path() -> Path:
    return _DB_PATH


def ensure_store_ready() -> None:
    """Ensure the SQLite file and schema exist before the app starts."""
    with _connect():
        pass
    logger.info("数据库就绪：%s", _DB_PATH)


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
            logger.warning("数据库锁定，第 %d 次重试 (delay=%.2fs)", attempt + 1, base_delay * (attempt + 1))
            time.sleep(base_delay * (attempt + 1))
    if last_exc is not None:
        raise last_exc
    raise sqlite3.OperationalError("database is locked")


def clear_universe() -> None:
    with _DB_WRITE_LOCK:
        _retry_locked(lambda: _clear_table("universe"))


def clear_history() -> None:
    with _DB_WRITE_LOCK:
        _retry_locked(lambda: (_clear_table("history"), _clear_table("fund_flow"), _clear_table("history_meta")))


def clear_scan_snapshots() -> None:
    with _DB_WRITE_LOCK:
        _retry_locked(lambda: _clear_table("scan_snapshots"))


def save_watchlist_item(item: Dict[str, Any]) -> None:
    code = str(item.get("code", "") or "").strip().zfill(6)
    if not code:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    added_at = str(item.get("added_at") or "").strip() or now

    def _write():
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO watchlist(
                    code, name, status, note, board, latest_close, score,
                    score_breakdown, added_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    name=excluded.name,
                    status=excluded.status,
                    note=excluded.note,
                    board=excluded.board,
                    latest_close=excluded.latest_close,
                    score=excluded.score,
                    score_breakdown=excluded.score_breakdown,
                    added_at=COALESCE(NULLIF(watchlist.added_at, ''), excluded.added_at),
                    updated_at=excluded.updated_at
                """,
                (
                    code,
                    str(item.get("name", "") or ""),
                    str(item.get("status", "") or ""),
                    str(item.get("note", "") or ""),
                    str(item.get("board", "") or ""),
                    _to_float(item.get("latest_close")),
                    _to_float(item.get("score")),
                    str(item.get("score_breakdown", "") or ""),
                    added_at,
                    now,
                ),
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_watchlist() -> List[Dict[str, Any]]:
    if not _DB_PATH.is_file():
        return []

    def _read():
        with _connect() as conn:
            return conn.execute(
                """
                SELECT code, name, status, note, board, latest_close, score,
                       score_breakdown, added_at, updated_at
                FROM watchlist
                ORDER BY updated_at DESC, code ASC
                """
            ).fetchall()

    rows = _retry_locked(_read)
    return [
        {
            "code": str(row["code"] or "").strip().zfill(6),
            "name": str(row["name"] or ""),
            "status": str(row["status"] or ""),
            "note": str(row["note"] or ""),
            "board": str(row["board"] or ""),
            "latest_close": _to_float(row["latest_close"]),
            "score": _to_float(row["score"]),
            "score_breakdown": str(row["score_breakdown"] or ""),
            "added_at": str(row["added_at"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
        for row in rows
    ]


def load_watchlist_item(stock_code: str) -> Optional[Dict[str, Any]]:
    code = str(stock_code or "").strip().zfill(6)
    if not code or not _DB_PATH.is_file():
        return None

    def _read():
        with _connect() as conn:
            return conn.execute(
                """
                SELECT code, name, status, note, board, latest_close, score,
                       score_breakdown, added_at, updated_at
                FROM watchlist
                WHERE code = ?
                """,
                (code,),
            ).fetchone()

    row = _retry_locked(_read)
    if row is None:
        return None
    return {
        "code": str(row["code"] or "").strip().zfill(6),
        "name": str(row["name"] or ""),
        "status": str(row["status"] or ""),
        "note": str(row["note"] or ""),
        "board": str(row["board"] or ""),
        "latest_close": _to_float(row["latest_close"]),
        "score": _to_float(row["score"]),
        "score_breakdown": str(row["score_breakdown"] or ""),
        "added_at": str(row["added_at"] or ""),
        "updated_at": str(row["updated_at"] or ""),
    }


def delete_watchlist_item(stock_code: str) -> None:
    code = str(stock_code or "").strip().zfill(6)
    if not code:
        return

    def _write():
        with _connect() as conn:
            conn.execute("DELETE FROM watchlist WHERE code = ?", (code,))

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def save_universe(df: pd.DataFrame) -> None:
    if df is None or df.empty or "code" not in df.columns:
        return
    out = df.copy()
    for col in ["name", "exchange", "board", "concepts"]:
        if col not in out.columns:
            out[col] = ""
    out["code"] = out["code"].astype(str).str.strip().str.zfill(6)
    out["name"] = out["name"].astype(str).fillna("")
    out["exchange"] = out["exchange"].astype(str).fillna("")
    out["board"] = out["board"].astype(str).fillna("")
    out["concepts"] = out["concepts"].astype(str).fillna("")
    rows = [
        (
            str(row["code"]),
            str(row.get("name", "") or ""),
            str(row.get("exchange", "") or ""),
            str(row.get("board", "") or ""),
            str(row.get("concepts", "") or ""),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        for _, row in out.iterrows()
    ]
    def _write():
        with _connect() as conn:
            conn.executemany(
                """
                INSERT INTO universe(code, name, exchange, board, concepts, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    name=excluded.name,
                    exchange=excluded.exchange,
                    board=excluded.board,
                    concepts=excluded.concepts,
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
                "SELECT code, name, exchange, board, concepts FROM universe ORDER BY code",
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


def save_history_meta(stock_code: str, latest_trade_date: str, row_count: int, source: str = "") -> None:
    code = str(stock_code).strip().zfill(6)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _write():
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO history_meta(code, latest_trade_date, row_count, refreshed_at, source)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    latest_trade_date=excluded.latest_trade_date,
                    row_count=excluded.row_count,
                    refreshed_at=excluded.refreshed_at,
                    source=excluded.source
                """,
                (code, str(latest_trade_date).strip(), int(row_count), now, str(source)),
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_history_meta(stock_code: str) -> Optional[Dict[str, Any]]:
    if not _DB_PATH.is_file():
        return None
    code = str(stock_code).strip().zfill(6)

    def _read():
        with _connect() as conn:
            return conn.execute(
                "SELECT latest_trade_date, row_count, refreshed_at, source FROM history_meta WHERE code = ?",
                (code,),
            ).fetchone()

    row = _retry_locked(_read)
    if row is None:
        return None
    return {
        "latest_trade_date": str(row["latest_trade_date"] or ""),
        "row_count": int(row["row_count"] or 0),
        "refreshed_at": str(row["refreshed_at"] or ""),
        "source": str(row["source"] or ""),
    }


def save_app_config(key: str, value: Any) -> None:
    if not key:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    value_json = json.dumps(value, ensure_ascii=False, default=str)

    def _write():
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO app_config(key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value_json=excluded.value_json,
                    updated_at=excluded.updated_at
                """,
                (str(key), value_json, now),
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_app_config(key: str, default: Any = None) -> Any:
    if not key or not _DB_PATH.is_file():
        return default

    def _read():
        with _connect() as conn:
            return conn.execute(
                "SELECT value_json FROM app_config WHERE key = ?",
                (str(key),),
            ).fetchone()

    row = _retry_locked(_read)
    if row is None:
        return default
    try:
        return json.loads(row["value_json"])
    except Exception:
        return default


def save_intraday_cache(
    stock_code: str,
    trade_date: str,
    df_json: str,
    auction_json: str = "",
    row_count: int = 0,
) -> None:
    code = str(stock_code).strip().zfill(6)
    td = str(trade_date).strip()
    if not code or not td:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _write():
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO intraday_cache(code, trade_date, data_json, auction_json, row_count, saved_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, trade_date) DO UPDATE SET
                    data_json=excluded.data_json,
                    auction_json=excluded.auction_json,
                    row_count=excluded.row_count,
                    saved_at=excluded.saved_at
                """,
                (code, td, str(df_json), str(auction_json or ""), int(row_count), now),
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_intraday_cache(stock_code: str, trade_date: str) -> Optional[Dict[str, Any]]:
    if not _DB_PATH.is_file():
        return None
    code = str(stock_code).strip().zfill(6)
    td = str(trade_date).strip()
    if not code or not td:
        return None

    def _read():
        with _connect() as conn:
            return conn.execute(
                "SELECT data_json, auction_json, row_count FROM intraday_cache WHERE code = ? AND trade_date = ?",
                (code, td),
            ).fetchone()

    row = _retry_locked(_read)
    if row is None:
        return None
    return {
        "data_json": str(row["data_json"] or ""),
        "auction_json": str(row["auction_json"] or ""),
        "row_count": int(row["row_count"] or 0),
    }


def history_coverage_summary() -> Dict[str, Any]:
    if not _DB_PATH.is_file():
        return {
            "universe_count": 0,
            "covered_count": 0,
            "coverage_ratio": 0.0,
            "latest_trade_date": "",
            "latest_updated_at": "",
        }

    def _read():
        with _connect() as conn:
            universe_count = conn.execute("SELECT COUNT(*) FROM universe").fetchone()[0]
            covered_count = conn.execute("SELECT COUNT(DISTINCT code) FROM history").fetchone()[0]
            latest_trade_date = conn.execute("SELECT MAX(trade_date) FROM history").fetchone()[0] or ""
            latest_updated_at = conn.execute("SELECT MAX(updated_at) FROM history").fetchone()[0] or ""
            return {
                "universe_count": int(universe_count or 0),
                "covered_count": int(covered_count or 0),
                "latest_trade_date": str(latest_trade_date or ""),
                "latest_updated_at": str(latest_updated_at or ""),
            }

    payload = _retry_locked(_read)
    universe_count = int(payload.get("universe_count", 0) or 0)
    covered_count = int(payload.get("covered_count", 0) or 0)
    payload["coverage_ratio"] = (covered_count / universe_count) if universe_count > 0 else 0.0
    return payload


def save_fund_flow(stock_code: str, df: pd.DataFrame) -> None:
    if df is None or df.empty or "date" not in df.columns:
        return
    code = str(stock_code).strip().zfill(6)
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    for col in [
        "close",
        "change_pct",
        "main_force_amount",
        "main_force_ratio",
        "big_order_amount",
        "big_order_ratio",
        "super_big_order_amount",
        "super_big_order_ratio",
    ]:
        if col not in out.columns:
            out[col] = None
    out = out.sort_values("date").reset_index(drop=True)
    rows = [
        (
            code,
            str(row["date"]),
            _to_float(row.get("close")),
            _to_float(row.get("change_pct")),
            _to_float(row.get("main_force_amount")),
            _to_float(row.get("main_force_ratio")),
            _to_float(row.get("big_order_amount")),
            _to_float(row.get("big_order_ratio")),
            _to_float(row.get("super_big_order_amount")),
            _to_float(row.get("super_big_order_ratio")),
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        )
        for _, row in out.iterrows()
    ]

    def _write():
        with _connect() as conn:
            conn.executemany(
                """
                INSERT INTO fund_flow(
                    code, trade_date, close, change_pct, main_force_amount, main_force_ratio,
                    big_order_amount, big_order_ratio, super_big_order_amount, super_big_order_ratio, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code, trade_date) DO UPDATE SET
                    close=excluded.close,
                    change_pct=excluded.change_pct,
                    main_force_amount=excluded.main_force_amount,
                    main_force_ratio=excluded.main_force_ratio,
                    big_order_amount=excluded.big_order_amount,
                    big_order_ratio=excluded.big_order_ratio,
                    super_big_order_amount=excluded.super_big_order_amount,
                    super_big_order_ratio=excluded.super_big_order_ratio,
                    updated_at=excluded.updated_at
                """,
                rows,
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


def load_fund_flow(stock_code: str, limit: Optional[int] = None) -> Optional[pd.DataFrame]:
    if not _DB_PATH.is_file():
        return None
    code = str(stock_code).strip().zfill(6)
    sql = """
        SELECT trade_date AS date, close, change_pct, main_force_amount, main_force_ratio,
               big_order_amount, big_order_ratio, super_big_order_amount, super_big_order_ratio
        FROM fund_flow
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


def load_latest_scan_snapshot() -> Optional[Dict[str, Any]]:
    if not _DB_PATH.is_file():
        return None
    sql = """
        SELECT payload_json, scan_date, complete
        FROM scan_snapshots
        WHERE complete = 1
        ORDER BY saved_at DESC
        LIMIT 1
    """

    def _read():
        with _connect() as conn:
            return conn.execute(sql).fetchone()

    row = _retry_locked(_read)
    if row is None:
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


_ALLOWED_CLEAR_TABLES = frozenset({
    "universe", "history", "fund_flow", "history_meta", "scan_snapshots",
    "intraday_cache", "limit_up_pool", "watchlist", "app_config",
})

def _clear_table(table_name: str) -> None:
    if table_name not in _ALLOWED_CLEAR_TABLES:
        raise ValueError(f"不允许清空的表: {table_name}")
    with _connect() as conn:
        conn.execute(f"DELETE FROM {table_name}")


# ---------------------------------------------------------------------------
# 数据过期清理
# ---------------------------------------------------------------------------

def cleanup_old_history(keep_days: int = 365) -> int:
    """删除超过 keep_days 天的历史数据，返回删除行数。"""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    deleted = 0

    def _write():
        nonlocal deleted
        with _connect() as conn:
            cur = conn.execute("DELETE FROM history WHERE trade_date < ?", (cutoff,))
            deleted = cur.rowcount
            logger.info("清理历史数据：删除 %d 条 (早于 %s)", deleted, cutoff)

    with _DB_WRITE_LOCK:
        _retry_locked(_write)
    return deleted


def cleanup_old_intraday(keep_days: int = 30) -> int:
    """删除超过 keep_days 天的分时缓存，返回删除行数。"""
    from datetime import timedelta
    cutoff = (datetime.now() - timedelta(days=keep_days)).strftime("%Y-%m-%d")
    deleted = 0

    def _write():
        nonlocal deleted
        with _connect() as conn:
            cur = conn.execute("DELETE FROM intraday_cache WHERE trade_date < ?", (cutoff,))
            deleted = cur.rowcount
            logger.info("清理分时缓存：删除 %d 条 (早于 %s)", deleted, cutoff)

    with _DB_WRITE_LOCK:
        _retry_locked(_write)
    return deleted


def cleanup_old_scan_snapshots(keep_count: int = 20) -> int:
    """保留最新 keep_count 条扫描快照，删除更旧的，返回删除行数。"""
    deleted = 0

    def _write():
        nonlocal deleted
        with _connect() as conn:
            cur = conn.execute(
                """
                DELETE FROM scan_snapshots
                WHERE signature NOT IN (
                    SELECT signature FROM scan_snapshots
                    ORDER BY saved_at DESC
                    LIMIT ?
                )
                """,
                (keep_count,),
            )
            deleted = cur.rowcount
            logger.info("清理扫描快照：删除 %d 条 (保留最新 %d 条)", deleted, keep_count)

    with _DB_WRITE_LOCK:
        _retry_locked(_write)
    return deleted


def cleanup_all(
    history_keep_days: int = 365,
    intraday_keep_days: int = 30,
    snapshot_keep_count: int = 20,
) -> Dict[str, int]:
    """执行所有清理操作，返回各表删除行数汇总。"""
    return {
        "history": cleanup_old_history(history_keep_days),
        "intraday": cleanup_old_intraday(intraday_keep_days),
        "scan_snapshots": cleanup_old_scan_snapshots(snapshot_keep_count),
    }


# ---------------------------------------------------------------------------
# 数据库备份 / 恢复
# ---------------------------------------------------------------------------

def backup_database(backup_dir: Optional[str] = None) -> Path:
    """备份数据库到指定目录，返回备份文件路径。

    默认备份到 data/backups/ 下，文件名含时间戳。
    """
    import shutil
    if backup_dir:
        dest_dir = Path(backup_dir)
    else:
        dest_dir = _DATA_DIR / "backups"
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest_file = dest_dir / f"stock_store_{stamp}.sqlite3"

    # 使用 SQLite 的 backup API 保证一致性
    import sqlite3
    src_conn = sqlite3.connect(str(_DB_PATH), timeout=30.0)
    dst_conn = sqlite3.connect(str(dest_file))
    try:
        src_conn.backup(dst_conn)
        logger.info("数据库备份完成：%s", dest_file)
    finally:
        dst_conn.close()
        src_conn.close()
    return dest_file


def restore_database(backup_path: str) -> bool:
    """从备份文件恢复数据库。

    恢复前会自动创建当前数据库的备份。
    返回 True 表示成功。
    """
    import shutil
    src = Path(backup_path)
    if not src.is_file():
        logger.error("备份文件不存在：%s", backup_path)
        return False

    # 先备份当前数据库
    try:
        backup_database()
    except Exception as exc:
        logger.warning("恢复前备份失败：%s", exc)

    # 替换
    try:
        shutil.copy2(str(src), str(_DB_PATH))
        logger.info("数据库恢复完成，来源：%s", backup_path)
        return True
    except Exception as exc:
        logger.error("数据库恢复失败：%s", exc)
        return False


def list_backups(backup_dir: Optional[str] = None) -> List[Dict[str, Any]]:
    """列出所有备份文件，返回 [{path, size_mb, created_at}]。"""
    if backup_dir:
        d = Path(backup_dir)
    else:
        d = _DATA_DIR / "backups"
    if not d.is_dir():
        return []
    files = sorted(d.glob("stock_store_*.sqlite3"), reverse=True)
    result = []
    for f in files:
        stat = f.stat()
        result.append({
            "path": str(f),
            "size_mb": round(stat.st_size / (1024 * 1024), 2),
            "created_at": datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return result


# ---------------------------------------------------------------------------
# 自选股导入 / 导出
# ---------------------------------------------------------------------------

def export_watchlist_csv(file_path: str) -> int:
    """导出自选股到 CSV，返回导出数量。"""
    import csv
    items = load_watchlist()
    if not items:
        return 0
    fieldnames = ["code", "name", "status", "note", "board", "latest_close", "score", "score_breakdown", "added_at"]
    with open(file_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for item in items:
            writer.writerow(item)
    logger.info("自选股导出完成：%d 只 -> %s", len(items), file_path)
    return len(items)


def import_watchlist_csv(file_path: str) -> int:
    """从 CSV 导入自选股，返回导入数量。

    CSV 至少需要 code 列，其他列可选。
    """
    import csv
    imported = 0
    with open(file_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = str(row.get("code", "") or "").strip()
            if not code:
                continue
            item = {
                "code": code,
                "name": str(row.get("name", "") or ""),
                "status": str(row.get("status", "") or ""),
                "note": str(row.get("note", "") or ""),
                "board": str(row.get("board", "") or ""),
                "score": row.get("score"),
            }
            save_watchlist_item(item)
            imported += 1
    logger.info("自选股导入完成：%d 只 <- %s", imported, file_path)
    return imported


# ============= 涨停池持久化 =============

def save_limit_up_pool(trade_date: str, df: pd.DataFrame, pool_type: str = "today") -> None:
    """将涨停池 DataFrame 按日期持久化到 SQLite。"""
    date_key = str(trade_date or "").strip().replace("-", "")
    if not date_key or df is None or df.empty:
        return
    data_json = df.to_json(orient="records", force_ascii=False)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    def _do():
        with _connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO limit_up_pool (trade_date, pool_type, data_json, row_count, saved_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (date_key, pool_type, data_json, len(df), now),
            )
    with _DB_WRITE_LOCK:
        _retry_locked(_do)


def load_limit_up_pool(trade_date: str, pool_type: str = "today") -> Optional[pd.DataFrame]:
    """从 SQLite 读取指定日期的涨停池，无数据返回 None。"""
    date_key = str(trade_date or "").strip().replace("-", "")
    if not date_key:
        return None
    try:
        with _connect() as conn:
            row = conn.execute(
                "SELECT data_json, row_count FROM limit_up_pool WHERE trade_date = ? AND pool_type = ?",
                (date_key, pool_type),
            ).fetchone()
        if row is None:
            return None
        data_json = row["data_json"]
        if not data_json or data_json == "[]":
            return None
        df = pd.read_json(StringIO(data_json), orient="records")
        if df is None or df.empty:
            return None
        return df
    except Exception as e:
        logger.warning("读取涨停池 %s/%s 失败: %s", date_key, pool_type, e)
        return None
