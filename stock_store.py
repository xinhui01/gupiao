from __future__ import annotations

import json
import sqlite3
import threading
import time
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

# 已发放的所有线程本地连接。restore 时需要统一关闭，避免其他线程继续握着旧 fd。
_OPEN_CONNECTIONS: "set[sqlite3.Connection]" = set()
_OPEN_CONNECTIONS_LOCK = threading.Lock()
# 每次恢复/重置后自增，线程可以据此判断手上连接是否过期。
_CONNECTION_GENERATION = 0


def _ensure_dir() -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)


def _connect() -> sqlite3.Connection:
    _ensure_dir()
    # 线程本地连接复用：同一线程内复用连接，避免每次操作都创建新连接
    conn = getattr(_THREAD_LOCAL, "conn", None)
    conn_path = getattr(_THREAD_LOCAL, "conn_path", None)
    conn_gen = getattr(_THREAD_LOCAL, "conn_gen", None)
    if (
        conn is not None
        and conn_path == str(_DB_PATH)
        and conn_gen == _CONNECTION_GENERATION
    ):
        try:
            conn.execute("SELECT 1")
            return conn
        except (sqlite3.ProgrammingError, sqlite3.OperationalError):
            _drop_thread_local_connection()
    elif conn is not None:
        # 路径变了或代数升了：主动关闭旧连接，避免悬挂句柄
        _drop_thread_local_connection()
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
    _THREAD_LOCAL.conn_gen = _CONNECTION_GENERATION
    with _OPEN_CONNECTIONS_LOCK:
        _OPEN_CONNECTIONS.add(conn)
    return conn


def _drop_thread_local_connection() -> None:
    conn = getattr(_THREAD_LOCAL, "conn", None)
    _THREAD_LOCAL.conn = None
    _THREAD_LOCAL.conn_path = None
    _THREAD_LOCAL.conn_gen = None
    if conn is None:
        return
    with _OPEN_CONNECTIONS_LOCK:
        _OPEN_CONNECTIONS.discard(conn)
    try:
        conn.close()
    except Exception:
        pass


def reset_all_connections() -> None:
    """Invalidate every outstanding SQLite connection and mark schema uninitialized.

    Callers must hold `_DB_WRITE_LOCK` (or otherwise guarantee no concurrent
    writers) for the reset to be meaningful. Other threads' next `_connect()`
    will create a fresh connection.
    """
    global _SCHEMA_INITIALIZED, _SCHEMA_INITIALIZED_PATH, _CONNECTION_GENERATION
    _CONNECTION_GENERATION += 1
    with _OPEN_CONNECTIONS_LOCK:
        conns = list(_OPEN_CONNECTIONS)
        _OPEN_CONNECTIONS.clear()
    for conn in conns:
        try:
            conn.close()
        except Exception:
            pass
    # 主动清掉当前线程本地引用，防止调用方立刻 _connect() 时命中死引用
    try:
        _THREAD_LOCAL.conn = None
        _THREAD_LOCAL.conn_path = None
        _THREAD_LOCAL.conn_gen = None
    except Exception:
        pass
    with _SCHEMA_LOCK:
        _SCHEMA_INITIALIZED = False
        _SCHEMA_INITIALIZED_PATH = ""


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
            source TEXT NOT NULL DEFAULT '',
            partial_fields TEXT NOT NULL DEFAULT '',
            needs_repair INTEGER NOT NULL DEFAULT 0,
            source_failure_streak INTEGER NOT NULL DEFAULT 0
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

        CREATE TABLE IF NOT EXISTS limit_up_predictions (
            trade_date TEXT PRIMARY KEY,
            payload_json TEXT NOT NULL,
            summary TEXT NOT NULL DEFAULT '',
            cont_count INTEGER NOT NULL DEFAULT 0,
            first_count INTEGER NOT NULL DEFAULT 0,
            fresh_count INTEGER NOT NULL DEFAULT 0,
            predicted_at TEXT NOT NULL DEFAULT '',
            saved_at TEXT NOT NULL DEFAULT ''
        );
        """
    )
    existing_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(universe)").fetchall()
    }
    if "concepts" not in existing_columns:
        conn.execute("ALTER TABLE universe ADD COLUMN concepts TEXT NOT NULL DEFAULT ''")

    meta_columns = {
        row[1] for row in conn.execute("PRAGMA table_info(history_meta)").fetchall()
    }
    if "partial_fields" not in meta_columns:
        conn.execute("ALTER TABLE history_meta ADD COLUMN partial_fields TEXT NOT NULL DEFAULT ''")
    if "needs_repair" not in meta_columns:
        conn.execute("ALTER TABLE history_meta ADD COLUMN needs_repair INTEGER NOT NULL DEFAULT 0")
    if "source_failure_streak" not in meta_columns:
        conn.execute("ALTER TABLE history_meta ADD COLUMN source_failure_streak INTEGER NOT NULL DEFAULT 0")


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
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    codes = out["code"].tolist()
    names = out["name"].tolist()
    exchanges = out["exchange"].tolist()
    boards = out["board"].tolist()
    concepts = out["concepts"].tolist()
    rows = [
        (codes[i], names[i], exchanges[i], boards[i], concepts[i], now_str)
        for i in range(len(codes))
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
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_series = out["date"].tolist()
    open_vals = out["open"].tolist()
    close_vals = out["close"].tolist()
    high_vals = out["high"].tolist()
    low_vals = out["low"].tolist()
    volume_vals = out["volume"].tolist()
    amount_vals = out["amount"].tolist()
    amplitude_vals = out["amplitude"].tolist()
    change_pct_vals = out["change_pct"].tolist()
    change_amount_vals = out["change_amount"].tolist()
    turnover_vals = out["turnover_rate"].tolist()
    rows = [
        (
            code,
            str(date_series[i]),
            _to_float(open_vals[i]),
            _to_float(close_vals[i]),
            _to_float(high_vals[i]),
            _to_float(low_vals[i]),
            _to_float(volume_vals[i]),
            _to_float(amount_vals[i]),
            _to_float(amplitude_vals[i]),
            _to_float(change_pct_vals[i]),
            _to_float(change_amount_vals[i]),
            _to_float(turnover_vals[i]),
            now_str,
        )
        for i in range(len(date_series))
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


def save_history_meta(
    stock_code: str,
    latest_trade_date: str,
    row_count: int,
    source: str = "",
    *,
    partial_fields: Optional[str] = None,
    needs_repair: Optional[int] = None,
    source_failure_streak: Optional[int] = None,
) -> None:
    code = str(stock_code).strip().zfill(6)
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pf = "" if partial_fields is None else str(partial_fields)
    nr = 0 if needs_repair is None else int(bool(needs_repair))
    sfs = 0 if source_failure_streak is None else int(source_failure_streak)

    def _write():
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO history_meta(
                    code, latest_trade_date, row_count, refreshed_at, source,
                    partial_fields, needs_repair, source_failure_streak
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(code) DO UPDATE SET
                    latest_trade_date=excluded.latest_trade_date,
                    row_count=excluded.row_count,
                    refreshed_at=excluded.refreshed_at,
                    source=excluded.source,
                    partial_fields=excluded.partial_fields,
                    needs_repair=excluded.needs_repair,
                    source_failure_streak=excluded.source_failure_streak
                """,
                (code, str(latest_trade_date).strip(), int(row_count), now, str(source), pf, nr, sfs),
            )

    with _DB_WRITE_LOCK:
        _retry_locked(_write)


_HISTORY_META_COLUMNS = (
    "latest_trade_date",
    "row_count",
    "refreshed_at",
    "source",
    "partial_fields",
    "needs_repair",
    "source_failure_streak",
)


def _row_to_history_meta(row) -> Dict[str, Any]:
    return {
        "latest_trade_date": str(row["latest_trade_date"] or ""),
        "row_count": int(row["row_count"] or 0),
        "refreshed_at": str(row["refreshed_at"] or ""),
        "source": str(row["source"] or ""),
        "partial_fields": str(row["partial_fields"] or ""),
        "needs_repair": int(row["needs_repair"] or 0),
        "source_failure_streak": int(row["source_failure_streak"] or 0),
    }


def load_history_meta(stock_code: str) -> Optional[Dict[str, Any]]:
    if not _DB_PATH.is_file():
        return None
    code = str(stock_code).strip().zfill(6)

    def _read():
        with _connect() as conn:
            return conn.execute(
                f"SELECT {', '.join(_HISTORY_META_COLUMNS)} FROM history_meta WHERE code = ?",
                (code,),
            ).fetchone()

    row = _retry_locked(_read)
    if row is None:
        return None
    return _row_to_history_meta(row)


def load_all_history_meta_map() -> Dict[str, Dict[str, Any]]:
    """一次性读取全部 history_meta，返回 {code: meta_dict}。

    用于历史缓存同步启动时预加载，避免在主循环中对每只股票单独 SELECT。
    """
    if not _DB_PATH.is_file():
        return {}

    def _read():
        with _connect() as conn:
            return conn.execute(
                f"SELECT code, {', '.join(_HISTORY_META_COLUMNS)} FROM history_meta"
            ).fetchall()

    rows = _retry_locked(_read)
    out: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        code = str(row["code"] or "").strip().zfill(6)
        if not code:
            continue
        out[code] = _row_to_history_meta(row)
    return out


def save_history_rows_batch(
    rows_by_code: Dict[str, pd.DataFrame],
    batch_size: int = 500,
) -> Tuple[List[str], List[str]]:
    """批量写入多只股票的 history 行。

    - 每个批独立事务，失败的批整批进入 `failed_codes`，不影响其他批。
    - 返回 `(success_codes, failed_codes)`，供调度层沉淀 repair 清单。

    参数 `rows_by_code`: `{code: DataFrame}`，DataFrame 至少要有 `date` 列。
    空 DataFrame 会被跳过且**不**计入 success/failed。
    """
    if not rows_by_code:
        return [], []
    batch_size = max(1, int(batch_size))

    prepared: List[Tuple[str, List[tuple]]] = []
    for raw_code, df in rows_by_code.items():
        raw = str(raw_code or "").strip()
        if not raw or df is None or df.empty or "date" not in df.columns:
            continue
        code = raw.zfill(6)
        prepared.append((code, _build_history_rows(code, df)))

    success: List[str] = []
    failed: List[str] = []

    for start in range(0, len(prepared), batch_size):
        batch = prepared[start : start + batch_size]
        flat_rows: List[tuple] = []
        for _, rows in batch:
            flat_rows.extend(rows)
        if not flat_rows:
            # 理论上入口已过滤 df.empty；若走到这里说明上游给了空 DataFrame，
            # 既没真正写入也没失败，直接跳过不计入 success/failed。
            continue

        def _write(rows=flat_rows):
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

        try:
            with _DB_WRITE_LOCK:
                _retry_locked(_write)
        except sqlite3.Error:
            logger.exception("save_history_rows_batch 写入失败，批 size=%d", len(batch))
            failed.extend(code for code, _ in batch)
            continue
        success.extend(code for code, _ in batch)

    return success, failed


def _build_history_rows(code: str, df: pd.DataFrame) -> List[tuple]:
    """复用 save_history 的字段规整逻辑，但只产出行元组，不直接写库。

    与 save_history 保持相同的 tolist()-预取模式：一次性把每列转成 Python list，
    循环里只做索引访问，避免 .iloc[i] 每次都走 pandas 的属性查找链。
    """
    out = df.copy()
    out["date"] = out["date"].astype(str).str.strip()
    for col in [
        "open", "close", "high", "low", "volume", "amount",
        "amplitude", "change_pct", "change_amount", "turnover_rate",
    ]:
        if col not in out.columns:
            out[col] = None
    out = out.sort_values("date").reset_index(drop=True)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_series = out["date"].tolist()
    open_vals = out["open"].tolist()
    close_vals = out["close"].tolist()
    high_vals = out["high"].tolist()
    low_vals = out["low"].tolist()
    volume_vals = out["volume"].tolist()
    amount_vals = out["amount"].tolist()
    amplitude_vals = out["amplitude"].tolist()
    change_pct_vals = out["change_pct"].tolist()
    change_amount_vals = out["change_amount"].tolist()
    turnover_vals = out["turnover_rate"].tolist()
    return [
        (
            code,
            str(date_series[i]),
            _to_float(open_vals[i]),
            _to_float(close_vals[i]),
            _to_float(high_vals[i]),
            _to_float(low_vals[i]),
            _to_float(volume_vals[i]),
            _to_float(amount_vals[i]),
            _to_float(amplitude_vals[i]),
            _to_float(change_pct_vals[i]),
            _to_float(change_amount_vals[i]),
            _to_float(turnover_vals[i]),
            now_str,
        )
        for i in range(len(date_series))
    ]


def save_history_meta_batch(
    metas: List[Dict[str, Any]],
    batch_size: int = 500,
) -> Tuple[List[str], List[str]]:
    """批量 upsert 多只股票的 history_meta。

    - `metas` 每项至少包含 `code`；其余字段缺失会落为默认值。
    - 每批独立事务，失败批进入 `failed_codes`。
    """
    if not metas:
        return [], []
    batch_size = max(1, int(batch_size))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    prepared: List[Tuple[str, tuple]] = []
    for item in metas:
        raw = str(item.get("code", "") or "").strip()
        if not raw:
            continue
        code = raw.zfill(6)
        prepared.append(
            (
                code,
                (
                    code,
                    str(item.get("latest_trade_date", "") or "").strip(),
                    int(item.get("row_count", 0) or 0),
                    str(item.get("refreshed_at") or now),
                    str(item.get("source", "") or ""),
                    str(item.get("partial_fields", "") or ""),
                    int(bool(item.get("needs_repair", 0))),
                    int(item.get("source_failure_streak", 0) or 0),
                ),
            )
        )

    success: List[str] = []
    failed: List[str] = []

    for start in range(0, len(prepared), batch_size):
        batch = prepared[start : start + batch_size]
        flat_rows = [row for _, row in batch]

        def _write(rows=flat_rows):
            with _connect() as conn:
                conn.executemany(
                    """
                    INSERT INTO history_meta(
                        code, latest_trade_date, row_count, refreshed_at, source,
                        partial_fields, needs_repair, source_failure_streak
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(code) DO UPDATE SET
                        latest_trade_date=excluded.latest_trade_date,
                        row_count=excluded.row_count,
                        refreshed_at=excluded.refreshed_at,
                        source=excluded.source,
                        partial_fields=excluded.partial_fields,
                        needs_repair=excluded.needs_repair,
                        source_failure_streak=excluded.source_failure_streak
                    """,
                    rows,
                )

        try:
            with _DB_WRITE_LOCK:
                _retry_locked(_write)
        except sqlite3.Error:
            logger.exception("save_history_meta_batch 写入失败，批 size=%d", len(batch))
            failed.extend(code for code, _ in batch)
            continue
        success.extend(code for code, _ in batch)

    return success, failed


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


def save_last_limit_up_prediction(payload: Dict[str, Any]) -> None:
    """保存上次的涨停预测结果到 `app_config` 表，key 为 `last_limit_up_prediction`。

    `payload` 应当是可序列化为 JSON 的字典。
    """
    try:
        save_app_config("last_limit_up_prediction", payload)
    except Exception:
        logger.exception("保存上次涨停预测结果失败")


def load_last_limit_up_prediction() -> Optional[Dict[str, Any]]:
    """读取上次保存的涨停预测结果，若不存在返回 None。"""
    try:
        return load_app_config("last_limit_up_prediction", default=None)
    except Exception:
        logger.exception("读取上次涨停预测结果失败")
        return None


def save_limit_up_prediction_record(payload: Dict[str, Any]) -> None:
    """按 trade_date 持久化每次涨停预测结果到 `limit_up_predictions` 表。

    同一交易日重复预测会覆盖之前的记录（PK 为 trade_date）。
    """
    if not isinstance(payload, dict):
        return
    trade_date = str(payload.get("trade_date") or "").strip()
    if not trade_date:
        return
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)
    except Exception:
        logger.exception("序列化涨停预测结果失败")
        return
    summary = str(payload.get("summary") or "")
    cont_count = len(payload.get("continuation_candidates") or [])
    first_count = len(payload.get("first_board_candidates") or [])
    fresh_count = len(payload.get("fresh_first_board_candidates") or [])

    def _write():
        with _connect() as conn:
            conn.execute(
                """
                INSERT INTO limit_up_predictions(
                    trade_date, payload_json, summary,
                    cont_count, first_count, fresh_count,
                    predicted_at, saved_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(trade_date) DO UPDATE SET
                    payload_json=excluded.payload_json,
                    summary=excluded.summary,
                    cont_count=excluded.cont_count,
                    first_count=excluded.first_count,
                    fresh_count=excluded.fresh_count,
                    predicted_at=excluded.predicted_at,
                    saved_at=excluded.saved_at
                """,
                (
                    trade_date, payload_json, summary,
                    int(cont_count), int(first_count), int(fresh_count),
                    now, now,
                ),
            )

    try:
        with _DB_WRITE_LOCK:
            _retry_locked(_write)
    except Exception:
        logger.exception("保存涨停预测历史记录失败")


def load_limit_up_prediction_by_date(trade_date: str) -> Optional[Dict[str, Any]]:
    """按交易日读取已保存的涨停预测结果，未找到返回 None。"""
    td = str(trade_date or "").strip()
    if not td or not _DB_PATH.is_file():
        return None

    def _read():
        with _connect() as conn:
            return conn.execute(
                "SELECT payload_json FROM limit_up_predictions WHERE trade_date = ?",
                (td,),
            ).fetchone()

    try:
        row = _retry_locked(_read)
    except Exception:
        logger.exception("读取涨停预测历史记录失败")
        return None
    if row is None:
        return None
    try:
        return json.loads(row["payload_json"])
    except Exception:
        logger.exception("解析涨停预测历史记录失败")
        return None


def list_limit_up_prediction_dates() -> List[str]:
    """列出所有已保存涨停预测的交易日，按日期降序返回。"""
    if not _DB_PATH.is_file():
        return []

    def _read():
        with _connect() as conn:
            return conn.execute(
                "SELECT trade_date FROM limit_up_predictions ORDER BY trade_date DESC"
            ).fetchall()

    try:
        rows = _retry_locked(_read)
    except Exception:
        logger.exception("列出涨停预测历史日期失败")
        return []
    return [str(row["trade_date"]) for row in rows if row and row["trade_date"]]


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
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    date_series = out["date"].tolist()
    close_vals = out["close"].tolist()
    change_pct_vals = out["change_pct"].tolist()
    main_force_amount = out["main_force_amount"].tolist()
    main_force_ratio = out["main_force_ratio"].tolist()
    big_order_amount = out["big_order_amount"].tolist()
    big_order_ratio = out["big_order_ratio"].tolist()
    super_big_order_amount = out["super_big_order_amount"].tolist()
    super_big_order_ratio = out["super_big_order_ratio"].tolist()
    rows = [
        (
            code,
            str(date_series[i]),
            _to_float(close_vals[i]),
            _to_float(change_pct_vals[i]),
            _to_float(main_force_amount[i]),
            _to_float(main_force_ratio[i]),
            _to_float(big_order_amount[i]),
            _to_float(big_order_ratio[i]),
            _to_float(super_big_order_amount[i]),
            _to_float(super_big_order_ratio[i]),
            now_str,
        )
        for i in range(len(date_series))
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


# DB 管理员操作（备份/恢复/清理/CSV 导入导出）已迁移到 src/services/db_admin_service。
# 为保持既有调用方（GUI、测试、脚本）无需改动，这里做薄薄的转发。
def cleanup_all(*args, **kwargs) -> Dict[str, int]:
    from src.services.db_admin_service import cleanup_all as _impl
    return _impl(*args, **kwargs)


def backup_database(*args, **kwargs) -> Path:
    from src.services.db_admin_service import backup_database as _impl
    return _impl(*args, **kwargs)


def restore_database(*args, **kwargs) -> bool:
    from src.services.db_admin_service import restore_database as _impl
    return _impl(*args, **kwargs)


def list_backups(*args, **kwargs) -> List[Dict[str, Any]]:
    from src.services.db_admin_service import list_backups as _impl
    return _impl(*args, **kwargs)


def export_watchlist_csv(*args, **kwargs) -> int:
    from src.services.db_admin_service import export_watchlist_csv as _impl
    return _impl(*args, **kwargs)


def import_watchlist_csv(*args, **kwargs) -> int:
    from src.services.db_admin_service import import_watchlist_csv as _impl
    return _impl(*args, **kwargs)


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
