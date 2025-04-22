# postgres_db.py
"""
Drop‑in PostgreSQL helper using psycopg2 **plus** a singleton you
initialise once in app.py and import everywhere else.

Quick start
-----------
# app.py ── initialise once
from postgres_db import init_db, DBConfig

init_db(
    DBConfig(
        host="127.0.0.1",
        database="mydb",
        user="myuser",
        password="secret",
        autocommit=False,
    )
)

# users.py ── reuse the pool anywhere
from postgres_db import db

def list_users():
    return db.fetchall("SELECT * FROM users")
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Optional, Sequence, List, Dict

import psycopg2
from psycopg2.pool import SimpleConnectionPool
from psycopg2.extras import RealDictCursor

# ────────────────────────────────────────────────────────────
# Core helper (unchanged from the first answer)
# ────────────────────────────────────────────────────────────

@dataclass(slots=True)
class DBConfig:
    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    user: str = "postgres"
    password: str = "postgres"
    minconn: int = 1
    maxconn: int = 10
    autocommit: bool = False                 # sensible default


class Database:
    """A lightweight psycopg2 connection‑pool wrapper."""

    _pool: SimpleConnectionPool
    _cfg: DBConfig

    # ── pool construction ────────────────────────────────
    def __init__(self, cfg: DBConfig) -> None:
        self._cfg = cfg
        self._pool = SimpleConnectionPool(
            cfg.minconn,
            cfg.maxconn,
            host=cfg.host,
            port=cfg.port,
            dbname=cfg.database,
            user=cfg.user,
            password=cfg.password,
        )

    def _acquire(self):
        conn = self._pool.getconn()
        conn.autocommit = self._cfg.autocommit
        return conn

    def _release(self, conn):
        self._pool.putconn(conn)

    # ── cursor helper ────────────────────────────────────
    @contextmanager
    def cursor(self, *, dict_rows: bool = True):
        conn = self._acquire()
        try:
            with conn.cursor(
                cursor_factory=RealDictCursor if dict_rows else None
            ) as cur:
                yield cur
                if not conn.autocommit:
                    conn.commit()
        except Exception:
            if not conn.autocommit:
                conn.rollback()
            raise
        finally:
            self._release(conn)

    # ── public helpers ───────────────────────────────────
    def execute(self, sql: str, params: Optional[Sequence[Any]] = None) -> None:
        with self.cursor(dict_rows=False) as cur:
            cur.execute(sql, params)

    def fetchone(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> Optional[Dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchone()

    def fetchall(
        self, sql: str, params: Optional[Sequence[Any]] = None
    ) -> List[Dict[str, Any]]:
        with self.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    # explicit multi‑statement block
    @contextmanager
    def transaction(self, *, dict_rows: bool = True):
        conn = self._acquire()
        try:
            with conn.cursor(
                cursor_factory=RealDictCursor if dict_rows else None
            ) as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            self._release(conn)

    def close(self):
        self._pool.closeall()


# ────────────────────────────────────────────────────────────
# Built‑in singleton
# ────────────────────────────────────────────────────────────
_db: Optional[Database] = None   # filled by init_db()


def init_db(cfg: DBConfig) -> Database:
    """
    Call exactly **once** (ideally at application start).
    Example:
        init_db(DBConfig(...))
    """
    global _db
    if _db is not None:
        raise RuntimeError("Database already initialised")

    _db = Database(cfg)
    return _db


@property  # gives attribute‑style access: db.fetchall(...)
def get_db() -> Database:
    if _db is None:
        raise RuntimeError("Database not initialised – call init_db() first")
    return _db
