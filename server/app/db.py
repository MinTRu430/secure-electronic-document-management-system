# server/app/db.py
import psycopg2
from psycopg2.pool import SimpleConnectionPool
from contextlib import contextmanager

from app.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS

_pool: SimpleConnectionPool | None = None


def init_db_pool():
    global _pool
    if _pool is None:
        _pool = SimpleConnectionPool(
            minconn=1,
            maxconn=10,
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
        )
    return _pool


@contextmanager
def get_conn():
    if _pool is None:
        init_db_pool()

    conn = _pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)
