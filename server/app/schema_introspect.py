# server/app/schema_introspect.py
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Set
from app.db import get_conn

# Таблицы, которые считаем служебными для приложения и не показываем в UI
DEFAULT_EXCLUDE: Set[str] = {
    "users",
    "audit_log",
    "app_settings",
}

def list_tables(schema: str = "public", exclude: Optional[Set[str]] = None) -> List[str]:
    ex = set(DEFAULT_EXCLUDE)
    if exclude:
        ex |= set(exclude)

    sql = """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (schema,))
        tables = [r[0] for r in cur.fetchall()]

    # Фильтруем "служебные" таблицы приложения
    return [t for t in tables if t not in ex]


def table_meta(table: str, schema: str = "public") -> Dict[str, Any]:
    with get_conn() as conn, conn.cursor() as cur:
        # columns
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position;
        """, (schema, table))
        columns = cur.fetchall()

        # pk
        cur.execute("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position;
        """, (schema, table))
        pk = [r[0] for r in cur.fetchall()]

        # fk
        cur.execute("""
            SELECT
              kcu.column_name,
              ccu.table_name,
              ccu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
            WHERE tc.table_schema = %s
              AND tc.table_name = %s
              AND tc.constraint_type = 'FOREIGN KEY';
        """, (schema, table))
        fks = cur.fetchall()

        # column comments (used for inline file metadata)
        cur.execute(
            """
            SELECT a.attname,
                   pg_catalog.col_description(c.oid, a.attnum) AS comment
            FROM pg_catalog.pg_attribute a
            JOIN pg_catalog.pg_class c ON c.oid = a.attrelid
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND a.attnum > 0
              AND NOT a.attisdropped;
            """,
            (schema, table),
        )
        col_comments = cur.fetchall()

    # Parse inline file metadata from comments.
    # We attach comment JSON on <base>_data column.
    file_columns: List[Dict[str, Any]] = []
    for col_name, comment in col_comments:
        if not comment:
            continue
        try:
            meta = json.loads(comment)
        except Exception:
            continue
        if not isinstance(meta, dict) or not meta.get("file"):
            continue
        base = str(meta.get("base") or "").strip()
        name_col = str(meta.get("name_col") or "").strip()
        mode = str(meta.get("mode") or "").strip()
        required = bool(meta.get("required", False))
        if not base or not name_col or mode not in ("base64", "blob", "fs"):
            continue

        file_columns.append(
            {
                "base": base,
                "name_column": name_col,
                "data_column": col_name,
                "storage_mode": mode,
                "required": required,
            }
        )

    return {
        "schema": schema,
        "table": table,
        "columns": [
            {
                "name": c[0],
                "type": c[1],
                "nullable": (c[2] == "YES"),
                "default": c[3],
            } for c in columns
        ],
        "primary_key": pk,
        "foreign_keys": [
            {"column": r[0], "ref_table": r[1], "ref_column": r[2]}
            for r in fks
        ],
        "file_columns": file_columns,
    }
