# server/app/crud_dynamic.py
from __future__ import annotations

from typing import Any, Dict, Optional, Tuple, List

from app.db import get_conn
from datetime import date, datetime
from decimal import Decimal

def _validate_ident(name: str) -> str:
    # Разрешаем только буквы/цифры/underscore, без кавычек/пробелов/точек.
    if not name or not name.replace("_", "").isalnum():
        raise ValueError(f"Bad identifier: {name}")
    return name


def _validate_table(schema: str, table: str) -> Tuple[str, str]:
    if schema != "public":
        raise ValueError("Only public schema allowed")
    return _validate_ident(schema), _validate_ident(table)



def _jsonify(v: Any) -> Any:
    # делаем значения JSON-safe
    if isinstance(v, memoryview):
        v = v.tobytes()
    if isinstance(v, (bytes, bytearray)):
        # чтобы не слать большие бинарные данные в UI
        return f"<BLOB {len(v)} bytes>"
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        # чтобы не терять точность, лучше строкой (или float если хочешь)
        return str(v)
    return v


def _jsonify_rows(rows: list[list[Any]]) -> list[list[Any]]:
    return [[_jsonify(x) for x in row] for row in rows]


def _jsonify_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _jsonify(v) for k, v in d.items()}


def _fetch_all(cur) -> tuple[list[str], list[list[Any]]]:
    cols = [d.name for d in cur.description] if cur.description else []
    rows = [list(r) for r in cur.fetchall()] if cols else []
    return cols, rows


def _fetch_one_mapping(cur) -> Optional[Dict[str, Any]]:
    row = cur.fetchone()
    if row is None:
        return None
    cols = [d.name for d in cur.description]
    return {cols[i]: row[i] for i in range(len(cols))}


def select_rows(table: str, schema: str = "public", limit: int = 200, offset: int = 0) -> Dict[str, Any]:
    schema, table = _validate_table(schema, table)
    limit = int(limit)
    offset = int(offset)

    sql = f'SELECT * FROM "{schema}"."{table}" ORDER BY 1 LIMIT %s OFFSET %s'

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit, offset))
            cols, rows = _fetch_all(cur)

    return {"columns": cols, "rows": _jsonify_rows(rows), "limit": limit, "offset": offset}


def insert_row(table: str, values: Dict[str, Any], schema: str = "public") -> Dict[str, Any]:
    schema, table = _validate_table(schema, table)
    if not values:
        raise ValueError("values is empty")

    cols = [_validate_ident(c) for c in values.keys()]
    col_sql = ", ".join([f'"{c}"' for c in cols])
    placeholders = ", ".join(["%s"] * len(cols))
    params = [values[c] for c in cols]

    sql = f'INSERT INTO "{schema}"."{table}" ({col_sql}) VALUES ({placeholders}) RETURNING *'

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = _fetch_one_mapping(cur)

    return {"row": _jsonify_dict(row) if row else None}


def update_row_by_pk(
    table: str,
    pk: Dict[str, Any],
    values: Dict[str, Any],
    schema: str = "public",
) -> Dict[str, Any]:
    schema, table = _validate_table(schema, table)
    if not pk:
        raise ValueError("pk is empty")
    if not values:
        raise ValueError("values is empty")

    set_cols = [_validate_ident(c) for c in values.keys()]
    where_cols = [_validate_ident(c) for c in pk.keys()]

    set_sql = ", ".join([f'"{c}" = %s' for c in set_cols])
    where_sql = " AND ".join([f'"{c}" = %s' for c in where_cols])

    params: list[Any] = [values[c] for c in set_cols] + [pk[c] for c in where_cols]

    sql = f'UPDATE "{schema}"."{table}" SET {set_sql} WHERE {where_sql} RETURNING *'

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = _fetch_one_mapping(cur)

    return {"row": _jsonify_dict(row) if row else None}


def delete_row_by_pk(table: str, pk: Dict[str, Any], schema: str = "public") -> Dict[str, Any]:
    schema, table = _validate_table(schema, table)
    if not pk:
        raise ValueError("pk is empty")

    where_cols = [_validate_ident(c) for c in pk.keys()]
    where_sql = " AND ".join([f'"{c}" = %s' for c in where_cols])
    params = [pk[c] for c in where_cols]

    sql = f'DELETE FROM "{schema}"."{table}" WHERE {where_sql} RETURNING *'

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = _fetch_one_mapping(cur)

    return {"row": _jsonify_dict(row) if row else None}


def search_rows(
    table: str,
    query: str,
    schema: str = "public",
    column: Optional[str] = None,
    limit: int = 200,
    offset: int = 0,
) -> Dict[str, Any]:
    schema, table = _validate_table(schema, table)
    if not query:
        raise ValueError("query is empty")
    limit = int(limit)
    offset = int(offset)

    cols_sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position;
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(cols_sql, (schema, table))
            cols_rows = cur.fetchall()

    text_cols = [r[0] for r in cols_rows if r[1] in ("character varying", "text", "character")]
    if not text_cols:
        return {"columns": [], "rows": [], "limit": limit, "offset": offset}

    if column:
        column = _validate_ident(column)
        if column not in text_cols:
            raise ValueError(f"column {column} is not a text column of {table}")
        where_sql = f'"{column}" ILIKE %s'
        where_params = [f"%{query}%"]
    else:
        where_sql = " OR ".join([f'"{c}" ILIKE %s' for c in text_cols])
        where_params = [f"%{query}%"] * len(text_cols)

    sql = f'SELECT * FROM "{schema}"."{table}" WHERE ({where_sql}) ORDER BY 1 LIMIT %s OFFSET %s'
    params = where_params + [limit, offset]

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns, rows = _fetch_all(cur)

    return {
        "columns": columns,
        "rows": _jsonify_rows(rows),
        "limit": limit,
        "offset": offset,
        "searched_columns": text_cols,
    }


def fk_options(
    ref_table: str,
    id_column: str = "id",
    label_column: Optional[str] = None,
    schema: str = "public",
    limit: int = 200,
    offset: int = 0,
) -> Dict[str, Any]:
    schema, ref_table = _validate_table(schema, ref_table)
    id_column = _validate_ident(id_column)
    if label_column:
        label_column = _validate_ident(label_column)

    guess_sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
          AND column_name IN ('full_name', 'title', 'email', 'name', 'status')
        ORDER BY CASE column_name
          WHEN 'full_name' THEN 1
          WHEN 'title' THEN 2
          WHEN 'email' THEN 3
          WHEN 'name' THEN 4
          WHEN 'status' THEN 5
          ELSE 100
        END
        LIMIT 1;
    """

    if label_column is None:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(guess_sql, (schema, ref_table))
                r = cur.fetchone()
        label_column = r[0] if r else id_column

    sql = (
        f'SELECT "{id_column}" as id, "{label_column}" as label '
        f'FROM "{schema}"."{ref_table}" '
        f'ORDER BY 1 LIMIT %s OFFSET %s'
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (limit, offset))
            cols = [d.name for d in cur.description]
            items = [dict(zip(cols, row)) for row in cur.fetchall()]

    return {"items": items, "id_column": id_column, "label_column": label_column}
