# server/app/files_service.py
from __future__ import annotations

import base64
import uuid
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from app.db import get_conn
from app.schema_introspect import table_meta

STORAGE_DIR = Path(__file__).resolve().parent / "storage" / "files_fs"
STORAGE_DIR.mkdir(parents=True, exist_ok=True)


def _ident(name: str) -> str:
    if not name or not name.replace("_", "").isalnum():
        raise ValueError(f"Bad identifier: {name}")
    return name


def _find_file_def(meta: Dict[str, Any], base: str) -> Dict[str, Any]:
    base = base.strip()
    for fc in meta.get("file_columns", []) or []:
        if str(fc.get("base", "")) == base:
            return fc
    raise ValueError("File column is not configured")


def _build_where_pk(pk: Dict[str, Any]) -> Tuple[str, list[Any]]:
    if not pk:
        raise ValueError("pk required")
    parts = []
    params: list[Any] = []
    for k, v in pk.items():
        k = _ident(str(k))
        parts.append(f'"{k}" = %s')
        params.append(v)
    return " AND ".join(parts), params


def prepare_inline_file_value(
    meta: Dict[str, Any],
    base: str,
    original_name: str,
    mime_type: Optional[str],
    content_bytes: bytes,
) -> Tuple[str, str, Any, Optional[str]]:
    """
    Used for INSERT_WITH_FILES (no pk yet).
    Returns:
      (name_col, data_col, stored_value, created_path_or_none)
    """
    base = _ident(base)
    if not original_name:
        raise ValueError("original_name required")

    fc = _find_file_def(meta, base)
    mode = fc["storage_mode"]
    name_col = _ident(fc["name_column"])
    data_col = _ident(fc["data_column"])

    created_path: Optional[str] = None

    if mode == "base64":
        stored_value: Any = base64.b64encode(content_bytes).decode("ascii")
    elif mode == "blob":
        stored_value = content_bytes
    elif mode == "fs":
        safe_name = original_name.replace("/", "_").replace("\\", "_")
        fname = f"{uuid.uuid4().hex}_{safe_name}"
        path = STORAGE_DIR / fname
        path.write_bytes(content_bytes)
        created_path = str(path)
        stored_value = created_path
    else:
        raise ValueError("Invalid storage_mode")

    return name_col, data_col, stored_value, created_path


def file_attach(
    table: str,
    pk: Dict[str, Any],
    base: str,
    original_name: str,
    mime_type: Optional[str],
    content_bytes: bytes,
    schema: str = "public",
) -> Dict[str, Any]:
    if schema != "public":
        raise ValueError("Only public schema allowed")

    table = _ident(table)
    base = _ident(base)

    meta = table_meta(table, schema=schema)
    name_col, data_col, new_data, new_path = prepare_inline_file_value(
        meta=meta,
        base=base,
        original_name=original_name,
        mime_type=mime_type,
        content_bytes=content_bytes,
    )
    mode = _find_file_def(meta, base)["storage_mode"]
    required = bool(_find_file_def(meta, base).get("required", False))

    where_sql, where_params = _build_where_pk(pk)

    old_path: Optional[str] = None
    if mode == "fs":
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f'SELECT "{data_col}" FROM "{schema}"."{table}" WHERE {where_sql} LIMIT 1;',
                tuple(where_params),
            )
            r = cur.fetchone()
            if r and r[0]:
                old_path = str(r[0])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f'UPDATE "{schema}"."{table}" '
            f'SET "{name_col}"=%s, "{data_col}"=%s '
            f'WHERE {where_sql} '
            f'RETURNING "{name_col}", "{data_col}";',
            tuple([original_name, new_data] + where_params),
        )
        updated = cur.fetchone()

    if not updated:
        if mode == "fs" and new_path:
            Path(str(new_path)).unlink(missing_ok=True)
        raise ValueError("row not found")

    if mode == "fs" and old_path and new_path and old_path != new_path:
        Path(old_path).unlink(missing_ok=True)

    return {
        "ok": True,
        "file": {
            "base": base,
            "original_name": original_name,
            "mime_type": mime_type,
            "storage_mode": mode,
            "required": required,
            "size_bytes": len(content_bytes),
        },
    }


def file_get(
    table: str,
    pk: Dict[str, Any],
    base: str,
    schema: str = "public",
) -> Dict[str, Any]:
    if schema != "public":
        raise ValueError("Only public schema allowed")

    table = _ident(table)
    base = _ident(base)

    meta = table_meta(table, schema=schema)
    fc = _find_file_def(meta, base)
    mode = fc["storage_mode"]
    name_col = _ident(fc["name_column"])
    data_col = _ident(fc["data_column"])

    where_sql, where_params = _build_where_pk(pk)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f'SELECT "{name_col}", "{data_col}" FROM "{schema}"."{table}" WHERE {where_sql} LIMIT 1;',
            tuple(where_params),
        )
        r = cur.fetchone()

    if not r:
        raise ValueError("row not found")

    original_name, stored = r
    if stored is None:
        raise ValueError("file not set")

    if mode == "base64":
        data = base64.b64decode(stored)
    elif mode == "blob":
        data = stored
    else:
        data = Path(str(stored)).read_bytes()

    return {
        "meta": {
            "original_name": original_name or f"{base}.bin",
            "mime_type": None,
            "storage_mode": mode,
        },
        "bytes": data,
    }


def file_delete(
    table: str,
    pk: Dict[str, Any],
    base: str,
    schema: str = "public",
) -> Dict[str, Any]:
    if schema != "public":
        raise ValueError("Only public schema allowed")

    table = _ident(table)
    base = _ident(base)

    meta = table_meta(table, schema=schema)
    fc = _find_file_def(meta, base)
    required = bool(fc.get("required", False))
    mode = fc["storage_mode"]
    name_col = _ident(fc["name_column"])
    data_col = _ident(fc["data_column"])

    if required:
        raise ValueError("file column is required (NOT NULL)")

    where_sql, where_params = _build_where_pk(pk)

    old_path: Optional[str] = None
    if mode == "fs":
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                f'SELECT "{data_col}" FROM "{schema}"."{table}" WHERE {where_sql} LIMIT 1;',
                tuple(where_params),
            )
            r = cur.fetchone()
            if r and r[0]:
                old_path = str(r[0])

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            f'UPDATE "{schema}"."{table}" '
            f'SET "{name_col}"=NULL, "{data_col}"=NULL '
            f'WHERE {where_sql};',
            tuple(where_params),
        )
        deleted = cur.rowcount > 0

    if deleted and mode == "fs" and old_path:
        Path(old_path).unlink(missing_ok=True)

    return {"deleted": bool(deleted)}
