from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from app.db import get_conn

LOG_DIR = Path(__file__).resolve().parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
AUDIT_FILE = LOG_DIR / "audit.log"

SENSITIVE_KEYS = {"password", "password_hash", "pass", "pwd", "token"}


def _redact(obj: Any) -> Any:
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            lk = str(k).lower()
            if lk in SENSITIVE_KEYS:
                out[k] = "***"
            elif lk in ("content_base64", "content_blob", "bytes"):
                out[k] = "<omitted>"
            else:
                out[k] = _redact(v)
        return out
    if isinstance(obj, list):
        return [_redact(x) for x in obj]
    return obj


def audit_log(
    level: str,
    action: str,
    user_login: Optional[str],
    user_role: Optional[str],
    table_name: Optional[str],
    details: Optional[Dict[str, Any]],
) -> None:
    level = (level or "INFO").upper()
    if level not in ("INFO", "WARNING", "ERROR"):
        level = "INFO"

    safe_details = _redact(details or {})
    ts_iso = datetime.now(timezone.utc).isoformat()

    # 1) write to file (append)
    line = {
        "ts": ts_iso,
        "level": level,
        "user_login": user_login,
        "user_role": user_role,
        "action": action,
        "table": table_name,
        "details": safe_details,
    }
    with AUDIT_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")

    # 2) write to DB
    sql = """
        INSERT INTO audit_log (level, user_login, user_role, action, table_name, details)
        VALUES (%s, %s, %s, %s, %s, %s::jsonb);
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            sql,
            (
                level,
                user_login,
                user_role,
                action,
                table_name,
                json.dumps(safe_details, ensure_ascii=False),
            ),
        )
