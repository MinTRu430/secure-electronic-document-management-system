# server/app/settings_service.py
import json
from typing import Any, Dict, Optional

from app.db import get_conn

DEFAULT_BACKUP_SCHEDULE: Dict[str, Any] = {
    "enabled": True,
    "hour": 2,
    "minute": 0,
    "timezone": "UTC",
}


def get_setting_json(key: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM app_settings WHERE key=%s", (key,))
            row = cur.fetchone()
            if not row:
                return None
            # psycopg2 обычно отдаёт jsonb как dict уже, но на всякий случай:
            val = row[0]
            if isinstance(val, str):
                return json.loads(val)
            return val


def set_setting_json(key: str, value: Dict[str, Any]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_settings (key, value)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value
                """,
                (key, json.dumps(value)),
            )


def get_backup_schedule() -> Dict[str, Any]:
    s = get_setting_json("backup_schedule")
    if not s:
        return DEFAULT_BACKUP_SCHEDULE.copy()

    # нормализация
    out = DEFAULT_BACKUP_SCHEDULE.copy()
    out.update(s)

    out["enabled"] = bool(out.get("enabled", True))
    out["hour"] = int(out.get("hour", 2))
    out["minute"] = int(out.get("minute", 0))
    out["timezone"] = str(out.get("timezone", "UTC"))

    # clamp на всякий случай
    if out["hour"] < 0: out["hour"] = 0
    if out["hour"] > 23: out["hour"] = 23
    if out["minute"] < 0: out["minute"] = 0
    if out["minute"] > 59: out["minute"] = 59

    return out


def set_backup_schedule(enabled: bool, hour: int, minute: int, timezone: str) -> Dict[str, Any]:
    schedule = {
        "enabled": bool(enabled),
        "hour": int(hour),
        "minute": int(minute),
        "timezone": str(timezone),
    }
    set_setting_json("backup_schedule", schedule)
    return schedule
