# server/app/auth_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

import bcrypt
import jwt

from app.config import JWT_SECRET, JWT_TTL_MIN
from app.db import get_conn


@dataclass
class AuthUser:
    id: int
    login: str
    full_name: str
    role: str


def hash_password(password: str) -> str:
    if not password or len(password) < 4:
        raise ValueError("Password too short")
    salt = bcrypt.gensalt()
    h = bcrypt.hashpw(password.encode("utf-8"), salt)
    return h.decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))
    except Exception:
        return False


def create_user(login: str, password: str, full_name: str, role: str = "user") -> Dict[str, Any]:
    if role not in ("admin", "user"):
        raise ValueError("role must be admin or user")

    login = login.strip()
    full_name = full_name.strip()
    if not login or not full_name:
        raise ValueError("login and full_name required")

    ph = hash_password(password)

    sql = """
        INSERT INTO users (login, password_hash, full_name, role)
        VALUES (%s, %s, %s, %s)
        RETURNING id, login, full_name, role, created_at;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (login, ph, full_name, role))
        row = cur.fetchone()

    if not row:
        return {"user": None}

    return {
        "user": {
            "id": row[0],
            "login": row[1],
            "full_name": row[2],
            "role": row[3],
            "created_at": row[4].isoformat() if hasattr(row[4], "isoformat") else row[4],
        }
    }


def authenticate(login: str, password: str) -> Optional[AuthUser]:
    sql = """
        SELECT id, login, password_hash, full_name, role
        FROM users
        WHERE login = %s
        LIMIT 1;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (login,))
        r = cur.fetchone()

    if not r:
        return None

    user_id, login, password_hash, full_name, role = r
    if not verify_password(password, password_hash):
        return None

    return AuthUser(id=int(user_id), login=str(login), full_name=str(full_name), role=str(role))


def issue_token(u: AuthUser) -> str:
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=JWT_TTL_MIN)
    payload = {
        "sub": str(u.id),
        "login": u.login,
        "role": u.role,
        "full_name": u.full_name,
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")


def verify_token(token: str) -> AuthUser:
    if not token:
        raise ValueError("token required")
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
    except jwt.ExpiredSignatureError:
        raise ValueError("token expired")
    except jwt.InvalidTokenError:
        raise ValueError("invalid token")

    return AuthUser(
        id=int(payload["sub"]),
        login=str(payload.get("login", "")),
        full_name=str(payload.get("full_name", "")),
        role=str(payload.get("role", "user")),
    )
