from __future__ import annotations

import subprocess
import tarfile
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List

from app.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASS
from app.audit_service import audit_log

BASE = Path(__file__).resolve().parent
BACKUP_DIR = BASE / "backups"
FILES_DIR = BASE / "storage" / "files_fs"
LOGS_DIR = BASE / "logs"
BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def _run(cmd: List[str], env: dict) -> None:
    p = subprocess.run(cmd, env=env, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(p.stderr.strip() or "backup command failed")


def create_backup(user_login: str, user_role: str) -> Dict[str, Any]:
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    name = f"backup_{ts}"
    out_dir = BACKUP_DIR / name
    out_dir.mkdir(parents=True, exist_ok=True)

    dump_path = out_dir / "db.dump"
    env = dict(**{**subprocess.os.environ, "PGPASSWORD": DB_PASS})

    # pg_dump custom format
    _run([
        "pg_dump",
        "-h", DB_HOST,
        "-p", str(DB_PORT),
        "-U", DB_USER,
        "-d", DB_NAME,
        "-F", "c",
        "-f", str(dump_path),
    ], env)

    # tar files + logs (если папок нет — ок)
    tar_path = out_dir / "files_logs.tar.gz"
    with tarfile.open(tar_path, "w:gz") as tar:
        if FILES_DIR.exists():
            tar.add(FILES_DIR, arcname="files_fs")
        if LOGS_DIR.exists():
            tar.add(LOGS_DIR, arcname="logs")

    audit_log("INFO", "backup_create", user_login, user_role, None, {"backup": name})
    return {"name": name, "path": str(out_dir)}


def list_backups() -> Dict[str, Any]:
    items = []
    for p in sorted(BACKUP_DIR.glob("backup_*")):
        if p.is_dir():
            items.append(p.name)
    return {"backups": items}

def restore_backup(name: str, user_login: str, user_role: str) -> Dict[str, Any]:
    # ВАЖНО: restore подразумевает, что сервер может потерять текущие соединения/данные,
    # поэтому мы делаем простой "maintenance lock" в main.py.
    bdir = BACKUP_DIR / name
    if not bdir.exists() or not bdir.is_dir():
        raise ValueError("backup not found")

    dump_path = bdir / "db.dump"
    tar_path = bdir / "files_logs.tar.gz"
    if not dump_path.exists():
        raise ValueError("db.dump missing")

    env = dict(**{**subprocess.os.environ, "PGPASSWORD": DB_PASS})

    # 1) drop schema public (очищаем базу) и создаём заново
    _run([
        "psql",
        "-h", DB_HOST,
        "-p", str(DB_PORT),
        "-U", DB_USER,
        "-d", DB_NAME,
        "-c", "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
    ], env)

    # 2) restore db
    _run([
        "pg_restore",
        "-h", DB_HOST,
        "-p", str(DB_PORT),
        "-U", DB_USER,
        "-d", DB_NAME,
        str(dump_path),
    ], env)


    # 3) restore files + logs
    if tar_path.exists():
        # распакуем в temp, потом подменим
        import tempfile
        import shutil

        tmp = Path(tempfile.mkdtemp(prefix="restore_"))
        with tarfile.open(tar_path, "r:gz") as tar:
            tar.extractall(tmp)

        # tmp/files_fs -> FILES_DIR
        if (tmp / "files_fs").exists():
            if FILES_DIR.exists():
                shutil.rmtree(FILES_DIR, ignore_errors=True)
            shutil.copytree(tmp / "files_fs", FILES_DIR)

        if (tmp / "logs").exists():
            if LOGS_DIR.exists():
                shutil.rmtree(LOGS_DIR, ignore_errors=True)
            shutil.copytree(tmp / "logs", LOGS_DIR)

    audit_log("WARNING", "backup_restore", user_login, user_role, None, {"backup": name})
    return {"restored": True, "name": name}
