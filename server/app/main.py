# server/app/main.py
from __future__ import annotations

import socket
import struct
from typing import Any, Dict, List

from app.protocol import recv_msg, send_msg
from app.schema_introspect import list_tables, table_meta
from app.crud_dynamic import (
    select_rows,
    insert_row,
    update_row_by_pk,
    delete_row_by_pk,
    search_rows,
    fk_options,
)
from app.ddl import create_table
from app.auth_service import authenticate, issue_token, verify_token, create_user
from app.audit_service import audit_log
from app.backup_service import create_backup, list_backups, restore_backup
from app.scheduler import start_scheduler, apply_backup_schedule, load_and_apply_backup_schedule
from app.crypto_ctx import init_crypto, pub_to_json, pub_from_json
from app.secure_protocol import (
    recv_encrypted,
    send_encrypted,
    recv_encrypted_bin,
    send_encrypted_bin,
)
from app.settings_service import get_backup_schedule, set_backup_schedule

# INLINE FILE API (no files table)
from app.files_service import file_attach, file_get, file_delete, prepare_inline_file_value

HOST = "0.0.0.0"
PORT = 9090
UPLOAD_PORT = 9091

MAINTENANCE = False
CRYPTO = init_crypto()


def _unpack_multi_files(blob: bytes) -> List[bytes]:
    """
    Custom framing for multiple files:
      [count:4]
      repeat count times:
        [len:4][bytes...]
    """
    off = 0
    if len(blob) < 4:
        raise ValueError("bad files blob")
    (count,) = struct.unpack(">I", blob[off:off + 4])
    off += 4
    out: List[bytes] = []
    for _ in range(count):
        if off + 4 > len(blob):
            raise ValueError("bad files blob")
        (n,) = struct.unpack(">I", blob[off:off + 4])
        off += 4
        if off + n > len(blob):
            raise ValueError("bad files blob")
        out.append(blob[off:off + n])
        off += n
    return out


def handle(req: Dict[str, Any]) -> Dict[str, Any]:
    global MAINTENANCE 
    t = req.get("type")

    if t == "ping":
        return {"ok": True, "type": "pong"}

    # ===== Auth: login only without token =====
    if t == "login":
        login = req.get("login", "")
        password = req.get("password", "")
        u = authenticate(login, password)
        if not u:
            return {"ok": False, "error": "bad credentials"}

        token = issue_token(u)
        audit_log("INFO", "login", u.login, u.role, None, {"login": u.login})
        return {
            "ok": True,
            "token": token,
            "user": {"id": u.id, "login": u.login, "full_name": u.full_name, "role": u.role},
        }

    # ===== All other calls require token =====
    token = req.get("token")
    try:
        auth_user = verify_token(token)

        global MAINTENANCE
        if MAINTENANCE and t not in ("backup_restore", "backup_list"):
            return {"ok": False, "error": "server in maintenance mode"}

        # base audit for most ops
        if t not in ("insert", "update", "delete", "file_get", "file_delete"):
            audit_log(
                "INFO",
                t or "unknown",
                auth_user.login,
                auth_user.role,
                req.get("table"),
                {"req": req},
            )
    except Exception as e:
        return {"ok": False, "error": str(e)}

    # ===== Schema =====
    if t == "list_tables":
        tables = list_tables()
        return {"ok": True, "tables": tables}

    if t == "table_meta":
        table = req.get("table")
        if not table:
            return {"ok": False, "error": "table is required"}
        meta = table_meta(table)
        return {"ok": True, "meta": meta}

    # ===== Read =====
    if t == "select":
        table = req.get("table")
        if not table:
            return {"ok": False, "error": "table is required"}
        limit = int(req.get("limit", 200))
        offset = int(req.get("offset", 0))
        data = select_rows(table, limit=limit, offset=offset)
        return {"ok": True, "data": data}

    # ===== Write =====
    if t == "insert":
        table = req.get("table")
        values = req.get("values")
        if not table or not isinstance(values, dict):
            return {"ok": False, "error": "table and dict values are required"}

        out = insert_row(table, values)

        audit_log(
            "INFO",
            "insert",
            auth_user.login,
            auth_user.role,
            table,
            {"values": values, "row": out.get("row")},
        )
        return {"ok": True, "data": out}

    if t == "update":
        table = req.get("table")
        pk = req.get("pk")
        values = req.get("values")
        if not table or not isinstance(pk, dict) or not isinstance(values, dict):
            return {"ok": False, "error": "table, dict pk and dict values are required"}

        out = update_row_by_pk(table, pk, values)

        audit_log(
            "INFO",
            "update",
            auth_user.login,
            auth_user.role,
            table,
            {"pk": pk, "values": values, "row": out.get("row")},
        )
        return {"ok": True, "data": out}

    if t == "delete":
        table = req.get("table")
        pk = req.get("pk")
        if not table or not isinstance(pk, dict):
            return {"ok": False, "error": "table and dict pk are required"}

        out = delete_row_by_pk(table, pk)

        audit_log(
            "INFO",
            "delete",
            auth_user.login,
            auth_user.role,
            table,
            {"pk": pk, "row": out.get("row")},
        )
        return {"ok": True, "data": out}

    # ===== Search / FK helpers =====
    if t == "search":
        table = req.get("table")
        query = req.get("query")
        column = req.get("column")
        if not table or not isinstance(query, str):
            return {"ok": False, "error": "table and query are required"}
        limit = int(req.get("limit", 200))
        offset = int(req.get("offset", 0))
        out = search_rows(table, query, column=column, limit=limit, offset=offset)
        return {"ok": True, "data": out}

    if t == "fk_options":
        ref_table = req.get("ref_table")
        id_column = req.get("id_column", "id")
        label_column = req.get("label_column")
        if not ref_table:
            return {"ok": False, "error": "ref_table is required"}
        limit = int(req.get("limit", 200))
        offset = int(req.get("offset", 0))
        out = fk_options(ref_table, id_column=id_column, label_column=label_column, limit=limit, offset=offset)
        return {"ok": True, "data": out}

    # ===== DDL =====
    if t == "create_table":
        payload = req.get("payload")
        if not isinstance(payload, dict):
            return {"ok": False, "error": "payload dict is required"}
        out = create_table(payload)
        return {"ok": True, "data": out}

    # ===== Admin: users =====
    if t == "user_create":
        if auth_user.role != "admin":
            return {"ok": False, "error": "admin only"}
        login = req.get("login", "")
        password = req.get("password", "")
        full_name = req.get("full_name", "")
        role = req.get("role", "user")
        out = create_user(login, password, full_name, role)
        audit_log(
            "INFO",
            "user_create",
            auth_user.login,
            auth_user.role,
            "users",
            {"created_login": login, "created_role": role, "created_full_name": full_name},
        )
        return {"ok": True, "data": out}

    # ===== Backups =====
    if t == "backup_create":
        if auth_user.role != "admin":
            return {"ok": False, "error": "admin only"}
        out = create_backup(auth_user.login, auth_user.role)
        return {"ok": True, "data": out}

    if t == "backup_list":
        if auth_user.role != "admin":
            return {"ok": False, "error": "admin only"}
        out = list_backups()
        return {"ok": True, "data": out}

    if t == "backup_restore":
        if auth_user.role != "admin":
            return {"ok": False, "error": "admin only"}
        name = req.get("name")
        if not name:
            return {"ok": False, "error": "name is required"}

        MAINTENANCE = True
        try:
            out = restore_backup(name, auth_user.login, auth_user.role)
        finally:
            MAINTENANCE = False

        return {"ok": True, "data": out}

    if t == "backup_schedule_get":
        # admin-only
        if auth_user.role != "admin":
            return {"ok": False, "error": "forbidden"}

        schedule = get_backup_schedule()
        # можно сразу вернуть расчёт next_run_time, чтобы UI видел эффект
        applied = apply_backup_schedule(schedule) if schedule.get("enabled") else {"enabled": False}
        # Важно: apply_backup_schedule удалит/создаст job; это ок, но можно и без этого.
        return {"ok": True, "schedule": {**schedule, **({"next_run_time": applied.get("next_run_time")} if schedule.get("enabled") else {})}}

    if t == "backup_schedule_set":
        # admin-only
        if auth_user.role != "admin":
            return {"ok": False, "error": "forbidden"}

        enabled = bool(req.get("enabled", True))
        hour = int(req.get("hour", 2))
        minute = int(req.get("minute", 0))
        timezone = str(req.get("timezone", "UTC"))

        if hour < 0 or hour > 23 or minute < 0 or minute > 59:
            return {"ok": False, "error": "invalid time"}

        # сохраняем в БД
        schedule = set_backup_schedule(enabled, hour, minute, timezone)

        # применяем сразу в scheduler
        applied = apply_backup_schedule(schedule) if enabled else {"enabled": False, "next_run_time": None}

        # аудит (если у тебя audit_log подключён)
        # audit_log("INFO", "backup_schedule_set", user.login, user.role, None, {"enabled": enabled, "hour": hour, "minute": minute, "timezone": timezone})

        return {"ok": True, "schedule": {**schedule, "next_run_time": applied.get("next_run_time")}}



    # ===== Files (INLINE MODEL) =====
    if t == "file_get":
        table = req.get("table")
        pk = req.get("pk")
        base = req.get("base")
        if not table or not isinstance(pk, dict) or not base:
            return {"ok": False, "error": "table, dict pk and base are required"}

        out = file_get(table=str(table), pk=pk, base=str(base))

        audit_log(
            "INFO",
            "file_get",
            auth_user.login,
            auth_user.role,
            str(table),
            {"table": str(table), "pk": pk, "base": str(base), "original_name": out["meta"].get("original_name")},
        )

        return {"__bin__": True, "header": {"ok": True, "meta": out["meta"]}, "bin": out["bytes"]}

    if t == "file_delete":
        table = req.get("table")
        pk = req.get("pk")
        base = req.get("base")
        if not table or not isinstance(pk, dict) or not base:
            return {"ok": False, "error": "table, dict pk and base are required"}

        out = file_delete(table=str(table), pk=pk, base=str(base))

        audit_log(
            "INFO",
            "file_delete",
            auth_user.login,
            auth_user.role,
            str(table),
            {"table": str(table), "pk": pk, "base": str(base), "deleted": out.get("deleted")},
        )

        return {"ok": True, "data": out}

    return {"ok": False, "error": f"unknown type: {t}"}



def serve() -> None:
    with socket.create_server((HOST, PORT), reuse_port=True) as server:
        print(f"Secure socket server listening on {HOST}:{PORT} mode={CRYPTO.mode}")
        while True:
            conn, _addr = server.accept()
            with conn:
                client_pub = None
                try:
                    hello = recv_msg(conn)
                    if hello.get("type") != "hello":
                        send_msg(conn, {"ok": False, "error": "expected hello"})
                        continue

                    client_pub = pub_from_json(hello["pub"])
                    send_msg(conn, {"ok": True, "type": "hello_ack", "pub": pub_to_json(CRYPTO.pub)})

                    req = recv_encrypted(conn, CRYPTO.priv, mode=CRYPTO.mode)
                    resp = handle(req)

                    if isinstance(resp, dict) and resp.get("__bin__"):
                        send_encrypted_bin(conn, resp["header"], resp["bin"], client_pub, mode=CRYPTO.mode)
                    else:
                        send_encrypted(conn, resp, client_pub, mode=CRYPTO.mode)

                except Exception as e:
                    err = {"ok": False, "error": str(e)}
                    try:
                        if client_pub is not None:
                            send_encrypted(conn, err, client_pub, mode=CRYPTO.mode)
                        else:
                            send_msg(conn, err)
                    except Exception:
                        pass


def serve_upload() -> None:
    """
    Upload server принимает encrypted bin request:
    1) file_attach (existing row):
       header: {type:"file_attach", table, pk:{...}, base:"...", original_name, mime_type, token}
       data: bytes

    2) insert_with_files (new row with required files):
       header: {type:"insert_with_files", table, values:{...}, files:[{base, original_name, mime_type}], token}
       data: multi-files framed blob (see _unpack_multi_files)

    Ответ: {ok:true, data:{...}}
    """
    with socket.create_server((HOST, UPLOAD_PORT), reuse_port=True) as server:
        print(f"Secure upload server listening on {HOST}:{UPLOAD_PORT} mode={CRYPTO.mode}")
        while True:
            conn, _addr = server.accept()
            with conn:
                client_pub = None
                try:
                    hello = recv_msg(conn)
                    if hello.get("type") != "hello":
                        send_msg(conn, {"ok": False, "error": "expected hello"})
                        continue

                    client_pub = pub_from_json(hello["pub"])
                    send_msg(conn, {"ok": True, "type": "hello_ack", "pub": pub_to_json(CRYPTO.pub)})

                    header, data = recv_encrypted_bin(conn, CRYPTO.priv, mode=CRYPTO.mode)
                    htype = header.get("type")

                    token = header.get("token")
                    auth_user = verify_token(token)

                    # ===== Existing-row attach/replace =====
                    if htype == "file_attach":
                        table = header.get("table")
                        pk = header.get("pk")
                        base = header.get("base")
                        original_name = header.get("original_name")
                        mime_type = header.get("mime_type")

                        if not table or not isinstance(pk, dict) or not base or not original_name:
                            send_encrypted(
                                conn,
                                {"ok": False, "error": "table, dict pk, base and original_name are required"},
                                client_pub,
                                mode=CRYPTO.mode,
                            )
                            continue

                        audit_log(
                            "INFO",
                            "file_attach",
                            auth_user.login,
                            auth_user.role,
                            str(table),
                            {
                                "table": str(table),
                                "pk": pk,
                                "base": str(base),
                                "original_name": str(original_name),
                                "mime_type": mime_type,
                                "size_bytes": len(data),
                            },
                        )

                        file_attach(
                            table=str(table),
                            pk=pk,
                            base=str(base),
                            original_name=str(original_name),
                            mime_type=mime_type,
                            content_bytes=data,
                        )

                        send_encrypted(conn, {"ok": True}, client_pub, mode=CRYPTO.mode)
                        continue

                    # ===== New-row insert with required files =====
                    if htype == "insert_with_files":
                        table = header.get("table")
                        values = header.get("values")
                        files = header.get("files")  # list[{base, original_name, mime_type}]
                        if not table or not isinstance(values, dict) or not isinstance(files, list):
                            send_encrypted(
                                conn,
                                {"ok": False, "error": "table, dict values and list files are required"},
                                client_pub,
                                mode=CRYPTO.mode,
                            )
                            continue

                        blobs = _unpack_multi_files(data)
                        if len(blobs) != len(files):
                            send_encrypted(
                                conn,
                                {"ok": False, "error": "files count mismatch"},
                                client_pub,
                                mode=CRYPTO.mode,
                            )
                            continue

                        # prepare inline values for each file (write FS / base64 / bytes)
                        created_paths: List[str] = []
                        try:
                            meta = table_meta(str(table))
                            for desc, b in zip(files, blobs):
                                base = str(desc.get("base") or "")
                                original_name = str(desc.get("original_name") or "")
                                mime_type = desc.get("mime_type")
                                if not base or not original_name:
                                    raise ValueError("bad file descriptor")

                                name_col, data_col, stored_value, created_path = prepare_inline_file_value(
                                    meta=meta,
                                    base=base,
                                    original_name=original_name,
                                    mime_type=mime_type,
                                    content_bytes=b,
                                )
                                values[name_col] = original_name
                                values[data_col] = stored_value
                                if created_path:
                                    created_paths.append(created_path)

                            out = insert_row(str(table), values)

                            audit_log(
                                "INFO",
                                "insert_with_files",
                                auth_user.login,
                                auth_user.role,
                                str(table),
                                {
                                    "table": str(table),
                                    "values_keys": sorted(list(values.keys())),
                                    "files": [{"base": f.get("base"), "original_name": f.get("original_name")} for f in files],
                                    "row": out.get("row"),
                                },
                            )

                            send_encrypted(conn, {"ok": True, "data": out}, client_pub, mode=CRYPTO.mode)

                        except Exception as e:
                            # cleanup created fs files if insert failed
                            from pathlib import Path
                            for p in created_paths:
                                Path(p).unlink(missing_ok=True)
                            send_encrypted(conn, {"ok": False, "error": str(e)}, client_pub, mode=CRYPTO.mode)

                        continue

                    send_encrypted(conn, {"ok": False, "error": f"unknown upload type: {htype}"}, client_pub, mode=CRYPTO.mode)

                except Exception as e:
                    err = {"ok": False, "error": str(e)}
                    try:
                        if client_pub is not None:
                            send_encrypted(conn, err, client_pub, mode=CRYPTO.mode)
                        else:
                            send_msg(conn, err)
                    except Exception:
                        pass


if __name__ == "__main__":
    import threading

    t = threading.Thread(target=serve_upload, daemon=True)
    t.start()

    start_scheduler()
    load_and_apply_backup_schedule()
    serve()
