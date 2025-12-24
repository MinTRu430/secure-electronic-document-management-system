import socket
import os
import struct
from typing import Any, Dict, Optional, List

from app.protocol import send_msg, recv_msg
from app.crypto_ctx import init_crypto, pub_to_json, pub_from_json
from app.secure_protocol import send_encrypted, recv_encrypted, send_encrypted_bin, recv_encrypted_bin


def _pack_multi_files(blobs: List[bytes]) -> bytes:
    """
    Custom framing for multiple files:
      [count:4]
      repeat:
        [len:4][bytes]
    """
    out = bytearray()
    out += struct.pack(">I", len(blobs))
    for b in blobs:
        out += struct.pack(">I", len(b))
        out += b
    return bytes(out)


class SocketClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 9090, timeout: float = 120.0):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.token: str | None = None
        self.crypto = init_crypto()

    def call(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        if self.token and "token" not in msg and msg.get("type") not in ("login", "ping"):
            msg["token"] = self.token

        with socket.create_connection((self.host, self.port), timeout=self.timeout) as s:
            send_msg(s, {"type": "hello", "pub": pub_to_json(self.crypto.pub)})
            hello_ack = recv_msg(s)
            if not hello_ack.get("ok"):
                return hello_ack
            server_pub = pub_from_json(hello_ack["pub"])

            send_encrypted(s, msg, server_pub, mode=self.crypto.mode)
            return recv_encrypted(s, self.crypto.priv, mode=self.crypto.mode)

    # --- sugar ---
    def list_tables(self) -> Dict[str, Any]:
        return self.call({"type": "list_tables"})

    def table_meta(self, table: str) -> Dict[str, Any]:
        return self.call({"type": "table_meta", "table": table})

    def select(self, table: str, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        return self.call({"type": "select", "table": table, "limit": limit, "offset": offset})

    def search(self, table: str, query: str, column: Optional[str] = None, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        msg: Dict[str, Any] = {"type": "search", "table": table, "query": query, "limit": limit, "offset": offset}
        if column:
            msg["column"] = column
        return self.call(msg)

    def insert(self, table: str, values: Dict[str, Any]) -> Dict[str, Any]:
        return self.call({"type": "insert", "table": table, "values": values})

    def update(self, table: str, pk: Dict[str, Any], values: Dict[str, Any]) -> Dict[str, Any]:
        return self.call({"type": "update", "table": table, "pk": pk, "values": values})

    def delete(self, table: str, pk: Dict[str, Any]) -> Dict[str, Any]:
        return self.call({"type": "delete", "table": table, "pk": pk})

    def fk_options(self, ref_table: str, id_column: str = "id", label_column: Optional[str] = None, limit: int = 200, offset: int = 0) -> Dict[str, Any]:
        msg: Dict[str, Any] = {"type": "fk_options", "ref_table": ref_table, "id_column": id_column, "limit": limit, "offset": offset}
        if label_column:
            msg["label_column"] = label_column
        return self.call(msg)

    def create_table(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return self.call({"type": "create_table", "payload": payload})

    # --- files (INLINE MODEL) ---

    def file_delete(self, table: str, pk: Dict[str, Any], base: str) -> Dict[str, Any]:
        return self.call({"type": "file_delete", "table": table, "pk": pk, "base": base})

    def file_get(self, table: str, pk: Dict[str, Any], base: str) -> tuple[Dict[str, Any], bytes]:
        with socket.create_connection((self.host, self.port), timeout=self.timeout) as s:
            send_msg(s, {"type": "hello", "pub": pub_to_json(self.crypto.pub)})
            hello_ack = recv_msg(s)
            if not hello_ack.get("ok"):
                raise RuntimeError(hello_ack.get("error", "hello failed"))
            server_pub = pub_from_json(hello_ack["pub"])

            req = {"type": "file_get", "table": table, "pk": pk, "base": base}
            if self.token:
                req["token"] = self.token
            send_encrypted(s, req, server_pub, mode=self.crypto.mode)

            header, data = recv_encrypted_bin(s, self.crypto.priv, mode=self.crypto.mode)
            if not header.get("ok"):
                raise RuntimeError(header.get("error", "file_get failed"))
            return header["meta"], data

    def file_attach(self, table: str, pk: Dict[str, Any], base: str, path: str, mime_type: Optional[str] = None) -> Dict[str, Any]:
        upload_port = 9091
        data = open(path, "rb").read()
        header: Dict[str, Any] = {
            "type": "file_attach",
            "table": table,
            "pk": pk,
            "base": base,
            "original_name": os.path.basename(path),
            "mime_type": mime_type,
        }
        if self.token:
            header["token"] = self.token

        with socket.create_connection((self.host, upload_port), timeout=self.timeout) as s:
            send_msg(s, {"type": "hello", "pub": pub_to_json(self.crypto.pub)})
            hello_ack = recv_msg(s)
            if not hello_ack.get("ok"):
                return hello_ack
            server_pub = pub_from_json(hello_ack["pub"])

            send_encrypted_bin(s, header, data, server_pub, mode=self.crypto.mode)
            return recv_encrypted(s, self.crypto.priv, mode=self.crypto.mode)

    # --- NEW: insert with required files in one operation (bin) ---

    def insert_with_files(self, table: str, values: Dict[str, Any], files: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        files: list of
          { "base": str, "path": str, "mime_type": Optional[str] }
        Server will store inline and insert row with *_name/*_data filled.
        """
        upload_port = 9091

        descs: List[Dict[str, Any]] = []
        blobs: List[bytes] = []
        for f in files:
            p = f["path"]
            b = open(p, "rb").read()
            blobs.append(b)
            descs.append(
                {
                    "base": f["base"],
                    "original_name": os.path.basename(p),
                    "mime_type": f.get("mime_type"),
                }
            )

        header: Dict[str, Any] = {
            "type": "insert_with_files",
            "table": table,
            "values": values,
            "files": descs,
        }
        if self.token:
            header["token"] = self.token

        packed = _pack_multi_files(blobs)

        with socket.create_connection((self.host, upload_port), timeout=self.timeout) as s:
            send_msg(s, {"type": "hello", "pub": pub_to_json(self.crypto.pub)})
            hello_ack = recv_msg(s)
            if not hello_ack.get("ok"):
                return hello_ack
            server_pub = pub_from_json(hello_ack["pub"])

            send_encrypted_bin(s, header, packed, server_pub, mode=self.crypto.mode)
            return recv_encrypted(s, self.crypto.priv, mode=self.crypto.mode)

    # --- auth / admin ---
    def login(self, login: str, password: str) -> Dict[str, Any]:
        resp = self.call({"type": "login", "login": login, "password": password})
        if resp.get("ok") and resp.get("token"):
            self.token = resp["token"]
        return resp

    def user_create(self, login: str, password: str, full_name: str, role: str = "user") -> Dict[str, Any]:
        return self.call({"type": "user_create", "login": login, "password": password, "full_name": full_name, "role": role})

    def backup_list(self) -> Dict[str, Any]:
        return self.call({"type": "backup_list"})

    def backup_create(self) -> Dict[str, Any]:
        return self.call({"type": "backup_create"})

    def backup_restore(self, name: str) -> Dict[str, Any]:
        return self.call({"type": "backup_restore", "name": name})

    def backup_schedule_get(self):
        return self.call({"type": "backup_schedule_get"})

    def backup_schedule_set(self, enabled: bool, hour: int, minute: int, timezone: str = "UTC"):
        return self.call({
            "type": "backup_schedule_set",
            "enabled": enabled,
            "hour": hour,
            "minute": minute,
            "timezone": timezone,
        })
