import socket
import struct
from typing import Any, Dict

import orjson


def send_msg(conn: socket.socket, obj: Dict[str, Any]) -> None:
    payload = orjson.dumps(obj)
    header = struct.pack(">I", len(payload))  # 4 bytes big-endian length
    conn.sendall(header + payload)


def recv_exact(conn: socket.socket, n: int) -> bytes:
    data = b""
    while len(data) < n:
        chunk = conn.recv(n - len(data))
        if not chunk:
            raise ConnectionError("socket closed")
        data += chunk
    return data


def recv_msg(conn: socket.socket) -> Dict[str, Any]:
    header = recv_exact(conn, 4)
    (length,) = struct.unpack(">I", header)
    payload = recv_exact(conn, length)
    return orjson.loads(payload)