from __future__ import annotations

import socket
import struct
import pickle
from typing import Any, Tuple

from app.rsa_block import PublicKey, PrivateKey, encrypt_bytes, decrypt_bytes
from app.protocol import recv_exact


def send_encrypted(conn: socket.socket, obj: Any, peer_pub: PublicKey, mode: str) -> None:
    plain = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)
    enc = encrypt_bytes(plain, peer_pub, mode=mode)
    conn.sendall(struct.pack(">I", len(enc)) + enc)


def recv_encrypted(conn: socket.socket, my_priv: PrivateKey, mode: str) -> Any:
    header = recv_exact(conn, 4)
    (length,) = struct.unpack(">I", header)
    enc = recv_exact(conn, length)
    plain = decrypt_bytes(enc, my_priv, mode=mode)
    return pickle.loads(plain)


# ===== BIN PACKETS =====

def pack_pickle_bin(header_obj: Any, bin_data: bytes) -> bytes:
    h = pickle.dumps(header_obj, protocol=pickle.HIGHEST_PROTOCOL)
    return struct.pack(">I", len(h)) + h + struct.pack(">I", len(bin_data)) + bin_data


def unpack_pickle_bin(packet: bytes) -> Tuple[Any, bytes]:
    off = 0
    (hlen,) = struct.unpack(">I", packet[off:off+4]); off += 4
    header = pickle.loads(packet[off:off+hlen]); off += hlen
    (blen,) = struct.unpack(">I", packet[off:off+4]); off += 4
    b = packet[off:off+blen]
    return header, b


def send_encrypted_bin(
    conn: socket.socket,
    header_obj: Any,
    bin_data: bytes,
    peer_pub: PublicKey,
    mode: str
) -> None:
    packet = pack_pickle_bin(header_obj, bin_data)
    enc = encrypt_bytes(packet, peer_pub, mode=mode)
    conn.sendall(struct.pack(">I", len(enc)) + enc)


def recv_encrypted_bin(
    conn: socket.socket,
    my_priv: PrivateKey,
    mode: str
) -> Tuple[Any, bytes]:
    header = recv_exact(conn, 4)
    (length,) = struct.unpack(">I", header)
    enc = recv_exact(conn, length)
    packet = decrypt_bytes(enc, my_priv, mode=mode)
    return unpack_pickle_bin(packet)
