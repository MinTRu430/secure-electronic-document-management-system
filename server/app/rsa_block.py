from __future__ import annotations

import secrets
from dataclasses import dataclass
from typing import Tuple

# ===== Math utils =====

def egcd(a: int, b: int) -> Tuple[int, int, int]:
    if b == 0:
        return a, 1, 0
    g, x, y = egcd(b, a % b)
    return g, y, x - (a // b) * y

def modinv(a: int, m: int) -> int:
    g, x, _ = egcd(a, m)
    if g != 1:
        raise ValueError("No modular inverse")
    return x % m

def is_probable_prime(n: int, k: int = 20) -> bool:
    if n < 2:
        return False
    small_primes = [2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37]
    for p in small_primes:
        if n == p:
            return True
        if n % p == 0:
            return False

    d = n - 1
    s = 0
    while d % 2 == 0:
        d //= 2
        s += 1

    def check(a: int) -> bool:
        x = pow(a, d, n)
        if x == 1 or x == n - 1:
            return True
        for _ in range(s - 1):
            x = (x * x) % n
            if x == n - 1:
                return True
        return False

    for _ in range(k):
        a = secrets.randbelow(n - 3) + 2
        if not check(a):
            return False
    return True

def gen_prime(bits: int) -> int:
    if bits < 16:
        raise ValueError("bits too small")
    while True:
        x = secrets.randbits(bits) | (1 << (bits - 1)) | 1
        if is_probable_prime(x):
            return x

# ===== RSA keys =====

@dataclass(frozen=True)
class PublicKey:
    n: int
    e: int

@dataclass(frozen=True)
class PrivateKey:
    n: int
    d: int

def generate_keypair(bits: int = 512, e: int = 65537) -> Tuple[PublicKey, PrivateKey]:
    half = bits // 2
    while True:
        p = gen_prime(half)
        q = gen_prime(bits - half)
        if p == q:
            continue
        n = p * q
        phi = (p - 1) * (q - 1)
        if phi % e == 0:
            continue
        d = modinv(e, phi)
        return PublicKey(n=n, e=e), PrivateKey(n=n, d=d)

def rsa_encrypt_int(m: int, pub: PublicKey) -> int:
    if m < 0 or m >= pub.n:
        raise ValueError("m out of range")
    return pow(m, pub.e, pub.n)

def rsa_decrypt_int(c: int, priv: PrivateKey) -> int:
    if c < 0 or c >= priv.n:
        raise ValueError("c out of range")
    return pow(c, priv.d, priv.n)

# ===== Block modes (4 variants) =====
# Мы делаем plaintext-блоки фиксированной длины (k-1) и кодируем длину внутри блока.
# Это решает проблему ведущих нулей и делает передачу бинарных данных корректной.

def _mod_bytes(n: int) -> int:
    return (n.bit_length() + 7) // 8

def encrypt_bytes(data: bytes, pub: PublicKey, mode: str = "rand_len") -> bytes:
    k = _mod_bytes(pub.n)      # cipher block size (bytes)
    plain_block = k - 1        # plaintext block size in bytes (strictly < n)

    if plain_block < 8:
        raise ValueError("RSA modulus too small")

    if mode in ("raw_fixed", "raw_len"):
        # layout: [len:2][payload...(plain_block-2)][pad...]
        payload_cap = plain_block - 2
        rand = False
    elif mode in ("rand_fixed", "rand_len"):
        # layout: [len:2][rand:1][payload...(plain_block-3)][pad...]
        payload_cap = plain_block - 3
        rand = True
    else:
        raise ValueError("bad mode")

    out = bytearray()
    i = 0
    while i < len(data):
        chunk = data[i:i + payload_cap]
        i += len(chunk)

        L = len(chunk)
        if L > payload_cap:
            raise ValueError("internal chunk overflow")

        if rand:
            r = secrets.randbelow(255) + 1  # 1..255 (не 0!)
            block = L.to_bytes(2, "big") + bytes([r]) + chunk
        else:
            block = L.to_bytes(2, "big") + chunk

        # pad to full plaintext block size
        if len(block) < plain_block:
            block += b"\x00" * (plain_block - len(block))

        m = int.from_bytes(block, "big")
        c = rsa_encrypt_int(m, pub)
        cbytes = c.to_bytes(k, "big")

        if mode in ("raw_fixed", "rand_fixed"):
            out += cbytes
        else:
            out += len(cbytes).to_bytes(2, "big") + cbytes

    return bytes(out)

def decrypt_bytes(data: bytes, priv: PrivateKey, mode: str = "rand_len") -> bytes:
    k = _mod_bytes(priv.n)
    plain_block = k - 1

    if mode in ("raw_fixed", "rand_fixed"):
        if len(data) % k != 0:
            raise ValueError("cipher length not aligned")
        blocks = [data[i:i + k] for i in range(0, len(data), k)]
    elif mode in ("raw_len", "rand_len"):
        blocks = []
        pos = 0
        while pos < len(data):
            if pos + 2 > len(data):
                raise ValueError("bad cipher format")
            blen = int.from_bytes(data[pos:pos + 2], "big")
            pos += 2
            if pos + blen > len(data):
                raise ValueError("bad cipher format")
            blocks.append(data[pos:pos + blen])
            pos += blen
    else:
        raise ValueError("bad mode")

    out = bytearray()

    for b in blocks:
        c = int.from_bytes(b, "big")
        m = rsa_decrypt_int(c, priv)

        # ВАЖНО: восстанавливаем plaintext блок фиксированной длины (plain_block),
        # чтобы не терять ведущие нули
        p = m.to_bytes(plain_block, "big")

        L = int.from_bytes(p[0:2], "big")

        if mode in ("rand_fixed", "rand_len"):
            # p[2] — случайный байт, игнорируем
            start = 3
            cap = plain_block - 3
        else:
            start = 2
            cap = plain_block - 2

        if L < 0 or L > cap:
            raise ValueError("bad plaintext length")

        out += p[start:start + L]

    return bytes(out)
