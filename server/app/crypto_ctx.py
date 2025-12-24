from __future__ import annotations

import os
from dataclasses import dataclass

from app.rsa_block import PublicKey, PrivateKey, generate_keypair

@dataclass
class CryptoCtx:
    pub: PublicKey
    priv: PrivateKey
    mode: str

def init_crypto() -> CryptoCtx:
    bits = int(os.getenv("RSA_BITS", "512"))   # для скорости 512, можно 768/1024
    mode = os.getenv("RSA_MODE", "rand_len")   # один из: raw_fixed/raw_len/rand_fixed/rand_len
    pub, priv = generate_keypair(bits=bits)
    return CryptoCtx(pub=pub, priv=priv, mode=mode)

def pub_to_json(pub: PublicKey) -> dict:
    return {"n": str(pub.n), "e": pub.e}

def pub_from_json(d: dict) -> PublicKey:
    return PublicKey(n=int(d["n"]), e=int(d["e"]))
