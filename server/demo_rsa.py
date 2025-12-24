# demo_rsa04.py
import os
import pickle
from app.crypto_ctx import init_crypto
from app.rsa_block import encrypt_bytes, decrypt_bytes

def demo_modes(ctx, plaintext: bytes):
    k = (ctx.pub.n.bit_length() + 7) // 8
    print(f"\nRSA_BITS={os.getenv('RSA_BITS','?')}  k(mod bytes)={k}")
    print(f"PLAINTEXT len={len(plaintext)} bytes, first20={plaintext[:20]!r}")

    for mode in ["raw_fixed", "raw_len", "rand_fixed", "rand_len"]:
        c1 = encrypt_bytes(plaintext, ctx.pub, mode=mode)
        c2 = encrypt_bytes(plaintext, ctx.pub, mode=mode)
        p1 = decrypt_bytes(c1, ctx.priv, mode=mode)

        same_cipher = (c1 == c2)
        ok = (p1 == plaintext)

        # форматность
        if mode.endswith("fixed"):
            aligned = (len(c1) % k == 0)
            fmt = f"len(cipher)={len(c1)} mod k={len(c1)%k} aligned={aligned}"
        else:
            # у len режимов каждый блок имеет +2 байта префикс длины (в нашем коде cbytes всё равно размера k)
            fmt = f"len(cipher)={len(c1)} (has per-block 2-byte length prefixes)"

        print(f"\nMODE={mode}")
        print("  decrypt_ok:", ok)
        print("  same_cipher_on_repeat:", same_cipher, "(raw_* обычно True, rand_* обычно False)")
        print("  format:", fmt)

def demo_a_string_no_pickle(ctx):
    print("\n=== a) STRING без pickle ===")
    s = "Привет RSA блоки"
    plaintext = s.encode("utf-8")      # <-- без pickle
    demo_modes(ctx, plaintext)

def demo_b_file_no_pickle(ctx):
    print("\n=== b) FILE bytes без pickle ===")
    # имитация файла
    file_bytes = b"%PDF-FAKE%\n" + b"A"*200 + b"\n%%EOF"
    demo_modes(ctx, file_bytes)

def demo_c_object_with_pickle(ctx):
    print("\n=== c) OBJECT через pickle ===")
    obj = {"id": 1, "roles": ["admin", "user"], "flags": {"a": True, "b": False}}
    plaintext = pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL)  # <-- pickle
    demo_modes(ctx, plaintext)

if __name__ == "__main__":
    ctx = init_crypto()
    demo_a_string_no_pickle(ctx)
    demo_b_file_no_pickle(ctx)
    demo_c_object_with_pickle(ctx)
