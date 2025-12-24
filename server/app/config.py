import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "ui_db")
DB_USER = os.getenv("DB_USER", "ui_user")
DB_PASS = os.getenv("DB_PASS", "ui_pass")
JWT_SECRET = os.getenv("JWT_SECRET", "change_me_super_secret")
JWT_TTL_MIN = int(os.getenv("JWT_TTL_MIN", "120"))

