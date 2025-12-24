BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- =========================
-- USERS (03)
-- =========================
CREATE TABLE IF NOT EXISTS users (
  id            SERIAL PRIMARY KEY,
  login         VARCHAR(64)  NOT NULL UNIQUE,
  password_hash VARCHAR(200) NOT NULL,
  full_name     VARCHAR(200) NOT NULL,
  role          VARCHAR(20)  NOT NULL DEFAULT 'user',
  created_at    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Дефолтный админ: admin/admin
INSERT INTO users (login, password_hash, full_name, role)
VALUES ('admin', crypt('admin', gen_salt('bf')), 'Administrator', 'admin')
ON CONFLICT (login) DO NOTHING;

-- =========================
-- AUDIT (03)
-- =========================
CREATE TABLE IF NOT EXISTS audit_log (
  id         SERIAL PRIMARY KEY,
  ts         TIMESTAMPTZ NOT NULL DEFAULT now(),
  level      VARCHAR(10) NOT NULL DEFAULT 'INFO',
  user_login VARCHAR(64),
  user_role  VARCHAR(20),
  action     VARCHAR(64) NOT NULL,
  table_name VARCHAR(128),
  details    JSONB
);

CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_log(ts);

-- =========================
-- DOMAIN TABLES (01)
-- минимум 3 + связи 1-1 и 1-N
-- =========================
CREATE TABLE IF NOT EXISTS customers (
  id         SERIAL PRIMARY KEY,
  full_name  VARCHAR(200) NOT NULL,
  email      VARCHAR(200) NOT NULL UNIQUE,
  created_at TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- 1-1: customer_profiles.customer_id UNIQUE -> customers(id)
CREATE TABLE IF NOT EXISTS customer_profiles (
  id          SERIAL PRIMARY KEY,
  customer_id INT NOT NULL UNIQUE REFERENCES customers(id) ON DELETE CASCADE,
  phone       VARCHAR(50),
  address     VARCHAR(300)
);

-- 1-N: orders.customer_id -> customers(id)
CREATE TABLE IF NOT EXISTS orders (
  id          SERIAL PRIMARY KEY,
  customer_id INT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
  title       VARCHAR(200) NOT NULL,
  status      VARCHAR(50)  NOT NULL DEFAULT 'new',
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT now(),

  -- Inline file storage (02): две колонки на один "файл"
  -- document_name: имя файла
  -- document_data: base64/blob/fs_path
  document_name TEXT NOT NULL,
  document_data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_orders_customer_id ON orders(customer_id);

-- Метаданные file-column храним в COMMENT (не в отдельной таблице)
COMMENT ON COLUMN public.orders.document_data IS
  '{"file":true,"base":"document","name_col":"document_name","mode":"fs","required":true}';

-- === App settings (key-value) ===
CREATE TABLE IF NOT EXISTS app_settings (
  key TEXT PRIMARY KEY,
  value JSONB NOT NULL
);

-- default backup schedule: enabled, every day at 02:00 UTC
INSERT INTO app_settings (key, value)
VALUES (
  'backup_schedule',
  '{"enabled": true, "hour": 2, "minute": 0, "timezone": "UTC"}'::jsonb
)
ON CONFLICT (key) DO NOTHING;


COMMIT;
