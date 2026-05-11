# db_utils.py

import os
import psycopg2
from dotenv import load_dotenv
import base64

load_dotenv()

# =========================
# 🔐 OPTIONAL DECRYPT
# =========================

def decrypt(value):
    try:
        return base64.b64decode(value).decode()
    except Exception:
        return value

# =========================
# 🔧 DB CONFIG
# =========================

def get_db_config():
    return {
        "host": os.getenv("DB_HOST"),
        "database": os.getenv("DB_NAME"),
        "user": os.getenv("DB_USER"),
        "password": decrypt(os.getenv("DB_PASSWORD"))
    }

def get_jdbc_url():
    cfg = get_db_config()
    return f"jdbc:postgresql://{cfg['host']}/{cfg['database']}"

# =========================
# 🔌 CONNECTION
# =========================

def get_connection():
    return psycopg2.connect(**get_db_config())

# =========================
# 📊 TABLE CHECK
# =========================

def table_exists(table_name):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = %s
                );
            """, (table_name,))
            return cur.fetchone()[0]

# =========================
# 📋 GET COLUMNS
# =========================

def get_db_columns(table_name):
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s;
            """, (table_name,))
            return [row[0] for row in cur.fetchall()]

# =========================
# ➕ ADD MISSING COLUMNS
# =========================

def add_missing_columns(table_name, new_cols):
    if not new_cols:
        return

    with get_connection() as conn:
        with conn.cursor() as cur:
            for col_name in new_cols:
                cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col_name}" TEXT;')