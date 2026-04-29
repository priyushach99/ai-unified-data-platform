from db import get_connection

def get_total_records():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM processed_data;")
    return cur.fetchone()[0]

def get_recent_records():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM processed_data ORDER BY id DESC LIMIT 5;")
    return cur.fetchall()