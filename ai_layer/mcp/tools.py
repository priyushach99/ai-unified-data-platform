from db import get_connection


def total_deposits():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT SUM("deposit_amt")
        FROM transactions
    """)

    return cur.fetchone()[0]


def total_withdrawals():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT SUM("withdrawal_amt")
        FROM transactions
    """)

    return cur.fetchone()[0]


def top_transactions():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT "transaction_details", SUM("withdrawal_amt")
        FROM transactions
        GROUP BY "transaction_details"
        ORDER BY SUM("withdrawal_amt") DESC
        LIMIT 5
    """)

    return cur.fetchall()