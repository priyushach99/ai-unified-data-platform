# test_postgres.py
import psycopg2

conn = psycopg2.connect("postgresql://neondb_owner:npg_2PiSOmvLDTa7@ep-winter-boat-am6haa8k.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require")

cur = conn.cursor()
cur.execute("SELECT version();")

print(cur.fetchone())

conn.close()