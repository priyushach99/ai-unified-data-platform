from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from db import get_connection
from schema.transformer import transaction_to_text
import numpy as np

model = SentenceTransformer("all-MiniLM-L6-v2")


def fetch_data():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT * FROM transactions
        LIMIT 200
    """)

    rows = cur.fetchall()

    # convert raw rows → meaningful text
    return [transaction_to_text(row) for row in rows]


def rag_answer(question: str):
    data = fetch_data()

    if not data:
        return "No transaction data available"

    embeddings = model.encode(data)
    q_embedding = model.encode([question])

    scores = cosine_similarity(q_embedding, embeddings)[0]
    idx = np.argmax(scores)

    return f"Relevant transaction insight: {data[idx]}"