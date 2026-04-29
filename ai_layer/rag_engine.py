from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from db import get_connection
import numpy as np

model = SentenceTransformer('all-MiniLM-L6-v2')

def fetch_data():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT text_column FROM processed_data LIMIT 100;")
    rows = cur.fetchall()
    return [r[0] for r in rows]

def rag_answer(question: str):
    data = fetch_data()

    if not data:
        return "No data available"

    embeddings = model.encode(data)
    q_embedding = model.encode([question])

    scores = cosine_similarity(q_embedding, embeddings)[0]
    idx = np.argmax(scores)

    return f"Relevant insight: {data[idx]}"