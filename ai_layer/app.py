from fastapi import FastAPI
from router import route_query
from sql_engine import handle_sql
from rag_engine import rag_answer

app = FastAPI()

@app.get("/")
def home():
    return {"message": "Hybrid Transaction Query Layer Running"}


@app.get("/ask")
def ask(q: str):

    try:
        route = route_query(q)

        if route.startswith("tool"):
            answer = handle_sql(route)
        else:
            answer = rag_answer(q)

        return {
            "query": q,
            "route": route,
            "answer": answer
        }

    except Exception as e:
        return {
            "error": str(e)
        }