from fastapi import FastAPI
from router import route_query
from sql_engine import handle_sql
from rag_engine import rag_answer

app = FastAPI()

@app.get("/")
def home():
    return {"message": "MCP AI Layer Running"}

@app.get("/ask")
def ask(q: str):
    route = route_query(q)

    if route.startswith("tool"):
        answer = handle_sql(route)
    else:
        answer = rag_answer(q)

    return {
        "question": q,
        "route": route,
        "answer": answer
    }