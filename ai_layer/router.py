def route_query(question: str):
    q = question.lower()

    if any(k in q for k in ["count", "total", "how many"]):
        return "tool_total"

    if any(k in q for k in ["recent", "latest"]):
        return "tool_recent"

    return "rag"