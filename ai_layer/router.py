def route_query(question: str):
    q = question.lower()

    # TOOL ROUTES (structured analytics)
    if any(k in q for k in ["total", "sum", "how much", "deposits"]):
        return "tool_deposits"

    if any(k in q for k in ["withdraw", "spent", "expense"]):
        return "tool_withdrawals"

    if any(k in q for k in ["top", "highest", "largest"]):
        return "tool_top"

    # RAG fallback (semantic understanding)
    return "rag"