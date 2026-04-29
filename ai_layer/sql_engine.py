from mcp.registry import TOOLS

def handle_sql(route):
    if route == "tool_total":
        result = TOOLS["total_records"]()
        return f"Total records: {result}"

    if route == "tool_recent":
        result = TOOLS["recent_records"]()
        return f"Recent records: {result}"

    return "SQL route not supported"