from mcp.registry import TOOLS


def handle_sql(route):
    if route == "tool_deposits":
        return f"Total Deposits: {TOOLS['total_deposits']()}"

    if route == "tool_withdrawals":
        return f"Total Withdrawals: {TOOLS['total_withdrawals']()}"

    if route == "tool_top":
        return TOOLS["top_transactions"]()

    return "Unknown tool route"