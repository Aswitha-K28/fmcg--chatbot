from langchain.tools import tool


@tool
def sql_validator_tool(state):
    """
    Validate SQL query syntax and safety.
    """
    pass