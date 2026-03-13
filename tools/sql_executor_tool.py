from langchain.tools import tool


@tool
def sql_executor_tool(state):
    """
    Execute SQL query on the database.
    """
    pass