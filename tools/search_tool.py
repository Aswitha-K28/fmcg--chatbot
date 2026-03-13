import requests
from langchain.tools import tool

MCP_SERVER_URL = "http://localhost:8000"

@tool
def schema_search_tool(query: str):
    """
    Perform multi-modal search (keyword + graph) to find specific database IDs for entities mentioned in the query.
    Use this to resolve brand names, region names, etc. into deterministic IDs for Malloy.
    """
    try:
        response = requests.post(f"{MCP_SERVER_URL}/tools/schematic_search", json={"query": query})
        if response.status_code == 200:
            return str(response.json())
        return f"Error: MCP server returned {response.status_code}"
    except Exception as e:
        return f"Error connecting to MCP server: {str(e)}"