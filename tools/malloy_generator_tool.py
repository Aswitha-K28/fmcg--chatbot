import requests
from langchain.tools import tool

MCP_SERVER_URL = "http://localhost:8000"

@tool
def malloy_generator_tool(query: str, semantic_context: str):
    """
    Generate a Malloy query based on the user question and the retrieved semantic context.
    'query' is the original question.
    'semantic_context' is the resolved entities/IDs from the search tool.
    """
    payload = {
        "query": query,
        "context": semantic_context
    }
    try:
        response = requests.post(f"{MCP_SERVER_URL}/tools/generate_malloy", json=payload)
        if response.status_code == 200:
            return response.json()["malloy_code"]
        return f"Error: MCP server returned {response.status_code}"
    except Exception as e:
        return f"Error connecting to MCP server: {str(e)}"
