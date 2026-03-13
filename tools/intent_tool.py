import requests
from langchain.tools import tool
import json

MCP_SERVER_URL = "http://localhost:8000"

@tool
def intent_entity_tool(query: str):
    """
    Extract intent and entities from a user's natural language business query.
    Input should be the raw user query string.
    """
    try:
        response = requests.post(f"{MCP_SERVER_URL}/tools/extract_intent", json={"query": query})
        if response.status_code == 200:
            return response.json()["result"]
        return f"Error: MCP server returned {response.status_code}"
    except Exception as e:
        return f"Error connecting to MCP server: {str(e)}"