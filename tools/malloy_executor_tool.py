import mysql.connector
import os
from langchain.tools import tool
from utils.config import MYSQL_CONFIG, GROQ_API_KEY, GROQ_MODEL
from groq import Groq

# Load the Malloy model once
MALLOY_MODEL_PATH = os.path.join(os.path.dirname(__file__), "..", "models", "fmcg.malloy")
try:
    with open(MALLOY_MODEL_PATH, "r") as f:
        MALLOY_MODEL_CONTENT = f.read()
except:
    MALLOY_MODEL_CONTENT = "Error: Malloy model file not found."

def translate_malloy_to_sql(malloy_query: str):
    """Bridge function to translate Malloy to MySQL using LLM."""
    client = Groq(api_key=GROQ_API_KEY)
    system_prompt = f"""
    You are a strict Malloy to MySQL transpiler.
    Translate the given Malloy query into a single valid MySQL query.
    Reference the following Malloy schema:
    
    {MALLOY_MODEL_CONTENT}
    
    CRITICAL RULES:
    1. Output ONLY the raw MySQL query string.
    2. NEVER use Cypher, Neo4j, or Graph syntax (no MATCH, no ->, no BELONGS_TO).
    3. Use ONLY standard MySQL syntax (SELECT, FROM, JOIN, WHERE, GROUP BY).
    4. Do NOT prepend table names with 'mysql:'. Use simple names like `sales`.
    5. Do NOT include markdown backticks or explanations.
    """
    response = client.chat.completions.create(
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": f"Translate this Malloy query to MySQL:\n{malloy_query}"}
        ],
        model=GROQ_MODEL,
        temperature=0
    )
    return response.choices[0].message.content.strip()

@tool
def malloy_executor_tool(malloy_query: str):
    """
    Execute the business logic by translating Malloy to SQL and running it on MySQL.
    'malloy_query' is the DSL code to execute.
    """
    print(f"DEBUG: Translating Malloy query: {malloy_query[:100]}...")
    try:
        sql = translate_malloy_to_sql(malloy_query)
        # Remove markdown backticks if any
        if sql.startswith("```sql"):
            sql = sql[6:]
        if sql.startswith("```"):
            sql = sql[3:]
        if sql.endswith("```"):
            sql = sql[:-3]
        sql = sql.strip()
        
        print(f"DEBUG: Executing translated SQL: {sql}")
        
        # Final cleanup for common LLM mess-ups
        sql = sql.replace("mysql:", "").replace("mysql.", "")
        
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        if not results:
            return "Execution Result: Query returned no data."
            
        import json
        from decimal import Decimal

        class DecimalEncoder(json.JSONEncoder):
            def default(self, obj):
                if isinstance(obj, Decimal):
                    return str(obj)
                return super(DecimalEncoder, self).default(obj)

        return "Malloy Query: " + malloy_query + "\nExecution Result (JSON): " + json.dumps(results, indent=2, cls=DecimalEncoder)
    except Exception as e:
        return f"Error executing query: {str(e)}"
