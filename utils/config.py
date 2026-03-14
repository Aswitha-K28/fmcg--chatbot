import os
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Aswitha@12",
    "database": "fmcg_database"
}

# Neo4j Configuration
NEO4J_CONFIG = {
    "uri": "neo4j+s://46991046.databases.neo4j.io",
    "user": "46991046",
    "password": "bT3ebt7nA6Cbz3CofkpdMfKBgVxUW7vJmpJOKverdu8"
}
