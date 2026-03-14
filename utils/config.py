import os
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_MODEL = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")

# MySQL Configuration
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "your_password",
    "database": "fmcg_database"
}

# Neo4j Configuration
NEO4J_CONFIG = {
    "uri": "your uri",
    "user": "user",
    "password": "your password"
}
