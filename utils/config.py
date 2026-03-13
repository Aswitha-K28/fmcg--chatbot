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
    "uri": "neo4j+s://91202a22.databases.neo4j.io",
    "user": "neo4j",
    "password": "8OzBvCYXTU9jlKQDjiszyn-ATOGd6RTOvoBRjKuJW8w"
}
