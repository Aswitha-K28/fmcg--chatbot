# tools/sql_generator_tool.py

from langchain.tools import tool
from services.sql_generator import SQLGeneratorService


sql_service = SQLGeneratorService()

MAX_RETRIES = 2
def validate_sql(sql):

    if sql is None:
        return False

    if "SELECT" not in sql.upper():
        return False

    return True


@tool
def sql_generator_tool(state):
    """
    Generate SQL query using LLM.
    """

    for attempt in range(MAX_RETRIES):

        sql = sql_service.generate_sql(
            query=state.user_query,
            intent=state.intent,
            entities=state.entities,
            schema_context=state.fusion_results
        )

        if validate_sql(sql):

            state.sql_query = sql

            return sql

        print("SQL generation invalid. Retrying...")

    raise Exception("SQL generation failed after retries")