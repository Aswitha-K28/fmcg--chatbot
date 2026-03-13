from langchain_community.chat_models import ChatOpenAI
from langchain_core.prompts import PromptTemplate


class SQLGeneratorService:

    def __init__(self):

        self.llm = ChatOpenAI(
            temperature=0,
            model="gpt-4o-mini"
        )

        self.prompt = PromptTemplate.from_template(
            """
        You are an expert SQL generator.

        Your job is to convert a business question into a correct SQL query.

        Use ONLY the tables and columns provided in the schema context.

        Do NOT hallucinate tables or columns.

        Return ONLY the SQL query.

        User Query:
        {query}

        Intent:
        {intent}

        Entities:
        {entities}

        Schema Context:
        {schema_context}

        Rules:
        - Use only columns present in schema context
        - Always generate valid SQL
        - Only SELECT queries
        - Add filters based on entities if possible

        SQL:
        """
        )

    def generate_sql(self, query, intent, entities, schema_context):

        chain = self.prompt | self.llm

        response = chain.invoke({
            "query": query,
            "intent": intent,
            "entities": entities,
            "schema_context": schema_context
        })

        sql = response.content.strip()

        return sql