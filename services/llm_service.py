import os
from groq import Groq
from utils.config import GROQ_API_KEY, GROQ_MODEL

class LLMService:
    def __init__(self):
        self.client = Groq(api_key=GROQ_API_KEY)
        self.model = GROQ_MODEL

    def chat_completion(self, system_prompt, user_prompt):
        response = self.client.chat.completions.create(
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            model=self.model,
            temperature=0
        )
        return response.choices[0].message.content

    def extract_intent(self, user_query):
        system_prompt = """
        You are a business intent extractor for an FMCG database.
        Extract the following in JSON format:
        - intent: One of [sales_ranking, sales_performance, market_share, inventory_status, pricing_analysis, campaign_performance, target_vs_achievement, consumer_sentiment]
        - entities: { "brands": [], "companies": [], "zones": [], "regions": [], "categories": [] }
        """
        return self.chat_completion(system_prompt, user_query)

    def generate_malloy(self, user_query, schema_context, recommended_tables):
        system_prompt = f"""
        You are a Malloy expert. Generate a Malloy query based on the user question and context.
        Reference the 'fmcg.malloy' model.
        
        SCHEMA CONTEXT:
        {schema_context}
        
        RECOMMENDED TABLES (From Search algorithms, ordered by relevance):
        {recommended_tables}
        
        Return ONLY the Malloy code block.
        """
        user_prompt = f"Question: {user_query}"
        return self.chat_completion(system_prompt, user_prompt)
