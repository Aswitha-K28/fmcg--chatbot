class PipelineState:
    def __init__(self, user_query):
        self.user_query = user_query
        self.intent = None
        self.entities = {}
        self.fusion_results = ""
        self.malloy_query = ""
        self.sql_query = ""
        self.final_answer = ""