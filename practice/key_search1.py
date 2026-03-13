import json
import os
import re
from rank_bm25 import BM25Okapi

class BM25SchemaRetriever:

    def __init__(self, schema_path):

        with open(schema_path, "r", encoding="utf-8") as f:
            raw_schema = json.load(f)
        print(f"Schema loaded from: {schema_path}")
        self.tables = raw_schema.get("tables", {})
        self.documents = []
        self.table_metadata = []
        self.tokenized_docs = []
        self.build_documents()
        self.bm25 = BM25Okapi(self.tokenized_docs)
        print(f"\nLoaded {len(self.documents)} tables from schema\n")

    def tokenize(self, text):
        text = text.lower()
        text = re.sub(r"[^a-z0-9 ]", " ", text)
        return text.split()

    def build_documents(self):
        for table_name, table_data in self.tables.items():
            description = table_data.get("description", "")
            columns = table_data.get("columns", {})
            keywords = table_data.get("keywords", [])
            column_names = []
            column_descriptions = []

            for col_name, col_data in columns.items():
                column_names.append(col_name)

                if isinstance(col_data, dict):
                    column_descriptions.append(col_data.get("description", ""))
            doc_text = " ".join([
                table_name,
                description,
                " ".join(column_names),
                " ".join(column_descriptions),
                " ".join(keywords)
            ])
            tokens = self.tokenize(doc_text)
            self.documents.append(doc_text)
            self.tokenized_docs.append(tokens)
            self.table_metadata.append({
                "table_name": table_name,
                "description": description,
                "columns": columns
            })

    def normalize_scores(self, scores):
        max_score = max(scores)

        if max_score == 0:
            return scores
        return [round(s / max_score, 3) for s in scores]

    def search(self, query, top_k=5):
        tokens = self.tokenize(query)
        scores = self.bm25.get_scores(tokens)
        scores = self.normalize_scores(scores)
        ranked = sorted(
            zip(scores, self.table_metadata),
            key=lambda x: x[0],
            reverse=True
        )
        return ranked[:top_k]

def print_results(results):

    for rank, (score, table) in enumerate(results, start=1):
        print(f"\nRank {rank} | Score {score} | Table {table['table_name']}")
        print(f"Description: {table['description']}")
        print("Columns:")

        for col_name, col_data in table["columns"].items():
            if isinstance(col_data, dict):
                desc = col_data.get("description", "")
            else:
                desc = ""
            print(f"- {col_name}: {desc}")
        print("-" * 50)

def main():

    print("BM25 SCHEMA RETRIEVAL")
    print("Type 'exit' to stop")

    current_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(current_dir, "schema.json")
    retriever = BM25SchemaRetriever(schema_path)

    while True:
        query = input("\nEnter question: ")
        if query.lower() == "exit":
            break
        results = retriever.search(query)
        print_results(results)

if __name__ == "__main__":
    main()