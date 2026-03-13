import time
import json
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings

start = time.time()

print("Loading embedding model...")

embedding_model = HuggingFaceEmbeddings(
    model_name="sentence-transformers/all-MiniLM-L6-v2",
    model_kwargs={"device": "cpu", "local_files_only": True}
)

print("Embedding load time:", time.time() - start)

start = time.time()

faiss_index = FAISS.load_local(
    "faiss_table_index",
    embedding_model,
    allow_dangerous_deserialization=True
)
print("FAISS load time:", time.time() - start)
print("Total vectors in FAISS index:", faiss_index.index.ntotal)

def query_tables(user_query, intent_output, top_k):
    
    query_parts = [user_query]
    
    intent = intent_output.get("intent")
    if intent:
        query_parts.append(f"Intent: {intent}")
    
    entities = intent_output.get("entities", {})
    for k, v in entities.items():
        if v: 
            if isinstance(v, list):
                query_parts.append(f"{k}: {', '.join(v)}")
            else:
                query_parts.append(f"{k}: {v}")
    
    query_text = " | ".join(query_parts)
    
    results = faiss_index.similarity_search_with_score(query_text, k=top_k)
    print(len(results))
    output = []
    for rank, (doc, score) in enumerate(results, start=1):
        output.append({
            "rank": rank,
            "score": float(score),
            "table_name": doc.metadata.get("table"),
            "table_description": doc.metadata.get("description"),
            "columns": doc.metadata.get("columns")
        })
    return output

if __name__ == "__main__":
    user_query = "Show me the sales for Dove brand across India"
    intent_output = {
        "intent": "get_sales",
        "entities": {
            "metric": "sales",
            "brand": "Dove",
            "company": None,
            "product": None,
            "category": None,
            "subcategory": None,
            "region": None,
            "zone": None,
            "distributor": None,
            "outlet": None,
            "warehouse": None,
            "supplier": None,
            "campaign": None,
            "channel": None,
            "quarter": None,
            "year": None,
            "time_period": None,
            "festival": None,
            "season": None,
            "top_n": None,
            "comparison_items": []
        }
    }

    results = query_tables(user_query, intent_output, top_k=5)

    for r in results:
        print(f"Rank: {r['rank']} | Score: {r['score']:.3f} | Table: {r['table_name']}")
        print(f"Description: {r['table_description']}")
        print("Columns:")
        for col, desc in r['columns'].items():
            print(f" - {col}: {desc}")
        print("\n---\n")