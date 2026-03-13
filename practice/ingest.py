import json
from langchain_community.vectorstores import FAISS
from langchain_community.embeddings import SentenceTransformerEmbeddings
from langchain_core.documents import Document

with open("schema.json", "r") as f:
    schema = json.load(f)

docs = []

for table_name, table_info in schema['tables'].items():
    table_desc = table_info.get("description", "")
    
    columns_text = ""
    for col_name, col_info in table_info['columns'].items():
        columns_text += f"{col_name} ({col_info['type']}): {col_info['description']}\n"
    
    doc_text = f"Table: {table_name}\nDescription: {table_desc}\nColumns:\n{columns_text}"
    print(doc_text)
    docs.append(Document(page_content=doc_text, metadata={
        "table": table_name,
        "description": table_desc,
        "columns": {col_name: col_info['description'] for col_name, col_info in table_info['columns'].items()}
    }))
print(docs)
print(f"Total table-level documents: {len(docs)}")

embedding_model = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")

faiss_index = FAISS.from_documents(docs, embedding_model)

faiss_index.save_local("faiss_table_index")
print("FAISS table-level index saved")