import re
import mysql.connector
from neo4j import GraphDatabase
from rank_bm25 import BM25Okapi
from utils.config import MYSQL_CONFIG, NEO4J_CONFIG

class SearchService:
    def __init__(self):
        self.bm25_index = None
        self.docs = []
        self.corpus = []
        self.neo4j_driver = GraphDatabase.driver(
            NEO4J_CONFIG["uri"],
            auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
        )

    def build_index(self):
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        docs = []

        # Indexing Products for context
        cursor.execute("""
            SELECT p.product_id, p.sku_name, b.brand_name, c.category_name
            FROM products p
            JOIN brands b ON p.brand_id = b.brand_id
            JOIN categories c ON p.category_id = c.category_id
        """)
        for row in cursor.fetchall():
            text = f"{row['sku_name']} {row['brand_name']} {row['category_name']}"
            docs.append({
                "id": row["product_id"],
                "source": "products",
                "text": text,
                "data": row
            })

        cursor.close()
        conn.close()

        def tokenize(t):
            return re.sub(r"[^a-z0-9\s]", "", t.lower()).split()

        self.corpus = [tokenize(d["text"]) for d in docs]
        if self.corpus:
            self.bm25_index = BM25Okapi(self.corpus)
        self.docs = docs

    def keyword_search(self, query, top_k=5):
        if not self.bm25_index:
            return []
        
        def tokenize(t):
            return re.sub(r"[^a-z0-9\s]", " ", t.lower()).split()
        
        qtoks = tokenize(query)
        # Add basic contains match for small queries
        scores = self.bm25_index.get_scores(qtoks)
        
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)
        
        results = []
        for rank, (idx, score) in enumerate(ranked[:top_k]):
            if score > 0:
                results.append(self.docs[idx])
        
        # Fallback if BM25 is too sparse
        if not results:
            for d in self.docs:
                if any(q.lower() in d["text"].lower() for q in qtoks):
                    results.append(d)
                    if len(results) >= top_k:
                        break
        return results

    def graph_search(self, entities):
        brands = entities.get("brands", []) + entities.get("companies", [])
        zones = entities.get("zones", [])
        
        results = {"paths": [], "ids": []}
        
        if not brands and not zones:
            return results

        try:
            with self.neo4j_driver.session() as s:
                if brands:
                    res = s.run("""
                        MATCH (b:Brand)-[:HAS_SKU]->(p:Product)
                        WHERE b.name IN $brands
                        RETURN p.product_id as id, p.name as name, b.name as brand
                    """, brands=brands)
                    for record in res:
                        results["ids"].append(record["id"])
                        results["paths"].append(f"{record['brand']} -> {record['name']}")
        except Exception as e:
            print(f"Neo4j search failed: {e}")
            results["paths"].append("Neo4j search unavailable - using keyword search only.")
            
        return results

    def close(self):
        self.neo4j_driver.close()
