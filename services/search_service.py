import re
import math
import asyncio
from collections import deque
import mysql.connector
import networkx as nx
from neo4j import GraphDatabase
from rank_bm25 import BM25Okapi
from langchain_community.vectorstores import FAISS
from langchain_huggingface import HuggingFaceEmbeddings
from sentence_transformers import SentenceTransformer
from node2vec import Node2Vec

from utils.config import MYSQL_CONFIG, NEO4J_CONFIG

# --- Helper Functions for Node2Vec ---
def cosine_sim(v1, v2):
    dot = sum(a * b for a, b in zip(v1, v2))
    norm1 = math.sqrt(sum(a * a for a in v1))
    norm2 = math.sqrt(sum(b * b for b in v2))
    if norm1 == 0 or norm2 == 0:
        return 0.0
    return dot / (norm1 * norm2)

def _singularize(word):
    irregular = {
        "companies": "company",
        "categories": "category",
        "territories": "territory",
        "inventories": "inventory",
    }
    if word in irregular:
        return irregular[word]
    if word.endswith("ies"):
        return word[:-3] + "y"
    if word.endswith("es") and len(word) > 3:
        return word[:-2]
    if word.endswith("s") and len(word) > 2:
        return word[:-1]
    return word

class SearchService:
    def __init__(self):
        # 1. BM25 (Keyword)
        self.bm25_index = None
        self.docs = []
        self.corpus = []
        
        # 2. FAISS (Semantic)
        print("Loading FAISS Embeddings...")
        self.embedding_model = HuggingFaceEmbeddings(
            model_name="sentence-transformers/all-MiniLM-L6-v2",
            model_kwargs={"device": "cpu", "local_files_only": True}
        )
        try:
            self.faiss_index = FAISS.load_local(
                "faiss_table_index",
                self.embedding_model,
                allow_dangerous_deserialization=True
            )
            print("FAISS index loaded successfully.")
        except Exception as e:
            print(f"FAISS load failed: {e}")
            self.faiss_index = None

        # 3. Node2Vec (Graph)
        print("Loading Neo4j and Node2Vec Graph...")
        self.neo4j_driver = GraphDatabase.driver(
            NEO4J_CONFIG["uri"],
            auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
        )
        self.graph_nodes = {}
        self.neo4j_graph = nx.Graph()
        self.n2v_model = None
        self.sbert_model = SentenceTransformer("all-MiniLM-L6-v2")

    def build_index(self):
        """Initializes both BM25 and Node2Vec on startup."""
        print("Building BM25 Index...")
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        docs = []

        # We index schemas instead of just products for parity with FAISS/Node2Vec
        cursor.execute("SHOW TABLES")
        tables = [row[f"Tables_in_{MYSQL_CONFIG['database']}"] for row in cursor.fetchall()]
        
        for table in tables:
            cursor.execute(f"DESCRIBE {table}")
            columns = [col['Field'] for col in cursor.fetchall()]
            text = f"Table {table} columns {' '.join(columns)}"
            docs.append({
                "id": table,
                "source": "schema",
                "text": text,
                "table_name": table
            })

        cursor.close()
        conn.close()

        def tokenize(t):
            return re.sub(r"[^a-z0-9\s]", "", t.lower()).split()

        self.corpus = [tokenize(d["text"]) for d in docs]
        if self.corpus:
            self.bm25_index = BM25Okapi(self.corpus)
        self.docs = docs
        print("BM25 index built successfully.")

        print("Pulling Sub-Graph for Node2Vec...")
        # Pull Graph
        with self.neo4j_driver.session() as session:
            # tables
            for row in session.run("""
                MATCH (t:Table)
                WHERE t.embedding IS NOT NULL
                RETURN t.name AS name, t.description AS desc, t.embedding AS embedding
            """).data():
                self.graph_nodes[row["name"]] = {
                    "id": row["name"], "type": "Table", "name": row["name"],
                    "desc": row.get("desc") or "", "embedding": row["embedding"]
                }
                self.neo4j_graph.add_node(row["name"], node_type="Table")

            # columns
            for row in session.run("""
                MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                WHERE c.embedding IS NOT NULL
                RETURN t.name AS parent_table, c.name AS name, c.description AS desc, c.embedding AS embedding
            """).data():
                nid = f"{row['parent_table']}__{row['name']}"
                self.graph_nodes[nid] = {
                    "id": nid, "type": "Column", "name": row["name"],
                    "desc": row.get("desc") or "", "embedding": row["embedding"]
                }
                self.neo4j_graph.add_node(nid, node_type="Column")
            
            # Edges
            for row in session.run("""
                MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
                WHERE t.embedding IS NOT NULL AND c.embedding IS NOT NULL
                RETURN t.name AS table_name, c.name AS col_name
            """).data():
                self.neo4j_graph.add_edge(row["table_name"], f"{row['table_name']}__{row['col_name']}")

            for row in session.run("""
                MATCH (t1:Table)-[:RELATED_TO]->(t2:Table)
                WHERE t1.embedding IS NOT NULL AND t2.embedding IS NOT NULL
                RETURN t1.name AS from_t, t2.name AS to_t
            """).data():
                self.neo4j_graph.add_edge(row["from_t"], row["to_t"])

        print("Training Node2Vec Model (Background)...")
        # In a real scenario, cache this model. Training here for completeness.
        if len(self.neo4j_graph.nodes) > 0:
             n2v = Node2Vec(self.neo4j_graph, dimensions=64, walk_length=10, num_walks=10, workers=2, quiet=True)
             self.n2v_model = n2v.fit(window=5, min_count=1, batch_words=4)
             print("Node2Vec model trained successfully.")
        else:
             print("Warning: Graph empty, skipping Node2Vec.")

    # --- Search Algorithms ---
    
    async def bm25_search_async(self, query: str, top_k=5):
        if not self.bm25_index: return []
        def tokenize(t): return re.sub(r"[^a-z0-9\s]", " ", t.lower()).split()
        qtoks = tokenize(query)
        scores = self.bm25_index.get_scores(qtoks)
        ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)[:top_k]
        
        results = []
        for _, (idx, score) in enumerate(ranked):
            if score > 0:
                results.append({"table_name": self.docs[idx]["table_name"], "score": float(score)})
        return results

    async def faiss_search_async(self, query: str, intent_data: dict, top_k=5):
        if not self.faiss_index: return []
        
        query_parts = [query]
        intent = intent_data.get("intent")
        if intent: query_parts.append(f"Intent: {intent}")
        for k, v in intent_data.get("entities", {}).items():
            if v: 
                query_parts.append(f"{k}: {', '.join(v) if isinstance(v, list) else v}")
                
        query_text = " | ".join(query_parts)
        results = await asyncio.to_thread(self.faiss_index.similarity_search_with_score, query_text, k=top_k)
        
        output = []
        # Note: FAISS returns distance, so lower score is better. We invert it for ranking parity later.
        for doc, score in results:
            output.append({"table_name": doc.metadata.get("table"), "score": -float(score)}) 
        return output

    async def node2vec_search_async(self, query: str, intent_data: dict, top_k=5):
        if not self.n2v_model or not self.graph_nodes: return []
        
        # 1. Build Query Text
        parts = [intent_data.get("intent", "").replace("_", " ")]
        for k, v in intent_data.get("entities", {}).items():
            parts.append(k.replace("_", " "))
            if isinstance(v, list): parts.extend([str(x) for x in v if x])
            else: parts.append(str(v))
        query_text = " ".join([p for p in parts if p]) + " " + query
        
        query_vec = self.sbert_model.encode([query_text])[0].tolist()
        
        # 2. Find Seed
        best_id, best_score = None, -1.0
        for nid, node in self.graph_nodes.items():
            if node["type"] == "Table":
                score = cosine_sim(query_vec, node["embedding"])
                if score > best_score:
                    best_score = score
                    best_id = nid
        
        if not best_id or best_id not in self.n2v_model.wv: return []
        
        # 3. BFS & Score combining
        similar = dict(self.n2v_model.wv.most_similar(best_id, topn=50))
        similar[best_id] = 1.0
        
        visited = set()
        queue = deque([best_id])
        tables = []
        
        while queue:
            curr = queue.popleft()
            if curr in visited: continue
            visited.add(curr)
            if self.graph_nodes.get(curr, {}).get("type") == "Table":
                tables.append(curr)
            for neighbor in self.neo4j_graph.neighbors(curr):
                if neighbor not in visited: queue.append(neighbor)
                
        scored_tables = [(t, similar.get(t, 0.0)) for t in tables]
        scored_tables.sort(key=lambda x: x[1], reverse=True)
        
        res = []
        for table, score in scored_tables[:top_k]:
            res.append({"table_name": table, "score": float(score)})
        return res

    # --- Orchestrator & RRF ---

    async def parallel_search(self, query: str, intent_data: dict, top_k=5):
        """Runs all three searches in parallel and fuses results via RRF."""
        # Run parallel tasks
        bm25_task = self.bm25_search_async(query, top_k)
        faiss_task = self.faiss_search_async(query, intent_data, top_k)
        n2v_task = self.node2vec_search_async(query, intent_data, top_k)
        
        results = await asyncio.gather(bm25_task, faiss_task, n2v_task, return_exceptions=True)
        
        bm25_res = results[0] if not isinstance(results[0], Exception) else []
        faiss_res = results[1] if not isinstance(results[1], Exception) else []
        n2v_res = results[2] if not isinstance(results[2], Exception) else []
        
        print(f"DEBUG Parallel Results lengths: BM25={len(bm25_res)}, FAISS={len(faiss_res)}, N2V={len(n2v_res)}")

        return self._reciprocal_rank_fusion([bm25_res, faiss_res, n2v_res], top_k=top_k)

    def _reciprocal_rank_fusion(self, ranked_lists, k=60, top_k=5):
        """
        Standard RRF formulation: RRF_score = 1 / (k + rank)
        """
        table_scores = {}
        
        for search_list in ranked_lists:
            # Re-rank based on native scores to ensure 1-based index
            search_list.sort(key=lambda x: x["score"], reverse=True)
            for rank, item in enumerate(search_list, start=1):
                table_name = item["table_name"]
                if table_name not in table_scores:
                    table_scores[table_name] = 0.0
                table_scores[table_name] += 1.0 / (k + rank)
                
        fused = sorted(table_scores.items(), key=lambda x: x[1], reverse=True)[:top_k]
        
        return [{"table_name": t, "rrf_score": s} for t, s in fused]

    def close(self):
        self.neo4j_driver.close()
