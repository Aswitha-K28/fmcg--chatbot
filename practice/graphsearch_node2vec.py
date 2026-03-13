import math
from collections import deque
import networkx as nx
from node2vec import Node2Vec
from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer

NEO4J_URI = "neo4j+s://46991046.databases.neo4j.io"
NEO4J_USER = "46991046"
NEO4J_PASSWORD = "bT3ebt7nA6Cbz3CofkpdMfKBgVxUW7vJmpJOKverdu8"
MODEL_NAME = "all-MiniLM-L6-v2"

NODE2VEC_PARAMS = {
    "dimensions": 64,
    "walk_length": 20,
    "num_walks": 100,
    "p": 1,
    "q": 0.5,
    "workers": 2,
}

TOP_TABLES = 5
TOP_N_SIMILAR = 100

_model = None


def get_model():
    global _model
    if _model is None:
        _model = SentenceTransformer(MODEL_NAME)
    return _model


def embed_text(text):
    return get_model().encode([text])[0].tolist()


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


def pull_graph(driver):
    nodes = {}
    G = nx.Graph()

    with driver.session() as session:
        # tables
        for row in session.run("""
            MATCH (t:Table)
            WHERE t.embedding IS NOT NULL
            RETURN t.name AS name,
                   t.description AS desc,
                   t.embedding AS embedding
        """).data():
            nid = row["name"]
            nodes[nid] = {
                "id": nid,
                "type": "Table",
                "name": row["name"],
                "desc": row.get("desc") or "",
                "embedding": row["embedding"]
            }
            G.add_node(nid, node_type="Table")

        # columns
        for row in session.run("""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            WHERE c.embedding IS NOT NULL
            RETURN t.name AS parent_table,
                   c.name AS name,
                   c.description AS desc,
                   c.data_type AS dtype,
                   c.embedding AS embedding
        """).data():
            nid = f"{row['parent_table']}__{row['name']}"
            nodes[nid] = {
                "id": nid,
                "type": "Column",
                "name": row["name"],
                "desc": row.get("desc") or "",
                "dtype": row.get("dtype") or "",
                "parent_table": row["parent_table"],
                "embedding": row["embedding"]
            }
            G.add_node(nid, node_type="Column")

        # table-column edges
        for row in session.run("""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            WHERE t.embedding IS NOT NULL AND c.embedding IS NOT NULL
            RETURN t.name AS table_name, c.name AS col_name
        """).data():
            G.add_edge(
                row["table_name"],
                f"{row['table_name']}__{row['col_name']}",
                relation="HAS_COLUMN"
            )

        # table-table edges
        for row in session.run("""
            MATCH (t1:Table)-[:RELATED_TO]->(t2:Table)
            WHERE t1.embedding IS NOT NULL AND t2.embedding IS NOT NULL
            RETURN t1.name AS from_t, t2.name AS to_t
        """).data():
            G.add_edge(
                row["from_t"],
                row["to_t"],
                relation="RELATED_TO"
            )

    return nodes, G


def train_node2vec(G):
    n2v = Node2Vec(
        G,
        dimensions=NODE2VEC_PARAMS["dimensions"],
        walk_length=NODE2VEC_PARAMS["walk_length"],
        num_walks=NODE2VEC_PARAMS["num_walks"],
        p=NODE2VEC_PARAMS["p"],
        q=NODE2VEC_PARAMS["q"],
        workers=NODE2VEC_PARAMS["workers"],
        quiet=True
    )
    return n2v.fit(window=5, min_count=1, batch_words=4)


def parse_classifier(clf_output):
    intent = clf_output.get("intent", "")
    entities = clf_output.get("entities", {})
    active = {}

    for key, val in entities.items():
        if val is None:
            continue
        if isinstance(val, list) and len(val) == 0:
            continue
        if val == "":
            continue
        active[key] = val

    return intent, active


def build_query_text(intent, active_entities):
    parts = []

    if intent:
        parts.append(intent.replace("_", " "))

    for key, val in active_entities.items():
        parts.append(key.replace("_", " "))
        if isinstance(val, list):
            parts.extend([str(v) for v in val if v])
        else:
            parts.append(str(val))

    return " ".join(parts)


def find_seed(query_text, nodes):
    query_vec = embed_text(query_text)
    best_id = None
    best_score = -1.0

    for nid, node in nodes.items():
        if node["type"] != "Table":
            continue
        score = cosine_sim(query_vec, node["embedding"])
        if score > best_score:
            best_score = score
            best_id = nid

    return best_id, best_score


def node2vec_search(seed_id, n2v_model, top_n=TOP_N_SIMILAR):
    if seed_id not in n2v_model.wv:
        return {}

    scored = {seed_id: 1.0}
    similar = n2v_model.wv.most_similar(seed_id, topn=top_n)

    for nid, score in similar:
        scored[nid] = round(float(score), 4)

    return scored


def bfs_tables_from_seed(seed_id, G, nodes):
    visited = set()
    queue = deque([seed_id])
    bfs_tables = []

    while queue:
        current = queue.popleft()

        if current in visited:
            continue
        visited.add(current)

        if current in nodes and nodes[current]["type"] == "Table":
            bfs_tables.append(current)

        for neighbor in G.neighbors(current):
            if neighbor not in visited:
                queue.append(neighbor)

    return bfs_tables


def build_output_bfs(seed_id, G, nodes, n2v_scored, active_entities):
    entity_words = set()
    entity_values = set()

    for key, val in active_entities.items():
        entity_words.add(_singularize(key.lower()))
        if isinstance(val, str) and val.strip():
            entity_values.add(val.lower().strip())

    bfs_tables = bfs_tables_from_seed(seed_id, G, nodes)

    scored_tables = []
    for table_id in bfs_tables:
        table_score = n2v_scored.get(table_id, 0.0)
        scored_tables.append((table_id, table_score))

    scored_tables.sort(key=lambda x: x[1], reverse=True)
    scored_tables = scored_tables[:TOP_TABLES]

    results = []

    for rank, (table_id, table_score) in enumerate(scored_tables, start=1):
        relevant_cols = []

        for neighbor in G.neighbors(table_id):
            if neighbor not in nodes:
                continue
            if nodes[neighbor]["type"] != "Column":
                continue

            col_name = nodes[neighbor]["name"].lower()
            col_desc = (nodes[neighbor].get("desc") or "").lower()

            entity_match = False

            if any(word in col_name for word in entity_words):
                entity_match = True

            if any(val in col_name or val in col_desc for val in entity_values):
                entity_match = True

            if not entity_match:
                continue

            col_score = n2v_scored.get(neighbor, 0.0)

            relevant_cols.append({
                "column": nodes[neighbor]["name"],
                "description": nodes[neighbor].get("desc", ""),
                "score": round(col_score, 4)
            })

        relevant_cols.sort(key=lambda x: x["score"], reverse=True)

        results.append({
            "rank": rank,
            "table": table_id,
            "score": round(table_score, 3),
            "description": nodes[table_id].get("desc", ""),
            "columns": relevant_cols
        })

    return results


def display_output(results):
    for r in results:
        print(f"Rank: {r['rank']} | Score: {r['score']:.3f} | Table: {r['table']}")
        print(f"Description: {r['description']}")
        print("Columns:")
        if r["columns"]:
            for col in r["columns"]:
                print(f" - {col['column']}: {col['description']}")
        else:
            print(" - No matching columns")
        print("\n---\n")


def graph_search(clf_output, nodes, G, n2v_model):
    intent, active_entities = parse_classifier(clf_output)
    query_text = build_query_text(intent, active_entities)

    seed_id, _ = find_seed(query_text, nodes)
    if not seed_id:
        return []

    n2v_scored = node2vec_search(seed_id, n2v_model)
    results = build_output_bfs(seed_id, G, nodes, n2v_scored, active_entities)

    display_output(results)
    return results


if __name__ == "__main__":
    driver = GraphDatabase.driver(
        NEO4J_URI,
        auth=(NEO4J_USER, NEO4J_PASSWORD)
    )
    driver.verify_connectivity()

    nodes, G = pull_graph(driver)
    driver.close()

    n2v_model = train_node2vec(G)

    test_inputs = [
        {
           
  "intent": "ranking_query",
  "entities": {
    "metric": "units sold",
    "distributor": "distributors",
    "region": "hyd",
    "top_n": 10
  }
        }
        
    ]

    for test in test_inputs:
        graph_search(test, nodes, G, n2v_model)