"""
add_embeddings.py
═════════════════
You already have Table + Column nodes in Neo4j.
This file just adds embedding property to each node.

Run ONCE:
  pip install neo4j sentence-transformers
  python add_embeddings.py

After this your nodes look like:
  (:Table {
      name: "sales",
      description: "transaction-level sales fact table",
      node_type: "Table",
      embedding: [0.12, -0.44, 0.89, ...]
  })
  (:Column {
      name: "brand_name",
      description: "name of the brand",
      data_type: "VARCHAR",
      node_type: "Column",
      embedding: [0.34, 0.78, -0.21, ...]
  })
"""

from neo4j import GraphDatabase
from sentence_transformers import SentenceTransformer

NEO4J_URI      = "neo4j+s://46991046.databases.neo4j.io"
NEO4J_USER     = "46991046"
NEO4J_PASSWORD = "bT3ebt7nA6Cbz3CofkpdMfKBgVxUW7vJmpJOKverdu8"
MODEL_NAME     = "all-MiniLM-L6-v2"

# ══════════════════════════════════════════════════
#  CONNECT
# ══════════════════════════════════════════════════
driver = GraphDatabase.driver(
    NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
)
driver.verify_connectivity()
print("✅ Neo4j connected")

model = SentenceTransformer(MODEL_NAME)
print("✅ Embedding model loaded")

# ══════════════════════════════════════════════════
#  EMBED TABLE NODES
#  Text = name + description + all column names
#  Richer text = better vector
# ══════════════════════════════════════════════════
def embed_tables():
    with driver.session() as session:

        # pull all tables with their columns
        rows = session.run("""
            MATCH (t:Table)
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            RETURN
                t.name        AS name,
                t.description AS description,
                collect(c.name) AS col_names
        """).data()

        print(f"\n📋 Embedding {len(rows)} Table nodes...")

        for row in rows:
            name        = row["name"] or ""
            description = row["description"] or ""
            col_names   = row["col_names"] or []

            # rich text for better embedding
            # includes table name + description + all column names
            text = (
                f"table {name} "
                f"{name.replace('_', ' ')} "
                f"{description} "
                f"columns: {' '.join(col_names)} "
                f"{' '.join(c.replace('_',' ') for c in col_names)}"
            )

            vector = model.encode(text).tolist()

            # write embedding + node_type back to Neo4j
            session.run("""
                MATCH (t:Table {name: $name})
                SET t.embedding = $vector,
                    t.node_type = 'Table'
            """, name=name, vector=vector)

            print(f"   ✅ {name}")

    print("✅ All Table nodes embedded")


# ══════════════════════════════════════════════════
#  EMBED COLUMN NODES
#  Text = column name + description + parent table name
# ══════════════════════════════════════════════════
def embed_columns():
    with driver.session() as session:

        rows = session.run("""
            MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
            RETURN
                c.name        AS name,
                c.description AS description,
                c.data_type   AS dtype,
                t.name        AS parent_table
        """).data()

        print(f"\n📊 Embedding {len(rows)} Column nodes...")

        for row in rows:
            name         = row["name"] or ""
            description  = row["description"] or ""
            dtype        = row["dtype"] or ""
            parent_table = row["parent_table"] or ""

            text = (
                f"column {name} "
                f"{name.replace('_', ' ')} "
                f"{description} "
                f"data type {dtype} "
                f"in table {parent_table} "
                f"{parent_table.replace('_', ' ')}"
            )

            vector = model.encode(text).tolist()

            # unique match: column under specific table
            session.run("""
                MATCH (t:Table {name: $parent_table})
                      -[:HAS_COLUMN]->
                      (c:Column {name: $name})
                SET c.embedding = $vector,
                    c.node_type = 'Column'
            """, parent_table=parent_table,
                 name=name,
                 vector=vector)

        print(f"   ✅ {len(rows)} columns embedded")

    print("✅ All Column nodes embedded")


# ══════════════════════════════════════════════════
#  VERIFY — check a few nodes
# ══════════════════════════════════════════════════
def verify():
    with driver.session() as session:

        tables = session.run("""
            MATCH (t:Table)
            WHERE t.embedding IS NOT NULL
            RETURN count(t) AS cnt
        """).single()["cnt"]

        columns = session.run("""
            MATCH (c:Column)
            WHERE c.embedding IS NOT NULL
            RETURN count(c) AS cnt
        """).single()["cnt"]

        sample = session.run("""
            MATCH (t:Table)
            WHERE t.embedding IS NOT NULL
            RETURN t.name AS name,
                   t.description AS desc,
                   t.node_type AS node_type,
                   size(t.embedding) AS dims
            LIMIT 3
        """).data()

        print(f"\n{'═'*50}")
        print(f"  VERIFICATION")
        print(f"{'═'*50}")
        print(f"  Tables with embedding  : {tables}")
        print(f"  Columns with embedding : {columns}")
        print(f"\n  Sample Table nodes:")
        for s in sample:
            print(f"    name      : {s['name']}")
            print(f"    desc      : {s['desc']}")
            print(f"    node_type : {s['node_type']}")
            print(f"    dims      : {s['dims']}")
            print()


# ══════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 50)
    print("  ADD EMBEDDINGS TO NEO4J NODES")
    print("=" * 50)

    embed_tables()
    embed_columns()
    verify()

    driver.close()
    print("\n✅ Done. Run graph_search.py now.")