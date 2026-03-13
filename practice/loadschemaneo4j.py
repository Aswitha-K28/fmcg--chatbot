"""
LOAD SCHEMA INTO NEO4J
=======================
This loads your schema.json into Neo4j as a graph.

What gets created in Neo4j:
  Nodes:
    (:Table  {name, description})
    (:Column {name, data_type, description})

  Relationships:
    (:Table)-[:HAS_COLUMN]->(:Column)
    (:Table)-[:RELATED_TO {via, description}]->(:Table)

Run:
  pip install neo4j
  python load_schema_neo4j.py
"""

import json
from neo4j import GraphDatabase

# ══════════════════════════════════════════════════
#  CONFIG
# ══════════════════════════════════════════════════
NEO4J_CONFIG = {
    "uri":      "neo4j+s://46991046.databases.neo4j.io",  # ← change
    "user":     "46991046",
    "password": "bT3ebt7nA6Cbz3CofkpdMfKBgVxUW7vJmpJOKverdu8"                      # ← change
}

SCHEMA_PATH = "schema.json"

# ══════════════════════════════════════════════════
#  LOAD SCHEMA JSON
# ══════════════════════════════════════════════════
def load_schema(path):
    with open(path) as f:
        return json.load(f)

# ══════════════════════════════════════════════════
#  CLEAR EXISTING SCHEMA NODES (safe re-run)
# ══════════════════════════════════════════════════
def clear_schema(tx):
    """
    Deletes only Table and Column nodes.
    Does NOT touch your FMCG data nodes
    (Company, Brand, Product etc).
    """
    tx.run("MATCH (t:Table)  DETACH DELETE t")
    tx.run("MATCH (c:Column) DETACH DELETE c")

# ══════════════════════════════════════════════════
#  CREATE TABLE NODE
# ══════════════════════════════════════════════════
def create_table(tx, table_name, table_info):
    """
    Creates one :Table node with:
      name        = table name
      description = what this table stores
      keywords    = list of domain keywords
    """
    keywords = table_info.get("keywords", [])

    tx.run("""
        MERGE (t:Table {name: $name})
        SET t.description = $description,
            t.keywords    = $keywords
    """,
    name        = table_name,
    description = table_info.get("description", ""),
    keywords    = keywords
    )

# ══════════════════════════════════════════════════
#  CREATE COLUMN NODES + HAS_COLUMN RELATIONSHIP
# ══════════════════════════════════════════════════
def create_columns(tx, table_name, table_info):
    """
    For each column in the table:
      Creates :Column node
      Creates (:Table)-[:HAS_COLUMN]->(:Column)

    Column node stores:
      name        = column name
      data_type   = VARCHAR / INT / DECIMAL / DATE etc
      description = what this column means
      table_name  = which table it belongs to
    """
    for col_name, col_info in table_info["columns"].items():
        tx.run("""
            MATCH (t:Table {name: $table_name})
            MERGE (c:Column {name: $col_name, table_name: $table_name})
            SET c.data_type   = $data_type,
                c.description = $description
            MERGE (t)-[:HAS_COLUMN]->(c)
        """,
        table_name  = table_name,
        col_name    = col_name,
        data_type   = col_info.get("type", "VARCHAR"),
        description = col_info.get("description", "")
        )

# ══════════════════════════════════════════════════
#  CREATE TABLE RELATIONSHIPS
# ══════════════════════════════════════════════════
def create_relationships(tx, relationships):
    """
    Creates (:Table)-[:RELATED_TO]->(:Table) relationships.

    Each relationship has:
      via         = foreign key column name
      description = what the relationship means

    Example:
      (brands)-[:RELATED_TO {via:"brand_id"}]->(products)
      meaning: "Brand has multiple SKUs"
    """
    for rel in relationships:
        tx.run("""
            MATCH (from:Table {name: $from_table})
            MATCH (to:Table   {name: $to_table})
            MERGE (from)-[r:RELATED_TO]->(to)
            SET r.via         = $via,
                r.description = $description
        """,
        from_table  = rel["from"],
        to_table    = rel["to"],
        via         = rel.get("via", ""),
        description = rel.get("description", "")
        )

# ══════════════════════════════════════════════════
#  MAIN LOADER
# ══════════════════════════════════════════════════
def load_schema_to_neo4j(schema_path, neo4j_config):

    # load schema
    print("="*55)
    print("  LOADING SCHEMA INTO NEO4J")
    print("="*55)

    schema = load_schema(schema_path)
    tables = schema["tables"]
    rels   = schema.get("relationships", [])

    print(f"\n📄  Schema loaded: {len(tables)} tables, {len(rels)} relationships")

    # connect neo4j
    print("\n🔄  Connecting to Neo4j...")
    try:
        driver = GraphDatabase.driver(
            neo4j_config["uri"],
            auth=(neo4j_config["user"], neo4j_config["password"])
        )
        driver.verify_connectivity()
        print("✅  Connected!")
    except Exception as e:
        print(f"❌  Connection failed: {e}")
        return

    with driver.session() as session:

        # Step 1 — clear old schema nodes
        print("\n🔄  Clearing old schema nodes...")
        session.execute_write(clear_schema)
        print("   ✅  Old Table + Column nodes deleted")

        # Step 2 — create Table nodes
        print("\n🔄  Creating Table nodes...")
        for table_name, table_info in tables.items():
            session.execute_write(create_table, table_name, table_info)
            print(f"   ✅  Table: {table_name}")

        # Step 3 — create Column nodes + HAS_COLUMN
        print("\n🔄  Creating Column nodes...")
        total_cols = 0
        for table_name, table_info in tables.items():
            session.execute_write(create_columns, table_name, table_info)
            col_count  = len(table_info["columns"])
            total_cols += col_count
            print(f"   ✅  {table_name} → {col_count} columns")

        # Step 4 — create RELATED_TO relationships
        print("\n🔄  Creating relationships...")
        session.execute_write(create_relationships, rels)
        print(f"   ✅  {len(rels)} RELATED_TO relationships created")

        # Step 5 — verify
        print("\n🔄  Verifying...")
        table_count = session.run("MATCH (t:Table) RETURN count(t) AS c").single()["c"]
        col_count   = session.run("MATCH (c:Column) RETURN count(c) AS c").single()["c"]
        rel_count   = session.run("MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS c").single()["c"]

        print(f"\n{'='*55}")
        print(f"  ✅  SCHEMA LOADED SUCCESSFULLY")
        print(f"{'='*55}")
        print(f"  Table nodes     : {table_count}")
        print(f"  Column nodes    : {col_count}")
        print(f"  RELATED_TO rels : {rel_count}")
        print(f"{'='*55}")

    driver.close()
    print("\n✅  Done!")

# ══════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════
if __name__ == "__main__":
    load_schema_to_neo4j(SCHEMA_PATH, NEO4J_CONFIG)