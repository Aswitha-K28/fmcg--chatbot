"""
CONNECTOR — Full Pipeline (no value_normalizer)
================================================
Flow:
  intentex → adapter → keyword_search
                     → semantic_search
                     → graph_search (with G)
                     → rrf
                     → sql_builder (schema.json aware)
                     → sql_validator
                     → sql_executor
"""

import os
import sys

# ── Fix import path — works regardless of where you run from ──
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from collections import defaultdict
from neo4j import GraphDatabase

from app1        import extract, ExtractionResult
from key_search1  import BM25SchemaRetriever
from retreival import query_tables
from graphsearch_node2vec    import pull_graph, train_node2vec, graph_search
from sqlbuilder     import build_sql
from sql_validator   import validate_sql, print_validation_report
from sql_executor    import execute_sql, print_results

NEO4J_URI      = "neo4j+s://46991046.databases.neo4j.io"
NEO4J_USER     = "46991046"
NEO4J_PASSWORD = "bT3ebt7nA6Cbz3CofkpdMfKBgVxUW7vJmpJOKverdu8"
SCHEMA_PATH    = "schema.json"
TOP_K          = 5


# ══════════════════════════════════════════════════
#  ADAPTER
# ══════════════════════════════════════════════════

def to_keyword_query(result, original_query):
    parts = [original_query]
    if result.intent.pattern:
        parts.append(result.intent.pattern.replace("_", " ").lower())
    if result.intent.domain:
        parts.append(result.intent.domain)
    if result.intent.group_by:
        parts.append(result.intent.group_by)
    for e in result.entities:
        if e.normalized: parts.append(e.normalized)
        if e.type:       parts.append(e.type)
    return " ".join(parts)


def to_semantic_input(result, original_query):
    entities_dict = {}
    for e in result.entities:
        etype = e.type
        val   = e.normalized or e.value
        if not val:
            continue
        if etype in entities_dict:
            existing = entities_dict[etype]
            if isinstance(existing, list):
                existing.append(val)
            else:
                entities_dict[etype] = [existing, val]
        else:
            entities_dict[etype] = val
    return {
        "intent":   result.intent.pattern.lower().replace("_query", ""),
        "entities": entities_dict
    }


def to_graph_input(result):
    entities_dict = {}
    for e in result.entities:
        etype = e.type
        val   = e.normalized or e.value or None
        if etype in entities_dict:
            existing = entities_dict[etype]
            if isinstance(existing, list):
                existing.append(val)
            else:
                entities_dict[etype] = [existing, val]
        else:
            entities_dict[etype] = val
    return {
        "intent":     result.intent.pattern.lower().replace("_query", ""),
        "entities":   entities_dict,
        "confidence": result.intent.confidence
    }


# ══════════════════════════════════════════════════
#  RRF FUSION
# ══════════════════════════════════════════════════

def rrf_fusion(kw_results, sem_results, graph_results, k=60):
    scores     = defaultdict(float)
    table_meta = {}

    def init_table(tname, description=""):
        """Create entry if not exists."""
        if tname not in table_meta:
            table_meta[tname] = {
                "description": description,
                "columns":     {},   # col_name → {description, sources:[]}
                "sources":     []
            }
        elif description and not table_meta[tname]["description"]:
            table_meta[tname]["description"] = description

    def merge_columns(tname, cols, source_label):
        """
        Merge columns into table_meta — never overwrite, always union.
        cols can be:
          dict  → {col_name: {description:...}}   (keyword / semantic format)
          list  → [{column:..., description:...}]  (graph format)
        """
        existing = table_meta[tname]["columns"]

        if isinstance(cols, list):
            # graph format: list of {column, description, score}
            for c in cols:
                col_name = c.get("column", "")
                col_desc = c.get("description", "")
                if not col_name:
                    continue
                if col_name not in existing:
                    existing[col_name] = {"description": col_desc, "found_in": []}
                existing[col_name].setdefault("found_in", [])
                if source_label not in existing[col_name]["found_in"]:
                    existing[col_name]["found_in"].append(source_label)

        elif isinstance(cols, dict):
            # keyword / semantic format: {col_name: {description:...}}
            for col_name, col_data in cols.items():
                if not col_name:
                    continue
                col_desc = col_data.get("description", "") if isinstance(col_data, dict) else ""
                if col_name not in existing:
                    existing[col_name] = {"description": col_desc, "found_in": []}
                existing[col_name].setdefault("found_in", [])
                if source_label not in existing[col_name]["found_in"]:
                    existing[col_name]["found_in"].append(source_label)

    # ── keyword: [(score, {"table_name":..., "columns":..., "description":...})]
    for rank, (score, meta) in enumerate(kw_results, start=1):
        tname = meta["table_name"]
        scores[tname] += 1.0 / (k + rank)
        init_table(tname, meta.get("description", ""))
        merge_columns(tname, meta.get("columns", {}), f"keyword")
        table_meta[tname]["sources"].append(f"keyword(rank={rank})")

    # ── semantic: [{"rank":1, "table_name":..., "score":..., "columns":...}]
    for item in sem_results:
        tname = item["table_name"]
        rank  = item["rank"]
        scores[tname] += 1.0 / (k + rank)
        init_table(tname, item.get("table_description", ""))
        merge_columns(tname, item.get("columns", {}), f"semantic")
        table_meta[tname]["sources"].append(f"semantic(rank={rank})")

    # ── graph: [{"rank":1, "table":..., "score":..., "columns":[...]}]
    for item in graph_results:
        tname = item["table"]
        rank  = item["rank"]
        scores[tname] += 1.0 / (k + rank)
        init_table(tname, item.get("description", ""))
        merge_columns(tname, item.get("columns", []), f"graph")
        table_meta[tname]["sources"].append(f"graph(rank={rank})")

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    return [
        {
            "rank":        i,
            "table":       tname,
            "rrf_score":   round(rrf_score, 6),
            "description": table_meta[tname]["description"],
            "columns":     table_meta[tname]["columns"],   # ALL columns merged
            "sources":     table_meta[tname]["sources"]
        }
        for i, (tname, rrf_score) in enumerate(ranked, start=1)
    ]


# ══════════════════════════════════════════════════
#  PIPELINE
# ══════════════════════════════════════════════════

def run_pipeline(question, bm25_retriever, nodes, G, n2v_model):

    print(f"\n{'═'*65}")
    print(f"  QUERY : {question}")
    print(f"{'═'*65}")

    # ── ① Intent Extraction ───────────────────────
    print("\n  ① INTENT EXTRACTOR")
    result = extract(question)
    i = result.intent
    print(f"     Pattern     : {i.pattern}")
    print(f"     Domain      : {i.domain}")
    print(f"     Direction   : {i.direction}")
    print(f"     Limit       : {i.limit}")
    print(f"     Group By    : {i.group_by}")
    print(f"     Aggregation : {i.aggregation}")
    print(f"     Confidence  : {i.confidence}")
    print(f"     Entities:")
    for e in result.entities:
        print(f"       [{e.type:<15}] {e.value} → {e.normalized}")

    if i.confidence < 0.4:
        print("\n  ⚠️  Confidence too low — query too vague")
        return None

    # ── ② Adapter ─────────────────────────────────
    kw_query    = to_keyword_query(result, question)
    sem_input   = to_semantic_input(result, question)
    graph_input = to_graph_input(result)

    # ── ③ Keyword Search ──────────────────────────
    print(f"\n  ③ KEYWORD SEARCH (BM25)")
    kw_results = bm25_retriever.search(kw_query, top_k=TOP_K)
    for rank, (score, meta) in enumerate(kw_results, 1):
        print(f"     #{rank} {meta['table_name']:<28} score={score}")

    # ── ④ Semantic Search ─────────────────────────
    print(f"\n  ④ SEMANTIC SEARCH (FAISS)")
    sem_results = query_tables(question, sem_input, top_k=TOP_K)
    for r in sem_results:
        print(f"     #{r['rank']} {r['table_name']:<28} score={r['score']:.4f}")

    # ── ⑤ Graph Search ────────────────────────────
    print(f"\n  ⑤ GRAPH SEARCH (Node2Vec + BFS)")
    graph_results = graph_search(graph_input, nodes, G, n2v_model)
    for r in graph_results:
        print(f"     #{r['rank']} {r['table']:<28} score={r['score']:.4f}")

    # ── ⑥ RRF Fusion ──────────────────────────────
    print(f"\n  ⑥ RRF FUSION")
    fused = rrf_fusion(kw_results, sem_results, graph_results)
    print(f"     {'Rank':<6} {'Table':<28} {'RRF Score':<12} Sources")
    print(f"     {'─'*65}")
    for r in fused[:TOP_K]:
        print(f"     #{r['rank']:<5} {r['table']:<28} {r['rrf_score']:<12} {', '.join(r['sources'])}")

    # ── ⑦ SQL Builder ─────────────────────────────
    print(f"\n  ⑦ SQL BUILDER (Groq + schema.json)")
    sql_result = build_sql(fused[:TOP_K], result, question)

    if not sql_result.get("sql"):
        print(f"  ❌ SQL Builder Error: {sql_result.get('error')}")
        return {"question": question, "intent": i.pattern,
                "tables": fused[:TOP_K], "sql": None, "rows": []}

    sql = sql_result["sql"]
    print(f"\n  Generated SQL:")
    print(f"  {'─'*60}")
    for line in sql.split("\n"):
        print(f"    {line}")

    # ── ⑧ SQL Validator ───────────────────────────
    print(f"\n  ⑧ SQL VALIDATOR")
    validation = validate_sql(sql)
    checks = validation["checks"]
    for check, passed in checks.items():
        icon = "✅" if passed else "❌"
        print(f"     {icon} {check.replace('_',' ')}")

    if validation["warnings"]:
        for w in validation["warnings"]:
            print(f"     ⚠  {w}")

    if not validation["valid"]:
        print(f"\n  ❌ SQL failed validation — not executing")
        for e in validation["errors"]:
            print(f"     • {e}")
        return {"question": question, "intent": i.pattern,
                "tables": fused[:TOP_K], "sql": sql,
                "validation": validation, "rows": []}

    print(f"     ✅ Validation passed!")

    # ── ⑨ SQL Executor ────────────────────────────
    print(f"\n  ⑨ SQL EXECUTOR (MySQL)")
    exec_result = execute_sql(sql)

    if exec_result["success"]:
        print(f"     ✅ Rows returned : {exec_result['row_count']}")
        print(f"     ✅ Time          : {exec_result['time_ms']} ms")
        print_results(exec_result, max_rows=10)
    else:
        print(f"     ❌ Execution failed: {exec_result['error']}")

    return {
        "question": question,
        "intent":   i.pattern,
        "tables":   fused[:TOP_K],
        "sql":      sql,
        "validation": validation,
        "columns":  exec_result.get("columns", []),
        "rows":     exec_result.get("rows", []),
        "row_count":exec_result.get("row_count", 0),
        "time_ms":  exec_result.get("time_ms", 0),
        "error":    exec_result.get("error")
    }


# ══════════════════════════════════════════════════
#  STARTUP
# ══════════════════════════════════════════════════

if __name__ == "__main__":

    print("=" * 65)
    print("  FMCG CHATBOT PIPELINE")
    print("=" * 65)

    # BM25
    print("\n🔄 Loading BM25...")
    bm25_retriever = BM25SchemaRetriever(SCHEMA_PATH)
    print("✅ BM25 ready!")

    # Neo4j
    print("\n🔄 Connecting Neo4j...")
    try:
        neo4j_driver = GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
        neo4j_driver.verify_connectivity()
        print("✅ Neo4j connected!")
    except Exception as e:
        print(f"❌ Neo4j failed: {e}")
        exit()

    # Pull graph + train node2vec
    print("\n🔄 Pulling graph from Neo4j...")
    nodes, G = pull_graph(neo4j_driver)
    neo4j_driver.close()
    print(f"✅ Graph — {len(nodes)} nodes, {G.number_of_edges()} edges")

    print("\n🔄 Training Node2Vec (~30 sec)...")
    n2v_model = train_node2vec(G)
    print("✅ Node2Vec ready!")

    # FAISS loaded at import
    print("\n✅ FAISS index loaded!")

    print("\n" + "=" * 65)
    print("  Ready! Type your question. 'exit' to quit.")
    print("=" * 65)

    while True:
        question = input("\n  Question: ").strip()
        if question.lower() == "exit":
            print("  Bye!")
            break
        if not question:
            continue
        try:
            run_pipeline(question, bm25_retriever, nodes, G, n2v_model)
        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()