# =============================================================
# sql_agent.py
# SQL Generation + Validation + Execution
# Inputs : intent, entities from app.py | rrf_output from rrf.py
# Schema : loaded from schema.json at runtime
# Output : query results → sent to nlg.py
# =============================================================
 
import os
import re
import json
import mysql.connector
from dotenv import load_dotenv
 
load_dotenv()
 
 
# ─────────────────────────────────────────────────────────────
# DB CONNECTION
# ─────────────────────────────────────────────────────────────
 
def get_connection():
    return mysql.connector.connect(
        host     = os.getenv("MYSQL_HOST",     "127.0.0.1"),
        port     = int(os.getenv("MYSQL_PORT", "3306")),
        user     = os.getenv("MYSQL_USER",     "root"),
        password = os.getenv("MYSQL_PASSWORD", "Aswitha@12"),
        database = os.getenv("MYSQL_DB",       "fmcg_database")
    )
 
 
# ─────────────────────────────────────────────────────────────
# SCHEMA LOADER
# ─────────────────────────────────────────────────────────────
 
def load_schema() -> dict:
    path = os.path.join(os.path.dirname(__file__), "schema.json")
    with open(path, "r") as f:
        raw = json.load(f)
    return raw["tables"] if "tables" in raw else raw
 
 
# ─────────────────────────────────────────────────────────────
# LINEAGE BUILDER
# Builds table relationship map from schema at runtime
# No hardcoded table or column names
# ─────────────────────────────────────────────────────────────
 
def build_lineage(schema: dict) -> dict:
    lineage = {table: [] for table in schema}
 
    for table, info in schema.items():
        columns = list(info.get("columns", {}).keys())
 
        for col in columns:
            # detect foreign key by _id suffix
            if col.endswith("_id"):
                ref_base = col.replace("_id", "")
                for candidate in [ref_base + "s", ref_base + "es", ref_base]:
                    if candidate in schema and candidate != table:
                        if not any(r["to"] == candidate for r in lineage[table]):
                            lineage[table].append({
                                "to"  : candidate,
                                "via" : col,
                                "join": f"{table}.{col} = {candidate}.{col}"
                            })
                        break
 
            # detect shared columns across tables
            for other_table, other_info in schema.items():
                if other_table == table:
                    continue
                other_cols = list(other_info.get("columns", {}).keys())
                if col in other_cols:
                    if not any(r["to"] == other_table for r in lineage[table]):
                        lineage[table].append({
                            "to"  : other_table,
                            "via" : col,
                            "join": f"{table}.{col} = {other_table}.{col}"
                        })
 
    return lineage
 
 
# ─────────────────────────────────────────────────────────────
# COLUMN EXTRACTOR
# Extracts columns for a specific table from RRF top_columns
# top_columns format: ["table.column", "table.column", ...]
# ─────────────────────────────────────────────────────────────
 
def get_table_columns_from_rrf(top_columns: list, table: str) -> list:
    return [
        c.split(".")[1] for c in top_columns
        if "." in c and c.split(".")[0].lower() == table.lower()
    ]
 
 
# ─────────────────────────────────────────────────────────────
# ENTITY TO COLUMN MAPPER
# Maps entity normalized value to exact column from RRF top_columns
# Falls back to schema columns if RRF has no match
# ─────────────────────────────────────────────────────────────
 
def map_entity_to_column(entity, top_columns: list, table: str,
                          schema_columns: list) -> str | None:
    # get RRF columns for this table
    rrf_cols = get_table_columns_from_rrf(top_columns, table)
    search_cols = rrf_cols if rrf_cols else schema_columns
 
    norm = entity.normalized.lower().replace(" ", "_")
 
    # direct match
    for col in search_cols:
        if norm in col.lower() or col.lower() in norm:
            return col
 
    # partial word match
    norm_words = [w for w in norm.split("_") if len(w) > 2]
    for col in search_cols:
        col_words = col.lower().split("_")
        if any(w in col_words for w in norm_words):
            return col
 
    # no match found — return None with clear signal
    return None
 
 
# ─────────────────────────────────────────────────────────────
# SQL GENERATOR
# Builds SQL from intent + entities + rrf_output + schema
# No hardcoded table or column names
# ─────────────────────────────────────────────────────────────
 
def generate_sql(intent, entities: list, rrf_output: dict, schema: dict) -> str:
 
    top_tables  = rrf_output.get("top_tables",  [])
    top_columns = rrf_output.get("top_columns", [])
 
    if not top_tables:
        raise ValueError("RRF returned no tables — cannot generate SQL")
 
    primary_table   = top_tables[0]
    schema_columns  = list(schema.get(primary_table, {}).get("columns", {}).keys())
    rrf_table_cols  = get_table_columns_from_rrf(top_columns, primary_table)
    available_cols  = rrf_table_cols if rrf_table_cols else schema_columns
 
    if not available_cols:
        raise ValueError(f"No columns found for table '{primary_table}' in RRF or schema")
 
    # ── find metric column from RRF top_columns ────────────────
    metric_col = None
    for e in entities:
        if e.type == "metric":
            metric_col = map_entity_to_column(e, top_columns,
                                               primary_table, schema_columns)
            if metric_col:
                break
    if not metric_col:
        raise ValueError("Could not determine metric column from entities or RRF")
 
    # ── find group_by column from RRF top_columns ──────────────
    group_col = None
    if intent.group_by:
        norm_group = intent.group_by.lower().replace(" ", "_")
        for col in available_cols:
            if norm_group in col.lower() or col.lower() in norm_group:
                group_col = col
                break
    if not group_col:

    # prefer name columns over id columns

     for col in available_cols:

        if col != metric_col and not col.endswith("_id"):

            group_col = col

            break
 
    if not group_col:
        raise ValueError("Could not determine group_by column from RRF or intent")
 
    # ── find time column from RRF top_columns ─────────────────
    time_col = None
    for col in available_cols:
        for e in entities:
            if e.type == "time":
                norm = e.normalized.lower()
                if norm in col.lower() or col.lower() in norm:
                    time_col = col
                    break
        if time_col:
            break
    if not time_col:
        time_entities = [e.normalized.lower() for e in entities if e.type == "time"]
        for col in available_cols:
            if any(t in col.lower() for t in time_entities):
                time_col = col
                break
    if not time_col and intent.pattern in ("TREND_QUERY", "FORECAST_QUERY"):
        raise ValueError("No time column found for TREND/FORECAST query")
    elif not time_col:
        time_col = available_cols[0] if available_cols else None
 
    # ── build WHERE filters from entities ─────────────────────
    filters      = []
    skipped      = []
    for e in entities:
        if e.type in ("metric", "ranking", "comparison"):
            continue
        col = map_entity_to_column(e, top_columns, primary_table, schema_columns)
        if col:
            norm = e.normalized
            if norm.isdigit():
                filters.append(f"{col} = {norm}")
            else:
                filters.append(f"{col} = '{norm}'")
        else:
            skipped.append(f"{e.type}:{e.normalized}")
 
    if skipped:
        print(f"  WARNING: Could not map entities to columns — {skipped}")
 
    agg       = intent.aggregation or "SUM"
    direction = intent.direction   or "DESC"
    limit     = intent.limit
    where     = f"WHERE {' AND '.join(filters)}" if filters else ""
 
    # ── pick SQL template based on intent pattern ──────────────
    if intent.pattern in ("RANKING_QUERY", "SUMMARY_QUERY"):
        if group_col:
            sql = (f"SELECT {group_col}, {agg}({metric_col}) AS total\n"
                   f"FROM {primary_table}\n"
                   f"{where}\n"
                   f"GROUP BY {group_col}\n"
                   f"ORDER BY total {direction}")
            if limit:
                sql += f"\nLIMIT {limit}"
        else:
            sql = (f"SELECT {agg}({metric_col}) AS total\n"
                   f"FROM {primary_table}\n"
                   f"{where}")
 
    elif intent.pattern == "TREND_QUERY":
        sql = (f"SELECT {time_col}, {agg}({metric_col}) AS total\n"
               f"FROM {primary_table}\n"
               f"{where}\n"
               f"GROUP BY {time_col}\n"
               f"ORDER BY {time_col} ASC")
 
    elif intent.pattern == "COMPARISON_QUERY":
        sql = (f"SELECT {group_col}, {agg}({metric_col}) AS total\n"
               f"FROM {primary_table}\n"
               f"{where}\n"
               f"GROUP BY {group_col}\n"
               f"ORDER BY total {direction}")
 
    elif intent.pattern == "SENTIMENT_QUERY":
        # use metric_col — RRF already mapped sentiment/rating to correct column
        sql = (f"SELECT {group_col}, AVG({metric_col}) AS avg_score, "
               f"COUNT(*) AS total_responses\n"
               f"FROM {primary_table}\n"
               f"{where}\n"
               f"GROUP BY {group_col}\n"
               f"ORDER BY avg_score {direction}")
 
    elif intent.pattern == "FORECAST_QUERY":
        sql = (f"SELECT {time_col}, {agg}({metric_col}) AS total\n"
               f"FROM {primary_table}\n"
               f"{where}\n"
               f"GROUP BY {time_col}\n"
               f"ORDER BY {time_col} ASC")
 
    elif intent.pattern == "ROOT_CAUSE_QUERY":
        rrf_cols = get_table_columns_from_rrf(top_columns, primary_table)
        select_cols = ", ".join(rrf_cols) if rrf_cols else ", ".join(schema_columns[:6])
        sql = (f"SELECT {select_cols}\n"
               f"FROM {primary_table}\n"
               f"{where}\n"
               f"ORDER BY {metric_col} {direction}\n"
               f"LIMIT {limit or 50}")
 
    else:
        rrf_cols = get_table_columns_from_rrf(top_columns, primary_table)
        select_cols = ", ".join(rrf_cols) if rrf_cols else ", ".join(schema_columns[:6])
        sql = (f"SELECT {select_cols}\n"
               f"FROM {primary_table}\n"
               f"{where}\n"
               f"LIMIT {limit or 100}")
 
    return "\n".join(line for line in sql.split("\n") if line.strip()).strip()
 
 
# ─────────────────────────────────────────────────────────────
# SQL VALIDATOR
# 1. Regex check — blocks dangerous keywords and patterns
# 2. Lineage check — validates tables, columns, joins
# ─────────────────────────────────────────────────────────────
 
BLOCKED_PATTERNS = [
    r"\bDROP\b", r"\bDELETE\b", r"\bUPDATE\b", r"\bINSERT\b",
    r"\bTRUNCATE\b", r"\bALTER\b", r"\bCREATE\b", r"\bGRANT\b",
    r"\bREVOKE\b", r"\bEXEC\b", r"\bEXECUTE\b", r"\bMERGE\b",
    r"\bREPLACE\b", r"\bRENAME\b", r"\bSELECT\s+\*",
    r"\bINTO\s+OUTFILE\b", r"\bLOAD\s+DATA\b",
    r"\bINFORMATION_SCHEMA\b", r"\bSHOW\s+TABLES\b",
    r"\bSHOW\s+DATABASES\b", r"\bSLEEP\s*\(", r"\bBENCHMARK\s*\(",
    r"--", r";\s*\w",
]
 
def validate_sql(sql: str, schema: dict, rrf_output: dict,
                 lineage: dict) -> tuple[bool, str]:
 
    sql_upper = sql.upper()
 
    # Rule 1 — regex check for dangerous patterns
    for pattern in BLOCKED_PATTERNS:
        if re.search(pattern, sql_upper):
            return False, f"Blocked pattern found: {pattern}"
 
    # Rule 2 — must have SELECT
    if "SELECT" not in sql_upper:
        return False, "SQL does not contain SELECT"
 
    # Rule 3 — extract tables used in SQL
    tables_in_sql = set()
    for t in (re.findall(r"FROM\s+(\w+)", sql_upper) +
              re.findall(r"JOIN\s+(\w+)", sql_upper)):
        tables_in_sql.add(t.lower())
 
    # Rule 4 — all tables must exist in schema
    for t in tables_in_sql:
        if t not in schema:
            return False, f"Table '{t}' does not exist in schema"
 
    # Rule 5 — all columns must exist in their tables
    for table in tables_in_sql:
        if table in schema:
            valid_cols  = set(c.lower() for c in
                              schema[table].get("columns", {}).keys())
            col_matches = re.findall(rf"\b{table}\.(\w+)\b", sql, re.IGNORECASE)
            for col in col_matches:
                if col.lower() not in valid_cols:
                    return False, f"Column '{col}' not in table '{table}'"
 
    # Rule 6 — lineage check: JOINs must follow valid relationships
    join_pairs = re.findall(
        r"JOIN\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)",
        sql, re.IGNORECASE
    )
    for join_table, left_col, right_col in join_pairs:
        jt = join_table.lower()
        valid_join = False
        for from_table, rels in lineage.items():
            for rel in rels:
                if rel["to"].lower() == jt or from_table.lower() == jt:
                    valid_join = True
                    break
        if not valid_join:
            return False, f"JOIN on '{join_table}' not found in lineage"
 
    # Rule 7 — SQL must use RRF recommended tables
    rrf_tables = set(t.lower() for t in rrf_output.get("top_tables", []))
    if rrf_tables and not any(t in rrf_tables for t in tables_in_sql):
        return False, "SQL does not use any RRF recommended tables"
 
    return True, "Valid"
 
 
# ─────────────────────────────────────────────────────────────
# SQL EXECUTOR
# ─────────────────────────────────────────────────────────────
 
def execute_sql(sql: str) -> dict:
    try:
        conn   = get_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(sql)
        rows    = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        cursor.close()
        conn.close()
        return {
            "success": True,
            "columns": columns,
            "rows"   : rows,
            "count"  : len(rows)
        }
    except Exception as e:
        return {
            "success": False,
            "error"  : str(e),
            "rows"   : [],
            "count"  : 0
        }
 
 
# ─────────────────────────────────────────────────────────────
# MAIN PIPELINE
# ─────────────────────────────────────────────────────────────
 
def convert_fused_to_rrf(fused: list) -> dict:
    """
    Converts connector fused RRF list into dict format sql_agent expects.
    fused format : [{"table": "sales", "columns": {"revenue": {}, ...}, ...}]
    output format: {"top_tables": [...], "top_columns": ["table.col", ...]}
    """
    top_tables  = [r["table"] for r in fused]
    top_columns = [
        f"{r['table']}.{col}"
        for r in fused
        for col in r.get("columns", {})
    ]
    return {
        "top_tables" : top_tables,
        "top_columns": top_columns
    }
 
 
def run_sql_pipeline(intent, entities: list, fused: list) -> dict:
    """
    Full pipeline:
    load schema → build lineage → convert rrf →
    generate sql → validate → execute → return results
 
    fused : RRF output list from connector.py
    """
 
    # Step 1 — load schema from folder
    schema = load_schema()
 
    # Step 2 — build lineage from schema
    lineage = build_lineage(schema)
 
    # Step 3 — convert fused RRF list to dict format
    rrf_output = convert_fused_to_rrf(fused)
 
    # Step 4 — generate SQL
    try:
        sql = generate_sql(intent, entities, rrf_output, schema)
    except Exception as e:
        return {"success": False, "error": f"SQL generation failed: {e}"}
 
    # Step 5 — validate SQL
    is_valid, reason = validate_sql(sql, schema, rrf_output, lineage)
 
    if not is_valid:
        return {"success": False, "error": reason, "sql": sql}
 
    # Step 6 — execute SQL
    result = execute_sql(sql)
 
    result["sql"] = sql
 
    return result
 
 
# ─────────────────────────────────────────────────────────────
# TEST
# ─────────────────────────────────────────────────────────────
 
if __name__ == "__main__":
    import sys
    sys.path.append(os.path.dirname(__file__))
    from app1 import extract
 
    question = "Top 3 brands by revenue in North zone Q3 2023"
 
    print("\n" + "=" * 60)
    print(f"  QUESTION : {question}")
    print("=" * 60)
 
    result = extract(question)
    # connector passes fused — use mock fused for test
    mock_fused = []
    run_sql_pipeline(result.intent, result.entities, mock_fused)