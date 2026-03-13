"""
SQL BUILDER — Schema-Aware Groq LLM
======================================
Sends full schema.json to Groq LLM so it knows:
  - Exact table names
  - Exact column names + types
  - Foreign key relationships
  - Column descriptions

LLM uses this to write correct SQL every time.
No guessing. No hallucination.
"""

import os
import json
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
SCHEMA_PATH  = "schema.json"

_client = Groq(api_key=GROQ_API_KEY)

# ══════════════════════════════════════════════════
#  LOAD SCHEMA ONCE AT STARTUP
# ══════════════════════════════════════════════════
def load_schema(schema_path=SCHEMA_PATH):
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)

_schema = None

def get_schema():
    global _schema
    if _schema is None:
        _schema = load_schema()
    return _schema


# ══════════════════════════════════════════════════
#  BUILD FULL SCHEMA CONTEXT FOR LLM
#  Send complete schema so LLM knows everything
# ══════════════════════════════════════════════════
def build_full_schema_context(rrf_tables):
    """
    Builds schema context from schema.json for the
    top tables returned by RRF.

    For each table shows:
      - table name + alias
      - description
      - every column with type + description
      - foreign key relationships to other tables

    Also appends the FK relationships section
    so LLM knows exactly how to JOIN.
    """
    schema = get_schema()
    tables = schema.get("tables", {})
    relationships = schema.get("relationships", [])

    ALIAS = {
        "sales":               "s",
        "products":            "p",
        "brands":              "b",
        "companies":           "co",
        "categories":          "cat",
        "subcategories":       "sub",
        "zones":               "z",
        "regions":             "r",
        "distributors":        "d",
        "outlets":             "o",
        "inventory":           "inv",
        "pricing_promotions":  "pr",
        "external_factors":    "ef",
        "consumer_feedback":   "cf",
        "sales_targets":       "st",
        "supply_chain":        "sc",
        "marketing_campaigns": "mc",
        "competitors":         "comp",
    }

    lines = []
    lines.append("=" * 60)
    lines.append("DATABASE SCHEMA")
    lines.append("=" * 60)

    # ── tables + columns ──────────────────────────
    for tname in rrf_tables:
        if tname not in tables:
            continue
        tdata = tables[tname]
        alias = ALIAS.get(tname, tname[:2])

        lines.append(f"\nTABLE: {tname}  (use alias: {alias})")
        lines.append(f"DESCRIPTION: {tdata.get('description','')}")
        lines.append("COLUMNS:")

        for col_name, col_data in tdata.get("columns", {}).items():
            col_type = col_data.get("type", "")
            col_desc = col_data.get("description", "")
            lines.append(
                f"  {alias}.{col_name:<30} {col_type:<10} -- {col_desc}"
            )

    # ── relationships ─────────────────────────────
    lines.append("\n" + "=" * 60)
    lines.append("FOREIGN KEY RELATIONSHIPS (for JOINs):")
    lines.append("=" * 60)

    rrf_set = set(rrf_tables)
    for rel in relationships:
        from_t = rel.get("from", "")
        to_t   = rel.get("to", "")
        via    = rel.get("via", "")
        desc   = rel.get("description", "")

        # only show rels between our top tables
        if from_t in rrf_set or to_t in rrf_set:
            from_alias = ALIAS.get(from_t, from_t[:2])
            to_alias   = ALIAS.get(to_t, to_t[:2])
            lines.append(
                f"  {from_t} → {to_t}  "
                f"ON {from_alias}.{via} = {to_alias}.{via}"
                f"  -- {desc}"
            )

    return "\n".join(lines)


# ══════════════════════════════════════════════════
#  BUILD INTENT CONTEXT
# ══════════════════════════════════════════════════
def build_intent_context(intent_result):
    i     = intent_result.intent
    lines = []
    lines.append(f"PATTERN     : {i.pattern}")
    lines.append(f"DIRECTION   : {i.direction}")
    lines.append(f"LIMIT       : {i.limit if i.limit else 'not specified'}")
    lines.append(f"GROUP BY    : {i.group_by if i.group_by else 'not specified'}")
    lines.append(f"AGGREGATION : {i.aggregation}")
    lines.append(f"DOMAIN      : {i.domain}")
    lines.append("")
    lines.append("ENTITIES:")
    for e in intent_result.entities:
        lines.append(
            f"  type={e.type:<15} "
            f"value={e.normalized or e.value}"
        )
    return "\n".join(lines)


# ══════════════════════════════════════════════════
#  SYSTEM PROMPT
# ══════════════════════════════════════════════════
SYSTEM_PROMPT = """
You are an expert MySQL query writer for FMCG (Fast Moving Consumer Goods) business intelligence.

Your job:
  Read the full database schema, the user question, intent, and entities
  Write ONE correct MySQL SELECT query that answers the question

RULES — follow every rule strictly:

RULE 1 — ONLY USE COLUMNS FROM SCHEMA
  The schema shows every table and every column exactly
  NEVER use a column that is not listed in the schema
  NEVER invent column names

RULE 2 — ALIAS SYNTAX
  Each table has a short alias shown in schema (e.g. sales → s)
  Define alias in FROM:    FROM sales s
  Prefix every column:     s.revenue   s.brand_name   s.zone_name
  WRONG:  SELECT brand_name b     ← this is wrong syntax
  WRONG:  SELECT b.brand_name     ← b alias not defined
  CORRECT: SELECT s.brand_name    ← s defined in FROM sales s

RULE 3 — JOIN CORRECTLY
  Use ONLY the join paths listed in FOREIGN KEY RELATIONSHIPS section
  NEVER guess a join column — only use what is shown

  EXACT JOIN PATHS IN THIS DATABASE:
    sales → distributors: JOIN distributors d ON s.distributor_id = d.distributor_id
    sales → products:     JOIN products p ON s.product_id = p.product_id
    sales → regions:      JOIN regions r ON s.region_id = r.region_id
    outlets → distributors: JOIN distributors d ON o.distributor_id = d.distributor_id

  WRONG: JOIN distributors d ON s.outlet_id = d.outlet_id
         ← sales has NO outlet_id column, this will error
  WRONG: JOIN outlets o ON s.outlet_id = o.outlet_id
         ← sales has NO outlet_id column, this will error
  CORRECT — sales has distributor_id directly:
    FROM sales s
    JOIN distributors d ON s.distributor_id = d.distributor_id

RULE 4 — GROUP BY
  Every non-aggregated SELECT column must be in GROUP BY
  Use full alias.column:   GROUP BY s.brand_name

RULE 5 — SELECT THE RIGHT COLUMN
  Read question to know what to SELECT and GROUP BY
  "top distributors"  → SELECT d.distributor_name ... GROUP BY d.distributor_name
  "top brands"        → SELECT s.brand_name        ... GROUP BY s.brand_name
  "top products"      → SELECT s.sku_name          ... GROUP BY s.sku_name
  "by zone"           → GROUP BY s.zone_name
  "by region"         → GROUP BY s.region_name

RULE 6 — WHERE FILTERS
  Apply all entity values as WHERE filters
  Check schema to find which table has that column
  Strings:   s.zone_name = 'South'
  Years:     s.year = 2024
  Quarters:  s.quarter = 'Q3'

RULE 7 — NOTE ABOUT sales TABLE
  The sales table already has denormalized columns:
    s.brand_name, s.company_name, s.zone_name, s.region_name
  Use these directly — no need to JOIN brands/zones/regions
  just to get their name column

RULE 8 — OUTPUT
  Return ONLY the raw SQL query
  No markdown, no backticks, no explanation
  End with semicolon
"""


# ══════════════════════════════════════════════════
#  SQL GENERATION PROMPT
# ══════════════════════════════════════════════════
SQL_PROMPT = """
QUESTION: {question}

{schema_context}

INTENT AND ENTITIES:
{intent_context}

KEY REMINDERS:
  1. sales table already has brand_name, company_name, zone_name, region_name
     → for brand/zone/region queries: use sales table directly, NO extra JOINs needed

  2. TO REACH distributors table — DIRECT 1-step join:
       FROM sales s
       JOIN distributors d ON s.distributor_id = d.distributor_id
     sales has distributor_id column directly — no outlets needed!
     NEVER write: JOIN distributors d ON s.outlet_id = d.outlet_id  ← sales has NO outlet_id
     NEVER write: JOIN outlets o ON s.outlet_id = o.outlet_id       ← sales has NO outlet_id

  3. WHERE filter columns — use sales table columns directly:
       zone_name  → s.zone_name  = 'South'
       region_name→ s.region_name = 'Hyderabad'
       quarter    → s.quarter    = 'Q3'
       year       → s.year       = 2024
       brand_name → s.brand_name = 'Dove'

Write the MySQL query now:
"""


# ══════════════════════════════════════════════════
#  CALL GROQ
# ══════════════════════════════════════════════════
def call_groq(prompt):
    response = _client.chat.completions.create(
        model    = GROQ_MODEL,
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt}
        ],
        temperature = 0.0
    )
    return response.choices[0].message.content.strip()


# ══════════════════════════════════════════════════
#  CLEAN SQL
# ══════════════════════════════════════════════════
def clean_sql(raw):
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines).strip()
    if not raw.endswith(";"):
        raw += ";"
    return raw


# ══════════════════════════════════════════════════
#  MAIN — BUILD SQL
# ══════════════════════════════════════════════════
def build_sql(rrf_results, intent_result, original_question=""):
    """
    Input:
      rrf_results       → output from rrf_fusion()
      intent_result     → ExtractionResult from intentex.py
      original_question → raw user question string

    Output:
      {
        "sql":         "SELECT ...",
        "tables_used": ["sales", "distributors"],
        "pattern":     "RANKING_QUERY"
      }
    """
    if not rrf_results:
        return {"sql": None, "error": "No tables from RRF"}

    # get top table names from RRF
    rrf_tables = [r["table"] for r in rrf_results[:5]]

    # build schema context from schema.json
    schema_context = build_full_schema_context(rrf_tables)

    # build intent context
    intent_context = build_intent_context(intent_result)

    # build final prompt
    prompt = SQL_PROMPT.format(
        question       = original_question,
        schema_context = schema_context,
        intent_context = intent_context
    )

    print(f"\n     Calling Groq ({GROQ_MODEL})...")
    raw  = call_groq(prompt)
    sql  = clean_sql(raw)

    return {
        "sql":         sql,
        "tables_used": rrf_tables,
        "pattern":     intent_result.intent.pattern
    }


# ══════════════════════════════════════════════════
#  TEST
# ══════════════════════════════════════════════════
if __name__ == "__main__":

    from dataclasses import dataclass
    from typing import Optional

    @dataclass
    class E:
        type: str; value: str; normalized: str; category: str

    @dataclass
    class I:
        pattern: str; direction: str; limit: Optional[int]
        group_by: str; aggregation: str; domain: str
        confidence: float; query_type: str

    @dataclass
    class R:
        intent: I
        entities: list

    mock_rrf = [
        {"rank": 1, "table": "sales",        "rrf_score": 0.049},
        {"rank": 2, "table": "distributors",  "rrf_score": 0.031},
        {"rank": 3, "table": "outlets",       "rrf_score": 0.020},
    ]

    tests = [
        {
            "q": "which distributor has more sales in Hyderabad Q2 2023",
            "r": R(
                intent=I("RANKING_QUERY","DESC",10,"distributor","SUM","sales",0.9,"STANDARD"),
                entities=[
                    E("metric",    "units sold","units sold","sales"),
                    E("geography", "Hyderabad", "Hyderabad", "location"),
                    E("time",      "Q2",        "Q2",        "time"),
                    E("time",      "2023",      "2023",      "time"),
                ]
            )
        },
        {
            "q": "top 5 brands by revenue in South zone Q3 2024",
            "r": R(
                intent=I("RANKING_QUERY","DESC",5,"brand","SUM","sales",0.9,"STANDARD"),
                entities=[
                    E("metric",    "revenue","revenue","sales"),
                    E("geography", "South",  "South",  "location"),
                    E("time",      "Q3",     "Q3",     "time"),
                    E("time",      "2024",   "2024",   "time"),
                ]
            )
        },
    ]

    print("=" * 62)
    print("  SQL BUILDER TEST — Schema-Aware Groq")
    print("=" * 62)

    for tc in tests:
        print(f"\n{'─'*62}")
        print(f"  Q: {tc['q']}")
        result = build_sql(mock_rrf, tc["r"], tc["q"])
        print(f"\n  SQL:")
        for line in result["sql"].split("\n"):
            print(f"    {line}")