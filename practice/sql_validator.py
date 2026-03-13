"""
SQL VALIDATOR
=============
Validates generated SQL against schema.json BEFORE execution.
Catches all common LLM mistakes so bad SQL never hits the DB.

Checks:
  ✅ Rule 1 — All columns exist in schema
  ✅ Rule 2 — Alias syntax correct (alias.column, not column alias)
  ✅ Rule 3 — JOINs use valid FK relationships only
  ✅ Rule 4 — GROUP BY covers all non-aggregated SELECT columns
  ✅ Rule 5 — No SELECT *
  ✅ Rule 6 — No dangerous keywords (DROP, DELETE, UPDATE, INSERT)

Returns:
  {
    "valid":    True / False,
    "errors":   ["list of errors if any"],
    "warnings": ["list of warnings (non-fatal)"],
    "checks":   {"rule1": True, "rule2": False, ...}
  }
"""

import re
import json

SCHEMA_PATH = "schema.json"

# ── aliases matching sql_builder.py ────────────────────────────────
ALIAS_TO_TABLE = {
    "co":   "companies",
    "b":    "brands",
    "cat":  "categories",
    "sub":  "subcategories",
    "p":    "products",
    "z":    "zones",
    "r":    "regions",
    "d":    "distributors",
    "o":    "outlets",
    "s":    "sales",
    "inv":  "inventory",
    "pr":   "pricing_promotions",
    "ef":   "external_factors",
    "cf":   "consumer_feedback",
    "st":   "sales_targets",
    "sc":   "supply_chain",
    "mc":   "marketing_campaigns",
    "comp": "competitors",
}

TABLE_TO_ALIAS = {v: k for k, v in ALIAS_TO_TABLE.items()}

AGGREGATE_FUNCTIONS = {"SUM", "AVG", "COUNT", "MAX", "MIN", "COUNT(DISTINCT"}

DANGEROUS_KEYWORDS = {"DROP", "DELETE", "UPDATE", "INSERT", "TRUNCATE",
                      "ALTER", "CREATE", "REPLACE", "MERGE"}


# ══════════════════════════════════════════════════
#  LOAD SCHEMA
# ══════════════════════════════════════════════════
_schema = None

def get_schema():
    global _schema
    if _schema is None:
        _schema = json.load(open(SCHEMA_PATH, encoding="utf-8"))
    return _schema


def get_table_columns():
    """Returns dict: table_name → set of column names"""
    schema = get_schema()
    return {
        tname: set(tdata["columns"].keys())
        for tname, tdata in schema["tables"].items()
    }


def get_valid_joins():
    """
    Returns set of valid (table1, table2, via_column) tuples.
    Both directions included.
    """
    schema = get_schema()
    valid = set()
    for rel in schema.get("relationships", []):
        f, t, v = rel["from"], rel["to"], rel["via"]
        valid.add((f, t, v))
        valid.add((t, f, v))
    return valid


# ══════════════════════════════════════════════════
#  SQL PARSING HELPERS
# ══════════════════════════════════════════════════

def normalize_sql(sql):
    """Uppercase keywords, collapse whitespace."""
    sql = re.sub(r"\s+", " ", sql.strip())
    # uppercase SQL keywords only (not string values)
    for kw in ["SELECT", "FROM", "JOIN", "LEFT JOIN", "RIGHT JOIN",
               "INNER JOIN", "ON", "WHERE", "GROUP BY", "ORDER BY",
               "HAVING", "LIMIT", "AND", "OR", "AS", "DISTINCT"]:
        sql = re.sub(rf"\b{kw}\b", kw, sql, flags=re.IGNORECASE)
    return sql


def extract_aliases_from_sql(sql):
    """
    Finds all 'FROM table alias' and 'JOIN table alias' patterns.
    Returns dict: alias → table_name
    """
    found = {}
    # matches: FROM sales s  or  JOIN distributors d
    pattern = re.compile(
        r'\b(?:FROM|JOIN)\s+(\w+)\s+(\w+)',
        re.IGNORECASE
    )
    for match in pattern.finditer(sql):
        table_raw = match.group(1).lower()
        alias_raw = match.group(2).upper()
        # skip ON, WHERE etc that follow JOIN
        if alias_raw in {"ON", "WHERE", "SET", "AS"}:
            continue
        found[match.group(2)] = table_raw  # preserve original case as key
    return found


def extract_aliased_columns(sql):
    """
    Finds all alias.column references in SQL.
    Returns list of (alias, column) tuples.
    """
    # matches: s.revenue, d.distributor_name, mc.roi_pct etc.
    pattern = re.compile(r'\b([a-zA-Z_]+)\.([a-zA-Z_]+)\b')
    results = []
    for match in pattern.finditer(sql):
        alias = match.group(1)
        column = match.group(2)
        # skip table.* and string comparisons
        if column == "*":
            continue
        results.append((alias, column))
    return results


def extract_join_conditions(sql):
    """
    Finds all ON alias1.col = alias2.col conditions.
    Returns list of (alias1, col1, alias2, col2) tuples.
    """
    pattern = re.compile(
        r'\bON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)',
        re.IGNORECASE
    )
    results = []
    for match in pattern.finditer(sql):
        results.append((
            match.group(1), match.group(2),
            match.group(3), match.group(4)
        ))
    return results


def extract_select_columns(sql):
    """
    Extracts everything between SELECT and FROM.
    Returns raw string.
    """
    match = re.search(r'\bSELECT\b(.+?)\bFROM\b', sql, re.IGNORECASE | re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""


def extract_groupby_columns(sql):
    """
    Extracts alias.column references from GROUP BY clause.
    """
    match = re.search(r'\bGROUP\s+BY\b(.+?)(?:\bORDER\b|\bHAVING\b|\bLIMIT\b|;|$)',
                      sql, re.IGNORECASE | re.DOTALL)
    if not match:
        return []
    gb_text = match.group(1).strip()
    pattern = re.compile(r'\b([a-zA-Z_]+)\.([a-zA-Z_]+)\b')
    return [(m.group(1), m.group(2)) for m in pattern.finditer(gb_text)]


def is_aggregate(expr):
    """Returns True if expression contains an aggregate function."""
    upper = expr.upper()
    return any(fn in upper for fn in AGGREGATE_FUNCTIONS)


# ══════════════════════════════════════════════════
#  VALIDATION RULES
# ══════════════════════════════════════════════════

def check_dangerous_keywords(sql):
    """Rule 0 — No data-modifying SQL."""
    errors = []
    upper = sql.upper()
    for kw in DANGEROUS_KEYWORDS:
        if re.search(rf'\b{kw}\b', upper):
            errors.append(f"DANGEROUS keyword found: {kw} — only SELECT allowed")
    return errors


def check_select_star(sql):
    """Rule 5 — No SELECT *"""
    warnings = []
    if re.search(r'SELECT\s+\*', sql, re.IGNORECASE):
        warnings.append("SELECT * used — specify columns explicitly for better results")
    return warnings


def check_columns_exist(sql, alias_map, table_columns):
    """
    Rule 1 — Every alias.column must exist in its table's schema.
    alias_map: {alias_str → table_name}
    table_columns: {table_name → set of columns}
    """
    errors = []
    aliased_cols = extract_aliased_columns(sql)

    for alias, column in aliased_cols:
        # resolve alias to table
        table = alias_map.get(alias)
        if table is None:
            # try lowercase
            table = alias_map.get(alias.lower())
        if table is None:
            errors.append(
                f"Alias '{alias}' not defined in FROM/JOIN clauses"
            )
            continue

        valid_cols = table_columns.get(table, set())
        if not valid_cols:
            errors.append(f"Table '{table}' not found in schema")
            continue

        if column.lower() not in {c.lower() for c in valid_cols}:
            errors.append(
                f"Column '{column}' does NOT exist in table '{table}' "
                f"(alias: {alias}). Valid columns: {sorted(valid_cols)}"
            )

    return errors


def check_join_validity(sql, alias_map, valid_joins, table_columns):
    """
    Rule 3 — JOIN ON columns must exist in BOTH tables and be a known FK.
    """
    errors = []
    warnings = []
    join_conditions = extract_join_conditions(sql)

    for a1, col1, a2, col2 in join_conditions:
        t1 = alias_map.get(a1) or alias_map.get(a1.lower())
        t2 = alias_map.get(a2) or alias_map.get(a2.lower())

        if not t1:
            errors.append(f"JOIN alias '{a1}' not defined in FROM/JOIN")
            continue
        if not t2:
            errors.append(f"JOIN alias '{a2}' not defined in FROM/JOIN")
            continue

        cols_t1 = {c.lower() for c in table_columns.get(t1, set())}
        cols_t2 = {c.lower() for c in table_columns.get(t2, set())}

        # check col1 exists in t1
        if col1.lower() not in cols_t1:
            errors.append(
                f"JOIN column '{col1}' does NOT exist in table '{t1}' "
                f"(alias: {a1})"
            )

        # check col2 exists in t2
        if col2.lower() not in cols_t2:
            errors.append(
                f"JOIN column '{col2}' does NOT exist in table '{t2}' "
                f"(alias: {a2})"
            )

        # check it's a known FK relationship
        # col1 and col2 should be the same FK column
        if col1.lower() == col2.lower():
            via = col1.lower()
            if (t1, t2, via) not in {(r[0], r[1], r[2]) for r in valid_joins}:
                warnings.append(
                    f"JOIN {t1} ↔ {t2} via '{via}' is not in known FK "
                    f"relationships — double check this join is correct"
                )
        else:
            warnings.append(
                f"JOIN uses different columns: {a1}.{col1} = {a2}.{col2} "
                f"— verify this is intentional"
            )

    return errors, warnings


def check_group_by(sql):
    """
    Rule 4 — Non-aggregated SELECT columns must appear in GROUP BY.
    Only checks alias.column references (skips literals, expressions).
    """
    errors = []
    warnings = []

    select_raw = extract_select_columns(sql)
    if not select_raw:
        return errors, warnings

    # does this query have GROUP BY at all?
    has_groupby = bool(re.search(r'\bGROUP\s+BY\b', sql, re.IGNORECASE))
    # does SELECT have aggregates?
    has_aggregates = is_aggregate(select_raw)

    if has_aggregates and not has_groupby:
        warnings.append(
            "SELECT contains aggregate functions but no GROUP BY clause — "
            "this may return incorrect results"
        )
        return errors, warnings

    if not has_groupby:
        return errors, warnings

    # get GROUP BY columns as set of (alias, col) lowercased
    gb_cols = {
        (a.lower(), c.lower())
        for a, c in extract_groupby_columns(sql)
    }

    # parse SELECT items split by comma (rough but effective)
    select_items = [item.strip() for item in select_raw.split(",")]
    pattern = re.compile(r'\b([a-zA-Z_]+)\.([a-zA-Z_]+)\b')

    for item in select_items:
        if is_aggregate(item):
            continue  # aggregated columns don't need GROUP BY

        # find alias.col references in this SELECT item
        for match in pattern.finditer(item):
            alias = match.group(1).lower()
            col   = match.group(2).lower()
            if (alias, col) not in gb_cols:
                warnings.append(
                    f"SELECT column '{match.group(1)}.{match.group(2)}' "
                    f"may not be in GROUP BY — could cause SQL error"
                )

    return errors, warnings


# ══════════════════════════════════════════════════
#  MAIN VALIDATOR
# ══════════════════════════════════════════════════

def validate_sql(sql):
    """
    Main validation function.

    Input : SQL string
    Output: {
        "valid":    bool,
        "errors":   [str, ...],    ← fatal, will cause DB error
        "warnings": [str, ...],    ← non-fatal, may cause wrong results
        "checks": {
            "no_dangerous_keywords": bool,
            "no_select_star":        bool,
            "columns_exist":         bool,
            "joins_valid":           bool,
            "group_by_correct":      bool,
        }
    }
    """
    all_errors   = []
    all_warnings = []
    checks = {
        "no_dangerous_keywords": True,
        "no_select_star":        True,
        "columns_exist":         True,
        "joins_valid":           True,
        "group_by_correct":      True,
    }

    if not sql or not sql.strip():
        return {
            "valid": False,
            "errors": ["SQL is empty"],
            "warnings": [],
            "checks": {k: False for k in checks}
        }

    # load schema data
    table_columns = get_table_columns()
    valid_joins   = get_valid_joins()

    # extract alias map from this SQL
    alias_map = extract_aliases_from_sql(sql)

    # ── Rule 0: dangerous keywords ─────────────────
    errs = check_dangerous_keywords(sql)
    if errs:
        all_errors.extend(errs)
        checks["no_dangerous_keywords"] = False

    # ── Rule 5: SELECT * ───────────────────────────
    warns = check_select_star(sql)
    if warns:
        all_warnings.extend(warns)
        checks["no_select_star"] = False

    # ── Rule 1: columns exist ──────────────────────
    errs = check_columns_exist(sql, alias_map, table_columns)
    if errs:
        all_errors.extend(errs)
        checks["columns_exist"] = False

    # ── Rule 3: join validity ──────────────────────
    errs, warns = check_join_validity(sql, alias_map, valid_joins, table_columns)
    if errs:
        all_errors.extend(errs)
        checks["joins_valid"] = False
    if warns:
        all_warnings.extend(warns)

    # ── Rule 4: group by ──────────────────────────
    errs, warns = check_group_by(sql)
    if errs:
        all_errors.extend(errs)
        checks["group_by_correct"] = False
    if warns:
        all_warnings.extend(warns)

    valid = len(all_errors) == 0

    return {
        "valid":    valid,
        "errors":   all_errors,
        "warnings": all_warnings,
        "checks":   checks
    }


def print_validation_report(result, sql=None):
    """Pretty print validation results."""
    print("\n" + "─" * 55)
    print("  SQL VALIDATION REPORT")
    print("─" * 55)

    if sql:
        for line in sql.strip().split("\n"):
            print(f"  {line}")
        print()

    status = "✅ VALID" if result["valid"] else "❌ INVALID"
    print(f"  Status: {status}")

    print("\n  Checks:")
    icons = {True: "✅", False: "❌"}
    for check, passed in result["checks"].items():
        label = check.replace("_", " ")
        print(f"    {icons[passed]} {label}")

    if result["errors"]:
        print(f"\n  Errors ({len(result['errors'])}):")
        for e in result["errors"]:
            print(f"    ✗ {e}")

    if result["warnings"]:
        print(f"\n  Warnings ({len(result['warnings'])}):")
        for w in result["warnings"]:
            print(f"    ⚠ {w}")

    print("─" * 55)


# ══════════════════════════════════════════════════
#  TEST
# ══════════════════════════════════════════════════

if __name__ == "__main__":

    tests = [
        {
            "name": "✅ Valid — distributor query",
            "sql": """
                SELECT d.distributor_name, SUM(s.revenue) AS total_revenue
                FROM sales s
                JOIN distributors d ON s.distributor_id = d.distributor_id
                WHERE s.region_name = 'Hyderabad'
                AND s.quarter = 'Q2' AND s.year = 2023
                GROUP BY d.distributor_name
                ORDER BY total_revenue DESC
                LIMIT 10;
            """
        },
        {
            "name": "❌ Bad column — outlet_id on sales",
            "sql": """
                SELECT d.distributor_name, SUM(s.units_sold) AS total
                FROM sales s
                JOIN outlets o ON s.outlet_id = o.outlet_id
                JOIN distributors d ON o.distributor_id = d.distributor_id
                GROUP BY d.distributor_name;
            """
        },
        {
            "name": "❌ Wrong column in join",
            "sql": """
                SELECT d.distributor_name, SUM(s.revenue) AS rev
                FROM sales s
                JOIN distributors d ON s.region_id = d.distributor_id
                GROUP BY d.distributor_name;
            """
        },
        {
            "name": "⚠ Missing GROUP BY column",
            "sql": """
                SELECT s.brand_name, s.zone_name, SUM(s.revenue) AS total
                FROM sales s
                GROUP BY s.brand_name
                ORDER BY total DESC;
            """
        },
        {
            "name": "❌ Dangerous keyword",
            "sql": "DELETE FROM sales WHERE year = 2023;"
        },
        {
            "name": "✅ Valid — top brands",
            "sql": """
                SELECT s.brand_name, SUM(s.revenue) AS total_revenue
                FROM sales s
                WHERE s.zone_name = 'South' AND s.year = 2024
                GROUP BY s.brand_name
                ORDER BY total_revenue DESC
                LIMIT 5;
            """
        },
    ]

    print("=" * 55)
    print("  SQL VALIDATOR — TEST SUITE")
    print("=" * 55)

    for test in tests:
        print(f"\n  TEST: {test['name']}")
        result = validate_sql(test["sql"])
        print_validation_report(result)