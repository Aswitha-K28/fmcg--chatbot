"""
SQL EXECUTOR
============
Executes validated SQL on MySQL fmcg_db.
Returns structured results with columns, rows, metadata.

Flow:
  validate_sql() → if valid → execute on MySQL → return results

Output:
  {
    "success":      True / False,
    "columns":      ["distributor_name", "total_revenue"],
    "rows":         [["Uppal-Krishnamurthy", 4520000.0], ...],
    "row_count":    15,
    "executed_sql": "SELECT ...",
    "error":        None or "error message"
  }
"""

import os
import time
import mysql.connector
from dotenv import load_dotenv, load_dotenvS
from sql_validator import validate_sql, print_validation_report

load_dotenv()

# ── MySQL config — update password ─────────────────
MYSQL_CONFIG = {
    "host":     os.getenv("MYSQL_HOST",     "localhost"),
    "user":     os.getenv("MYSQL_USER",     "root"),
    "password": os.getenv("MYSQL_PASSWORD", "Aswitha@12"),
    "database": os.getenv("MYSQL_DATABASE", "fmcg_database"),
    "port":     int(os.getenv("MYSQL_PORT", "3306")),
    "connect_timeout": 10,
}

MAX_ROWS = 1000  # safety cap — never return more than this


# ══════════════════════════════════════════════════
#  CONNECTION
# ══════════════════════════════════════════════════

def get_connection():
    """Opens a fresh MySQL connection."""
    return mysql.connector.connect(**MYSQL_CONFIG)


# ══════════════════════════════════════════════════
#  EXECUTE SQL
# ══════════════════════════════════════════════════

def execute_sql(sql, skip_validation=False):
    """
    Validates and executes SQL on MySQL.

    Input:
      sql              → SQL string to execute
      skip_validation  → set True to bypass validator (not recommended)

    Output:
      {
        "success":      bool,
        "columns":      [col_name, ...],
        "rows":         [[val, val, ...], ...],
        "row_count":    int,
        "executed_sql": str,
        "validation":   {...},   ← full validation report
        "time_ms":      float,   ← execution time in ms
        "error":        str or None
      }
    """

    result = {
        "success":      False,
        "columns":      [],
        "rows":         [],
        "row_count":    0,
        "executed_sql": sql.strip(),
        "validation":   None,
        "time_ms":      0.0,
        "error":        None
    }

    # ── Step 1: Validate ───────────────────────────
    if not skip_validation:
        validation = validate_sql(sql)
        result["validation"] = validation

        if not validation["valid"]:
            result["error"] = (
                "SQL failed validation. Errors:\n" +
                "\n".join(f"  - {e}" for e in validation["errors"])
            )
            return result
    else:
        result["validation"] = {"valid": True, "skipped": True}

    # ── Step 2: Execute ────────────────────────────
    conn   = None
    cursor = None

    try:
        conn   = get_connection()
        cursor = conn.cursor()

        start = time.perf_counter()
        cursor.execute(sql)
        rows = cursor.fetchmany(MAX_ROWS)
        elapsed = (time.perf_counter() - start) * 1000

        # column names from cursor description
        columns = [desc[0] for desc in cursor.description] if cursor.description else []

        # convert rows to plain Python lists (mysql returns tuples + Decimal types)
        clean_rows = []
        for row in rows:
            clean_row = []
            for val in row:
                if val is None:
                    clean_row.append(None)
                elif hasattr(val, "__float__"):     # Decimal → float
                    clean_row.append(float(val))
                elif hasattr(val, "isoformat"):     # date/datetime → string
                    clean_row.append(val.isoformat())
                else:
                    clean_row.append(val)
            clean_rows.append(clean_row)

        result.update({
            "success":   True,
            "columns":   columns,
            "rows":      clean_rows,
            "row_count": len(clean_rows),
            "time_ms":   round(elapsed, 2),
            "error":     None
        })

        # warn if we hit the row cap
        if len(clean_rows) == MAX_ROWS:
            result["warning"] = (
                f"Result capped at {MAX_ROWS} rows. "
                f"Add LIMIT to your query for full control."
            )

    except mysql.connector.Error as e:
        result["error"] = f"MySQL Error {e.errno}: {e.msg}"

    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"

    finally:
        if cursor: cursor.close()
        if conn:   conn.close()

    return result


# ══════════════════════════════════════════════════
#  PRETTY PRINT
# ══════════════════════════════════════════════════

def print_results(result, max_rows=20):
    """
    Prints execution results in a clean table format.
    """
    print("\n" + "═" * 60)
    print("  SQL EXECUTION RESULTS")
    print("═" * 60)

    # validation summary
    v = result.get("validation", {})
    if v and not v.get("skipped"):
        warns = v.get("warnings", [])
        if warns:
            print(f"\n  ⚠ Validation warnings:")
            for w in warns:
                print(f"    • {w}")

    # execution status
    if not result["success"]:
        print(f"\n  ❌ FAILED")
        print(f"  Error: {result['error']}")
        print("═" * 60)
        return

    print(f"\n  ✅ SUCCESS")
    print(f"  Rows returned : {result['row_count']}")
    print(f"  Time          : {result['time_ms']} ms")

    if result.get("warning"):
        print(f"  ⚠  {result['warning']}")

    # print table
    columns = result["columns"]
    rows    = result["rows"][:max_rows]

    if not columns:
        print("\n  (no columns returned)")
        print("═" * 60)
        return

    # compute column widths
    col_widths = [len(str(c)) for c in columns]
    for row in rows:
        for i, val in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(val) if val is not None else "NULL"))

    # header
    print()
    header = "  | " + " | ".join(
        str(c).ljust(col_widths[i]) for i, c in enumerate(columns)
    ) + " |"
    separator = "  +-" + "-+-".join("-" * w for w in col_widths) + "-+"

    print(separator)
    print(header)
    print(separator)

    # rows
    for row in rows:
        row_str = "  | " + " | ".join(
            str(val if val is not None else "NULL").ljust(col_widths[i])
            for i, val in enumerate(row)
        ) + " |"
        print(row_str)

    print(separator)

    if result["row_count"] > max_rows:
        print(f"\n  ... showing {max_rows} of {result['row_count']} rows")

    print("═" * 60)


# ══════════════════════════════════════════════════
#  CONVENIENCE WRAPPER
# ══════════════════════════════════════════════════

def run_query(sql, verbose=True):
    """
    One-call function: validate → execute → print → return.

    Usage:
        result = run_query("SELECT s.brand_name ... FROM sales s ...")
        rows    = result["rows"]
        columns = result["columns"]
    """
    result = execute_sql(sql)

    if verbose:
        print_results(result)

    return result


# ══════════════════════════════════════════════════
#  TEST
# ══════════════════════════════════════════════════

if __name__ == "__main__":

    print("=" * 60)
    print("  SQL EXECUTOR — TEST SUITE")
    print("=" * 60)

    tests = [
        {
            "name": "✅ Top brands by revenue",
            "sql": """
                SELECT s.brand_name, SUM(s.revenue) AS total_revenue
                FROM sales s
                WHERE s.year = 2024
                GROUP BY s.brand_name
                ORDER BY total_revenue DESC
                LIMIT 5;
            """
        },
        {
            "name": "✅ Distributor sales in Hyderabad",
            "sql": """
                SELECT d.distributor_name, SUM(s.units_sold) AS total_units
                FROM sales s
                JOIN distributors d ON s.distributor_id = d.distributor_id
                WHERE s.region_name = 'Hyderabad'
                GROUP BY d.distributor_name
                ORDER BY total_units DESC
                LIMIT 10;
            """
        },
        {
            "name": "❌ Invalid SQL — bad column",
            "sql": """
                SELECT s.outlet_id, SUM(s.revenue) AS total
                FROM sales s
                GROUP BY s.outlet_id;
            """
        },
    ]

    for test in tests:
        print(f"\n  {'─'*58}")
        print(f"  TEST: {test['name']}")
        run_query(test["sql"])