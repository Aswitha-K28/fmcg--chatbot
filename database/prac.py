"""
FMCG CHATBOT — KEYWORD SEARCH + GRAPH SEARCH
=============================================
Connects to YOUR actual MySQL + Neo4j
Run: python search_engine.py

Requirements:
    pip install mysql-connector-python neo4j rank_bm25
"""

import re
import math
from collections import defaultdict

# ══════════════════════════════════════════════════
#  CONFIG — change these to your credentials
# ══════════════════════════════════════════════════
MYSQL_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "Aswitha@12",   # ← change
    "database": "fmcg_database"
}

NEO4J_CONFIG = {
    "uri":      "neo4j+s://91202a22.databases.neo4j.io",  # ← change
    "user":     "neo4j",
    "password": "8OzBvCYXTU9jlKQDjiszyn-ATOGd6RTOvoBRjKuJW8w"                      # ← change
}

# ══════════════════════════════════════════════════
#  REFERENCE DATA — for classifier
# ══════════════════════════════════════════════════
BRANDS    = [
    "HUL","ITC","Nestle","P&G","Dabur","Marico","Godrej","Britannia",
    "Surf Excel","Dove","Lipton","Knorr","Lux","Pepsodent",
    "Maggi","KitKat","Munch","Nescafe","Ariel","Pantene","Olay",
    "Real","Hajmola","Chyawanprash","Parachute","Saffola",
    "Cinthol","Good Knight","Good Day","Marie Gold","Bourbon",
    "Aashirvaad","Sunfeast","Bingo","Yippee","Engage",
    "Whisper","Gillette","Head & Shoulders","Vatika","Odomos",
    "Set Wet","Nihar","Livon","Hit","Godrej No.1","Ezee",
    "NutriChoice","Milk Bikis","50-50","Fiama","Honitus",
    "Milkmaid","Bar-One","Dettol","Colgate","Harpic"
]
COMPANIES = ["HUL","ITC","Nestle","P&G","Dabur","Marico","Godrej","Britannia"]
ZONES     = ["North","South","East","West"]
REGIONS   = [
    "Delhi","Mumbai","Pune","Ahmedabad","Bangalore","Chennai",
    "Kolkata","Hyderabad","Surat","Nagpur","Nashik","Vizag",
    "Patna","Guwahati","Ranchi","Bhubaneswar","Siliguri",
    "Coimbatore","Kerala","Punjab","Haryana","Rajasthan","Himachal Pradesh",
    "Uttar Pradesh"
]
CATEGORIES = [
    "Detergent","Personal Care","Food","Beverages","Snacks",
    "Health","Home Care","Shampoo","Soap","Noodles","Biscuits",
    "Fabric Care","Mosquito Repellent","Hair Care","Oral Care"
]
OUTLETS   = ["Kirana","Supermarket","Hypermarket","Pharmacy","Convenience","Online"]
CHANNELS  = ["TV","Digital","OOH","Print","Radio","In-Store"]
FESTIVALS = ["Diwali","Holi","Eid","Christmas","Navratri","Durga Puja"]
SEASONS   = ["Summer","Monsoon","Winter","Spring"]

INTENT_CONFIG = {
    "sales_ranking":         {"tables":["sales","products","brands"],           "metrics":["revenue","units_sold"]},
    "sales_performance":     {"tables":["sales","products","brands","regions"], "metrics":["revenue","units_sold","discount_pct"]},
    "market_share":          {"tables":["competitors","sales"],                 "metrics":["estimated_market_share_pct","estimated_revenue_cr"]},
    "inventory_status":      {"tables":["inventory","products"],                "metrics":["stock_quantity","days_of_stock","is_stockout"]},
    "pricing_analysis":      {"tables":["pricing_promotions","products"],       "metrics":["base_price","promo_price","discount_pct"]},
    "campaign_performance":  {"tables":["marketing_campaigns","brands"],        "metrics":["roi_pct","impressions","spend_cr","sales_lift_pct"]},
    "target_vs_achievement": {"tables":["sales_targets","brands","regions"],    "metrics":["target_revenue","achieved_revenue","achievement_pct"]},
    "supply_chain_analysis": {"tables":["supply_chain","products"],             "metrics":["lead_time_days","transit_days","on_time_delivery_pct"]},
    "consumer_sentiment":    {"tables":["consumer_feedback","products"],        "metrics":["rating","sentiment"]},
    "trend_analysis":        {"tables":["sales","sales_targets"],               "metrics":["revenue","units_sold"]},
    "general_query":         {"tables":["products","brands","sales"],           "metrics":["revenue","units_sold"]},
}

INTENT_KEYWORDS = {
    "sales_ranking":         ["top","best","highest","most sold","best selling","leading","rank","which product sold most"],
    "sales_performance":     ["sales","revenue","performance","how much","sold","total sales","how many units","total revenue"],
    "market_share":          ["market share","share","vs","versus","compared to","competitor","competing"],
    "inventory_status":      ["stock","inventory","stockout","out of stock","reorder","available","days of stock"],
    "pricing_analysis":      ["price","pricing","discount","promo","promotion","offer","mrp","deal"],
    "campaign_performance":  ["campaign","marketing","roi","reach","impressions","digital","tv","awareness","sales lift"],
    "target_vs_achievement": ["target","achievement","missed","hit target","achieved","goal"],
    "supply_chain_analysis": ["supply chain","lead time","delivery","supplier","warehouse","transit","defect"],
    "consumer_sentiment":    ["feedback","review","rating","sentiment","customer","saying","opinion","complaint"],
    "trend_analysis":        ["trend","over time","quarter by quarter","qoq","yoy","year on year","growth","decline","pattern"],
}


# ══════════════════════════════════════════════════
#  STEP 1 — CLASSIFIER
# ══════════════════════════════════════════════════
def classify(query: str) -> dict:
    q = query.lower()

    brands    = sorted([b for b in BRANDS    if b.lower() in q], key=len, reverse=True)
    companies = [c for c in COMPANIES if c.lower() in q]
    zones     = [z for z in ZONES     if z.lower() in q]
    regions   = [r for r in REGIONS   if r.lower() in q]
    quarters  = [x.upper() for x in re.findall(r'\bq[1-4]\b', q, re.I)]
    years     = [int(y) for y in re.findall(r'\b(202\d)\b', q)]
    channels  = [c for c in CHANNELS  if c.lower() in q]
    festivals = [f for f in FESTIVALS if f.lower() in q]
    categories= [c for c in CATEGORIES if c.lower() in q]
    outlets   = [o for o in OUTLETS   if o.lower() in q]

    # intent
    best_intent, best_score = "general_query", 0
    for intent, keywords in INTENT_KEYWORDS.items():
        score = sum(len(kw.split()) for kw in keywords if kw in q)
        if score > best_score:
            best_score, best_intent = score, intent

    # sql mode
    is_comparison = (len(brands) >= 2 or "vs" in q or "versus" in q or "compare" in q)
    is_trend      = any(w in q for w in ["trend","over time","qoq","yoy","year on year","quarter by quarter"])
    is_aggregation= any(w in q for w in ["total","sum","average","avg","top","best","highest","how many","how much","count"])

    if is_comparison:   sql_mode = "comparison"
    elif is_trend:      sql_mode = "trend"
    elif is_aggregation:sql_mode = "aggregation"
    else:               sql_mode = "lookup"

    return {
        "query":    query,
        "intent":   best_intent,
        "sql_mode": sql_mode,
        "entities": {
            "brands":     brands,
            "companies":  companies,
            "zones":      zones,
            "regions":    regions,
            "quarters":   quarters,
            "years":      years,
            "channels":   channels,
            "festivals":  festivals,
            "categories": categories,
            "outlets":    outlets,
        },
        "metrics": INTENT_CONFIG.get(best_intent, INTENT_CONFIG["general_query"])["metrics"],
        "tables":  INTENT_CONFIG.get(best_intent, INTENT_CONFIG["general_query"])["tables"],
    }


# ══════════════════════════════════════════════════
#  STEP 2 — KEYWORD SEARCH (BM25)
# ══════════════════════════════════════════════════

# Global index — built once at startup
_bm25_index   = None
_bm25_docs    = []
_bm25_corpus  = []

def build_keyword_index(conn):
    """
    Pulls rows from MySQL, flattens to text docs,
    builds BM25 index in memory.
    Called ONCE at startup.
    """
    global _bm25_index, _bm25_docs, _bm25_corpus

    print("🔄  Building keyword index from MySQL...")
    cursor = conn.cursor(dictionary=True)
    docs   = []

    # ── sales rows ───────────────────────────────
    cursor.execute("""
        SELECT s.sale_id, s.sku_name, s.brand_name,
               s.zone_name, s.region_name, s.channel,
               s.quarter, s.year, c.category_name
        FROM   sales s
        JOIN   products   p ON s.product_id  = p.product_id
        JOIN   brands     b ON p.brand_id    = b.brand_id
        JOIN   categories c ON p.category_id = c.category_id
        LIMIT  5000
    """)
    for row in cursor.fetchall():
        text = " ".join(filter(None,[
            str(row.get("sku_name","")),
            str(row.get("brand_name","")),
            str(row.get("zone_name","")),
            str(row.get("region_name","")),
            str(row.get("channel","")),
            str(row.get("category_name","")),
            str(row.get("quarter","")),
            str(row.get("year",""))
        ]))
        docs.append({
            "id":     row["sale_id"],
            "source": "sales",
            "text":   text,
            "data":   dict(row)
        })

    # ── products rows ─────────────────────────────
    cursor.execute("""
        SELECT p.product_id, p.sku_name, p.mrp,
               p.weight_variant, b.brand_name,
               b.company_name,  c.category_name,
               sc.category_name AS sub_category
        FROM   products p
        JOIN   brands     b  ON p.brand_id    = b.brand_id
        JOIN   categories c  ON p.category_id = c.category_id
        LEFT JOIN subcategories sc ON p.subcategory_id = sc.subcategory_id
    """)
    for row in cursor.fetchall():
        text = " ".join(filter(None,[
            str(row.get("sku_name","")),
            str(row.get("brand_name","")),
            str(row.get("company_name","")),
            str(row.get("category_name","")),
            str(row.get("sub_category","")),
            str(row.get("weight_variant",""))
        ]))
        docs.append({
            "id":     row["product_id"],
            "source": "products",
            "text":   text,
            "data":   dict(row)
        })

    # ── marketing campaigns ───────────────────────
    cursor.execute("""
        SELECT campaign_id, campaign_name, brand_name,
               channel, zone_name, region_name,
               festival_tie_in, quarter, year
        FROM   marketing_campaigns
    """)
    for row in cursor.fetchall():
        text = " ".join(filter(None,[
            str(row.get("campaign_name","")),
            str(row.get("brand_name","")),
            str(row.get("channel","")),
            str(row.get("zone_name","")),
            str(row.get("region_name","")),
            str(row.get("festival_tie_in","")),
            str(row.get("quarter","")),
            str(row.get("year",""))
        ]))
        docs.append({
            "id":     row["campaign_id"],
            "source": "campaigns",
            "text":   text,
            "data":   dict(row)
        })

    # ── competitors ──────────────────────────────
    cursor.execute("""
        SELECT competitor_id, competitor_company,
               competitor_brand, zone_name,
               category, quarter, year
        FROM   competitors
    """)
    for row in cursor.fetchall():
        text = " ".join(filter(None,[
            str(row.get("competitor_company","")),
            str(row.get("competitor_brand","")),
            str(row.get("zone_name","")),
            str(row.get("category","")),
            str(row.get("quarter","")),
            str(row.get("year",""))
        ]))
        docs.append({
            "id":     row["competitor_id"],
            "source": "competitors",
            "text":   text,
            "data":   dict(row)
        })

    cursor.close()

    # ── Build BM25 index ─────────────────────────
    def tokenize(t):
        return re.sub(r"[^a-z0-9\s]", "", t.lower()).split()

    corpus = [tokenize(d["text"]) for d in docs]

    try:
        from rank_bm25 import BM25Okapi
        index = BM25Okapi(corpus)
        print(f"   ✅  BM25 index built — {len(docs)} documents")
    except ImportError:
        index = None
        print("   ⚠️  rank_bm25 not installed — using fallback TF scoring")

    _bm25_index  = index
    _bm25_docs   = docs
    _bm25_corpus = corpus

    return docs


def keyword_search(query: str, top_k: int = 5,
                   filter_brands: list = None,
                   filter_zones:  list = None) -> list:
    """
    BM25 keyword search on pre-built index.
    Optionally filter by brands/zones before scoring.
    """
    global _bm25_index, _bm25_docs, _bm25_corpus

    if not _bm25_docs:
        return []

    def tokenize(t):
        return re.sub(r"[^a-z0-9\s]", "", t.lower()).split()

    # ── KG filter — only search relevant docs ────
    docs_to_search = _bm25_docs
    corpus_to_search = _bm25_corpus

    if filter_brands or filter_zones:
        filtered = []
        filtered_corpus = []
        fb = [b.lower() for b in (filter_brands or [])]
        fz = [z.lower() for z in (filter_zones  or [])]
        for doc, corp in zip(_bm25_docs, _bm25_corpus):
            text = doc["text"].lower()
            brand_ok = any(b in text for b in fb) if fb else True
            zone_ok  = any(z in text for z in fz) if fz else True
            if brand_ok or zone_ok:
                filtered.append(doc)
                filtered_corpus.append(corp)
        if filtered:
            docs_to_search   = filtered
            corpus_to_search = filtered_corpus

    # ── BM25 score ────────────────────────────────
    qtoks = tokenize(query)

    if _bm25_index and docs_to_search is _bm25_docs:
        # use pre-built index directly
        scores = _bm25_index.get_scores(qtoks)
    else:
        # rebuild mini index on filtered subset
        try:
            from rank_bm25 import BM25Okapi
            mini = BM25Okapi(corpus_to_search) if corpus_to_search else None
            scores = mini.get_scores(qtoks) if mini else [0]*len(docs_to_search)
        except ImportError:
            # simple TF fallback
            scores = []
            for corp in corpus_to_search:
                s = sum(1 for w in corp if w in qtoks)
                scores.append(s)

    ranked = sorted(enumerate(scores), key=lambda x: x[1], reverse=True)

    results = []
    for rank, (idx, score) in enumerate(ranked[:top_k]):
        if score > 0:
            results.append({
                "rank":   rank + 1,
                "score":  round(float(score), 4),
                "doc_id": docs_to_search[idx]["id"],
                "source": docs_to_search[idx]["source"],
                "text":   docs_to_search[idx]["text"],
                "data":   docs_to_search[idx]["data"],
            })

    return results


# ══════════════════════════════════════════════════
#  STEP 3 — GRAPH SEARCH (Neo4j)
# ══════════════════════════════════════════════════
def graph_search(clf: dict) -> dict:
    """
    Uses Neo4j to:
    1. Resolve entity names → actual product_ids + region_ids
    2. Return relationship paths as context
    These IDs are used to filter SQL query.
    """
    entities = clf["entities"]
    brands   = entities["brands"] + entities["companies"]
    zones    = entities["zones"]
    regions  = entities["regions"]
    intent   = clf["intent"]

    result = {
        "product_ids": [],
        "region_ids":  [],
        "brand_ids":   [],
        "paths":       [],
        "raw":         []
    }

    if not brands and not zones and not regions:
        return result   # no entities to traverse

    try:
        from neo4j import GraphDatabase
        driver = GraphDatabase.driver(
            NEO4J_CONFIG["uri"],
            auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
        )
    except Exception as e:
        print(f"   ⚠️  Neo4j connection failed: {e}")
        return result

    try:
        with driver.session() as s:

            # ── Query 1: Brand+Zone → product_ids + region_ids ──
            if brands and zones:
                rows = s.run("""
                    MATCH (c:Company)-[:OWNS]->(b:Brand)
                    WHERE c.name IN $brands OR b.name IN $brands
                    MATCH (b)-[:HAS_SKU]->(p:Product)
                    MATCH (p)-[:SOLD_IN]->(r:Region)
                    MATCH (r)-[:PART_OF]->(z:Zone)
                    WHERE z.name IN $zones
                    RETURN DISTINCT
                        p.product_id AS product_id,
                        p.name       AS product_name,
                        r.region_id  AS region_id,
                        r.name       AS region_name,
                        b.name       AS brand_name,
                        z.name       AS zone_name
                    LIMIT 200
                """, brands=brands, zones=zones).data()

                for row in rows:
                    if row.get("product_id"):
                        result["product_ids"].append(row["product_id"])
                    if row.get("region_id"):
                        result["region_ids"].append(row["region_id"])
                    result["paths"].append(
                        f"{row.get('brand_name')} → {row.get('product_name')} "
                        f"→ SOLD_IN → {row.get('region_name')} "
                        f"→ PART_OF → {row.get('zone_name')}"
                    )
                    result["raw"].append(row)

            # ── Query 2: Brand only → product_ids ───────────────
            elif brands:
                rows = s.run("""
                    MATCH (c:Company)-[:OWNS]->(b:Brand)
                    WHERE c.name IN $brands OR b.name IN $brands
                    MATCH (b)-[:HAS_SKU]->(p:Product)
                    RETURN DISTINCT
                        p.product_id AS product_id,
                        p.name       AS product_name,
                        b.name       AS brand_name,
                        b.brand_id   AS brand_id
                    LIMIT 200
                """, brands=brands).data()

                for row in rows:
                    if row.get("product_id"):
                        result["product_ids"].append(row["product_id"])
                    if row.get("brand_id"):
                        result["brand_ids"].append(row["brand_id"])
                    result["paths"].append(
                        f"{row.get('brand_name')} → HAS_SKU → {row.get('product_name')}"
                    )
                    result["raw"].append(row)

            # ── Query 3: Zone only → region_ids ─────────────────
            elif zones:
                rows = s.run("""
                    MATCH (r:Region)-[:PART_OF]->(z:Zone)
                    WHERE z.name IN $zones
                    RETURN DISTINCT
                        r.region_id AS region_id,
                        r.name      AS region_name,
                        z.name      AS zone_name
                """, zones=zones).data()

                for row in rows:
                    if row.get("region_id"):
                        result["region_ids"].append(row["region_id"])
                    result["paths"].append(
                        f"{row.get('region_name')} → PART_OF → {row.get('zone_name')}"
                    )
                    result["raw"].append(row)

            # ── Query 4: Campaign intent → campaign nodes ────────
            if intent == "campaign_performance" and brands:
                rows = s.run("""
                    MATCH (cam:Campaign)-[:RUN_BY]->(b:Brand)
                    WHERE b.name IN $brands
                    RETURN cam.campaign_id AS campaign_id,
                           cam.name        AS campaign_name,
                           cam.channel     AS channel,
                           cam.roi_pct     AS roi_pct,
                           b.name          AS brand_name
                    LIMIT 20
                """, brands=brands).data()
                for row in rows:
                    result["raw"].append(row)
                    result["paths"].append(
                        f"Campaign:{row.get('campaign_name')} "
                        f"→ RUN_BY → {row.get('brand_name')} "
                        f"(ROI:{row.get('roi_pct')}%)"
                    )

            # ── Query 5: Sentiment intent → feedback ─────────────
            if intent == "consumer_sentiment" and brands:
                rows = s.run("""
                    MATCH (p:Product)-[rel:HAS_FEEDBACK]->(r:Region)
                    WHERE p.brand_name IN $brands
                    RETURN p.product_id  AS product_id,
                           p.name        AS product_name,
                           r.name        AS region_name,
                           rel.avg_rating     AS avg_rating,
                           rel.positive_count AS positive,
                           rel.negative_count AS negative
                    ORDER BY rel.avg_rating DESC
                    LIMIT 10
                """, brands=brands).data()
                for row in rows:
                    result["raw"].append(row)
                    result["paths"].append(
                        f"{row.get('product_name')} → HAS_FEEDBACK → "
                        f"{row.get('region_name')} "
                        f"(rating:{row.get('avg_rating')} "
                        f"pos:{row.get('positive')} neg:{row.get('negative')})"
                    )

        # deduplicate IDs
        result["product_ids"] = list(set(result["product_ids"]))
        result["region_ids"]  = list(set(result["region_ids"]))
        result["brand_ids"]   = list(set(result["brand_ids"]))

    except Exception as e:
        print(f"   ⚠️  Neo4j query error: {e}")
    finally:
        driver.close()

    return result


# ══════════════════════════════════════════════════
#  STEP 4 — SQL BUILDER
# ══════════════════════════════════════════════════
def build_and_run_sql(clf: dict, kg: dict, conn) -> dict:
    """
    Builds SQL using:
      - clf entities   (brand, zone, quarter, year)
      - kg product_ids / region_ids (from graph search)
      - clf intent     (which table + metrics)
      - clf sql_mode   (lookup / aggregation / comparison / trend)
    """
    intent   = clf["intent"]
    mode     = clf["sql_mode"]
    entities = clf["entities"]
    brands   = entities["brands"] + entities["companies"]
    zones    = entities["zones"]
    regions  = entities["regions"]
    quarters = entities["quarters"]
    years    = entities["years"]
    channels = entities["channels"]
    festivals= entities["festivals"]

    p_ids = kg["product_ids"]
    r_ids = kg["region_ids"]
    b_ids = kg["brand_ids"]

    def w(col, vals):
        if not vals: return ""
        if len(vals) == 1:
            return f"{col} = '{vals[0]}'"
        return f"{col} IN ({','.join(repr(v) for v in vals)})"

    def wnum(col, vals):
        if not vals: return ""
        if len(vals) == 1:
            return f"{col} = {vals[0]}"
        return f"{col} IN ({','.join(str(v) for v in vals)})"

    def build_where(clauses):
        parts = [c for c in clauses if c]
        return ("WHERE " + "\n      AND ".join(parts)) if parts else ""

    sql = None

    # ── Decide filters: prefer KG IDs when available ──
    if p_ids:
        product_filter = w("s.product_id", p_ids[:50])
    else:
        # fallback to brand name filter
        product_filter = w("b.company_name", brands) if brands else ""

    if r_ids:
        region_filter = w("s.region_id", r_ids[:50])
    else:
        region_filter = w("s.zone_name", zones)

    # ── SALES RANKING / PERFORMANCE ──────────────────
    if intent in ("sales_ranking", "sales_performance"):
        wh = build_where([
            product_filter,
            region_filter,
            w("s.quarter", quarters),
            wnum("s.year", years),
        ])

        if mode in ("aggregation", "trend", "comparison"):
            if mode == "trend":
                sql = f"""
SELECT s.brand_name, s.sku_name,
       s.year, s.quarter,
       SUM(s.units_sold)             AS total_units,
       ROUND(SUM(s.revenue), 2)      AS total_revenue,
       ROUND(AVG(s.discount_pct), 1) AS avg_discount
FROM   sales s
JOIN   products   p ON s.product_id  = p.product_id
JOIN   brands     b ON p.brand_id    = b.brand_id
JOIN   categories c ON p.category_id = c.category_id
{wh}
GROUP BY s.brand_name, s.sku_name, s.year, s.quarter
ORDER BY s.brand_name, s.year,
         FIELD(s.quarter,'Q1','Q2','Q3','Q4')
LIMIT 20"""

            elif mode == "comparison":
                sqls = []
                for brand in brands[:2]:
                    bf = w("b.company_name",[brand]) or w("s.brand_name",[brand])
                    w2 = build_where([bf, region_filter,
                                      w("s.quarter",quarters), wnum("s.year",years)])
                    sqls.append(f"""
SELECT '{brand}' AS entity, s.zone_name,
       s.quarter, s.year,
       SUM(s.units_sold)             AS total_units,
       ROUND(SUM(s.revenue), 2)      AS total_revenue,
       ROUND(AVG(s.discount_pct), 1) AS avg_discount
FROM   sales s
JOIN   products p ON s.product_id = p.product_id
JOIN   brands   b ON p.brand_id   = b.brand_id
{w2}
GROUP BY s.zone_name, s.quarter, s.year""")
                sql = "\nUNION ALL\n".join(sqls) + \
                      "\nORDER BY entity, total_revenue DESC LIMIT 20"

            else:  # aggregation
                sql = f"""
SELECT s.sku_name, s.brand_name,
       b.company_name, s.zone_name, c.category_name,
       SUM(s.units_sold)             AS total_units,
       ROUND(SUM(s.revenue), 2)      AS total_revenue,
       ROUND(AVG(s.discount_pct), 1) AS avg_discount,
       COUNT(*)                      AS num_transactions
FROM   sales s
JOIN   products   p ON s.product_id  = p.product_id
JOIN   brands     b ON p.brand_id    = b.brand_id
JOIN   categories c ON p.category_id = c.category_id
{wh}
GROUP BY s.sku_name, s.brand_name, b.company_name,
         s.zone_name, c.category_name
ORDER BY total_revenue DESC
LIMIT 10"""
        else:
            # simple lookup
            sql = f"""
SELECT s.sku_name, s.brand_name, s.zone_name,
       s.region_name, s.channel,
       s.units_sold, s.revenue,
       s.quarter, s.year
FROM   sales s
JOIN   products p ON s.product_id = p.product_id
JOIN   brands   b ON p.brand_id   = b.brand_id
{wh}
ORDER BY s.revenue DESC
LIMIT 10"""

    # ── INVENTORY ────────────────────────────────────
    elif intent == "inventory_status":
        pf = w("p.product_id", p_ids[:50]) if p_ids else w("b.brand_name", brands)
        wh = build_where([pf])
        sql = f"""
SELECT p.sku_name, b.brand_name, i.warehouse,
       i.stock_quantity, i.reorder_level,
       i.days_of_stock, i.is_stockout
FROM   inventory i
JOIN   products p ON i.product_id = p.product_id
JOIN   brands   b ON p.brand_id   = b.brand_id
{wh}
ORDER BY i.days_of_stock ASC
LIMIT 15"""

    # ── PRICING ──────────────────────────────────────
    elif intent == "pricing_analysis":
        pf = w("pp.product_id", p_ids[:50]) if p_ids else w("pp.brand_name", brands)
        rf = w("pp.region_id",  r_ids[:50]) if r_ids else w("pp.region_name", regions or zones)
        wh = build_where([pf, rf, w("pp.quarter", quarters), wnum("pp.year", years)])
        sql = f"""
SELECT p.sku_name, pp.brand_name, pp.region_name,
       pp.base_price, pp.promo_price,
       pp.discount_pct, pp.promo_type,
       pp.promo_start, pp.promo_end
FROM   pricing_promotions pp
JOIN   products p ON pp.product_id = p.product_id
{wh}
ORDER BY pp.discount_pct DESC
LIMIT 10"""

    # ── CAMPAIGN ─────────────────────────────────────
    elif intent == "campaign_performance":
        wh = build_where([
            w("mc.brand_name", brands),
            w("mc.zone_name",  zones),
            w("mc.channel",    channels),
            w("mc.festival_tie_in", festivals),
            w("mc.quarter",    quarters),
            wnum("mc.year",    years),
        ])
        sql = f"""
SELECT mc.brand_name, mc.campaign_name,
       mc.channel, mc.zone_name,
       mc.quarter, mc.year,
       ROUND(SUM(mc.budget_cr), 2)      AS total_budget,
       ROUND(SUM(mc.spend_cr),  2)      AS total_spend,
       ROUND(AVG(mc.roi_pct),   1)      AS avg_roi,
       SUM(mc.impressions)              AS total_impressions,
       SUM(mc.conversions)              AS total_conversions,
       ROUND(AVG(mc.sales_lift_pct), 1) AS avg_sales_lift
FROM   marketing_campaigns mc
{wh}
GROUP BY mc.brand_name, mc.campaign_name,
         mc.channel, mc.zone_name, mc.quarter, mc.year
ORDER BY avg_roi DESC
LIMIT 10"""

    # ── TARGET VS ACHIEVEMENT ────────────────────────
    elif intent == "target_vs_achievement":
        bf = w("st.brand_id", b_ids[:50]) if b_ids else w("st.brand_name", brands)
        rf = w("st.region_id", r_ids[:50]) if r_ids else w("st.region_name", regions or zones)
        wh = build_where([bf, rf, w("st.quarter", quarters), wnum("st.year", years)])
        sql = f"""
SELECT st.brand_name, st.region_name,
       st.quarter, st.year,
       ROUND(SUM(st.target_revenue),   2) AS total_target,
       ROUND(SUM(st.achieved_revenue), 2) AS total_achieved,
       ROUND(AVG(st.achievement_pct),  1) AS avg_achievement_pct
FROM   sales_targets st
{wh}
GROUP BY st.brand_name, st.region_name, st.quarter, st.year
ORDER BY avg_achievement_pct ASC
LIMIT 10"""

    # ── SUPPLY CHAIN ─────────────────────────────────
    elif intent == "supply_chain_analysis":
        pf = w("sc.product_id", p_ids[:50]) if p_ids else w("b.brand_name", brands)
        wh = build_where([pf])
        sql = f"""
SELECT p.sku_name, b.brand_name,
       sc.supplier_name, sc.supplier_type, sc.warehouse,
       sc.lead_time_days, sc.transit_days,
       sc.on_time_delivery_pct, sc.defect_rate_pct
FROM   supply_chain sc
JOIN   products p ON sc.product_id = p.product_id
JOIN   brands   b ON p.brand_id    = b.brand_id
{wh}
LIMIT 10"""

    # ── CONSUMER SENTIMENT ───────────────────────────
    elif intent == "consumer_sentiment":
        pf = w("cf.product_id", p_ids[:50]) if p_ids else w("cf.brand_name", brands)
        rf = w("cf.region_id",  r_ids[:20]) if r_ids else w("cf.region_name", regions)
        wh = build_where([pf, rf])
        sql = f"""
SELECT p.sku_name, cf.brand_name, cf.region_name,
       ROUND(AVG(cf.rating), 2)          AS avg_rating,
       SUM(cf.sentiment = 'Positive')    AS positive,
       SUM(cf.sentiment = 'Negative')    AS negative,
       SUM(cf.sentiment = 'Neutral')     AS neutral,
       COUNT(*)                          AS total_reviews
FROM   consumer_feedback cf
JOIN   products p ON cf.product_id = p.product_id
{wh}
GROUP BY p.sku_name, cf.brand_name, cf.region_name
ORDER BY avg_rating DESC
LIMIT 10"""

    # ── MARKET SHARE ─────────────────────────────────
    elif intent == "market_share":
        wh = build_where([
            w("c.zone_name", zones),
            w("c.quarter",   quarters),
            wnum("c.year",   years)
        ])
        sql = f"""
SELECT c.competitor_company, c.competitor_brand,
       c.zone_name, c.category,
       c.quarter, c.year,
       ROUND(AVG(c.estimated_market_share_pct), 2) AS avg_market_share,
       ROUND(SUM(c.estimated_revenue_cr),       2) AS total_revenue_cr
FROM   competitors c
{wh}
GROUP BY c.competitor_company, c.competitor_brand,
         c.zone_name, c.category, c.quarter, c.year
ORDER BY avg_market_share DESC
LIMIT 10"""

    # ── FALLBACK ─────────────────────────────────────
    else:
        pf = w("s.product_id", p_ids[:50]) if p_ids else w("b.company_name", brands)
        rf = w("s.region_id",  r_ids[:50]) if r_ids else w("s.zone_name", zones)
        wh = build_where([pf, rf, w("s.quarter", quarters), wnum("s.year", years)])
        sql = f"""
SELECT s.sku_name, s.brand_name, s.zone_name,
       SUM(s.units_sold)        AS total_units,
       ROUND(SUM(s.revenue), 2) AS total_revenue
FROM   sales s
JOIN   products p ON s.product_id = p.product_id
JOIN   brands   b ON p.brand_id   = b.brand_id
{wh}
GROUP BY s.sku_name, s.brand_name, s.zone_name
ORDER BY total_revenue DESC
LIMIT 10"""

    # ── Run SQL ───────────────────────────────────────
    rows = []
    if sql:
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(sql)
            rows = cursor.fetchall()
            # convert Decimal to float
            clean = []
            for row in rows:
                clean.append({
                    k: float(v) if hasattr(v, "__round__") and not isinstance(v, (int, str)) else v
                    for k, v in row.items()
                })
            rows = clean
            cursor.close()
        except Exception as e:
            print(f"   ⚠️  SQL error: {e}")
            print(f"   SQL was: {sql[:200]}")

    return {"sql": sql, "rows": rows}


# ══════════════════════════════════════════════════
#  STEP 5 — RRF FUSION
# ══════════════════════════════════════════════════
def rrf_fusion(kw_results: list, kg_paths: list, k: int = 60) -> list:
    """
    Fuse keyword results + KG paths into one ranked list.
    KG paths are wrapped as documents before fusion.
    """
    # wrap KG paths as doc-like objects
    kg_docs = []
    for i, path in enumerate(kg_paths[:10]):
        kg_docs.append({
            "rank":   i + 1,
            "score":  1.0,
            "doc_id": f"kg_{i}",
            "source": "knowledge_graph",
            "text":   path,
            "data":   {}
        })

    scores    = defaultdict(float)
    doc_store = {}

    for label, result_list in [("kw", kw_results), ("kg", kg_docs)]:
        for item in result_list:
            did = item["doc_id"]
            scores[did] += 1.0 / (k + item["rank"])
            if did not in doc_store:
                doc_store[did] = item

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    results = []
    for rank, (did, score) in enumerate(ranked[:10]):
        item = doc_store[did].copy()
        item["rrf_rank"]  = rank + 1
        item["rrf_score"] = round(score, 6)
        results.append(item)

    return results


# ══════════════════════════════════════════════════
#  STEP 6 — CONTEXT BUILDER
# ══════════════════════════════════════════════════
def build_context(clf: dict, sql_out: dict, rrf_results: list) -> str:
    lines = [
        f"QUERY   : {clf['query']}",
        f"INTENT  : {clf['intent']}",
        f"MODE    : {clf['sql_mode']}",
        f"ENTITIES: brands={clf['entities']['brands']} "
        f"zones={clf['entities']['zones']} "
        f"quarters={clf['entities']['quarters']} "
        f"years={clf['entities']['years']}",
        "",
        "── SQL RESULTS (actual numbers from MySQL) ──────────"
    ]

    for i, row in enumerate(sql_out["rows"][:10], 1):
        lines.append(f"  {i}. " + " | ".join(
            f"{k}={v}" for k, v in list(row.items())[:7]
        ))

    if rrf_results:
        lines.append("")
        lines.append("── SEARCH CONTEXT (keyword + graph) ─────────────────")
        for r in rrf_results[:5]:
            lines.append(
                f"  [#{r['rrf_rank']} {r['source']}] {r['text'][:70]}"
            )

    return "\n".join(lines)


# ══════════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════════
def run(query: str, conn, verbose: bool = True) -> dict:
    sep = "═" * 62

    if verbose:
        print(f"\n{sep}")
        print(f"  QUERY: \"{query}\"")
        print(sep)

    # Step 1 — Classify
    clf = classify(query)
    if verbose:
        print(f"\n  ① CLASSIFIER")
        print(f"     intent   : {clf['intent']}")
        print(f"     sql_mode : {clf['sql_mode']}")
        ents = {k: v for k, v in clf["entities"].items() if v}
        print(f"     entities : {ents}")

    # Step 2 — Keyword Search
    kw = keyword_search(
        query,
        top_k=5,
        filter_brands=clf["entities"]["brands"] + clf["entities"]["companies"],
        filter_zones=clf["entities"]["zones"]
    )
    if verbose:
        print(f"\n  ② KEYWORD SEARCH — {len(kw)} results")
        for r in kw[:3]:
            print(f"     #{r['rank']} bm25={r['score']} [{r['source']}] {r['text'][:55]}...")

    # Step 3 — Graph Search
    kg = graph_search(clf)
    if verbose:
        print(f"\n  ③ GRAPH SEARCH")
        print(f"     product_ids : {len(kg['product_ids'])} found")
        print(f"     region_ids  : {len(kg['region_ids'])} found")
        for path in kg["paths"][:3]:
            print(f"     path: {path[:70]}")

    # Step 4 — RRF Fusion
    rrf = rrf_fusion(kw, kg["paths"])
    if verbose:
        print(f"\n  ④ RRF FUSION — top {len(rrf)} results")
        for r in rrf[:3]:
            print(f"     #{r['rrf_rank']} score={r['rrf_score']} [{r['source']}] {r['text'][:55]}...")

    # Step 5 — SQL Builder + Run
    sql_out = build_and_run_sql(clf, kg, conn)
    if verbose:
        print(f"\n  ⑤ SQL QUERY")
        if sql_out["sql"]:
            for line in sql_out["sql"].strip().split("\n"):
                print(f"     {line}")
        print(f"\n     MySQL returned {len(sql_out['rows'])} rows")
        if sql_out["rows"]:
            print(f"     {'─'*55}")
            headers = list(sql_out["rows"][0].keys())
            print("     " + " | ".join(f"{h[:15]:<15}" for h in headers[:6]))
            print(f"     {'─'*55}")
            for row in sql_out["rows"][:5]:
                vals = [str(list(row.values())[i])[:15]
                        for i in range(min(6, len(row)))]
                print("     " + " | ".join(f"{v:<15}" for v in vals))

    # Step 6 — Context
    context = build_context(clf, sql_out, rrf)
    if verbose:
        print(f"\n  ⑥ CONTEXT BUILT ({len(context)} chars) → ready for LLM")

    return {
        "query":      query,
        "classifier": clf,
        "kw_results": kw,
        "kg_results": kg,
        "rrf_results":rrf,
        "sql_output": sql_out,
        "context":    context,
    }


# ══════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════
if __name__ == "__main__":
    import mysql.connector

    print("=" * 62)
    print("  FMCG SEARCH ENGINE — Keyword + Graph Search")
    print("=" * 62)

    # connect MySQL
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        print("✅  MySQL connected!")
    except Exception as e:
        print(f"❌  MySQL failed: {e}")
        exit()

    # build keyword index ONCE
    build_keyword_index(conn)

    # test queries
    test_queries = [
        "HUL top products in West zone Q3 2024",
        "compare HUL vs ITC sales in West",
        "what is the lead time for Surf Excel?",
        "which zones missed Q3 2024 targets?",
        "best ROI campaign in Diwali 2024",
        "Maggi customer reviews in Mumbai",
        "stockout risk products in South zone",
        "show quarterly revenue trend for HUL",
    ]

    for q in test_queries:
        result = run(q, conn, verbose=True)
        print(f"\n  Context preview:")
        print(f"  {result['context'][:200]}...")
        print()

    conn.close()
    print("\n✅  Done!")