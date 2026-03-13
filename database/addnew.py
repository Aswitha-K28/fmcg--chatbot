import os
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import mysql.connector

# ==========================================
# CONFIG
# ==========================================
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Aswitha@12",   # change
    "database": "fmcg_database"
}

NEO4J_CONFIG = {
    "uri": "neo4j+s://91202a22.databases.neo4j.io",
    "user": "91202a22",
    "password": "8OzBvCYXTU9jlKQDjiszyn-ATOGd6RTOvoBRjKuJW8w"
}

fake = Faker("en_IN")
random.seed(42)

# ==========================================
# REFERENCE LISTS
# ==========================================
QUARTERS       = ["Q1", "Q2", "Q3", "Q4"]
YEARS          = [2022, 2023, 2024]
OUTLET_TYPES   = ["Kirana", "Supermarket", "Hypermarket", "Pharmacy", "Convenience", "Online"]
MKTG_CHANNELS  = ["TV", "Digital", "OOH", "Print", "Radio", "In-Store"]
FESTIVALS      = ["Diwali", "Holi", "Eid", "Christmas", "Navratri", "Durga Puja", "None"]
SEASONS        = ["Summer", "Monsoon", "Winter", "Spring"]
SENTIMENTS     = ["Positive", "Negative", "Neutral"]
AGE_GROUPS     = ["18-24", "25-34", "35-44", "45-54", "55+"]
GENDERS        = ["Male", "Female", "Other"]
REVIEW_SOURCES = ["Amazon", "Flipkart", "BigBasket", "Survey", "Social Media"]
PROMO_TYPES    = ["Flat Discount", "BOGO", "Cashback", "Bundle", "Seasonal", "Festive"]
SUPPLIER_TYPES = ["Local Manufacturer", "Contract Packer", "Import", "JV Partner"]
WAREHOUSES     = ["Delhi WH", "Mumbai WH", "Chennai WH", "Kolkata WH", "Hyderabad WH"]
SALE_CHANNELS  = ["Modern Trade", "General Trade", "E-Commerce", "Institutional", "Direct"]

Q_MONTH = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}

def uid():
    return str(uuid.uuid4())

def rand_date(start, end):
    delta = (end - start).days
    return (start + timedelta(days=random.randint(0, max(delta, 1)))).isoformat()

# ==========================================
# STEP 1 — READ EXISTING IDs
# ==========================================
def fetch_existing_ids(cursor):
    print("\n🔄 Reading existing IDs from your database...")

    cursor.execute("SELECT product_id, brand_id, brand_name, mrp, cost_price FROM products")
    raw = cursor.fetchall()
    products = [{**p, "mrp": float(p["mrp"]), "cost_price": float(p["cost_price"])} for p in raw]

    cursor.execute("SELECT brand_id, brand_name, company_name FROM brands")
    brands = cursor.fetchall()

    cursor.execute("SELECT region_id, region_name, zone_name, zone_id FROM regions")
    regions = cursor.fetchall()

    cursor.execute("SELECT distributor_id FROM distributors")
    dist_rows = cursor.fetchall()
    dist_ids = [r["distributor_id"] for r in dist_rows]

    print(f"   ✅ products    : {len(products)}")
    print(f"   ✅ brands      : {len(brands)}")
    print(f"   ✅ regions     : {len(regions)}")
    print(f"   ✅ distributors: {len(dist_ids)}")

    return products, brands, regions, dist_ids

# ==========================================
# STEP 2 — CREATE NEW TABLES
# ==========================================
def create_new_tables(cursor):
    print("\n🔄 Creating new tables...")

    new_tables = [
        ("outlets", """
            CREATE TABLE IF NOT EXISTS outlets (
                outlet_id        VARCHAR(36)  PRIMARY KEY,
                region_id        VARCHAR(36),
                zone_name        VARCHAR(50),
                region_name      VARCHAR(100),
                distributor_id   VARCHAR(36),
                outlet_name      VARCHAR(200),
                outlet_type      VARCHAR(50),
                address          TEXT,
                monthly_footfall INT,
                shelf_space_sqft DECIMAL(8,1),
                is_active        TINYINT(1),
                onboarding_date  DATE,
                FOREIGN KEY (region_id) REFERENCES regions(region_id),
                FOREIGN KEY (distributor_id) REFERENCES distributors(distributor_id)
            )
        """),

        ("inventory", """
            CREATE TABLE IF NOT EXISTS inventory (
                inventory_id          VARCHAR(36) PRIMARY KEY,
                product_id            VARCHAR(36),
                outlet_id             VARCHAR(36),
                warehouse             VARCHAR(100),
                stock_quantity        INT,
                reorder_level         INT,
                days_of_stock         DECIMAL(6,1),
                last_replenished_date DATE,
                is_stockout           TINYINT(1),
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (outlet_id) REFERENCES outlets(outlet_id)
            )
        """),

        ("pricing_promotions", """
            CREATE TABLE IF NOT EXISTS pricing_promotions (
                pricing_id   VARCHAR(36)  PRIMARY KEY,
                product_id   VARCHAR(36),
                brand_name   VARCHAR(100),
                region_id    VARCHAR(36),
                region_name  VARCHAR(100),
                base_price   DECIMAL(10,2),
                promo_price  DECIMAL(10,2),
                discount_pct DECIMAL(5,2),
                promo_type   VARCHAR(50),
                promo_start  DATE,
                promo_end    DATE,
                quarter      VARCHAR(5),
                year         INT,
                is_active    TINYINT(1),
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (region_id) REFERENCES regions(region_id)
            )
        """),

        ("external_factors", """
            CREATE TABLE IF NOT EXISTS external_factors (
                factor_id                  VARCHAR(36) PRIMARY KEY,
                region_id                  VARCHAR(36),
                region_name                VARCHAR(100),
                quarter                    VARCHAR(5),
                year                       INT,
                festival_name              VARCHAR(100),
                season                     VARCHAR(50),
                rainfall_mm                DECIMAL(6,1),
                avg_temperature_c          DECIMAL(4,1),
                inflation_rate_pct         DECIMAL(5,2),
                consumer_confidence_index  DECIMAL(6,1),
                gdp_growth_pct             DECIMAL(5,2),
                unemployment_pct           DECIMAL(5,2),
                FOREIGN KEY (region_id) REFERENCES regions(region_id)
            )
        """),

        ("consumer_feedback", """
            CREATE TABLE IF NOT EXISTS consumer_feedback (
                feedback_id   VARCHAR(36) PRIMARY KEY,
                product_id    VARCHAR(36),
                brand_name    VARCHAR(100),
                region_id     VARCHAR(36),
                region_name   VARCHAR(100),
                rating        INT,
                sentiment     VARCHAR(20),
                review_source VARCHAR(50),
                age_group     VARCHAR(10),
                gender        VARCHAR(10),
                review_date   DATE,
                quarter       VARCHAR(5),
                year          INT,
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (region_id)  REFERENCES regions(region_id)
            )
        """),

        ("sales_targets", """
            CREATE TABLE IF NOT EXISTS sales_targets (
                target_id        VARCHAR(36)  PRIMARY KEY,
                brand_id         VARCHAR(36),
                brand_name       VARCHAR(100),
                region_id        VARCHAR(36),
                region_name      VARCHAR(100),
                quarter          VARCHAR(5),
                year             INT,
                target_revenue   DECIMAL(16,2),
                target_units     INT,
                achieved_revenue DECIMAL(16,2),
                achieved_units   INT,
                achievement_pct  DECIMAL(6,2),
                FOREIGN KEY (brand_id)  REFERENCES brands(brand_id),
                FOREIGN KEY (region_id) REFERENCES regions(region_id)
            )
        """),

        ("supply_chain", """
            CREATE TABLE IF NOT EXISTS supply_chain (
                sc_id                  VARCHAR(36) PRIMARY KEY,
                product_id             VARCHAR(36),
                brand_name             VARCHAR(100),
                supplier_name          VARCHAR(200),
                supplier_type          VARCHAR(50),
                manufacturing_location VARCHAR(100),
                warehouse              VARCHAR(100),
                lead_time_days         INT,
                transit_days           INT,
                on_time_delivery_pct   DECIMAL(5,2),
                defect_rate_pct        DECIMAL(5,2),
                cost_per_unit          DECIMAL(10,2),
                min_order_qty          INT,
                last_audit_date        DATE,
                FOREIGN KEY (product_id) REFERENCES products(product_id)
            )
        """),

        ("marketing_campaigns", """
            CREATE TABLE IF NOT EXISTS marketing_campaigns (
                campaign_id           VARCHAR(36)  PRIMARY KEY,
                brand_id              VARCHAR(36),
                brand_name            VARCHAR(100),
                product_id            VARCHAR(36),
                zone_name             VARCHAR(50),
                region_id             VARCHAR(36),
                region_name           VARCHAR(100),
                campaign_name         VARCHAR(255),
                channel               VARCHAR(50),
                budget_cr             DECIMAL(10,2),
                spend_cr              DECIMAL(10,2),
                impressions           BIGINT,
                reach                 BIGINT,
                conversions           INT,
                conversion_rate_pct   DECIMAL(8,4),
                roi_pct               DECIMAL(8,2),
                sales_lift_pct        DECIMAL(6,2),
                brand_awareness_score DECIMAL(5,1),
                cost_per_acquisition  DECIMAL(14,2),
                start_date            DATE,
                end_date              DATE,
                quarter               VARCHAR(5),
                year                  INT,
                festival_tie_in       VARCHAR(100),
                FOREIGN KEY (brand_id)   REFERENCES brands(brand_id),
                FOREIGN KEY (product_id) REFERENCES products(product_id),
                FOREIGN KEY (region_id)  REFERENCES regions(region_id)
            )
        """),
    ]

    for table_name, ddl in new_tables:
        cursor.execute(ddl)
        print(f"   ✅ {table_name} — table created")

# ==========================================
# STEP 3 — BULK INSERT
# ==========================================
def bulk_insert(cursor, conn, table, rows, cols):
    if not rows:
        return
    ph = ",".join(["%s"] * len(cols))
    sql = f"INSERT IGNORE INTO {table} ({','.join(cols)}) VALUES ({ph})"
    vals = [[r[c] for c in cols] for r in rows]

    for i in range(0, len(vals), 500):
        cursor.executemany(sql, vals[i:i+500])
        conn.commit()

    print(f"   ✅ {table:<25} {len(rows):>7,} rows inserted")

# ==========================================
# STEP 4 — GENERATE + INSERT DATA
# ==========================================
def generate_and_insert(cursor, conn, products, brands, regions, dist_ids):
    print("\n🔄 Generating and inserting new data...")

    outlets = []
    outlet_ids = []

    for reg in regions:
        for _ in range(random.randint(15, 30)):
            oid = uid()
            outlets.append({
                "outlet_id": oid,
                "region_id": reg["region_id"],
                "zone_name": reg["zone_name"],
                "region_name": reg["region_name"],
                "distributor_id": random.choice(dist_ids) if dist_ids else None,
                "outlet_name": fake.company() + " Store",
                "outlet_type": random.choice(OUTLET_TYPES),
                "address": fake.address().replace("\n", ", "),
                "monthly_footfall": random.randint(200, 15000),
                "shelf_space_sqft": round(random.uniform(10, 500), 1),
                "is_active": random.choice([1, 1, 1, 0]),
                "onboarding_date": rand_date(datetime(2018, 1, 1), datetime(2023, 6, 1))
            })
            outlet_ids.append(oid)

    bulk_insert(cursor, conn, "outlets", outlets, [
        "outlet_id","region_id","zone_name","region_name","distributor_id",
        "outlet_name","outlet_type","address","monthly_footfall",
        "shelf_space_sqft","is_active","onboarding_date"
    ])

    inventory = []
    for prod in products:
        stock = random.randint(0, 500)
        inventory.append({
            "inventory_id": uid(),
            "product_id": prod["product_id"],
            "outlet_id": random.choice(outlet_ids),
            "warehouse": random.choice(WAREHOUSES),
            "stock_quantity": stock,
            "reorder_level": random.randint(20, 100),
            "days_of_stock": round(stock / max(random.randint(5, 50), 1), 1),
            "last_replenished_date": rand_date(datetime(2024, 1, 1), datetime(2024, 12, 1)),
            "is_stockout": 1 if stock == 0 else 0
        })

    bulk_insert(cursor, conn, "inventory", inventory, [
        "inventory_id","product_id","outlet_id","warehouse",
        "stock_quantity","reorder_level","days_of_stock",
        "last_replenished_date","is_stockout"
    ])

    pricing_promotions = []
    for prod in products:
        for year in YEARS:
            for quarter in QUARTERS:
                if random.random() < 0.4:
                    base = prod["mrp"]
                    disc = round(random.uniform(5, 30), 2)
                    promo = round(base * (1 - disc / 100), 2)
                    sm = Q_MONTH[quarter]
                    start = datetime(year, sm, random.randint(1, 20))
                    end = start + timedelta(days=random.randint(7, 45))
                    reg = random.choice(regions)

                    pricing_promotions.append({
                        "pricing_id": uid(),
                        "product_id": prod["product_id"],
                        "brand_name": prod["brand_name"],
                        "region_id": reg["region_id"],
                        "region_name": reg["region_name"],
                        "base_price": base,
                        "promo_price": promo,
                        "discount_pct": disc,
                        "promo_type": random.choice(PROMO_TYPES),
                        "promo_start": start.date().isoformat(),
                        "promo_end": end.date().isoformat(),
                        "quarter": quarter,
                        "year": year,
                        "is_active": random.choice([1, 0])
                    })

    bulk_insert(cursor, conn, "pricing_promotions", pricing_promotions, [
        "pricing_id","product_id","brand_name","region_id","region_name",
        "base_price","promo_price","discount_pct","promo_type",
        "promo_start","promo_end","quarter","year","is_active"
    ])

    external_factors = []
    for reg in regions:
        for year in YEARS:
            for quarter in QUARTERS:
                external_factors.append({
                    "factor_id": uid(),
                    "region_id": reg["region_id"],
                    "region_name": reg["region_name"],
                    "quarter": quarter,
                    "year": year,
                    "festival_name": random.choice(FESTIVALS),
                    "season": random.choice(SEASONS),
                    "rainfall_mm": round(random.uniform(0, 400), 1),
                    "avg_temperature_c": round(random.uniform(10, 45), 1),
                    "inflation_rate_pct": round(random.uniform(3, 9), 2),
                    "consumer_confidence_index": round(random.uniform(60, 130), 1),
                    "gdp_growth_pct": round(random.uniform(4, 9), 2),
                    "unemployment_pct": round(random.uniform(4, 12), 2)
                })

    bulk_insert(cursor, conn, "external_factors", external_factors, [
        "factor_id","region_id","region_name","quarter","year",
        "festival_name","season","rainfall_mm","avg_temperature_c",
        "inflation_rate_pct","consumer_confidence_index",
        "gdp_growth_pct","unemployment_pct"
    ])

    consumer_feedback = []
    for _ in range(5000):
        prod = random.choice(products)
        reg = random.choice(regions)
        sentiment = random.choice(SENTIMENTS)
        rating = {
            "Positive": random.randint(4, 5),
            "Negative": random.randint(1, 2),
            "Neutral": 3
        }[sentiment]
        year = random.choice(YEARS)
        quarter = random.choice(QUARTERS)
        sm = Q_MONTH[quarter]

        consumer_feedback.append({
            "feedback_id": uid(),
            "product_id": prod["product_id"],
            "brand_name": prod["brand_name"],
            "region_id": reg["region_id"],
            "region_name": reg["region_name"],
            "rating": rating,
            "sentiment": sentiment,
            "review_source": random.choice(REVIEW_SOURCES),
            "age_group": random.choice(AGE_GROUPS),
            "gender": random.choice(GENDERS),
            "review_date": rand_date(datetime(year, sm, 1), datetime(year, min(sm + 2, 12), 28)),
            "quarter": quarter,
            "year": year
        })

    bulk_insert(cursor, conn, "consumer_feedback", consumer_feedback, [
        "feedback_id","product_id","brand_name","region_id","region_name",
        "rating","sentiment","review_source","age_group","gender",
        "review_date","quarter","year"
    ])

    sales_targets = []
    for brand in brands:
        for reg in regions:
            for year in YEARS:
                for quarter in QUARTERS:
                    target_rev = round(random.uniform(500000, 10000000), 2)
                    target_units = random.randint(1000, 50000)
                    ach_pct = round(random.uniform(60, 130), 2)

                    sales_targets.append({
                        "target_id": uid(),
                        "brand_id": brand["brand_id"],
                        "brand_name": brand["brand_name"],
                        "region_id": reg["region_id"],
                        "region_name": reg["region_name"],
                        "quarter": quarter,
                        "year": year,
                        "target_revenue": target_rev,
                        "target_units": target_units,
                        "achieved_revenue": round(target_rev * ach_pct / 100, 2),
                        "achieved_units": int(target_units * ach_pct / 100),
                        "achievement_pct": ach_pct
                    })

    bulk_insert(cursor, conn, "sales_targets", sales_targets, [
        "target_id","brand_id","brand_name","region_id","region_name",
        "quarter","year","target_revenue","target_units",
        "achieved_revenue","achieved_units","achievement_pct"
    ])

    supply_chain = []
    for prod in products:
        supply_chain.append({
            "sc_id": uid(),
            "product_id": prod["product_id"],
            "brand_name": prod["brand_name"],
            "supplier_name": fake.company(),
            "supplier_type": random.choice(SUPPLIER_TYPES),
            "manufacturing_location": fake.city(),
            "warehouse": random.choice(WAREHOUSES),
            "lead_time_days": random.randint(3, 30),
            "transit_days": random.randint(1, 10),
            "on_time_delivery_pct": round(random.uniform(70, 99), 2),
            "defect_rate_pct": round(random.uniform(0.1, 5), 2),
            "cost_per_unit": round(random.uniform(5, 200), 2),
            "min_order_qty": random.randint(100, 5000),
            "last_audit_date": rand_date(datetime(2023, 1, 1), datetime(2024, 12, 1))
        })

    bulk_insert(cursor, conn, "supply_chain", supply_chain, [
        "sc_id","product_id","brand_name","supplier_name","supplier_type",
        "manufacturing_location","warehouse","lead_time_days","transit_days",
        "on_time_delivery_pct","defect_rate_pct","cost_per_unit",
        "min_order_qty","last_audit_date"
    ])

    marketing_campaigns = []
    for brand in brands:
        brand_products = [p for p in products if p["brand_id"] == brand["brand_id"]]

        for year in YEARS:
            for _ in range(random.randint(2, 5)):
                quarter = random.choice(QUARTERS)
                sm = Q_MONTH[quarter]
                start = datetime(year, sm, random.randint(1, 20))
                end = start + timedelta(days=random.randint(15, 90))
                budget = round(random.uniform(1, 50), 2)
                spend = round(budget * random.uniform(0.7, 1.1), 2)
                impr = random.randint(100000, 50000000)
                conv = int(impr * random.uniform(0.001, 0.05))
                reg = random.choice(regions)
                prod = random.choice(brand_products) if brand_products else random.choice(products)

                marketing_campaigns.append({
                    "campaign_id": uid(),
                    "brand_id": brand["brand_id"],
                    "brand_name": brand["brand_name"],
                    "product_id": prod["product_id"],
                    "zone_name": reg["zone_name"],
                    "region_id": reg["region_id"],
                    "region_name": reg["region_name"],
                    "campaign_name": f"{brand['brand_name']} {fake.word().capitalize()} {year}",
                    "channel": random.choice(MKTG_CHANNELS),
                    "budget_cr": budget,
                    "spend_cr": spend,
                    "impressions": impr,
                    "reach": int(impr * random.uniform(0.6, 0.9)),
                    "conversions": conv,
                    "conversion_rate_pct": round(conv / max(impr, 1) * 100, 4),
                    "roi_pct": round((spend * random.uniform(0.8, 3.5) - spend) / spend * 100, 2),
                    "sales_lift_pct": round(random.uniform(-5, 40), 2),
                    "brand_awareness_score": round(random.uniform(30, 95), 1),
                    "cost_per_acquisition": round(spend * 1e7 / max(conv, 1), 2),
                    "start_date": start.date().isoformat(),
                    "end_date": end.date().isoformat(),
                    "quarter": quarter,
                    "year": year,
                    "festival_tie_in": random.choice(FESTIVALS)
                })

    bulk_insert(cursor, conn, "marketing_campaigns", marketing_campaigns, [
        "campaign_id","brand_id","brand_name","product_id","zone_name",
        "region_id","region_name","campaign_name","channel","budget_cr",
        "spend_cr","impressions","reach","conversions","conversion_rate_pct",
        "roi_pct","sales_lift_pct","brand_awareness_score","cost_per_acquisition",
        "start_date","end_date","quarter","year","festival_tie_in"
    ])

# ==========================================
# STEP 5 — VERIFY
# ==========================================
def verify(cursor):
    print("\n📊 Final row counts in fmcg_database:")
    print(f"   {'Table':<28} {'Rows':>8}")
    print(f"   {'-'*40}")

    all_tables = [
        "companies","brands","categories","subcategories",
        "products","zones","regions","distributors","sales","competitors",
        "outlets","inventory","pricing_promotions","external_factors",
        "consumer_feedback","sales_targets","supply_chain","marketing_campaigns"
    ]

    new_tables = {
        "outlets","inventory","pricing_promotions","external_factors",
        "consumer_feedback","sales_targets","supply_chain","marketing_campaigns"
    }

    for t in all_tables:
        try:
            cursor.execute(f"SELECT COUNT(*) AS cnt FROM {t}")
            cnt = cursor.fetchone()["cnt"]
            tag = " ← NEW" if t in new_tables else ""
            print(f"   {t:<28} {cnt:>8,}{tag}")
        except Exception:
            print(f"   {t:<28} {'(not found)':>8}")

# ==========================================
# STEP 6 — NEO4J LOAD
# ==========================================
def load_neo4j_new(mysql_cursor):
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("❌ Please install neo4j driver: pip install neo4j")
        return

    print("\n🔄 Connecting to Neo4j...")
    driver = GraphDatabase.driver(
        NEO4J_CONFIG["uri"],
        auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"])
    )
    print("✅ Connected to Neo4j!")

    with driver.session() as s:
        print("🔄 Creating Neo4j constraints...")
        constraints = [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Outlet) REQUIRE n.outlet_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Campaign) REQUIRE n.campaign_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (n:Supplier) REQUIRE n.supplier_name IS UNIQUE"
        ]
        for c in constraints:
            s.run(c)
        print("   ✅ Constraints ready")

        # Outlet nodes + LOCATED_IN + SUPPLIED_BY
        print("🔄 Loading Outlet nodes...")
        mysql_cursor.execute("""
            SELECT outlet_id, outlet_name, outlet_type, monthly_footfall,
                   shelf_space_sqft, is_active, region_id, distributor_id
            FROM outlets
        """)
        outlets = mysql_cursor.fetchall()

        count = 0
        for row in outlets:
            s.run("""
                MERGE (o:Outlet {outlet_id: $outlet_id})
                SET o.name = $outlet_name,
                    o.type = $outlet_type,
                    o.footfall = $monthly_footfall,
                    o.shelf_sqft = $shelf_space_sqft,
                    o.is_active = $is_active
                WITH o
                MATCH (r:Region {region_id: $region_id})
                MERGE (o)-[:LOCATED_IN]->(r)
            """,
            outlet_id=row["outlet_id"],
            outlet_name=row["outlet_name"],
            outlet_type=row["outlet_type"],
            monthly_footfall=int(row["monthly_footfall"]),
            shelf_space_sqft=float(row["shelf_space_sqft"]),
            is_active=int(row["is_active"]),
            region_id=row["region_id"]
            )

            if row["distributor_id"]:
                s.run("""
                    MATCH (o:Outlet {outlet_id: $outlet_id})
                    MATCH (d:Distributor {distributor_id: $distributor_id})
                    MERGE (o)-[:SUPPLIED_BY]->(d)
                """,
                outlet_id=row["outlet_id"],
                distributor_id=row["distributor_id"]
                )
            count += 1
        print(f"   ✅ {count} Outlet nodes loaded")

        # Campaign nodes + relations
        print("🔄 Loading Campaign nodes...")
        mysql_cursor.execute("""
            SELECT campaign_id, campaign_name, channel, budget_cr, spend_cr, roi_pct,
                   sales_lift_pct, brand_awareness_score, impressions, conversions,
                   festival_tie_in, quarter, year, brand_id, product_id, region_id
            FROM marketing_campaigns
        """)
        campaigns = mysql_cursor.fetchall()

        count = 0
        for row in campaigns:
            s.run("""
                MERGE (cam:Campaign {campaign_id: $campaign_id})
                SET cam.name = $campaign_name,
                    cam.channel = $channel,
                    cam.budget_cr = $budget_cr,
                    cam.spend_cr = $spend_cr,
                    cam.roi_pct = $roi_pct,
                    cam.sales_lift_pct = $sales_lift_pct,
                    cam.awareness_score = $brand_awareness_score,
                    cam.impressions = $impressions,
                    cam.conversions = $conversions,
                    cam.festival = $festival_tie_in,
                    cam.quarter = $quarter,
                    cam.year = $year
                WITH cam
                MATCH (b:Brand {brand_id: $brand_id})
                MERGE (cam)-[:RUN_BY]->(b)
                WITH cam
                MATCH (p:Product {product_id: $product_id})
                MERGE (cam)-[:PROMOTES]->(p)
                WITH cam
                MATCH (r:Region {region_id: $region_id})
                MERGE (cam)-[:TARGETED_AT]->(r)
            """,
            campaign_id=row["campaign_id"],
            campaign_name=row["campaign_name"],
            channel=row["channel"],
            budget_cr=float(row["budget_cr"]),
            spend_cr=float(row["spend_cr"]),
            roi_pct=float(row["roi_pct"]),
            sales_lift_pct=float(row["sales_lift_pct"]),
            brand_awareness_score=float(row["brand_awareness_score"]),
            impressions=int(row["impressions"]),
            conversions=int(row["conversions"]),
            festival_tie_in=row["festival_tie_in"],
            quarter=row["quarter"],
            year=int(row["year"]),
            brand_id=row["brand_id"],
            product_id=row["product_id"],
            region_id=row["region_id"]
            )
            count += 1
        print(f"   ✅ {count} Campaign nodes loaded")

        # HAS_PROMO
        print("🔄 Building HAS_PROMO relationships...")
        mysql_cursor.execute("""
            SELECT product_id, region_id,
                   COUNT(*) AS promo_count,
                   ROUND(AVG(discount_pct),2) AS avg_discount,
                   MAX(discount_pct) AS max_discount,
                   MIN(promo_price) AS min_promo_price
            FROM pricing_promotions
            GROUP BY product_id, region_id
        """)
        promos = mysql_cursor.fetchall()

        count = 0
        for row in promos:
            s.run("""
                MATCH (p:Product {product_id: $product_id})
                MATCH (r:Region {region_id: $region_id})
                MERGE (p)-[rel:HAS_PROMO]->(r)
                SET rel.promo_count = $promo_count,
                    rel.avg_discount = $avg_discount,
                    rel.max_discount = $max_discount,
                    rel.min_promo_price = $min_promo_price
            """,
            product_id=row["product_id"],
            region_id=row["region_id"],
            promo_count=int(row["promo_count"]),
            avg_discount=float(row["avg_discount"]),
            max_discount=float(row["max_discount"]),
            min_promo_price=float(row["min_promo_price"])
            )
            count += 1
        print(f"   ✅ {count} HAS_PROMO relationships")

        # HAS_FEEDBACK
        print("🔄 Building HAS_FEEDBACK relationships...")
        mysql_cursor.execute("""
            SELECT product_id, region_id,
                   COUNT(*) AS total_reviews,
                   ROUND(AVG(rating), 2) AS avg_rating,
                   SUM(sentiment='Positive') AS positive_count,
                   SUM(sentiment='Negative') AS negative_count,
                   SUM(sentiment='Neutral') AS neutral_count
            FROM consumer_feedback
            GROUP BY product_id, region_id
        """)
        feedbacks = mysql_cursor.fetchall()

        count = 0
        for row in feedbacks:
            s.run("""
                MATCH (p:Product {product_id: $product_id})
                MATCH (r:Region {region_id: $region_id})
                MERGE (p)-[rel:HAS_FEEDBACK]->(r)
                SET rel.total_reviews = $total_reviews,
                    rel.avg_rating = $avg_rating,
                    rel.positive_count = $positive_count,
                    rel.negative_count = $negative_count,
                    rel.neutral_count = $neutral_count
            """,
            product_id=row["product_id"],
            region_id=row["region_id"],
            total_reviews=int(row["total_reviews"]),
            avg_rating=float(row["avg_rating"]),
            positive_count=int(row["positive_count"]),
            negative_count=int(row["negative_count"]),
            neutral_count=int(row["neutral_count"])
            )
            count += 1
        print(f"   ✅ {count} HAS_FEEDBACK relationships")

        # Supplier nodes + SUPPLIED_BY_PRODUCT
        print("🔄 Building supply chain graph...")
        mysql_cursor.execute("""
            SELECT product_id, supplier_name, supplier_type, manufacturing_location,
                   warehouse, lead_time_days, transit_days, on_time_delivery_pct,
                   defect_rate_pct, cost_per_unit, min_order_qty, last_audit_date
            FROM supply_chain
        """)
        supplies = mysql_cursor.fetchall()

        count = 0
        for row in supplies:
            s.run("""
                MERGE (sup:Supplier {supplier_name: $supplier_name})
                SET sup.supplier_type = $supplier_type,
                    sup.manufacturing_location = $manufacturing_location,
                    sup.warehouse = $warehouse

                WITH sup
                MATCH (p:Product {product_id: $product_id})
                MERGE (p)-[rel:SUPPLIED_BY_PRODUCT]->(sup)
                SET rel.lead_time_days = $lead_time_days,
                    rel.transit_days = $transit_days,
                    rel.on_time_delivery_pct = $on_time_delivery_pct,
                    rel.defect_rate_pct = $defect_rate_pct,
                    rel.cost_per_unit = $cost_per_unit,
                    rel.min_order_qty = $min_order_qty,
                    rel.last_audit_date = $last_audit_date
            """,
            product_id=row["product_id"],
            supplier_name=row["supplier_name"],
            supplier_type=row["supplier_type"],
            manufacturing_location=row["manufacturing_location"],
            warehouse=row["warehouse"],
            lead_time_days=int(row["lead_time_days"]),
            transit_days=int(row["transit_days"]),
            on_time_delivery_pct=float(row["on_time_delivery_pct"]),
            defect_rate_pct=float(row["defect_rate_pct"]),
            cost_per_unit=float(row["cost_per_unit"]),
            min_order_qty=int(row["min_order_qty"]),
            last_audit_date=str(row["last_audit_date"])
            )
            count += 1
        print(f"   ✅ {count} supply chain relationships")

        # HAS_TARGET
        print("🔄 Building HAS_TARGET relationships...")
        mysql_cursor.execute("""
            SELECT brand_id, region_id,
                   SUM(target_revenue) AS total_target,
                   SUM(achieved_revenue) AS total_achieved,
                   ROUND(AVG(achievement_pct), 2) AS avg_achievement
            FROM sales_targets
            GROUP BY brand_id, region_id
        """)
        targets = mysql_cursor.fetchall()

        count = 0
        for row in targets:
            s.run("""
                MATCH (b:Brand {brand_id: $brand_id})
                MATCH (r:Region {region_id: $region_id})
                MERGE (b)-[rel:HAS_TARGET]->(r)
                SET rel.total_target = $total_target,
                    rel.total_achieved = $total_achieved,
                    rel.avg_achievement = $avg_achievement
            """,
            brand_id=row["brand_id"],
            region_id=row["region_id"],
            total_target=float(row["total_target"]),
            total_achieved=float(row["total_achieved"]),
            avg_achievement=float(row["avg_achievement"])
            )
            count += 1
        print(f"   ✅ {count} HAS_TARGET relationships")

    driver.close()
    print("\n✅ Neo4j loading complete!")

# ==========================================
# MAIN
# ==========================================
if __name__ == "__main__":
    print("=" * 60)
    print("  ADD NEW TABLES TO EXISTING FMCG DATABASE")
    print("=" * 60)
    print("  1 → MySQL only")
    print("  2 → Neo4j only (from already existing MySQL data)")
    print("  3 → Both MySQL + Neo4j")
    print("=" * 60)

    choice = input("Enter choice (1/2/3): ").strip()

    if choice not in ("1", "2", "3"):
        print("❌ Invalid choice.")
        raise SystemExit

    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor(dictionary=True)
        print("\n✅ Connected to MySQL!")
    except Exception as e:
        print(f"❌ MySQL connection failed: {e}")
        raise SystemExit

    try:
        if choice in ("1", "3"):
            products, brands, regions, dist_ids = fetch_existing_ids(cursor)
            create_new_tables(cursor)
            conn.commit()
            generate_and_insert(cursor, conn, products, brands, regions, dist_ids)
            verify(cursor)

        if choice == "2":
            # Neo4j only means read existing MySQL data and push it to Neo4j
            load_neo4j_new(cursor)

        if choice == "3":
            load_neo4j_new(cursor)

    except Exception as e:
        print(f"\n❌ Error: {e}")
    finally:
        cursor.close()
        conn.close()

    if choice == "1":
        print("\n🎉 Done! All 8 new tables added to MySQL.")
    elif choice == "2":
        print("\n🎉 Done! Neo4j updated using existing MySQL data.")
    elif choice == "3":
        print("\n🎉 Done! MySQL + Neo4j both updated successfully.")