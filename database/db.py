"""
FMCG Data Generator
====================
Generates fake FMCG data using Faker and loads it into:
  1. MySQL  → actual data (sales, products, distributors, competitors)
  2. Neo4j  → schema + relationships only

Requirements:
    pip install faker mysql-connector-python neo4j

Usage:
    1. Fill in your MySQL and Neo4j credentials below
    2. Run: python fmcg_data_generator.py
"""

import random
import uuid
from datetime import datetime, timedelta
from faker import Faker

# ─────────────────────────────────────────
#  CONFIG — fill in your credentials
# ─────────────────────────────────────────
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Aswitha@12",
    "database": "fmcg_database"
}

NEO4J_CONFIG = {
    "uri": "neo4j+s://91202a22.databases.neo4j.io",
    "user": "91202a22",
    "password": "8OzBvCYXTU9jlKQDjiszyn-ATOGd6RTOvoBRjKuJW8w"
}

# ─────────────────────────────────────────
#  MASTER FMCG DATA
# ─────────────────────────────────────────
fake = Faker("en_IN")

COMPANIES = ["HUL", "ITC", "Nestle", "P&G", "Dabur", "Marico", "Godrej", "Britannia"]

BRANDS = {
    "HUL":      ["Surf Excel", "Dove", "Lipton", "Knorr", "Lux", "Pepsodent"],
    "ITC":      ["Aashirvaad", "Sunfeast", "Bingo", "Yippee", "Fiama", "Engage"],
    "Nestle":   ["Maggi", "KitKat", "Munch", "Nescafe", "Milkmaid", "Bar-One"],
    "P&G":      ["Ariel", "Pantene", "Olay", "Whisper", "Gillette", "Head & Shoulders"],
    "Dabur":    ["Real", "Hajmola", "Chyawanprash", "Vatika", "Odomos", "Honitus"],
    "Marico":   ["Parachute", "Saffola", "Set Wet", "Nihar", "Mediker", "Livon"],
    "Godrej":   ["Cinthol", "Good Knight", "Hit", "Godrej No.1", "Ezee", "Aer"],
    "Britannia": ["Good Day", "Marie Gold", "Bourbon", "NutriChoice", "Milk Bikis", "50-50"]
}

CATEGORIES = {
    "Detergent & Fabric Care": ["Detergent Powder", "Fabric Softener", "Stain Remover"],
    "Personal Care":           ["Shampoo", "Soap", "Face Wash", "Deodorant"],
    "Food & Beverages":        ["Noodles", "Biscuits", "Juice", "Coffee", "Tea"],
    "Snacks":                  ["Chips", "Namkeen", "Cookies", "Crackers"],
    "Health & Wellness":       ["Chyawanprash", "Glucose Powder", "Protein Bar"],
    "Home Care":               ["Mosquito Repellent", "Air Freshener", "Dish Wash"]
}

BRAND_CATEGORY = {
    "Surf Excel": "Detergent & Fabric Care", "Ariel": "Detergent & Fabric Care",
    "Dove": "Personal Care",                 "Pantene": "Personal Care",
    "Lux": "Personal Care",                  "Cinthol": "Personal Care",
    "Fiama": "Personal Care",                "Head & Shoulders": "Personal Care",
    "Gillette": "Personal Care",             "Olay": "Personal Care",
    "Vatika": "Personal Care",               "Parachute": "Personal Care",
    "Lipton": "Food & Beverages",            "Nescafe": "Food & Beverages",
    "Real": "Food & Beverages",              "Aashirvaad": "Food & Beverages",
    "Maggi": "Food & Beverages",             "Yippee": "Food & Beverages",
    "Bingo": "Snacks",                       "Sunfeast": "Snacks",
    "Good Day": "Snacks",                    "Marie Gold": "Snacks",
    "Bourbon": "Snacks",                     "NutriChoice": "Snacks",
    "Milk Bikis": "Snacks",                  "50-50": "Snacks",
    "KitKat": "Snacks",                      "Munch": "Snacks",
    "Knorr": "Food & Beverages",             "Milkmaid": "Food & Beverages",
    "Bar-One": "Snacks",
    "Chyawanprash": "Health & Wellness",     "Hajmola": "Health & Wellness",
    "Honitus": "Health & Wellness",
    "Good Knight": "Home Care",              "Hit": "Home Care",
    "Aer": "Home Care",                      "Ezee": "Detergent & Fabric Care",
    "Godrej No.1": "Personal Care",
    "Saffola": "Food & Beverages",           "Set Wet": "Personal Care",
    "Nihar": "Personal Care",               "Mediker": "Personal Care",
    "Livon": "Personal Care",
    "Whisper": "Personal Care",
    "Pepsodent": "Personal Care",
    "Odomos": "Home Care",
    "Engage": "Personal Care",
}

ZONES = ["North", "South", "East", "West"]

REGIONS = {
    "North": ["Delhi", "Punjab", "Uttar Pradesh", "Haryana", "Rajasthan", "Himachal Pradesh"],
    "South": ["Chennai", "Bangalore", "Hyderabad", "Kerala", "Coimbatore", "Vizag"],
    "East":  ["Kolkata", "Bhubaneswar", "Patna", "Guwahati", "Ranchi", "Siliguri"],
    "West":  ["Mumbai", "Pune", "Ahmedabad", "Surat", "Nagpur", "Nashik"]
}

WEIGHTS = ["50g", "100g", "200g", "500g", "1kg", "2kg", "250ml", "500ml", "1L", "2L"]
CHANNELS = ["Modern Trade", "General Trade", "E-Commerce", "Institutional", "Direct"]
QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
YEARS = [2022, 2023, 2024]

# ─────────────────────────────────────────
#  GENERATE DATA
# ─────────────────────────────────────────

def generate_all_data():
    print("🔄 Generating FMCG fake data...")

    # --- Companies ---
    companies = []
    for name in COMPANIES:
        companies.append({
            "company_id": str(uuid.uuid4()),
            "name": name,
            "hq": fake.city(),
            "founded_year": random.randint(1900, 1990),
            "annual_revenue_cr": round(random.uniform(1000, 50000), 2),
            "country": "India"
        })

    company_map = {c["name"]: c["company_id"] for c in companies}

    # --- Brands ---
    brands = []
    brand_map = {}
    for company_name, brand_list in BRANDS.items():
        for brand_name in brand_list:
            bid = str(uuid.uuid4())
            brands.append({
                "brand_id": bid,
                "company_id": company_map[company_name],
                "company_name": company_name,
                "brand_name": brand_name,
                "segment": random.choice(["Premium", "Mass", "Economy"]),
                "launch_year": random.randint(1950, 2015)
            })
            brand_map[brand_name] = bid

    # --- Categories & Subcategories ---
    categories = []
    subcategories = []
    cat_map = {}
    subcat_map = {}

    for cat_name, subcats in CATEGORIES.items():
        cid = str(uuid.uuid4())
        categories.append({"category_id": cid, "category_name": cat_name})
        cat_map[cat_name] = cid
        for sub in subcats:
            sid = str(uuid.uuid4())
            subcategories.append({"subcategory_id": sid, "category_id": cid, "subcategory_name": sub})
            subcat_map[sub] = sid

    # --- Products / SKUs ---
    products = []
    product_map = {}
    for brand_name, bid in brand_map.items():
        cat_name = BRAND_CATEGORY.get(brand_name, random.choice(list(CATEGORIES.keys())))
        cid = cat_map.get(cat_name, list(cat_map.values())[0])
        subcats_for_cat = CATEGORIES.get(cat_name, ["General"])
        subcat_name = random.choice(subcats_for_cat)
        sid = subcat_map.get(subcat_name, list(subcat_map.values())[0])

        for _ in range(random.randint(4, 8)):  # 4-8 SKUs per brand
            pid = str(uuid.uuid4())
            weight = random.choice(WEIGHTS)
            sku_name = f"{brand_name} {weight}"
            products.append({
                "product_id": pid,
                "brand_id": bid,
                "brand_name": brand_name,
                "category_id": cid,
                "subcategory_id": sid,
                "sku_name": sku_name,
                "barcode": fake.ean13(),
                "weight_variant": weight,
                "mrp": round(random.uniform(20, 800), 2),
                "cost_price": round(random.uniform(10, 400), 2),
                "launch_date": fake.date_between(start_date="-10y", end_date="today").isoformat(),
                "is_active": random.choice([True, True, True, False])
            })
            product_map[sku_name] = pid

    # --- Regions & Zones ---
    zones_data = []
    regions_data = []
    zone_map = {}
    region_map = {}

    for zone_name in ZONES:
        zid = str(uuid.uuid4())
        zones_data.append({"zone_id": zid, "zone_name": zone_name})
        zone_map[zone_name] = zid
        for region_name in REGIONS[zone_name]:
            rid = str(uuid.uuid4())
            regions_data.append({
                "region_id": rid,
                "zone_id": zid,
                "zone_name": zone_name,
                "region_name": region_name,
                "state": region_name,
                "population_lakh": round(random.uniform(10, 200), 1)
            })
            region_map[region_name] = rid

    # --- Distributors ---
    distributors = []
    distributor_map = {}
    for zone_name, zid in zone_map.items():
        for _ in range(random.randint(8, 15)):  # 8-15 distributors per zone
            did = str(uuid.uuid4())
            region_name = random.choice(REGIONS[zone_name])
            distributors.append({
                "distributor_id": did,
                "zone_id": zid,
                "region_id": region_map[region_name],
                "zone_name": zone_name,
                "region_name": region_name,
                "distributor_name": fake.company(),
                "contact_person": fake.name(),
                "phone": fake.phone_number(),
                "email": fake.email(),
                "address": fake.address().replace("\n", ", "),
                "channel": random.choice(CHANNELS),
                "credit_limit": round(random.uniform(100000, 5000000), 2),
                "onboarding_date": fake.date_between(start_date="-8y", end_date="-1y").isoformat(),
                "is_active": random.choice([True, True, False])
            })
            distributor_map[did] = {"zone": zone_name, "region": region_name}

    distributor_ids = [d["distributor_id"] for d in distributors]

    # --- Sales Records (bulk - 10k+ rows) ---
    print("🔄 Generating 12,000+ sales records...")
    sales = []
    for _ in range(12000):
        product = random.choice(products)
        zone_name = random.choice(ZONES)
        region_name = random.choice(REGIONS[zone_name])
        distributor_id = random.choice(distributor_ids)
        units = random.randint(50, 5000)
        mrp = product["mrp"]
        discount_pct = round(random.uniform(0, 25), 2)
        revenue = round(units * mrp * (1 - discount_pct / 100), 2)
        quarter = random.choice(QUARTERS)
        year = random.choice(YEARS)
        start_month = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}[quarter]
        sale_date = fake.date_between(
            start_date=datetime(year, start_month, 1),
            end_date=datetime(year, start_month + 2, 28)
        ).isoformat()

        sales.append({
            "sale_id": str(uuid.uuid4()),
            "product_id": product["product_id"],
            "sku_name": product["sku_name"],
            "brand_name": product["brand_name"],
            "distributor_id": distributor_id,
            "region_id": region_map[region_name],
            "zone_id": zone_map[zone_name],
            "zone_name": zone_name,
            "region_name": region_name,
            "channel": random.choice(CHANNELS),
            "units_sold": units,
            "mrp": mrp,
            "discount_pct": discount_pct,
            "revenue": revenue,
            "cogs": round(units * product["cost_price"], 2),
            "quarter": quarter,
            "year": year,
            "sale_date": sale_date
        })

    # --- Competitors ---
    print("🔄 Generating competitor data...")
    competitor_companies = ["Colgate", "Reckitt", "Johnson & Johnson", "Emami", "CavinKare"]
    competitor_brands = {
        "Colgate": ["Colgate Strong Teeth", "Colgate MaxFresh", "Colgate Sensitive"],
        "Reckitt": ["Dettol", "Strepsils", "Veet", "Harpic", "Vanish"],
        "Johnson & Johnson": ["Johnson Baby", "Band-Aid", "Neutrogena", "Clean & Clear"],
        "Emami": ["Fair and Handsome", "Navratna", "Zandu", "BoroPlus"],
        "CavinKare": ["Chik", "Meera", "Nyle", "Spinz"]
    }

    competitors = []
    for comp_name, comp_brands in competitor_brands.items():
        for brand in comp_brands:
            for zone_name in ZONES:
                for year in YEARS:
                    for quarter in QUARTERS:
                        competitors.append({
                            "competitor_id": str(uuid.uuid4()),
                            "competitor_company": comp_name,
                            "competitor_brand": brand,
                            "zone_name": zone_name,
                            "zone_id": zone_map[zone_name],
                            "category": random.choice(list(CATEGORIES.keys())),
                            "estimated_market_share_pct": round(random.uniform(1, 35), 2),
                            "estimated_revenue_cr": round(random.uniform(10, 500), 2),
                            "avg_mrp": round(random.uniform(20, 600), 2),
                            "distribution_reach_pct": round(random.uniform(20, 95), 2),
                            "quarter": quarter,
                            "year": year,
                            "data_source": random.choice(["Nielsen", "IQVIA", "Kantar", "Internal Estimate"])
                        })

    print(f"✅ Data generated:")
    print(f"   Companies:    {len(companies)}")
    print(f"   Brands:       {len(brands)}")
    print(f"   Products:     {len(products)}")
    print(f"   Distributors: {len(distributors)}")
    print(f"   Sales:        {len(sales)}")
    print(f"   Competitors:  {len(competitors)}")

    return {
        "companies": companies,
        "brands": brands,
        "categories": categories,
        "subcategories": subcategories,
        "products": products,
        "zones": zones_data,
        "regions": regions_data,
        "distributors": distributors,
        "sales": sales,
        "competitors": competitors
    }


# ─────────────────────────────────────────
#  MYSQL LOADER
# ─────────────────────────────────────────

def load_to_mysql(data):
    try:
        import mysql.connector
    except ImportError:
        print("❌ mysql-connector-python not installed. Run: pip install mysql-connector-python")
        return

    print("\n🔄 Connecting to MySQL...")
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    print("🔄 Creating MySQL tables...")

    cursor.executescript = lambda s: [cursor.execute(stmt.strip()) for stmt in s.split(";") if stmt.strip()]

    # Create tables
    tables_sql = """
    CREATE TABLE IF NOT EXISTS companies (
        company_id VARCHAR(36) PRIMARY KEY,
        name VARCHAR(100),
        hq VARCHAR(100),
        founded_year INT,
        annual_revenue_cr DECIMAL(12,2),
        country VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS brands (
        brand_id VARCHAR(36) PRIMARY KEY,
        company_id VARCHAR(36),
        company_name VARCHAR(100),
        brand_name VARCHAR(100),
        segment VARCHAR(50),
        launch_year INT,
        FOREIGN KEY (company_id) REFERENCES companies(company_id)
    );

    CREATE TABLE IF NOT EXISTS categories (
        category_id VARCHAR(36) PRIMARY KEY,
        category_name VARCHAR(100)
    );

    CREATE TABLE IF NOT EXISTS subcategories (
        subcategory_id VARCHAR(36) PRIMARY KEY,
        category_id VARCHAR(36),
        subcategory_name VARCHAR(100),
        FOREIGN KEY (category_id) REFERENCES categories(category_id)
    );

    CREATE TABLE IF NOT EXISTS products (
        product_id VARCHAR(36) PRIMARY KEY,
        brand_id VARCHAR(36),
        brand_name VARCHAR(100),
        category_id VARCHAR(36),
        subcategory_id VARCHAR(36),
        sku_name VARCHAR(200),
        barcode VARCHAR(50),
        weight_variant VARCHAR(20),
        mrp DECIMAL(10,2),
        cost_price DECIMAL(10,2),
        launch_date DATE,
        is_active BOOLEAN,
        FOREIGN KEY (brand_id) REFERENCES brands(brand_id)
    );

    CREATE TABLE IF NOT EXISTS zones (
        zone_id VARCHAR(36) PRIMARY KEY,
        zone_name VARCHAR(50)
    );

    CREATE TABLE IF NOT EXISTS regions (
        region_id VARCHAR(36) PRIMARY KEY,
        zone_id VARCHAR(36),
        zone_name VARCHAR(50),
        region_name VARCHAR(100),
        state VARCHAR(100),
        population_lakh DECIMAL(8,1),
        FOREIGN KEY (zone_id) REFERENCES zones(zone_id)
    );

    CREATE TABLE IF NOT EXISTS distributors (
        distributor_id VARCHAR(36) PRIMARY KEY,
        zone_id VARCHAR(36),
        region_id VARCHAR(36),
        zone_name VARCHAR(50),
        region_name VARCHAR(100),
        distributor_name VARCHAR(200),
        contact_person VARCHAR(100),
        phone VARCHAR(50),
        email VARCHAR(100),
        address TEXT,
        channel VARCHAR(50),
        credit_limit DECIMAL(12,2),
        onboarding_date DATE,
        is_active BOOLEAN,
        FOREIGN KEY (zone_id) REFERENCES zones(zone_id)
    );

    CREATE TABLE IF NOT EXISTS sales (
        sale_id VARCHAR(36) PRIMARY KEY,
        product_id VARCHAR(36),
        sku_name VARCHAR(200),
        brand_name VARCHAR(100),
        distributor_id VARCHAR(36),
        region_id VARCHAR(36),
        zone_id VARCHAR(36),
        zone_name VARCHAR(50),
        region_name VARCHAR(100),
        channel VARCHAR(50),
        units_sold INT,
        mrp DECIMAL(10,2),
        discount_pct DECIMAL(5,2),
        revenue DECIMAL(12,2),
        cogs DECIMAL(12,2),
        quarter VARCHAR(5),
        year INT,
        sale_date DATE,
        FOREIGN KEY (product_id) REFERENCES products(product_id)
    );

    CREATE TABLE IF NOT EXISTS competitors (
        competitor_id VARCHAR(36) PRIMARY KEY,
        competitor_company VARCHAR(100),
        competitor_brand VARCHAR(100),
        zone_name VARCHAR(50),
        zone_id VARCHAR(36),
        category VARCHAR(100),
        estimated_market_share_pct DECIMAL(5,2),
        estimated_revenue_cr DECIMAL(10,2),
        avg_mrp DECIMAL(10,2),
        distribution_reach_pct DECIMAL(5,2),
        quarter VARCHAR(5),
        year INT,
        data_source VARCHAR(50)
    );
    """

    for stmt in tables_sql.split(";"):
        stmt = stmt.strip()
        if stmt:
            cursor.execute(stmt)
    conn.commit()

    def bulk_insert(table, rows, columns):
        if not rows:
            return
        placeholders = ", ".join(["%s"] * len(columns))
        col_str = ", ".join(columns)
        sql = f"INSERT IGNORE INTO {table} ({col_str}) VALUES ({placeholders})"
        values = [[row[c] for c in columns] for row in rows]
        cursor.executemany(sql, values)
        conn.commit()
        print(f"   ✅ {table}: {len(rows)} rows inserted")

    print("🔄 Inserting data into MySQL...")
    bulk_insert("companies",    data["companies"],    ["company_id","name","hq","founded_year","annual_revenue_cr","country"])
    bulk_insert("brands",       data["brands"],       ["brand_id","company_id","company_name","brand_name","segment","launch_year"])
    bulk_insert("categories",   data["categories"],   ["category_id","category_name"])
    bulk_insert("subcategories",data["subcategories"],["subcategory_id","category_id","subcategory_name"])
    bulk_insert("products",     data["products"],     ["product_id","brand_id","brand_name","category_id","subcategory_id","sku_name","barcode","weight_variant","mrp","cost_price","launch_date","is_active"])
    bulk_insert("zones",        data["zones"],        ["zone_id","zone_name"])
    bulk_insert("regions",      data["regions"],      ["region_id","zone_id","zone_name","region_name","state","population_lakh"])
    bulk_insert("distributors", data["distributors"], ["distributor_id","zone_id","region_id","zone_name","region_name","distributor_name","contact_person","phone","email","address","channel","credit_limit","onboarding_date","is_active"])
    bulk_insert("sales",        data["sales"],        ["sale_id","product_id","sku_name","brand_name","distributor_id","region_id","zone_id","zone_name","region_name","channel","units_sold","mrp","discount_pct","revenue","cogs","quarter","year","sale_date"])
    bulk_insert("competitors",  data["competitors"],  ["competitor_id","competitor_company","competitor_brand","zone_name","zone_id","category","estimated_market_share_pct","estimated_revenue_cr","avg_mrp","distribution_reach_pct","quarter","year","data_source"])

    cursor.close()
    conn.close()
    print("✅ MySQL loading complete!")


# ─────────────────────────────────────────
#  NEO4J LOADER (Schema + Relationships)
# ─────────────────────────────────────────

def load_to_neo4j(data):
    try:
        from neo4j import GraphDatabase
    except ImportError:
        print("❌ neo4j not installed. Run: pip install neo4j")
        return

    print("\n🔄 Connecting to Neo4j...")
    driver = GraphDatabase.driver(NEO4J_CONFIG["uri"], auth=(NEO4J_CONFIG["user"], NEO4J_CONFIG["password"]))

    def run(tx, query, **params):
        tx.run(query, **params)

    with driver.session() as session:
        print("🔄 Creating Neo4j schema (nodes + relationships)...")

        # Constraints
        for constraint in [
            "CREATE CONSTRAINT IF NOT EXISTS FOR (c:Company)     REQUIRE c.company_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (b:Brand)       REQUIRE b.brand_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Product)     REQUIRE p.product_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (cat:Category)  REQUIRE cat.category_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (z:Zone)        REQUIRE z.zone_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (r:Region)      REQUIRE r.region_id IS UNIQUE",
            "CREATE CONSTRAINT IF NOT EXISTS FOR (d:Distributor) REQUIRE d.distributor_id IS UNIQUE",
        ]:
            session.run(constraint)

        # Companies
        for row in data["companies"]:
            session.execute_write(run, """
                MERGE (c:Company {company_id: $company_id})
                SET c.name = $name, c.hq = $hq, c.founded_year = $founded_year
            """, **row)

        # Brands + OWNS relationship
        for row in data["brands"]:
            session.execute_write(run, """
                MERGE (b:Brand {brand_id: $brand_id})
                SET b.name = $brand_name, b.segment = $segment
                WITH b
                MATCH (c:Company {company_id: $company_id})
                MERGE (c)-[:OWNS]->(b)
            """, **row)

        # Categories
        for row in data["categories"]:
            session.execute_write(run, """
                MERGE (cat:Category {category_id: $category_id})
                SET cat.name = $category_name
            """, **row)

        # Subcategories
        for row in data["subcategories"]:
            session.execute_write(run, """
                MERGE (sub:SubCategory {subcategory_id: $subcategory_id})
                SET sub.name = $subcategory_name
                WITH sub
                MATCH (cat:Category {category_id: $category_id})
                MERGE (cat)-[:HAS_SUBCATEGORY]->(sub)
            """, **row)

        # Products + HAS_SKU + BELONGS_TO
        for row in data["products"]:
            session.execute_write(run, """
                MERGE (p:Product {product_id: $product_id})
                SET p.name = $sku_name, p.mrp = $mrp, p.weight = $weight_variant, p.is_active = $is_active
                WITH p
                MATCH (b:Brand {brand_id: $brand_id})
                MERGE (b)-[:HAS_SKU]->(p)
                WITH p
                MATCH (cat:Category {category_id: $category_id})
                MERGE (p)-[:BELONGS_TO]->(cat)
            """, **row)

        # Zones
        for row in data["zones"]:
            session.execute_write(run, """
                MERGE (z:Zone {zone_id: $zone_id})
                SET z.name = $zone_name
            """, **row)

        # Regions + PART_OF
        for row in data["regions"]:
            session.execute_write(run, """
                MERGE (r:Region {region_id: $region_id})
                SET r.name = $region_name, r.state = $state
                WITH r
                MATCH (z:Zone {zone_id: $zone_id})
                MERGE (r)-[:PART_OF]->(z)
            """, **row)

        # Distributors + OPERATES_IN
        for row in data["distributors"]:
            session.execute_write(run, """
                MERGE (d:Distributor {distributor_id: $distributor_id})
                SET d.name = $distributor_name, d.channel = $channel, d.is_active = $is_active
                WITH d
                MATCH (r:Region {region_id: $region_id})
                MERGE (d)-[:OPERATES_IN]->(r)
            """, **row)

        # Product → Region SOLD_IN (aggregated, not per sale row)
        print("🔄 Creating SOLD_IN relationships (aggregated)...")
        sold_in = {}
        for s in data["sales"]:
            key = (s["product_id"], s["region_id"])
            if key not in sold_in:
                sold_in[key] = {"units": 0, "revenue": 0}
            sold_in[key]["units"]   += s["units_sold"]
            sold_in[key]["revenue"] += s["revenue"]

        for (pid, rid), agg in sold_in.items():
            session.execute_write(run, """
                MATCH (p:Product {product_id: $pid})
                MATCH (r:Region  {region_id:  $rid})
                MERGE (p)-[rel:SOLD_IN]->(r)
                SET rel.total_units   = $units,
                    rel.total_revenue = $revenue
            """, pid=pid, rid=rid, units=agg["units"], revenue=round(agg["revenue"], 2))

    driver.close()
    print("✅ Neo4j schema + relationships created!")


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────

if __name__ == "__main__":
    data = generate_all_data()

    print("\n" + "="*50)
    print("Choose what to load:")
    print("  1 → MySQL only")
    print("  2 → Neo4j only")
    print("  3 → Both MySQL + Neo4j")
    choice = input("Enter choice (1/2/3): ").strip()

    if choice == "1":
        load_to_mysql(data)
    elif choice == "2":
        load_to_neo4j(data)
    elif choice == "3":
        load_to_mysql(data)
        load_to_neo4j(data)
    else:
        print("Invalid choice. Exiting.")

    print("\n🎉 Done! Your FMCG data is ready.")