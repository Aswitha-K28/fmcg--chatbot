from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

import os
# --- OTEL INSTRUMENTATION MUST BE FIRST ---
from openinference.instrumentation.langchain import LangChainInstrumentor
try:
    os.environ["PHOENIX_COLLECTOR_HTTP_ENDPOINT"] = "http://localhost:6006/v1/traces"
    LangChainInstrumentor().instrument()
    print("MCP Tool instrumentation active. Exporting to http://localhost:6006/v1/traces")
except Exception as e:
    print(f"MCP Observability initialization failed: {e}")
# ------------------------------------------

from services.llm_service import LLMService
from services.search_service import SearchService
import uvicorn
import json

app = FastAPI(title="FMCG BI MCP Simulator")
llm_service = LLMService()
search_service = SearchService()

@app.on_event("startup")
async def startup():
    # Only build once
    if not search_service.docs:
        search_service.build_index()

# Detailed schema context for legacy/smaller models
FULL_SCHEMA_CONTEXT = """
TABLES & COLUMNS:
- brands (brand_id, brand_name, company_id)
- products (product_id, sku_name, brand_id, category_id, subcategory_id)
- sales (sale_id, product_id, region_id, distributor_id, revenue, units_sold, cogs, discount_pct)
- regions (region_id, region_name, zone_id)
- zones (zone_id, zone_name)
- distributors (distributor_id, distributor_name, region_id)
- categories (category_id, category_name)
- companies (company_id, company_name)
- marketing_campaigns (campaign_id, campaign_name, budget_cr, spend_cr, roi_pct, brand_id, product_id, region_id)
- sales_targets (target_id, target_revenue, achieved_revenue, achievement_pct, brand_id, region_id)

JOINS:
- sales.product_id -> products.product_id
- sales.region_id -> regions.region_id
- sales.distributor_id -> distributors.distributor_id
- products.brand_id -> brands.brand_id
- regions.zone_id -> zones.zone_id
- brands.company_id -> companies.company_id
"""

class QueryRequest(BaseModel):
    query: str

class MalloyRequest(BaseModel):
    query: str
    intent: str = None
    entities: dict = None
    context: str = None

def parse_llm_json(raw):
    content = raw.strip()
    if content.startswith("```json"):
        content = content[7:]
    elif content.startswith("```"):
        content = content[3:]
    if content.endswith("```"):
        content = content[:-3]
    return json.loads(content.strip())

@app.post("/tools/extract_intent")
async def extract_intent(request: QueryRequest):
    try:
        result = llm_service.extract_intent(request.query)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/schematic_search")
async def schematic_search(request: QueryRequest):
    try:
        # Re-extract intent for context if not provided
        intent_raw = llm_service.extract_intent(request.query)
        intent_json = parse_llm_json(intent_raw)
        
        # Call the new parallel search orchestrator
        fused_tables = await search_service.parallel_search(request.query, intent_json, top_k=5)
        
        return {
            "query": request.query,
            "intent_extracted": intent_json,
            "recommended_tables": fused_tables
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/generate_malloy")
async def generate_malloy(request: MalloyRequest):
    try:
        malloy_code = llm_service.generate_malloy(
            request.query, 
            FULL_SCHEMA_CONTEXT, 
            request.context
        )
        return {"malloy_code": malloy_code}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
