from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from services.llm_service import LLMService
from services.search_service import SearchService
import uvicorn
import json

app = FastAPI(title="FMCG BI MCP Simulator")
llm_service = LLMService()
search_service = SearchService()

@app.on_event("startup")
async def startup():
    search_service.build_index()

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
        entities = intent_json.get("entities", {})
        
        kw_results = search_service.keyword_search(request.query)
        graph_results = search_service.graph_search(entities)
        
        return {
            "keyword": kw_results,
            "graph": graph_results,
            "entities": entities
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tools/generate_malloy")
async def generate_malloy(request: MalloyRequest):
    try:
        # Malloy Generation logic
        schema_context = "Source: brands, products, sales, regions, categories." # Simplified for MVP
        malloy_code = llm_service.generate_malloy(
            request.query, 
            schema_context, 
            request.context
        )
        return {"malloy_code": malloy_code}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
