#  Intent and Entity Extraction
 
import json
import os
from dataclasses import dataclass, field
from typing import Optional
from groq import Groq
from dotenv import load_dotenv
 
load_dotenv()
 
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "")
GROQ_MODEL   = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile")
_client      = Groq(api_key=GROQ_API_KEY)
 
# DATA MODELS
 
@dataclass
class Entity:
    type:       str
    value:      str
    normalized: str
    category:   str
 
@dataclass
class Intent:
    pattern:     str
    direction:   str
    limit:       Optional[int]
    group_by:    str
    aggregation: str
    domain:      str
    confidence:  float
    query_type:  str
 
@dataclass
class ExtractionResult:
    intent:   Intent
    entities: list[Entity] = field(default_factory=list)
 
# SYSTEM PROMPT
 
SYSTEM_PROMPT = """
You are a Senior Business Intelligence Analyst.
Your only job is to read a business question and extract two things:
  1. INTENT  — what the user wants to achieve
  2. ENTITIES — the specific business dimensions they mentioned
 
Rules you must always follow:
  - Extract ONLY what is explicitly present in the question
  - Never assume, invent, or add anything not mentioned in the question
  - Every extracted value must come directly from the question text
  - Respond ONLY in valid JSON — no explanation, no markdown, no extra text
"""
 
# EXTRACTION PROMPT
 
EXTRACTION_PROMPT = """
Analyze the business question below and extract the Intent and Entities.
 
ROLE     : {role}
QUESTION : "{question}"
 
INTENT EXTRACTION
 
Carefully read the question and determine the following:
 
1. PATTERN
   What type of analysis is the user asking for?
   Choose exactly one from the following:
 
   RANKING_QUERY
     User wants to rank or order items by a measure.
     Look for: top, bottom, best, worst, highest, lowest, rank, leading
 
   TREND_QUERY
     User wants to see how something changes over time.
     Look for: trend, growth, decline, change, increase, decrease,
               over time, month by month, quarter by quarter
 
   COMPARISON_QUERY
     User wants to compare two or more things against each other.
     Look for: compare, vs, versus, difference, which is better,
               how does X compare to Y
 
   SUMMARY_QUERY
     User wants a total, average or overall view of something.
     Look for: total, overall, how much, what is, aggregate, sum, average
 
   SENTIMENT_QUERY
     User wants to know how people feel or what they think.
     Look for: sentiment, feedback, opinion, rating, review, perception,
               what do customers think, satisfaction, NPS
 
   ROOT_CAUSE_QUERY
     User wants to understand why something happened.
     Look for: why, reason, cause, what caused, what happened, explain
 
   FORECAST_QUERY
     User wants a prediction or future projection.
     Look for: forecast, predict, project, next period, expected, outlook
 
2. DIRECTION
   DESC → user wants highest, best, top, most
   ASC  → user wants lowest, worst, bottom, least
   If neither applies, default to DESC.
 
3. LIMIT
   If the user mentions a specific number of results, extract that number.
   If no number is mentioned, set null.
 
4. GROUP_BY
   Read the question carefully and identify what the results should be 
   broken down by or listed per.
 
   Step 1 — Find the primary subject:
     Ask: "One [WHAT] per row in the result?"
     That [WHAT] is the group_by.
 
   Step 2 — Rules:
     - Extract it in singular form exactly as implied by the question
     - If the question groups by time (month, quarter, year) → use that time unit
     - If no grouping is implied → set null
     - Never use a metric as group_by (metrics are what you measure)
     - Never use a filter value as group_by (filters narrow the data, not group it)
     - Never invent or assume a group_by not implied by the question
 
   Step 3 — Verify:
     Ask: "Does each row in the result represent one [group_by]?"
     If yes → correct. If no → reconsider.
     
5. AGGREGATION
   How should the metric be calculated?
   SUM   → when question is about totals or cumulative values
   AVG   → when question is about averages, rates or scores
   COUNT → when question is about counting items or occurrences
   Derive this from the metric and context in the question.
 
6. DOMAIN
   What is the primary business area this question is about?
   Derive this entirely from the question — do not use a fixed list.
   Use your understanding of the question to name the business area.
 
7. CONFIDENCE
   How clearly and completely is the question expressed?
   1.0 → every dimension is clearly stated, no ambiguity
   0.7 → mostly clear, minor ambiguity
   0.5 → moderately clear, some dimensions are implied
   0.4 → vague but something can be extracted
   Below 0.4 → too vague to extract meaningful information
 
8. QUERY_TYPE
   STANDARD   → a fresh standalone question
   FOLLOW_UP  → the question refers to a previous question
                look for: they, them, their, same, what about, now show
   ROOT_CAUSE → the question asks why something happened
   FORECAST   → the question asks about future values
   EXPORT     → the question asks to download or save data
 
ENTITY EXTRACTION
 
Extract every specific business dimension mentioned in the question.
For each entity provide:
 
  type       → classify what kind of business dimension it is
  value      → copy the exact words from the question as written
  normalized → the clean, standard form of that value
  category   → the broader business area this entity belongs to
 
Use these entity types to classify what you find:
 
  metric     → any measurable business value or KPI
  brand      → any product brand name
  company    → any parent organization or business entity
  product    → any specific product or item
  segment    → any product category, subcategory or market segment
  geography  → any location reference mentioned in the question
  time       → any time reference mentioned in the question
  channel    → any sales, distribution or communication channel mentioned
  ranking    → any ranking or ordering reference mentioned
  comparison → any explicit comparison between two or more items mentioned
  sentiment  → any reference to opinion, feedback or customer perception
 
Strict extraction rules:
  - Extract ONLY dimensions that are explicitly stated in the question
  - value must be copied word for word from the question — never paraphrase
  - normalized must be the clean standard version — remove filler words
  - category must reflect the business area of that entity based on the question
  - If a dimension appears more than once, extract each instance separately
  - Never extract the same entity twice
 
RESPOND WITH ONLY THIS JSON — NO TEXT BEFORE OR AFTER:
 
{{
  "intent": {{
    "pattern"    : "<one pattern from the 7 above>",
    "direction"  : "<DESC or ASC>",
    "limit"      : <integer or null>,
    "group_by"   : "<primary subject noun in singular form or null>",
    "aggregation": "<SUM or AVG or COUNT>",
    "domain"     : "<business area derived from the question>",
    "confidence" : <0.0 to 1.0>,
    "query_type" : "<STANDARD or FOLLOW_UP or ROOT_CAUSE or FORECAST or EXPORT>"
  }},
  "entities": [
    {{
      "type"      : "<entity type from the list above>",
      "value"     : "<exact words from the question>",
      "normalized": "<clean standard form>",
      "category"  : "<business area this entity belongs to>"
    }}
  ]
}}
"""
 
# QUERY TYPE DETECTOR
 
QUERY_KEYWORDS = {
    "EXPORT":      ["export", "download", "save as", "excel", "pdf", "ppt"],
    "ROOT_CAUSE":  ["why", "reason", "cause", "what caused", "what happened", "explain"],
    "FORECAST":    ["forecast", "predict", "next quarter", "next month", "next year",
                    "projection", "expected", "outlook"],
    "FOLLOW_UP":   ["they", "them", "their", "same zone", "same brand",
                    "what about", "now show"],
}
 
def detect_query_type(question: str) -> str:
    q = question.lower().strip()
    for qtype, keywords in QUERY_KEYWORDS.items():
        if any(w in q for w in keywords):
            return qtype
    return "STANDARD"
 
# GROQ API CALL
 
def _call_groq(prompt: str) -> str:
    response = _client.chat.completions.create(
        model    = GROQ_MODEL,
        messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user",   "content": prompt}
        ]
    )
    return response.choices[0].message.content.strip()
 
# EXTRACT FUNCTION
 
def extract(question: str, role: str = "Business Analyst") -> ExtractionResult:
    query_type = detect_query_type(question)
    prompt     = EXTRACTION_PROMPT.format(role=role, question=question)
    raw        = _call_groq(prompt)
 
    try:
        clean = raw.replace("```json", "").replace("```", "").strip()
        data  = json.loads(clean)
    except json.JSONDecodeError:
        return _clarification()
 
    confidence  = float(data.get("intent", {}).get("confidence", 0.0))
    intent_data = data.get("intent", {})
 
    intent = Intent(
        pattern     = intent_data.get("pattern",     "SUMMARY_QUERY"),
        direction   = intent_data.get("direction",   "DESC"),
        limit       = intent_data.get("limit"),
        group_by    = intent_data.get("group_by",    ""),
        aggregation = intent_data.get("aggregation", "SUM"),
        domain      = intent_data.get("domain",      ""),
        confidence  = confidence,
        query_type  = query_type
    )
 
    entities = [
        Entity(
            type       = e.get("type",       ""),
            value      = e.get("value",      ""),
            normalized = e.get("normalized", ""),
            category   = e.get("category",   "")
        )
        for e in data.get("entities", [])
    ]
 
    return ExtractionResult(intent=intent, entities=entities)
 
 
def _clarification() -> ExtractionResult:
    return ExtractionResult(
        intent=Intent(
            pattern="CLARIFICATION_NEEDED", direction="DESC", limit=None,
            group_by="", aggregation="SUM", domain="", confidence=0.0,
            query_type="STANDARD"
        )
    )
 
# PRINT OUTPUT
 
def run(question: str, role: str = "Business Analyst"):
 
    result = extract(question, role)
    i      = result.intent
 
    print(f"  QUESTION  :  {question}")
    print(f"  ROLE      :  {role}")
 
    print("\n  INTENT")
    print(f"  Pattern      :  {i.pattern}")
    print(f"  Domain       :  {i.domain}")
    print(f"  Direction    :  {i.direction}")
    print(f"  Limit        :  {i.limit}")
    print(f"  Group By     :  {i.group_by}")
    print(f"  Aggregation  :  {i.aggregation}")
    print(f"  Query Type   :  {i.query_type}")
    print(f"  Confidence   :  {i.confidence}")
 
    print("\n  ENTITIES")
    if result.entities:
        for idx, e in enumerate(result.entities, 1):
            print(f"\n  [{idx}]  Type        :  {e.type}")
            print(f"        Value       :  {e.value}")
            print(f"        Normalized  :  {e.normalized}")
            print(f"        Category    :  {e.category}")
    else:
        print("  No entities extracted")
 
    if i.pattern == "CLARIFICATION_NEEDED":
        print("\n  Question is too vague — please clarify.\n")
    else:
        print(f"\n  Extraction complete — {len(result.entities)} entities found\n")
 
# RUN
 
if __name__ == "__main__":
    run("top 10 distributors by units sold in Hyderabad")