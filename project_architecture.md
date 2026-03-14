# Project Architecture & Data Flow

This document explains the end-to-end flow of the FMCG BI Chatbot, detailing which files are involved at each stage of a user query.

---

## 1. High-Level Architecture
The system follows a **Hub-and-Spoke** model:
*   **Hub**: The [supervisor_agent.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/agents/supervisor_agent.py) (The Brain) makes all decisions.
*   **Spokes**: The MCP Server and various local tools provide specialized services (Search, Logic, Execution).

---

## 2. The Step-by-Step Request Lifecycle

### Step A: The Frontend (React)
*   **Location**: `talk2data/`
*   **File**: `src/App.js` (or similar component)
*   **Action**: User types a question (e.g., "Top 5 brands in Hyderabad").
*   **Flow**: Sends a POST request to `http://localhost:8002/chat`.

### Step B: The Entry Point (FastAPI)
*   **File**: [backend_server.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/backend_server.py)
*   **Contents**: Sets up the FastAPI server, enables CORS, and initializes the Supervisor Agent.
*   **Action**: Receives the query and creates an **Asynchronous Stream (SSE)** to send "thinking" steps back to the UI.

### Step C: The Decision Engine (Supervisor Agent)
*   **File**: [agents/supervisor_agent.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/agents/supervisor_agent.py)
*   **Contents**: Defines the LangChain agent, the "Decision Tool" (Groq), and the list of available tools.
*   **Action**: Analyzes the query and decides: *"I first need to find which tables support this query. I'll call the search tool."*

### Step D: The Search Orchestrator (MCP Server)
*   **Files**: [tools/search_tool.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/tools/search_tool.py) → [mcp_server.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/mcp_server.py) → [services/search_service.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/services/search_service.py)
*   **Contents**:
    *   [mcp_server.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/mcp_server.py): Acts as a tool provider (Port 8000).
    *   [search_service.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/services/search_service.py): Contains the **Parallel Search** logic (BM25, FAISS, Node2Vec).
*   **Action**:
    1.  **BM25**: Keywords (Exact matches).
    2.  **FAISS**: Semantic (Vector meaning).
    3.  **Node2Vec**: Graph (How tables are connected in Neo4j).
*   **RRF Fusion**: Combines all three into a single ranked list of tables (e.g., `sales`, `brands`).

### Step E: Logic Generation (Malloy Generator)
*   **Files**: [tools/malloy_generator_tool.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/tools/malloy_generator_tool.py)
*   **Contents**: A tool that sends the user's question and the "Retrieved Tables" back to the LLM.
*   **Action**: Generates **Malloy DSL code**. This code is a semantic layer above SQL that understands measures like `total_revenue`.

### Step F: Database Execution (Malloy to SQL)
*   **File**: [tools/malloy_executor_tool.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/tools/malloy_executor_tool.py)
*   **Contents**: The bridge to the production MySQL database.
*   **Action**:
    1.  **Translation**: Uses LLM to translate Malloy DSL into raw MySQL based on the [models/fmcg.malloy](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/models/fmcg.malloy) schema.
    2.  **Execution**: Runs the SQL on MySQL using `mysql-connector-python`.
    3.  **Result**: Returns raw JSON data (Actual names and numbers).

### Step G: Final Response
*   **Flow**: [supervisor_agent.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/agents/supervisor_agent.py) receives the JSON data and formats it into a human-readable sentence.
*   **Result**: [backend_server.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/backend_server.py) streams the final text back to the React UI.

---

## 3. Supporting Infrastructure

| File | Purpose |
| :--- | :--- |
| [.env](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/.env) | Stores your `GROQ_API_KEY` and the `GROQ_MODEL`. |
| [utils/config.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/utils/config.py) | Centralizes database credentials for MySQL and Neo4j. |
| [models/fmcg.malloy](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/models/fmcg.malloy) | The source of truth for your business logic (how Revenue is calculated). |
| `faiss_table_index/` | Pre-computed mathematical groupings of your table descriptions. |
| [test_phoenix.py](file:///c:/Users/katragadda.aswitha/Documents/fmcgchatbot/test_phoenix.py) | A utility to verify that tracing is working without running the whole app. |

---

## 4. Observability (Arize Phoenix)
*   **Endpoint**: `http://localhost:6006`
*   **Logic**: Every file above that starts with `# --- OTEL INSTRUMENTATION ---` sends a "trace" to Phoenix. This allows you to see exactly how long each search took and what JSON was passed between tools.
