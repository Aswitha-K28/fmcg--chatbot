import os
import json
import asyncio
import sys
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from agents.supervisor_agent import create_supervisor_agent
from langchain.callbacks.base import BaseCallbackHandler
from typing import Any, Dict, List

VERSION = "v4.1"

app = FastAPI(title=f"FMCG BI Chatbot Backend {VERSION}")

# Enable CORS for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize agent ONCE at global level
print(f"--- backend_server.py {VERSION} Global Startup ---")
try:
    global_agent = create_supervisor_agent()
    print("✅ Global Agent initialized successfully.")
except Exception as e:
    print(f"❌ Global Agent initialization FAILED: {e}")
    global_agent = None

class StreamThinkingHandler(BaseCallbackHandler):
    def __init__(self, queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.queue = queue
        self.loop = loop

    def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs: Any) -> None:
        tool_name = serialized.get("name", "Tool")
        # Use call_soon_threadsafe to interact with the queue from a different thread
        self.loop.call_soon_threadsafe(
            self.queue.put_nowait, 
            json.dumps({"type": "thinking", "step": tool_name})
        )

@app.post("/chat")
async def chat_endpoint(request: Request):
    data = await request.json()
    user_query = data.get("query")
    print(f"--- New Query ({VERSION}): {user_query} ---")
    
    async def event_generator():
        if global_agent is None:
            yield f"data: {json.dumps({'type': 'error', 'message': 'Agent not initialized'})}\n\n"
            return

        queue = asyncio.Queue()
        loop = asyncio.get_event_loop()
        handler = StreamThinkingHandler(queue, loop)
        
        async def run_agent():
            try:
                # Use the global agent
                # The callback handler will use self.loop to communicate safely
                result = await asyncio.to_thread(
                    global_agent.invoke,
                    {"input": user_query},
                    {"callbacks": [handler]}
                )
                final_answer = result.get("output", "")
                await queue.put(json.dumps({"type": "content", "text": final_answer}))
                await queue.put(json.dumps({"type": "done"}))
            except Exception as e:
                print(f"Agent Execution Error: {e}")
                await queue.put(json.dumps({"type": "error", "message": str(e)}))
            finally:
                await queue.put(None)

        asyncio.create_task(run_agent())

        while True:
            item = await queue.get()
            if item is None:
                break
            yield f"data: {item}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)
