import chainlit as cl
import phoenix as px
from openinference.instrumentation.langchain import LangChainInstrumentor
from agents.supervisor_agent import create_supervisor_agent
from langchain.callbacks.base import BaseCallbackHandler
from typing import Any, Dict
import os

# Start Phoenix for observability
print("🔭 Starting Observability Session (Arize Phoenix)...")
try:
    session = px.launch_app()
    print(f"✅ Phoenix Web UI available at: {session.url}")
except Exception as e:
    print(f"⚠️ Could not start Phoenix: {e}")

# Instrument LangChain
LangChainInstrumentor().instrument()

def get_agent():
    """Helper to ensure agent is always available."""
    agent = cl.user_session.get("agent_executor")
    if agent is None:
        print("💡 Initializing agent for new session...")
        agent = create_supervisor_agent()
        cl.user_session.set("agent_executor", agent)
    return agent

@cl.on_chat_start
async def on_chat_start():
    get_agent()
    await cl.Message(content="🚀 **FMCG BI AI Chatbot** is ready! \n(Manual Agent Mode enabled)").send()

@cl.on_message
async def on_message(message: cl.Message):
    agent_executor = get_agent()
    
    # Placeholder for the final answer
    final_msg = cl.Message(content="")
    
    # Custom callback to show thinking blocks
    class ThinkingHandler(BaseCallbackHandler):
        def __init__(self, parent_id):
            self.parent_id = parent_id

        def on_tool_start(self, serialized: Dict[str, Any], input_str: str, **kwargs: Any) -> Any:
            tool_name = serialized.get("name", "Tool")
            cl.run_sync(cl.Message(content=f"⚙️ **Thinking:** Using `{tool_name}`...", parent_id=self.parent_id).send())

    try:
        # LangChain's native callback doesn't always show up well with manual agents
        # so we use a custom one + the standard one
        res = await cl.make_async(agent_executor.invoke)(
            {"input": message.content},
            config={"callbacks": [ThinkingHandler(message.id), cl.LangchainCallbackHandler()]}
        )
        
        final_answer = res.get("output", "No response generated.")
        final_msg.content = final_answer
        await final_msg.send()
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Agent Error: {error_msg}")
        await cl.Message(content=f"❌ **Error:** {error_msg}").send()

if __name__ == "__main__":
    pass
