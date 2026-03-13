import phoenix as px
from openinference.instrumentation.langchain import LangChainInstrumentor
from agents.supervisor_agent import create_supervisor_agent
import time
import os

def main():
    # Start Phoenix for observability
    print("🔭 Starting Observability Session (Arize Phoenix)...")
    session = px.launch_app()
    print(f"✅ Phoenix Web UI available at: {session.url}")
    
    # Instrument LangChain
    LangChainInstrumentor().instrument()
    
    print("🚀 FMCG BI AI Chatbot Starting...")
    print("Ensure MCP Simulator is running on http://localhost:8000")
    
    query = input("\nEnter your business query: ")
    
    agent_executor = create_supervisor_agent()
    
    try:
        result = agent_executor.invoke(
            {
                "input": query
            }
        )
        print("\n" + "="*50)
        print("Final Assistant Output:")
        print(result.get("output", "No output returned."))
        print("="*50)
        
        print(f"\n🔗 View Traces at: {session.url}")
        input("\nPress Enter to close observability session...")
        
    except Exception as e:
        print(f"❌ Error during execution: {str(e)}")

if __name__ == "__main__":
    main()