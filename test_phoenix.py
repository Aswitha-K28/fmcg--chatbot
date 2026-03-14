import phoenix as px
from openinference.instrumentation.langchain import LangChainInstrumentor
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage
import os
from dotenv import load_dotenv

load_dotenv()

from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain.tools import tool
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder

# --- OTEL INSTRUMENTATION MUST BE FIRST ---
LangChainInstrumentor().instrument()

@tool
def get_weather(city: str) -> str:
    """Get the weather for a city."""
    return f"The weather in {city} is sunny."

print("Testing tool trace export to Phoenix...")
llm = ChatGroq(api_key=os.getenv("GROQ_API_KEY"), model_name="llama-3.3-70b-versatile")
tools = [get_weather]
prompt = ChatPromptTemplate.from_messages([
    ("system", "You are a helpful assistant."),
    ("human", "{input}"),
    MessagesPlaceholder(variable_name="agent_scratchpad"),
])

agent = create_tool_calling_agent(llm, tools, prompt)
agent_executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

try:
    response = agent_executor.invoke({"input": "What is the weather in San Francisco?"})
    print(f"Agent Response: {response['output']}")
    print("Check http://localhost:6006 for traces.")
except Exception as e:
    print(f"Error: {e}")
