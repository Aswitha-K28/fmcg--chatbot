from langchain_groq import ChatGroq
from langchain.agents import AgentExecutor, create_tool_calling_agent
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from utils.config import GROQ_API_KEY, GROQ_MODEL

from tools.intent_tool import intent_entity_tool
from tools.search_tool import schema_search_tool
from tools.malloy_generator_tool import malloy_generator_tool
from tools.malloy_executor_tool import malloy_executor_tool

VERSION = "v4.0"

def create_supervisor_agent():
    print(f"--- supervisor_agent.py {VERSION} Initializing ---")
    if not GROQ_API_KEY:
        raise ValueError("GROQ_API_KEY is not set.")
    
    llm = ChatGroq(
        api_key=GROQ_API_KEY,
        model_name=GROQ_MODEL,
        temperature=0
    )

    tools = [
        intent_entity_tool,
        schema_search_tool,
        malloy_generator_tool,
        malloy_executor_tool
    ]

    prompt = ChatPromptTemplate.from_messages([
        ("system", "You are a senior BI supervisor. Resolve natural language queries into Malloy. Use tools sequentially."),
        ("human", "{input}"),
        MessagesPlaceholder(variable_name="agent_scratchpad"),
    ])
    
    # Back to standard agent but with extreme caution
    try:
        agent = create_tool_calling_agent(llm, tools, prompt)
        print("DEBUG: Agent created via create_tool_calling_agent")
    except Exception as e:
        print(f"DEBUG: create_tool_calling_agent failed, trying manual: {e}")
        # Final fallback: manual construction without RunnablePassthrough.assign
        from langchain.agents.format_scratchpad import format_to_tool_messages
        from langchain.agents.output_parsers import ToolsAgentOutputParser
        
        agent = (
            {
                "input": lambda x: x["input"],
                "agent_scratchpad": lambda x: format_to_tool_messages(x["intermediate_steps"]),
            }
            | prompt
            | llm.bind_tools(tools)
            | ToolsAgentOutputParser()
        )
        print("DEBUG: Agent created via manual dict mapping")

    agent_executor = AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=False,
        handle_parsing_errors=True
    )

    return agent_executor