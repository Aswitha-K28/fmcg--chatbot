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
    
    system_msg = """
    You are an expert BI Supervisor for FMCG data.
    Goal: Resolve natural language queries into insights using these tools.
    
    STEPS:
    1. Extract intent and entities using `intent_entity_tool`.
    2. Resolve entities and find relevant tables using `schema_search_tool`.
    3. Generate the required Malloy DSL using `malloy_generator_tool`.
    4. Execute the Malloy using `malloy_executor_tool`.
    5. Once you have the numerical results (success or failure), present the final answer to the user and STOP.
    
    CRITICAL: 
    - Do NOT call the same tool with the same input twice.
    - If a tool returns results, do not repeat the search.
    - If the user asks for a specific region or brand, ensure you join the relevant tables.
    """

    prompt = ChatPromptTemplate.from_messages([
        ("system", system_msg),
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