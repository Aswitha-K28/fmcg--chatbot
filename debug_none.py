from agents.supervisor_agent import create_supervisor_agent
from tools.intent_tool import intent_entity_tool
from tools.search_tool import schema_search_tool
from tools.malloy_generator_tool import malloy_generator_tool
from tools.malloy_executor_tool import malloy_executor_tool
from langchain_core.runnables import coerce_to_runnable
import json

print("--- Tool Integrity Check ---")
tools = [
    ("intent", intent_entity_tool),
    ("search", schema_search_tool),
    ("malloy_gen", malloy_generator_tool),
    ("malloy_exec", malloy_executor_tool)
]

for name, t in tools:
    print(f"Checking {name}: type={type(t)}")
    if t is None:
        print(f"CRITICAL: {name} is None!")
    else:
        try:
            runnable = coerce_to_runnable(t)
            print(f"  {name} is a valid runnable")
        except Exception as e:
            print(f"  {name} coercion FAILED: {e}")

print("\n--- Agent Creation Check ---")
try:
    agent = create_supervisor_agent()
    print("Agent created successfully!")
except Exception as e:
    print(f"Agent creation failed with error: {e}")
    import traceback
    traceback.print_exc()
