from agents.supervisor_agent import create_supervisor_agent

try:
    executor = create_supervisor_agent()
    print(f"Executor: {executor}")
    print(f"Agent: {executor.agent}")
except Exception as e:
    print(f"Error: {e}")
