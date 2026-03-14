import requests
import json
import time

url = 'http://localhost:8002/chat'
payload = {'query': 'top 5 brands by revenue', 'session_id': 'test_trace'}

print("--- Requesting top 5 brands by revenue ---")
try:
    with requests.post(url, json=payload, stream=True, timeout=60) as r:
        print(f"Status: {r.status_code}")
        for line in r.iter_lines():
            if line:
                decoded = line.decode('utf-8')
                if decoded.startswith('data: '):
                    try:
                        data = json.loads(decoded[6:])
                        if data['type'] == 'thinking':
                            print(f"[Thinking] {data['step']}")
                        elif data['type'] == 'content':
                            print(f"RESULT: {data['text']}")
                        elif data['type'] == 'done':
                            print("--- Finished ---")
                            break
                    except:
                        pass
except Exception as e:
    print(f"Connection failed: {e}")

print("\n--- Checking Phoenix Traces ---")
try:
    # Give it a second to export
    time.sleep(3)
    resp = requests.get('http://localhost:6006/v1/traces', timeout=5)
    print(f"Phoenix Trace Endpoint Status: {resp.status_code}")
    if resp.status_code == 200:
        print("✅ SUCCESS: Phoenix is reachable and traces should be updated.")
    else:
        print("❌ Phoenix is reachable but returned non-200.")
except:
    print("❌ Phoenix is not reachable.")
