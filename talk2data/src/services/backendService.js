/**
 * Service to handle interaction with our custom FMCG BI backend.
 * Supports streaming (SSE) for real-time thinking steps and content.
 */
export const streamChat = (query, onEvent, onError) => {
  const url = "http://localhost:8002/chat";
  
  const fetchPromise = fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query }),
  });

  fetchPromise.then(response => {
    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    function read() {
      reader.read().then(({ done, value }) => {
        if (done) return;
        
        const chunk = decoder.decode(value, { stream: true });
        const lines = chunk.split("\n");
        
        for (const line of lines) {
          if (line.startsWith("data: ")) {
            try {
              const data = JSON.parse(line.substring(5));
              onEvent(data);
            } catch (e) {
              console.error("Error parsing SSE line:", line, e);
            }
          }
        }
        read();
      }).catch(onError);
    }
    read();
  }).catch(onError);
};
