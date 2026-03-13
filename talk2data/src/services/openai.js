import axios from "axios";

const openaiClient = axios.create({
  baseURL: "https://api.openai.com/v1",
  headers: { "Content-Type": "application/json" },
});

// Attach API key from runtime config before each request
openaiClient.interceptors.request.use((config) => {
  const key = localStorage.getItem("openai_api_key");
  if (key) config.headers.Authorization = `Bearer ${key}`;
  return config;
});

/**
 * Send messages to OpenAI Chat Completions and return the reply text.
 * @param {Array<{role:string, content:string}>} messages
 * @returns {Promise<string>}
 */
export const fetchChatCompletion = async (messages) => {
  const response = await openaiClient.post("/chat/completions", {
    model: "gpt-3.5-turbo",
    messages,
    temperature: 0.7,
    max_tokens: 500,
  });
  return response.data.choices[0].message.content;
};

export default openaiClient;
