export const SENDER = {
  BOT: "bot",
  USER: "user",
};

export const THINKING_STEPS = [
  "Parsing your query...",
  "Searching data sources...",
  "Running aggregations...",
  "Formatting response...",
];

export const INITIAL_MESSAGES = [
  {
    id: 1,
    sender: SENDER.BOT,
    text: "Hi! I'm Talk2Data. Connect your data source and ask me anything.",
    time: "9:00 AM",
  },
];

export const STREAM_INTERVAL_MS = 55;
export const THINKING_STEP_INTERVAL_MS = 700;
