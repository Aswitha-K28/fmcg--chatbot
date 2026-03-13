# Talk2Data 📊

AI-powered data analysis chat app built with React 18, Redux Toolkit, React Router, and Parcel.

## Tech Stack

| Tool | Purpose |
|---|---|
| React 18 | UI library |
| Redux Toolkit | Global state (chat + theme) |
| React Router v6 | Client-side routing |
| Axios | API requests to OpenAI |
| Parcel | Zero-config bundler |
| CSS Modules | Scoped component styles |

## Getting Started

```bash
# 1. Install dependencies
npm install

# 2. Add your OpenAI key (or enter it via the UI at runtime)
cp .env.example .env
# Edit .env and set OPENAI_API_KEY=sk-...

# 3. Start dev server
npm start
# Opens at http://localhost:1234
```

## Project Structure

```
src/
├── components/
│   ├── Header/
│   │   ├── Header.jsx          # Sticky header with theme toggle + API key input
│   │   └── Header.module.css
│   ├── Chat/
│   │   ├── ChatPage.jsx        # Main chat page, orchestrates all chat UI
│   │   ├── ChatMessage.jsx     # Single message bubble (bot/user)
│   │   ├── ChatInput.jsx       # Textarea + send button
│   │   └── *.module.css
│   ├── ThinkingLog/
│   │   ├── ThinkingLog.jsx     # Animated step-by-step thinking display
│   │   └── ThinkingLog.module.css
│   └── Skeleton/
│       ├── Skeleton.jsx        # Shimmer placeholder while loading
│       └── Skeleton.module.css
├── hooks/
│   └── useChat.js              # All chat logic: send, stream, thinking steps
├── services/
│   └── openai.js               # Axios client + fetchChatCompletion()
├── store/
│   ├── store.js                # Redux store
│   ├── chatSlice.js            # Messages, typing, thinking state
│   └── themeSlice.js           # Light/dark toggle
├── constants/
│   └── index.js                # SENDER, THINKING_STEPS, timing constants
├── utils/
│   └── time.js                 # getFormattedTime, generateId
├── App.jsx                     # Root component with Router + theme class
├── index.js                    # ReactDOM.createRoot, Provider, BrowserRouter
├── index.css                   # Global styles + CSS variable tokens
└── index.html                  # HTML entry point
```

## Features

- **Token streaming** — bot replies appear word by word
- **Thinking log** — animated steps shown before each reply
- **Skeleton loading** — shimmer placeholder on first load
- **Light / Dark mode** — toggled via header, persisted in Redux
- **API key input** — enter your OpenAI key at runtime via the header
- **Demo mode** — works without an API key for local testing
- **Error handling** — shows inline error banner on API failure
- **CSS Modules** — zero class name collisions across components
- **Redux Toolkit** — clean slice-based state management

## Build for Production

```bash
npm run build
# Output goes to dist/
```
