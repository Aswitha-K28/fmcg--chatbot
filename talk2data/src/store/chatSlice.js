import { createSlice } from "@reduxjs/toolkit";
import { INITIAL_MESSAGES } from "../constants";

const chatSlice = createSlice({
  name: "chat",
  initialState: {
    messages: INITIAL_MESSAGES,
    isTyping: false,
    thinkingSteps: [],
    error: null,
  },
  reducers: {
    addMessage(state, action) {
      state.messages.push(action.payload);
    },
    updateLastBotMessage(state, action) {
      const last = [...state.messages].reverse().find((m) => m.sender === "bot");
      if (last) last.text = action.payload;
    },
    setIsTyping(state, action) {
      state.isTyping = action.payload;
    },
    setThinkingSteps(state, action) {
      state.thinkingSteps = action.payload;
    },
    appendThinkingStep(state, action) {
      state.thinkingSteps.push(action.payload);
    },
    clearThinkingSteps(state) {
      state.thinkingSteps = [];
    },
    setError(state, action) {
      state.error = action.payload;
    },
    clearError(state) {
      state.error = null;
    },
  },
});

export const {
  addMessage,
  updateLastBotMessage,
  setIsTyping,
  setThinkingSteps,
  appendThinkingStep,
  clearThinkingSteps,
  setError,
  clearError,
} = chatSlice.actions;

export default chatSlice.reducer;
