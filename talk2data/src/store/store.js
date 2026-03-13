import { configureStore } from "@reduxjs/toolkit";
import chatReducer from "./chatSlice";
import themeReducer from "./themeSlice";

const store = configureStore({
  reducer: {
    chat: chatReducer,
    theme: themeReducer,
  },
});

export default store;
