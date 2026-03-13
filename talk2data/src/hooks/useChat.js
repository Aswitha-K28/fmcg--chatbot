import { useDispatch, useSelector } from "react-redux";
import {
  addMessage,
  updateLastBotMessage,
  setIsTyping,
  appendThinkingStep,
  clearThinkingSteps,
  setError,
  clearError,
} from "../store/chatSlice";
import { streamChat } from "../services/backendService";
import { getFormattedTime, generateId } from "../utils/time";
import { SENDER } from "../constants";

const useChat = () => {
  const dispatch = useDispatch();
  const { messages, isTyping, thinkingSteps, error } = useSelector(
    (state) => state.chat
  );

  const sendMessage = async (inputText) => {
    if (!inputText.trim()) return;

    dispatch(clearError());
    dispatch(clearThinkingSteps());

    // Add user message
    dispatch(
      addMessage({
        id: generateId(),
        sender: SENDER.USER,
        text: inputText.trim(),
        time: getFormattedTime(),
      })
    );

    dispatch(setIsTyping(true));

    // Add empty bot message shell (will be updated via SSE)
    const botMsgId = generateId();
    dispatch(
      addMessage({
        id: botMsgId,
        sender: SENDER.BOT,
        text: "Analyzing your request...",
        time: getFormattedTime(),
      })
    );

    try {
      streamChat(
        inputText.trim(),
        (event) => {
          if (event.type === "thinking") {
            dispatch(appendThinkingStep(`⚙️ Using ${event.step}...`));
          } else if (event.type === "content") {
            dispatch(setIsTyping(false));
            dispatch(updateLastBotMessage(event.text));
          } else if (event.type === "done") {
            dispatch(setIsTyping(false));
            dispatch(clearThinkingSteps());
          } else if (event.type === "error") {
            dispatch(setError(event.message));
            dispatch(setIsTyping(false));
          }
        },
        (err) => {
          console.error("Stream Error:", err);
          dispatch(setError("Connection to backend failed."));
          dispatch(setIsTyping(false));
        }
      );
    } catch (err) {
      dispatch(setError(err.message));
      dispatch(setIsTyping(false));
    }
  };

  return { messages, isTyping, thinkingSteps, error, sendMessage };
};

export default useChat;
