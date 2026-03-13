import { useState } from "react";
import styles from "./ChatInput.module.css";

const SendIcon = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
    <path d="M2 21l21-9L2 3v7l15 2-15 2v7z" />
  </svg>
);

const ChatInput = ({ onSend, disabled }) => {
  const [value, setValue] = useState("");

  const handleSend = () => {
    if (!value.trim() || disabled) return;
    onSend(value);
    setValue("");
  };

  const handleKeyDown = (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  return (
    <div className={styles.bar}>
      <textarea
        className={styles.input}
        placeholder="Ask something about your data…"
        value={value}
        rows={1}
        onChange={(e) => setValue(e.target.value)}
        onKeyDown={handleKeyDown}
        disabled={disabled}
      />
      <button
        className={styles.sendBtn}
        onClick={handleSend}
        disabled={!value.trim() || disabled}
        aria-label="Send message"
      >
        <SendIcon />
      </button>
    </div>
  );
};

export default ChatInput;
