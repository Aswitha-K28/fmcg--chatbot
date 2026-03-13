import { useEffect, useRef } from "react";
import useChat from "../../hooks/useChat";
import ChatMessage from "./ChatMessage";
import ChatInput from "./ChatInput";
import ThinkingLog from "../ThinkingLog/ThinkingLog";
import Skeleton from "../Skeleton/Skeleton";
import styles from "./ChatPage.module.css";

const TypingBubble = () => (
  <div className={styles.typingRow}>
    <div className={styles.typingAvatar}>AI</div>
    <div className={styles.typingBubble}>
      <span className={styles.dot} />
      <span className={styles.dot} />
      <span className={styles.dot} />
    </div>
  </div>
);

const ChatPage = () => {
  const { messages, isTyping, thinkingSteps, error, sendMessage } = useChat();
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isTyping, thinkingSteps]);

  return (
    <div className={styles.page}>
      <div className={styles.messages}>
        {messages.map((msg) => (
          <ChatMessage key={msg.id} message={msg} />
        ))}

        {/* Thinking log — shows steps before bot reply */}
        {thinkingSteps.length > 0 && (
          <div className={styles.thinkingWrapper}>
            <div className={styles.thinkingAvatar}>AI</div>
            <ThinkingLog steps={thinkingSteps} />
          </div>
        )}

        {/* Typing dots while waiting */}
        {isTyping && <TypingBubble />}

        {/* Skeleton shimmer while first load */}
        {isTyping && messages.length === 1 && <Skeleton />}

        {/* Error banner */}
        {error && (
          <div className={styles.errorBanner}>
            ⚠️ {error}
          </div>
        )}

        <div ref={bottomRef} />
      </div>

      <ChatInput onSend={sendMessage} disabled={isTyping} />
    </div>
  );
};

export default ChatPage;
