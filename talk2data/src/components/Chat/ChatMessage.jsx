import { SENDER } from "../../constants";
import styles from "./ChatMessage.module.css";

const ChatMessage = ({ message }) => {
  const isBot = message.sender === SENDER.BOT;

  return (
    <div className={`${styles.row} ${isBot ? styles.bot : styles.user}`}>
      {isBot && (
        <div className={`${styles.avatar} ${styles.botAvatar}`}>AI</div>
      )}

      <div className={styles.bubbleWrap}>
        <div className={`${styles.bubble} ${isBot ? styles.botBubble : styles.userBubble}`}>
          {message.text || <span className={styles.cursor}>▍</span>}
        </div>
        <span className={styles.time}>{message.time}</span>
      </div>

      {!isBot && (
        <div className={`${styles.avatar} ${styles.userAvatar}`}>U</div>
      )}
    </div>
  );
};

export default ChatMessage;
