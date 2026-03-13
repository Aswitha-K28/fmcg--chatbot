import styles from "./ThinkingLog.module.css";

const ThinkingLog = ({ steps }) => {
  if (!steps.length) return null;

  return (
    <div className={styles.log}>
      <span className={styles.label}>Thinking</span>
      {steps.map((step, i) => (
        <div key={i} className={styles.step}>
          <span className={styles.dot} />
          <span className={styles.text}>{step}</span>
        </div>
      ))}
    </div>
  );
};

export default ThinkingLog;
