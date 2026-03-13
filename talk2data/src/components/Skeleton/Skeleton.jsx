import styles from "./Skeleton.module.css";

const Skeleton = () => (
  <div className={styles.row}>
    <div className={styles.avatar} />
    <div className={styles.bubble}>
      <div className={`${styles.line} ${styles.wide}`} />
      <div className={`${styles.line} ${styles.medium}`} />
      <div className={`${styles.line} ${styles.narrow}`} />
    </div>
  </div>
);

export default Skeleton;
