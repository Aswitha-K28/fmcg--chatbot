import { useState } from "react";
import { useDispatch, useSelector } from "react-redux";
import { toggleTheme } from "../../store/themeSlice";
import styles from "./Header.module.css";

const Header = () => {
  const dispatch = useDispatch();
  const isDark = useSelector((state) => state.theme.isDark);
  const [showKeyInput, setShowKeyInput] = useState(false);
  const [apiKey, setApiKey] = useState(localStorage.getItem("openai_api_key") || "");

  const handleSaveKey = () => {
    if (apiKey.trim()) {
      localStorage.setItem("openai_api_key", apiKey.trim());
      alert("API key saved!");
    }
    setShowKeyInput(false);
  };

  return (
    <header className={styles.header}>
      <div className={styles.brand}>
        <span className={styles.logo}>📊</span>
        <h1 className={styles.title}>Talk2Data</h1>
      </div>

      <nav className={styles.nav}>
        <button className={styles.navBtn} onClick={() => dispatch(toggleTheme())}>
          {isDark ? "☀️ Light" : "🌙 Dark"}
        </button>

        <a
          className={styles.navBtn}
          href="https://github.com"
          target="_blank"
          rel="noopener noreferrer"
        >
          GitHub
        </a>

        <div className={styles.keyWrapper}>
          <button
            className={`${styles.navBtn} ${styles.keyBtn}`}
            onClick={() => setShowKeyInput((v) => !v)}
          >
            🔑 API Key
          </button>

          {showKeyInput && (
            <div className={styles.keyDropdown}>
              <input
                type="password"
                className={styles.keyInput}
                placeholder="sk-..."
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && handleSaveKey()}
                autoFocus
              />
              <button className={styles.saveBtn} onClick={handleSaveKey}>
                Save
              </button>
            </div>
          )}
        </div>
      </nav>
    </header>
  );
};

export default Header;
