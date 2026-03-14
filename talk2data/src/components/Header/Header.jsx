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
        <h1 className={styles.title}>Talk2Data</h1>
      </div>
    </header>
  );
};

export default Header;
