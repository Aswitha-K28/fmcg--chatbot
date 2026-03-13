import { useSelector } from "react-redux";
import { Routes, Route } from "react-router-dom";
import Header from "./components/Header/Header";
import ChatPage from "./components/Chat/ChatPage";

const App = () => {
  const isDark = useSelector((state) => state.theme.isDark);

  return (
    <div className={`app${isDark ? " dark" : ""}`}>
      <Header />
      <Routes>
        <Route path="/" element={<ChatPage />} />
      </Routes>
    </div>
  );
};

export default App;
