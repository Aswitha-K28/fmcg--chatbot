export const getFormattedTime = () =>
  new Date().toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

export const generateId = () =>
  `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
