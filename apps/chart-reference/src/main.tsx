import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

import { App } from "./App";
import "./styles-modern.css";

const root = document.getElementById("root");

if (root == null) {
  throw new Error("Chart reference root element was not found");
}

createRoot(root).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
