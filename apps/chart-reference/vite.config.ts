import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const gatewayProxy = {
  "/v1/candles": {
    target: "http://127.0.0.1:8000",
    changeOrigin: true,
  },
  "/v1/stream": {
    target: "ws://127.0.0.1:8000",
    ws: true,
    changeOrigin: true,
  },
} as const;

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 4173,
    strictPort: true,
    proxy: gatewayProxy,
  },
  preview: {
    host: "127.0.0.1",
    port: 4173,
    strictPort: true,
    proxy: gatewayProxy,
  },
});
