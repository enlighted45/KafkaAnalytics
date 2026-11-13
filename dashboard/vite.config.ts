/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const analytics = process.env.ANALYTICS_URL ?? 'http://localhost:8090';
const producer = process.env.PRODUCER_URL ?? 'http://localhost:8081';

// Dev server proxies the same paths nginx proxies in production.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/api/generator': producer,
      '/api': analytics,
      '/ws': { target: analytics, ws: true },
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
    css: false,
  },
});
