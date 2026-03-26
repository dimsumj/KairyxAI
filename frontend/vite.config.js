import path from 'node:path';
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

const frontendRoot = path.resolve(__dirname);
const appRoot = path.resolve(frontendRoot, 'app');

export default defineConfig({
  root: appRoot,
  base: '/static/',
  publicDir: false,
  plugins: [react()],
  server: {
    fs: {
      allow: [frontendRoot],
    },
  },
  build: {
    outDir: path.resolve(frontendRoot, 'dist'),
    emptyOutDir: true,
    assetsDir: '',
    sourcemap: false,
    minify: false,
    cssCodeSplit: false,
    modulePreload: false,
    rollupOptions: {
      input: path.resolve(appRoot, 'index.html'),
      output: {
        inlineDynamicImports: true,
        entryFileNames: 'operator-console.js',
        chunkFileNames: 'operator-console-[name].js',
        assetFileNames: (assetInfo) => {
          const name = assetInfo.name || '';
          if (name.endsWith('.css')) {
            return 'operator-console.css';
          }
          if (name.endsWith('favicon.svg')) {
            return 'favicon.svg';
          }
          return 'assets/[name][extname]';
        },
      },
    },
  },
});
