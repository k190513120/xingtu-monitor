import { defineConfig } from 'vite';

export default defineConfig({
  base: './',
  server: {
    proxy: {
      '/api': 'https://xingtu.xiaomiao.win',
    },
  },
});
