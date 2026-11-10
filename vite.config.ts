import { defineConfig } from "vite-plus";
export default defineConfig({
  pack: {
    entry: { plugin: "src/plugin.ts" },
    outDir: "dist",
    format: "esm",
    platform: "node",
    target: "node24",
    deps: { alwaysBundle: () => true, onlyBundle: false },
  },
});
