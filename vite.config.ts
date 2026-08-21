import { defineConfig } from "vite-plus";

export default defineConfig({
  fmt: {
    ignorePatterns: [".alchemy/**", ".repos/**", "coverage/**", "dist/**"],
    printWidth: 100,
    semi: true,
    sortPackageJson: true,
  },
  lint: {
    ignorePatterns: [".alchemy/**", ".repos/**", "coverage/**", "dist/**"],
    options: {
      typeAware: true,
      typeCheck: true,
    },
  },
  test: {
    coverage: {
      reporter: ["text", "html"],
    },
    include: ["test/**/*.test.ts"],
  },
});
