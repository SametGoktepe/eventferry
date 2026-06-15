import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    globalSetup: ["./test/setup/containers.ts"],
    testTimeout: 60_000,
    hookTimeout: 120_000,
    pool: "forks",
    // Containers are heavy and shared; run files serially to avoid contention.
    fileParallelism: false,
  },
});
