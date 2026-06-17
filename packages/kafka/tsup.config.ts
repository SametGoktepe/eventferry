import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts", "src/consume.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  target: "es2022",
  external: ["kafkajs", "@confluentinc/kafka-javascript", "@eventferry/core"],
});
