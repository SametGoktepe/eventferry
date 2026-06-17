import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  target: "es2022",
  external: ["aws-msk-iam-sasl-signer-js", "@eventferry/kafka", "@eventferry/core"],
});
