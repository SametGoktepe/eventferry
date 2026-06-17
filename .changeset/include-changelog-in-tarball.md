---
"@eventferry/core": patch
"@eventferry/postgres": patch
"@eventferry/mysql": patch
"@eventferry/kafka": patch
"@eventferry/schema-registry": patch
"@eventferry/all": patch
---

**chore: ship `CHANGELOG.md` inside the npm tarball**

Previously, each package's `files` allowlist contained only `"dist"` (and `"sql"` for `@eventferry/postgres`), so the auto-generated `CHANGELOG.md` was never published. Users browsing the package on npmjs.com or unpacking the tarball couldn't see release notes — they had to navigate to the GitHub repo.

This release adds `"CHANGELOG.md"` to the `files` array of every publishable package. Starting with this version, the per-version release notes are accessible:

- Directly in `node_modules/@eventferry/<pkg>/CHANGELOG.md` after `npm install`
- In the file listing on npmjs.com (under the "Code" / "Files" tab, depending on the npm UI)
- Inside the tarball downloaded from `https://registry.npmjs.org/...`

No code or API surface changes.
