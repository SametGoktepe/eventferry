# @eventferry/schema-registry

## 3.4.0

### Minor Changes

- 0f328d2: Typed authentication for the Schema Registry HTTP API. New `auth?: SchemaRegistryAuth` option on `SchemaRegistrySerializer` with two shapes:

  - `{ type: "basic", username, password }` — HTTP Basic, forwarded straight to the underlying client's `auth` config (the conventional Confluent Cloud + commercial-registry shape).
  - `{ type: "bearer", token: string | () => string | Promise<string> }` — adds an `Authorization: Bearer <token>` header via a small middleware on the upstream client. Callable tokens are resolved on **every** request, so rotation logic lives in the caller's provider (cache inside your callable if rotation cost matters).

  Ignored when an already-constructed `registry` client is injected — configure auth there yourself. mTLS to the registry stays out of scope: it's handled by a custom `https.Agent` on a self-constructed client (registry TLS is independent of broker TLS, and we don't want to fold an unrelated knob into this surface).

  The middleware factory is exported as `bearerAuthMiddleware` for testing only — not part of the supported public API and may move in a future version.

### Patch Changes

- Updated dependencies [715523f]
- Updated dependencies [fb0549d]
  - @eventferry/core@3.4.0

## 3.3.0

### Minor Changes

- d983d90: Schema Registry serializer gains three production-grade controls without breaking the existing API:

  - **Subject naming strategy** — `subjectStrategy: "TopicNameStrategy" | "RecordNameStrategy" | "TopicRecordNameStrategy"` mirrors Confluent's three built-ins. The two record-based strategies take a `recordName: (record, isKey) => string` resolver. Default stays `TopicNameStrategy`; the existing `subject` callable still wins when set (now optionally receiving `(topic, isKey, record)` — the single-arg legacy form keeps working).
  - **Avro key serialization** — new `keySchemas` option + `serializeKey(record): Promise<Buffer | null>` method. Returns `null` for keyless records (matching the kafka "no key" semantics) and uses the `-key` subject by default. Key and value subject ids cache independently. Not wired into the relay automatically — call it from your publish glue when you want Avro-encoded keys instead of UTF-8 strings.
  - **`autoRegister: false`** — never call `register()`, always resolve schemas via `getLatestSchemaId` on the computed subject. Locally supplied `schemas` / `keySchemas` bytes become docs-only in this mode. Matches `auto.register.schemas=false` on every Confluent client, for production clusters where schemas are managed out-of-band.

## 3.2.3

### Patch Changes

- 3c33f71: **chore: ship `CHANGELOG.md` inside the npm tarball**

  Previously, each package's `files` allowlist contained only `"dist"` (and `"sql"` for `@eventferry/postgres`), so the auto-generated `CHANGELOG.md` was never published. Users browsing the package on npmjs.com or unpacking the tarball couldn't see release notes — they had to navigate to the GitHub repo.

  This release adds `"CHANGELOG.md"` to the `files` array of every publishable package. Starting with this version, the per-version release notes are accessible:

  - Directly in `node_modules/@eventferry/<pkg>/CHANGELOG.md` after `npm install`
  - In the file listing on npmjs.com (under the "Code" / "Files" tab, depending on the npm UI)
  - Inside the tarball downloaded from `https://registry.npmjs.org/...`

  No code or API surface changes.

- Updated dependencies [3c33f71]
  - @eventferry/core@3.3.1

## 3.2.2

### Patch Changes

- Updated dependencies [cdc20cf]
  - @eventferry/core@3.3.0

## 3.2.1

### Patch Changes

- 9beb3e2: **chore: migrate to independent versioning (Astro pattern)**

  Fixes the major-version inflation that produced four consecutive surprise majors (`1.0.4 → 2.0.0`, `2.0.0 → 3.0.0`, `3.0.0 → 4.0.0 corrected to 3.1.0`, `3.1.0 → 4.0.0 corrected to 3.2.0`) from changesets whose frontmatter only asked for `minor`.

  **Root cause** (cited in [changesets/changesets#1759](https://github.com/changesets/changesets/issues/1759) and [docs/decisions.md](https://github.com/changesets/changesets/blob/main/docs/decisions.md)): the adapters listed `@eventferry/core` as a `peerDependency` with `workspace:*`. Changesets' documented rule is that an internal bump of a peer forces a major bump on the dependent — and the `fixed: [["@eventferry/*"]]` group reconciler then propagated that major across every package in the group.

  **Fix** (exactly the [Astro config](https://github.com/withastro/astro/blob/main/.changeset/config.json)):

  1. `.changeset/config.json` — drop `fixed`, set `linked: []`, enable
     `___experimentalUnsafeOptions_WILL_CHANGE_IN_PATCH.onlyUpdatePeerDependentsWhenOutOfRange: true`.
  2. Move `@eventferry/core` from `peerDependencies` to `dependencies` in
     `@eventferry/postgres`, `@eventferry/mysql`, `@eventferry/kafka`, and
     `@eventferry/schema-registry`. External user-facing peers (`pg`,
     `mysql2`, `kafkajs`, `@confluentinc/kafka-javascript`,
     `@kafkajs/confluent-schema-registry`) stay unchanged.

  **Effect on releases.** Packages now evolve at independent semver tempos: a `core: minor` changeset produces `core@3.3.0` alongside `postgres@3.2.1` (patch, from "Updated dependencies"). No more major surprises. No more manual force-push corrections.

  **Effect on consumers.** Pure-additive at the install boundary: `npm i @eventferry/kafka` now resolves `@eventferry/core` automatically (it's a regular dep). Previously consumers had to install it themselves as a peer; the typical flow already did this. No source-code changes required.

- Updated dependencies [9beb3e2]
  - @eventferry/core@3.2.1

## 3.2.0

### Patch Changes

- @eventferry/core@3.2.0

## 3.1.0

### Patch Changes

- Updated dependencies [da39b08]
  - @eventferry/core@3.1.0

## 3.0.0

### Patch Changes

- Updated dependencies [f0c7483]
  - @eventferry/core@3.0.0

## 2.0.0

### Patch Changes

- @eventferry/core@2.0.0

## 1.0.4

### Patch Changes

- Updated dependencies [64d115d]
  - @eventferry/core@1.0.4

## 1.0.3

### Patch Changes

- Updated dependencies [aaca9a2]
  - @eventferry/core@1.0.3

## 1.0.2

### Patch Changes

- 89f1867: Declare `engines.node` (>=18) so npm shows the supported Node version and tooling can warn on unsupported runtimes.
- Updated dependencies [89f1867]
  - @eventferry/core@1.0.2

## 1.0.1

### Patch Changes

- docs: polish per-package READMEs (npm page content). No code changes.
- Updated dependencies
  - @eventferry/core@1.0.1

## 1.0.0

### Minor Changes

- b06f8ec: New package: `@eventferry/schema-registry`.

  A core `Serializer` that encodes outbox payloads with a Confluent-compatible Schema
  Registry (Avro / Protobuf / JSON Schema) instead of plain JSON — drop it into the
  `serializer` option of `Relay` or `PostgresStreamingRelay`.

  - Wraps `@kafkajs/confluent-schema-registry` as an **optional** peer dependency
    (dynamically imported); inject your own client for tests or custom config.
  - Per-topic schema resolution: register a supplied schema, or use the subject's latest
    (default subject `${topic}-value`, configurable). Schema ids are resolved once and
    cached.
  - Consumers decode with the same registry client; no changes to `core`/`postgres`/`kafka`.

### Patch Changes

- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
- Updated dependencies [b06f8ec]
  - @eventferry/core@1.0.0
