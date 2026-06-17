---
"@eventferry/core": patch
"@eventferry/postgres": patch
"@eventferry/mysql": patch
"@eventferry/kafka": patch
"@eventferry/schema-registry": patch
"@eventferry/all": patch
---

**chore: migrate to independent versioning (Astro pattern)**

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
