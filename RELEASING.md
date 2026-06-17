# Releasing `eventferry`

eventferry uses [changesets](https://github.com/changesets/changesets)
with **independent versioning**: each `@eventferry/*` package moves at
its own semver tempo, and the meta-package `@eventferry/all` automatically
follows whichever of its siblings change in a given release.

This document captures the workflow and explains the historical reason we
keep the `scripts/preview-release.sh` belt-and-suspenders check.

## TL;DR — happy path

```bash
# Add a changeset when you make a user-facing change.
# It records which packages to bump and at what level.
pnpm changeset

# Before merging your feature PR, preview what `release.yml` will compute:
./scripts/preview-release.sh

# Inspect the output — each package gets exactly the bump its changeset
# requested. Sibling packages that consume a bumped package via workspace:*
# get a `patch` bump (via `updateInternalDependencies: "patch"`).
#
# Open the PR; once it merges, release.yml opens a "Version Packages" PR.
# Merge that PR; release.yml publishes via npm OIDC trusted publishing.
```

That's it. No manual corrections. The "Path B" force-push procedure that
used to live here is no longer needed.

## How the config works

`.changeset/config.json`:

```json
{
  "fixed": [],
  "linked": [],
  "updateInternalDependencies": "patch",
  "___experimentalUnsafeOptions_WILL_CHANGE_IN_PATCH": {
    "onlyUpdatePeerDependentsWhenOutOfRange": true
  }
}
```

- `fixed: []` — packages are NOT locked to a single version. Each one
  evolves independently.
- `linked: []` — no soft-linking either. Full independence.
- `updateInternalDependencies: "patch"` — when package A's version bumps,
  any sibling B that depends on A via `workspace:*` gets a **patch** bump
  reflecting the new dependency, never a major.
- `onlyUpdatePeerDependentsWhenOutOfRange: true` — peer dependents are
  only bumped when the peer would otherwise fall out of the declared
  range. Without this flag, **every** internal version bump cascades a
  major to peer-dependents, which is the bug that produced our
  `1.0.4 → 2.0.0`, `2.0.0 → 3.0.0`, `3.0.0 → 3.1.0 (after manual fix)`
  history.

## How `@eventferry/*` packages reference each other

`@eventferry/core` is a regular **dependency** (NOT a peerDependency) of
the adapters that need it (`postgres`, `mysql`, `kafka`,
`schema-registry`). This was the structural fix for the
[peer-dependency major-bump cascade](https://github.com/changesets/changesets/issues/1759).

External peer deps (`pg`, `mysql2`, `kafkajs`, `@confluentinc/kafka-javascript`,
`@kafkajs/confluent-schema-registry`, etc.) stay as `peerDependencies` —
those are user choices about which driver to install.

`@eventferry/all` lists every sibling under `dependencies` via
`workspace:*`. pnpm rewrites `workspace:*` to the exact published version
at `npm publish` time, so `@eventferry/all@x.y.z` always pins coherent
sibling versions.

## Why `preview-release.sh` is still here

The script is now a **safety net**, not a workflow gate. It catches:

- A future changesets bug that re-introduces unwanted version inflation.
- A new package added to the repo without the right
  `dependencies` / `peerDependencies` shape.
- An accidentally-committed test changeset.

If you ever see `⚠ MAJOR` flagged in the preview output without an
explicit `major` request in your changeset frontmatter, **stop and
investigate** — something has regressed the config.

## Adding a new `@eventferry/<name>` package

When introducing a brand-new adapter:

1. Use `pnpm create` or copy an existing adapter as a template.
2. **List `@eventferry/core` (and any other internal package you need) under
   `dependencies`, never `peerDependencies`.** External user-facing
   peers (drivers, clients) go under `peerDependencies` as usual.
3. Add the package to `@eventferry/all`'s `dependencies` (via `workspace:*`).
4. Write a changeset noting the new package — `minor` is the right bump
   level for "adds new package X".
5. Run `./scripts/preview-release.sh`. The new package should appear in
   the bump table; sibling packages should NOT take a major bump.

## Trade-offs we accept with independent versioning

- **Sibling drift is normal.** `@eventferry/core@3.5.0` alongside
  `@eventferry/postgres@3.2.1` and `@eventferry/kafka@3.4.0` is OK —
  it reflects which packages actually changed.
- **CHANGELOG.md per package is the source of truth.** There is no
  "platform v3.5 changelog" — each package's CHANGELOG tells its own
  story. Look at `@eventferry/all`'s CHANGELOG to see which siblings
  shifted on a given release.
- **`@eventferry/all` gets a patch bump on almost every release.** That
  is correct: any time one of its deps moves, the meta-package's deps
  block also moves and needs a release. The patches reflect "I now pin
  newer versions of my constituents."

## Out of scope

- Single-line "platform version" marketing. (We use per-package
  versions; `@eventferry/all` is the closest analogue.)
- Synchronized CHANGELOG histories. (Each package documents only its
  own changes.)
- Lockstep major bumps across the whole repo. (A real breaking change
  to `core` only marks core as major; adapters keep their own version
  unless they themselves break.)

## Historical context (kept for the record)

Before this migration, the repo used `fixed: [["@eventferry/*"]]` to
keep all packages on the same version, and `@eventferry/core` was a
`peerDependency` of the adapters. Every multi-package changeset produced
an unexpected major bump:

| From → To | Pending changesets | Expected | Actual |
|---|---|:--:|:--:|
| `1.0.4 → 2.0.0` | `mysql: minor` (new package) | `1.1.0` | `2.0.0` ❌ |
| `2.0.0 → 3.0.0` | `core: minor` + `kafka: minor` | `2.1.0` | `3.0.0` ❌ |
| `3.0.0 → 4.0.0` (corrected to `3.1.0`) | `core: minor` + `kafka: minor` | `3.1.0` | `4.0.0` ❌ |
| `3.1.0 → 4.0.0` (corrected to `3.2.0`) | `kafka: minor` ×2 | `3.2.0` | `4.0.0` ❌ |

Root cause: changesets'
[`docs/decisions.md`](https://github.com/changesets/changesets/blob/main/docs/decisions.md)
states that when one package lists another as a `peerDependency`, an
internal version bump of the depended-upon package forces a major on
the dependent. Combined with `fixed:` reconciliation, every minor
became a major. Documented in
[changesets issue #1759](https://github.com/changesets/changesets/issues/1759).

The fix shipped in this repo is exactly the
[Astro pattern](https://github.com/withastro/astro/blob/main/.changeset/config.json):
drop `fixed`, set `linked: []`, enable
`onlyUpdatePeerDependentsWhenOutOfRange`, and move internal `@eventferry/*`
deps from `peerDependencies` to `dependencies`.
