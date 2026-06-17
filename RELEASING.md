# Releasing `eventferry`

This document is the workflow we follow before every release. It exists
because changesets/cli@2.31 + this repo's fixed-group + workspace-deps
layout can inflate a "minor" changeset into a major version bump
without warning. Run the preview, accept or correct the result, then
let the GitHub Actions release flow do the rest.

## TL;DR

```bash
# On the PR branch (or right after merge), before triggering the release:
./scripts/preview-release.sh

# Inspect the output. If the bumps match what you expected, you're done —
# merge the "Version Packages" PR when it appears.
# If a package took an unexpected major bump, see "Correcting an
# unexpected bump" below.
```

## Background — why this exists

eventferry publishes six `@eventferry/*` packages in lockstep. The
`.changeset/config.json` declares `fixed: [["@eventferry/*"]]`, which
means **every package must release at the same version on every release**.
The packages also reference each other via `workspace:*`, so any change
to one triggers an `Updated dependencies` patch bump in the others.

The combination — fixed group + workspace-deps update chain + multiple
packages listed in a single changeset — has produced unexpected major
bumps **twice** in our history:

| From → To | Pending changesets | What we expected | What changesets produced |
|---|---|:--:|:--:|
| `1.0.4 → 2.0.0` | `@eventferry/mysql: minor` (new package) | `1.1.0` | `2.0.0` |
| `2.0.0 → 3.0.0` | `@eventferry/core: minor` + `@eventferry/kafka: minor` | `2.1.0` | `3.0.0` |

The CHANGELOGs show each affected package was marked "Minor Changes" or
"Patch Changes" — never "Major Changes". The major version increment
came out of the version-computation pass, not from any human-authored
intent. The behavior is reproducible on the exact repo state (we have
a self-contained repro in `scripts/preview-release.sh`).

We are not the first project to hit this. Rather than fight the tool,
we've decided to:
1. **Preview the version** locally before letting release.yml apply it.
2. **Accept the major bump** when the cost of correcting is higher than
   the cost of a louder version label.
3. **Correct the bump** when we genuinely want the version we expected
   and are willing to do a manual `force-push` to the
   `changeset-release/main` branch.

## The runbook

### Step 1 — Preview before merging the feature PR

When your feature PR is approved and has a changeset:

```bash
git checkout main && git pull
git checkout your-feature-branch
./scripts/preview-release.sh
```

The script:

- Archives your current ref into a temp dir (does NOT touch your tree).
- Lists every pending changeset and its requested bumps.
- Installs `@changesets/cli` in the temp dir.
- Runs `changeset version` against the archived state.
- Prints a `before → after` version table for every workspace package.
- Flags any **major** jump with a `⚠ MAJOR` marker.

You see exactly what `release.yml` will compute. No surprises.

### Step 2 — Decide

**Bumps look right** → merge the PR. `release.yml` runs, opens the
"Version Packages" PR with the expected versions, you merge it,
packages publish.

**A package took an unexpected major bump** → see Step 3.

### Step 3 — Correcting an unexpected bump

Two paths, pick by cost:

#### Path A — accept the major (the easy one)

Honestly, this is what we've done both previous times. Semver-wise
it's not "wrong" — bumping the major version means "consumers should
read the changelog before upgrading", which is true of every release
anyway. The cost is one confused user thread asking "why is `mysql`
on v3 when there was no v2?"

Action: merge the feature PR. Let `release.yml` produce the major
version PR. Merge that. Done.

#### Path B — correct the bump (the manual one)

You really want `2.1.0` instead of `3.0.0`. Steps:

1. **Merge the feature PR** into `main`. `release.yml` runs and opens
   the **"Version Packages"** PR. Do **not** merge it yet.

2. **Pull the auto-generated version branch locally:**

   ```bash
   git fetch origin changeset-release/main
   git checkout changeset-release/main
   ```

3. **Edit every `packages/*/package.json`** to the version you want
   (e.g. `2.1.0`). For each one, also fix the matching `CHANGELOG.md`:
   change the `## 3.0.0` heading to `## 2.1.0`.

4. **Fix `workspace:*` references**: since the `CHANGELOG.md` of
   `@eventferry/all` (and other downstream packages) shows
   `@eventferry/core@3.0.0` etc. under "Updated dependencies", rewrite
   those to the corrected version.

5. **Commit and force-push:**

   ```bash
   git add packages/*/package.json packages/*/CHANGELOG.md
   git commit -m "chore: correct version bump to 2.1.0"
   git push --force-with-lease origin changeset-release/main
   ```

   `force-with-lease` is safer than plain `--force` — it refuses if
   someone else pushed in the meantime.

6. **Re-verify** the Version Packages PR on GitHub now shows the
   corrected versions, then merge it. `release.yml` re-runs and
   publishes those exact versions.

### Step 4 — Verify the publish

After the "Version Packages" PR merges:

```bash
for p in core postgres kafka schema-registry mysql all; do
  echo "@eventferry/$p → $(npm view @eventferry/$p version)"
done
```

All six should show the same expected version.

## Adding new packages

Adding a brand-new `@eventferry/<name>` package to the workspace
**triggers the inflation behavior reliably** — the `1.0.4 → 2.0.0`
jump was caused by adding `@eventferry/mysql`. Plan accordingly:

- Set the new package's initial `version` to **the current
  fixed-group baseline** (e.g. `2.0.0` if everyone else is at
  `2.0.0`). This avoids the "starting from `0.x`" mismatch that
  changesets sometimes interprets as "needs a major to reconcile".
- Use a `minor` (or `patch`) changeset for the introduction. The
  changesets CLI may still inflate to major; preview to confirm
  before merging.
- If it inflates and you can't accept the major, follow Path B
  in Step 3.

## When NOT to use this runbook

- For purely-internal changes (docs, CI, scripts under `docs/`,
  `scripts/`) that don't have a `.changeset/*.md`: nothing to
  release; merge and move on.
- For changeset updates only (editing an existing changeset's
  bump type): the preview still works, just re-run it.

## Future improvements

The right long-term fix is to either:

- Pin `@changesets/cli` to a version that does not exhibit this
  behavior (if we identify which release broke it).
- Migrate away from `fixed: [["@eventferry/*"]]` to per-package
  versioning, accepting that `@eventferry/all` becomes a meta-package
  whose pinned versions need explicit maintenance.

Neither has obviously-better trade-offs than the current "preview +
correct on demand" approach. Revisit if the manual corrections start
costing more time than the preview saves.
