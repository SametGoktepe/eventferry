#!/usr/bin/env bash
#
# preview-release.sh — show the versions `changeset version` would produce on
# *this* repo state, WITHOUT touching the working tree. Used before merging
# a PR (whose changeset is committed to .changeset/) to catch surprise
# bumps — eventferry's fixed-group + workspace-deps layout has caused
# changesets/cli@2.31 to inflate apparent-minor bumps to majors.
#
# Usage:   ./scripts/preview-release.sh [REF]
#   REF  optional git ref to preview (default: HEAD)
#
# Exits 0 on success, non-zero on tooling failures. The script always
# cleans up its temp dir on exit.
set -Eeuo pipefail

REF="${1:-HEAD}"
ROOT="$(git rev-parse --show-toplevel)"
TMPDIR="$(mktemp -d -t eventferry-release-preview.XXXXXX)"
cleanup() { rm -rf "$TMPDIR"; }
trap cleanup EXIT

echo "▶ Previewing release of ref: $REF"
echo "  workspace root: $ROOT"
echo "  temp dir:       $TMPDIR"
echo

# Copy the working state out without touching the user's tree or node_modules.
git -C "$ROOT" archive --format=tar "$REF" | tar -x -C "$TMPDIR"

cd "$TMPDIR"

# Snapshot CURRENT versions to a flat file (`name<TAB>version`) — keeps
# the script portable to bash 3.x (macOS default), which lacks associative
# arrays.
BEFORE_FILE="$TMPDIR/.before.tsv"
: >"$BEFORE_FILE"
for p in packages/*/package.json; do
  name=$(node -e "console.log(require('./$p').name)")
  ver=$(node -e "console.log(require('./$p').version)")
  printf "%s\t%s\n" "$name" "$ver" >>"$BEFORE_FILE"
done

# Print pending changesets (skip config.json + README).
echo "▶ Changesets in .changeset/:"
shopt -s nullglob
md_files=( .changeset/*.md )
if (( ${#md_files[@]} == 0 )); then
  echo "  (none) — nothing to release."
  exit 0
fi
for cs in "${md_files[@]}"; do
  base=$(basename "$cs")
  if [[ "$base" == "README.md" ]]; then continue; fi
  echo "  · $base"
  awk '/^---$/{n++; next} n==1{print "      "$0}' "$cs" | head -10
done
echo

# Install changesets/cli isolated to the temp dir. --ignore-scripts because
# we don't need the workspace deps to compile, only changesets to run.
echo "▶ Installing @changesets/cli (isolated)…"
node -e "
const fs=require('fs');
const p=JSON.parse(fs.readFileSync('package.json'));
p.devDependencies=p.devDependencies||{};
p.devDependencies['@changesets/cli']='^2.31.0';
fs.writeFileSync('package.json',JSON.stringify(p,null,2));
"
npm i --ignore-scripts --silent --no-audit --no-fund --no-progress >/dev/null

# Run the version computation.
echo "▶ Running 'changeset version'…"
echo
node_modules/.bin/changeset version 2>&1 | sed 's/^/    /'
echo

# Diff each package's version BEFORE → AFTER.
echo "▶ Resulting version bumps:"
printf "    %-32s %-10s   %-10s\n" "package" "before" "after"
printf "    %-32s %-10s   %-10s\n" "-------" "------" "-----"
warned=0
for p in packages/*/package.json; do
  name=$(node -e "console.log(require('./$p').name)")
  after=$(node -e "console.log(require('./$p').version)")
  before=$(awk -v n="$name" -F'\t' '$1==n{print $2; exit}' "$BEFORE_FILE")
  before=${before:-?}
  flag=""
  # Surface unexpected major bumps that the changeset frontmatter didn't ask for.
  if [[ "$before" != "?" ]]; then
    bmaj="${before%%.*}"
    amaj="${after%%.*}"
    if [[ "$bmaj" != "$amaj" ]]; then
      flag="  ⚠ MAJOR"
      warned=1
    fi
  fi
  printf "    %-32s %-10s → %-10s%s\n" "$name" "$before" "$after" "$flag"
done
echo

if (( warned == 1 )); then
  echo "⚠  At least one package took a major bump."
  echo
  echo "   With the current independent-versioning config a major bump should"
  echo "   only happen when a changeset's frontmatter explicitly requests"
  echo "   'major'. If none of your changesets did, the config may have"
  echo "   regressed — see RELEASING.md > 'Why preview-release.sh is still here'."
fi

echo "✓ Preview complete. Temp dir cleaned up."
