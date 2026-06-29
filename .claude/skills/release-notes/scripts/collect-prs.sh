#!/usr/bin/env bash
#
# collect-prs.sh — gather the PRs that make up a StarRocks patch release.
#
# StarRocks patch releases are cut as backports onto a release branch (branch-X.Y).
# Each squash-merged commit subject carries its PR number as "(#NNNNN)". This script
# diffs the previous tag against the new tag via the GitHub compare API, extracts those
# PR numbers, and enriches each with title/labels/body so the skill can categorize and
# rewrite them into release-note entries.
#
# Read-only. Does NOT write release notes or make any prose decisions — that is the
# skill's job. Output is JSON on stdout.
#
# Usage:
#   collect-prs.sh <prev_tag> <new_tag> [repo]
#
# Examples:
#   collect-prs.sh 3.5.18 3.5.19
#   collect-prs.sh v3.5.18 v3.5.19 StarRocks/starrocks
#
# Requires: gh (authenticated), jq.

set -euo pipefail

PREV_TAG="${1:?usage: collect-prs.sh <prev_tag> <new_tag> [repo]}"
NEW_TAG="${2:?usage: collect-prs.sh <prev_tag> <new_tag> [repo]}"
REPO="${3:-StarRocks/starrocks}"

for bin in gh jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' is required" >&2; exit 1; }
done

# Resolve the release date. Prefer the published GitHub release for the new tag; fall back
# to the tag's commit date when no release is published yet. gh emits an error JSON body on
# a 404, so accept the value only if it looks like an ISO-8601 timestamp.
iso_re='^[0-9]{4}-[0-9]{2}-[0-9]{2}T'

release_date="$(
  gh api "repos/${REPO}/releases/tags/${NEW_TAG}" --jq '.published_at' 2>/dev/null || true
)"
if ! [[ "${release_date}" =~ ${iso_re} ]]; then
  release_date="$(
    gh api "repos/${REPO}/commits/${NEW_TAG}" --jq '.commit.committer.date' 2>/dev/null || true
  )"
fi
if ! [[ "${release_date}" =~ ${iso_re} ]]; then
  release_date=""
fi

# List commits between the two tags and resolve each to the ORIGINAL main PR number.
#
# Patch releases are backports onto branch-X.Y. A backport's squash subject looks like:
#   "<title> (backport #70072) (#74494)"
# where #70072 is the original PR merged to main and #74494 is the backport PR on the
# release branch. Release notes cite the ORIGINAL main PR (#70072) — that is the number
# StarRocks release notes have always used and what reviewers expect — so we extract the
# first "backport #N" reference (the root original). For a commit with no backport marker
# (a change merged directly to the branch), we fall back to its own trailing "(#N)".
#
# Compare API caps at 250 commits per page; patch releases are well under that.
# Use a read loop (not mapfile) so this works on macOS bash 3.2.
pr_numbers=()
while IFS= read -r subject; do
  [[ -z "$subject" ]] && continue
  # First "backport #N" = the original PR on main (leftmost in a chained backport).
  # `|| true` keeps a no-match grep from tripping `set -e`.
  n="$(printf '%s' "$subject" | grep -oiE 'backport #[0-9]+' | head -1 | grep -oE '[0-9]+' || true)"
  if [[ -z "$n" ]]; then
    # No backport marker: use the trailing "(#N)" (direct merge to the branch).
    n="$(printf '%s' "$subject" | grep -oE '\(#[0-9]+\)' | tail -1 | grep -oE '[0-9]+' || true)"
  fi
  [[ -n "$n" ]] && pr_numbers+=("$n")
done < <(
  gh api "repos/${REPO}/compare/${PREV_TAG}...${NEW_TAG}" \
    --jq '.commits[].commit.message | split("\n")[0]' 2>/dev/null
)
# Dedupe while preserving uniqueness (guard against an empty array under set -u).
if [[ "${#pr_numbers[@]}" -gt 0 ]]; then
  pr_numbers=($(printf '%s\n' "${pr_numbers[@]}" | sort -un))
fi

if [[ "${#pr_numbers[@]}" -eq 0 ]]; then
  echo "warning: no PR numbers found between ${PREV_TAG} and ${NEW_TAG} in ${REPO}." >&2
  echo "         check that both tags exist (gh api repos/${REPO}/tags) and the order is prev...new." >&2
fi

# Enrich each PR and resolve it to the ROOT main PR.
#
# The candidate number above is usually the original main PR, but in a multi-level backport
# chain (main -> branch-4.1 -> branch-4.0 -> branch-3.5) the leftmost "backport #N" on the
# branch-3.5 commit can point at an intermediate release-branch PR rather than main. So when
# the fetched PR's base branch is not `main`, follow its own "backport #M" reference toward
# the root (capped at a few hops). This keeps the cited number consistent with how StarRocks
# release notes reference PRs (always the main PR). PRs that 404 are skipped.
prs_json="[]"
for n in "${pr_numbers[@]}"; do
  cur="$n"
  pr=""
  base=""
  for _hop in 1 2 3 4; do
    pr="$(gh pr view "$cur" --repo "$REPO" \
          --json number,title,labels,body,url,baseRefName 2>/dev/null || true)"
    [[ -z "$pr" ]] && break
    base="$(jq -r '.baseRefName' <<<"$pr")"
    [[ "$base" == "main" ]] && break
    parent="$(jq -r '.title' <<<"$pr" | grep -oiE 'backport #[0-9]+' | head -1 | grep -oE '[0-9]+' || true)"
    # Stop if there is no parent backport reference or it would loop.
    [[ -z "$parent" || "$parent" == "$cur" ]] && break
    cur="$parent"
  done
  [[ -z "$pr" ]] && continue
  # `base` is the base branch of the PR we resolved to. When it is not `main`, the backport
  # chain did not trace back to a main PR -- usually because the fix was authored directly
  # against a release branch (or its chain metadata is broken). Flag it so the skill can warn.
  pr="$(jq -c --arg base "$base" '{
          number,
          title,
          url,
          base: $base,
          resolved_to_main: ($base == "main"),
          labels: [.labels[].name],
          body_excerpt: ((.body // "") | gsub("\r";"") | .[0:600])
        }' <<<"$pr")"
  prs_json="$(jq -c --argjson p "$pr" '. + [$p]' <<<"$prs_json")"
done
# Resolution can map two branch PRs onto the same main PR; keep one entry per number.
prs_json="$(jq -c 'unique_by(.number)' <<<"$prs_json")"
# PRs whose chain never reached main -- the skill surfaces these as a PR-quality warning.
unresolved_json="$(jq -c '[.[] | select(.resolved_to_main == false)]' <<<"$prs_json")"

jq -n \
  --arg repo "$REPO" \
  --arg prev "$PREV_TAG" \
  --arg new "$NEW_TAG" \
  --arg date "$release_date" \
  --argjson prs "$prs_json" \
  --argjson unresolved "$unresolved_json" \
  '{repo: $repo, prev_tag: $prev, new_tag: $new, release_date_raw: $date,
    pr_count: ($prs | length), prs: $prs,
    unresolved_count: ($unresolved | length), unresolved: $unresolved}'
