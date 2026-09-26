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
#   collect-prs.sh <prev_tag> <new_tag> [repo] [--commit <sha>] [--branch <name>]
#
#   <new_tag> may name a version that is not released or not even tagged yet:
#     - tag pushed, no GitHub release published  -> diff runs to the tag, `unreleased: true`.
#     - tag does not exist on GitHub              -> diff runs to the head of the release
#                                                    branch (branch-X.Y, derived from
#                                                    <new_tag>), `unreleased: true`.
#
#   --commit <sha>   Diff up to this commit instead of <new_tag>. Use it to pin the release
#                    point of an untagged version: <new_tag> is then only the version label,
#                    and the commit (full or short SHA, or any ref GitHub resolves, pushed to
#                    <repo>) marks the release point on the release branch.
#   --branch <name>  Release branch to use when <new_tag> does not exist and no --commit is
#                    given. Defaults to branch-<major>.<minor> of <new_tag>.
#
# Examples:
#   collect-prs.sh 3.5.18 3.5.19
#   collect-prs.sh v3.5.18 v3.5.19 StarRocks/starrocks
#   collect-prs.sh 3.5.18 3.5.19                     # 3.5.19 untagged: diffs to branch-3.5 head
#   collect-prs.sh 3.5.18 3.5.19 --commit 1a2b3c4d   # 3.5.19 untagged: diffs to this commit
#
# Requires: gh (authenticated), jq.

set -euo pipefail

usage="usage: collect-prs.sh <prev_tag> <new_tag> [repo] [--commit <sha>] [--branch <name>]"

COMMIT=""
BRANCH=""
positional=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --commit)   COMMIT="${2:?--commit requires a commit SHA}"; shift 2 ;;
    --commit=*) COMMIT="${1#--commit=}"; shift ;;
    --branch)   BRANCH="${2:?--branch requires a branch name}"; shift 2 ;;
    --branch=*) BRANCH="${1#--branch=}"; shift ;;
    -h|--help)  echo "$usage"; exit 0 ;;
    -*)         echo "error: unknown option '$1'" >&2; echo "$usage" >&2; exit 1 ;;
    *)          positional+=("$1"); shift ;;
  esac
done
PREV_TAG="${positional[0]:-}"
NEW_TAG="${positional[1]:-}"
REPO="${positional[2]:-StarRocks/starrocks}"
if [[ -z "$PREV_TAG" || -z "$NEW_TAG" ]]; then
  echo "$usage" >&2
  exit 1
fi

for bin in gh jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "error: '$bin' is required" >&2; exit 1; }
done

iso_re='^[0-9]{4}-[0-9]{2}-[0-9]{2}T'

# resolve_commit <ref> -> prints "<full sha>\t<committer date>", or nothing if GitHub cannot
# resolve the ref in REPO.
resolve_commit() {
  local info
  info="$(gh api "repos/${REPO}/commits/$1" \
            --jq '[.sha, .commit.committer.date] | @tsv' 2>/dev/null || true)"
  [[ "$(cut -f1 <<<"$info")" =~ ^[0-9a-f]{40}$ ]] && printf '%s' "$info"
  return 0
}

# Decide where the diff ends (NEW_REF) and whether the version is released:
#   1. --commit given          -> that commit (must be pushed); unreleased.
#   2. <new_tag> exists        -> the tag; unreleased unless a GitHub release is published.
#   3. <new_tag> doesn't exist -> head of the release branch; unreleased.
# Resolve everything to a full SHA up front so a typo or an unpushed ref fails loudly instead
# of producing an empty diff.
if [[ -n "$COMMIT" ]]; then
  commit_info="$(resolve_commit "$COMMIT")"
  if [[ -z "$commit_info" ]]; then
    echo "error: commit '${COMMIT}' not found in ${REPO}. Make sure it is pushed to GitHub." >&2
    exit 1
  fi
  NEW_REF="$(cut -f1 <<<"$commit_info")"
  ref_source="commit"
else
  commit_info="$(resolve_commit "$NEW_TAG")"
  if [[ -n "$commit_info" ]]; then
    NEW_REF="$NEW_TAG"
    ref_source="tag"
  else
    if [[ -z "$BRANCH" ]]; then
      # 3.5.19 / v3.5.19 -> branch-3.5
      if ! [[ "$NEW_TAG" =~ ^v?([0-9]+)\.([0-9]+) ]]; then
        echo "error: tag '${NEW_TAG}' not found in ${REPO} and no release branch can be derived from it." >&2
        echo "       pass --branch <name> or --commit <sha>." >&2
        exit 1
      fi
      BRANCH="branch-${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"
    fi
    commit_info="$(resolve_commit "$BRANCH")"
    if [[ -z "$commit_info" ]]; then
      echo "error: tag '${NEW_TAG}' not found in ${REPO}, and neither is branch '${BRANCH}'." >&2
      echo "       pass --branch <name> or --commit <sha>." >&2
      exit 1
    fi
    NEW_REF="$(cut -f1 <<<"$commit_info")"
    ref_source="branch"
    echo "note: tag '${NEW_TAG}' not found in ${REPO}; diffing to the head of ${BRANCH} (${NEW_REF:0:10})." >&2
    echo "      commits merged to ${BRANCH} after the release is cut will not be in ${NEW_TAG}; pin it with --commit." >&2
  fi
fi

# Resolve the release date. Prefer the published GitHub release for the tag; fall back to
# the ref's commit date when no release is published yet. A tag without a published release
# is treated as unreleased. gh emits an error JSON body on a 404, so accept the value only if
# it looks like an ISO-8601 timestamp.
unreleased=true
release_date=""
if [[ "$ref_source" == "tag" ]]; then
  release_date="$(
    gh api "repos/${REPO}/releases/tags/${NEW_TAG}" --jq '.published_at' 2>/dev/null || true
  )"
  [[ "${release_date}" =~ ${iso_re} ]] && unreleased=false
fi
if ! [[ "${release_date}" =~ ${iso_re} ]]; then
  release_date="$(cut -f2 <<<"$commit_info")"
fi
if ! [[ "${release_date}" =~ ${iso_re} ]]; then
  release_date=""
fi

# Fetch the comparison once and sanity-check the relationship between the two refs.
# Release tags usually sit a few commits off the branch (e.g. a version-bump commit), so a
# small "diverged" is normal. A large behind_by means NEW_REF is on a different branch (say
# `main` instead of branch-X.Y) and the diff would pull in unrelated history.
compare_path="repos/${REPO}/compare/${PREV_TAG}...${NEW_REF}"
compare_json="$(gh api "${compare_path}?per_page=1" 2>/dev/null || true)"
compare_status="$(jq -r '.status // empty' <<<"$compare_json" 2>/dev/null || true)"
case "$compare_status" in
  ahead) ;;
  diverged)
    behind_by="$(jq -r '.behind_by' <<<"$compare_json")"
    if [[ "$behind_by" -gt 50 ]]; then
      echo "warning: ${NEW_REF} is ${behind_by} commits behind ${PREV_TAG}; it is probably not on the same release branch." >&2
      echo "         the PR list likely includes unrelated commits." >&2
    fi ;;
  behind|identical)
    echo "warning: ${NEW_REF} has no commits beyond ${PREV_TAG} (status: ${compare_status})." >&2 ;;
  *)
    echo "error: cannot compare ${PREV_TAG}...${NEW_REF} in ${REPO}." >&2
    echo "       check that both refs exist (gh api repos/${REPO}/tags)." >&2
    exit 1 ;;
esac
total_commits="$(jq -r '.total_commits' <<<"$compare_json")"

# Commit subjects between the two refs. Paginate: an untagged version diffed to a branch head
# can easily exceed the compare API's 250-commit single-page cap.
subjects="$(gh api --paginate "${compare_path}?per_page=100" \
              --jq '.commits[].commit.message | split("\n")[0]')"
listed_commits="$(grep -c '' <<<"$subjects" || true)"
if [[ "$listed_commits" -lt "$total_commits" ]]; then
  echo "warning: compare API returned only ${listed_commits} of ${total_commits} commits; the PR list is incomplete." >&2
fi

# List commits between the two refs and resolve each to the ORIGINAL main PR number.
#
# Patch releases are backports onto branch-X.Y. A backport's squash subject looks like:
#   "<title> (backport #70072) (#74494)"
# where #70072 is the original PR merged to main and #74494 is the backport PR on the
# release branch. Release notes cite the ORIGINAL main PR (#70072) — that is the number
# StarRocks release notes have always used and what reviewers expect — so we extract the
# first "backport #N" reference (the root original). For a commit with no backport marker
# (a change merged directly to the branch), we fall back to its own trailing "(#N)".
#
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
done <<<"$subjects"
# Dedupe while preserving uniqueness (guard against an empty array under set -u).
if [[ "${#pr_numbers[@]}" -gt 0 ]]; then
  pr_numbers=($(printf '%s\n' "${pr_numbers[@]}" | sort -un))
fi

if [[ "${#pr_numbers[@]}" -eq 0 ]]; then
  echo "warning: no PR numbers found between ${PREV_TAG} and ${NEW_REF} in ${REPO}." >&2
  echo "         check that both refs exist (gh api repos/${REPO}/tags) and the order is prev...new." >&2
fi

# Enrich each PR and resolve it to the ROOT main PR.
#
# The candidate number above is usually the original main PR, but in a multi-level backport
# chain (main -> branch-4.1 -> branch-4.0 -> branch-3.5) the leftmost "backport #N" on the
# branch-3.5 commit can point at an intermediate release-branch PR rather than main. So when
# the fetched PR's base branch is not `main`, follow its own "backport #M" reference toward
# the root (capped at a few hops). This keeps the cited number consistent with how StarRocks
# release notes reference PRs (always the main PR). PRs that cannot be fetched are skipped
# with a warning.
prs_json="[]"
for n in ${pr_numbers[@]+"${pr_numbers[@]}"}; do
  cur="$n"
  pr=""
  base=""
  for _hop in 1 2 3 4; do
    # Retry: a transient API failure (e.g. rate limiting) would otherwise drop the PR silently.
    pr=""
    for _try in 1 2 3; do
      pr="$(gh pr view "$cur" --repo "$REPO" \
            --json number,title,labels,body,url,baseRefName 2>/dev/null || true)"
      [[ -n "$pr" ]] && break
      sleep 2
    done
    [[ -z "$pr" ]] && break
    base="$(jq -r '.baseRefName' <<<"$pr")"
    [[ "$base" == "main" ]] && break
    parent="$(jq -r '.title' <<<"$pr" | grep -oiE 'backport #[0-9]+' | head -1 | grep -oE '[0-9]+' || true)"
    # Stop if there is no parent backport reference or it would loop.
    [[ -z "$parent" || "$parent" == "$cur" ]] && break
    cur="$parent"
  done
  if [[ -z "$pr" ]]; then
    echo "warning: could not fetch PR #${cur} from ${REPO}; skipped." >&2
    continue
  fi
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
  --arg ref "$NEW_REF" \
  --arg ref_source "$ref_source" \
  --argjson unreleased "$unreleased" \
  --arg date "$release_date" \
  --argjson prs "$prs_json" \
  --argjson unresolved "$unresolved_json" \
  '{repo: $repo, prev_tag: $prev, new_tag: $new, new_ref: $ref, new_ref_source: $ref_source,
    unreleased: $unreleased, release_date_raw: $date,
    pr_count: ($prs | length), prs: $prs,
    unresolved_count: ($unresolved | length), unresolved: $unresolved}'
