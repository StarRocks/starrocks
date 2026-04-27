#!/usr/bin/env bash
#
# backport_to_cc.sh
# Compare branch-3.5 with branch-3.5-cc and backport PRs across.
#
# Requirements: git, gh (run `gh auth login`), jq
#
# Env overrides:
#   REMOTE=origin            # git remote name
#   SRC_BRANCH=branch-3.5
#   DST_BRANCH=branch-3.5-cc

set -euo pipefail

REPO="StarRocks/starrocks"
SRC_BRANCH="${SRC_BRANCH:-branch-3.5}"
DST_BRANCH="${DST_BRANCH:-branch-3.5-cc}"
REMOTE="${REMOTE:-origin}"

ensure_repo() {
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || {
    echo "Run inside the starrocks git repo."; exit 1; }
  git fetch "$REMOTE" "$SRC_BRANCH" "$DST_BRANCH"
}

# True if <sha> is already reachable from DST_BRANCH (shared ancestry,
# merge, or cherry-pick of the very same commit).
is_in_dst() {
  local sha="$1"
  [[ -z "$sha" || "$sha" == "null" ]] && return 1
  git merge-base --is-ancestor "$sha" "$REMOTE/$DST_BRANCH" 2>/dev/null
}

# SHAs cherry-picked into DST_BRANCH (need `-x` trailer in commit message).
already_backported_shas() {
  git log "$REMOTE/$DST_BRANCH" --pretty=format:'%B' \
    | awk '/cherry picked from commit/ {
        for (i=1;i<=NF;i++) if ($i ~ /^[0-9a-f]{7,40}\)?$/) {
          gsub(/\)/,"",$i); print $i
        }
      }' \
    | sort -u
}

# PR numbers referenced by commit subjects on DST_BRANCH, e.g.
#   "... (backport #63692)"
#   "[Backport] foo (#63692)"
already_backported_prs() {
  git log "$REMOTE/$DST_BRANCH" --pretty=format:'%s' \
    | grep -Eo '\(backport #[0-9]+\)|\(#[0-9]+\)' \
    | grep -Eo '[0-9]+' \
    | sort -u
}

cmd_diff() {
  ensure_repo
  echo "=== Commits only in $SRC_BRANCH (not in $DST_BRANCH) ==="
  git log --oneline --no-merges "$REMOTE/$DST_BRANCH..$REMOTE/$SRC_BRANCH"
  echo
  echo "=== Commits only in $DST_BRANCH (not in $SRC_BRANCH) ==="
  git log --oneline --no-merges "$REMOTE/$SRC_BRANCH..$REMOTE/$DST_BRANCH"
  echo
  echo "=== File-level diff stat ==="
  git diff --stat "$REMOTE/$DST_BRANCH..$REMOTE/$SRC_BRANCH"
}

# List PRs merged into SRC_BRANCH (exact base match).
cmd_list_prs() {
  local since="${1:-2024-01-01}"
  echo "PRs merged into $SRC_BRANCH since $since:"
  gh pr list --repo "$REPO" --state merged \
     --search "base:$SRC_BRANCH merged:>=$since" \
     --limit 1000 \
     --json number,title,mergedAt,author,baseRefName \
   | jq -r --arg b "$SRC_BRANCH" '
       .[] | select(.baseRefName==$b)
           | "#\(.number)  \(.mergedAt)  @\(.author.login)  \(.title)"'
}

# List PRs merged into SRC_BRANCH but NOT yet present in DST_BRANCH.
cmd_pending() {
  ensure_repo
  local since="${1:-2024-01-01}"
  echo "Pending backports from $SRC_BRANCH -> $DST_BRANCH since $since:"

  local done_shas done_prs
  done_shas="$(already_backported_shas)"
  done_prs="$(already_backported_prs)"

  gh pr list --repo "$REPO" --state merged \
     --search "base:$SRC_BRANCH merged:>=$since" \
     --limit 1000 \
     --json number,title,mergedAt,author,baseRefName,mergeCommit \
   | jq -r --arg b "$SRC_BRANCH" '
       .[] | select(.baseRefName==$b)
           | "\(.mergeCommit.oid)\t\(.number)\t\(.mergedAt)\t\(.author.login)\t\(.title)"' \
   | while IFS=$'\t' read -r sha num merged_at user title; do
       # 1) Already reachable from DST_BRANCH (shared ancestry / merge / cherry-pick)
       if is_in_dst "$sha"; then continue; fi
       # 2) Cherry-pick `-x` trailer recorded the original SHA
       if [[ -n "$sha" ]] && grep -qx "$sha" <<<"$done_shas"; then continue; fi
       # 3) PR number referenced in a DST_BRANCH commit subject
       if grep -qx "$num" <<<"$done_prs"; then continue; fi
       echo "#$num  $merged_at  $sha  @$user  $title"
     done
}

# Backport a single PR.
cmd_backport() {
  local pr_num="${1:-}"
  [[ -z "$pr_num" ]] && { echo "Usage: $0 backport <PR_NUMBER>"; exit 1; }

  ensure_repo

  local pr_json state merged_at base merge_sha title orig_body
  pr_json="$(gh pr view "$pr_num" --repo "$REPO" \
              --json number,title,state,mergedAt,mergeCommit,baseRefName,body)"
  state="$(    jq -r '.state'              <<<"$pr_json")"
  merged_at="$(jq -r '.mergedAt // ""'     <<<"$pr_json")"
  base="$(     jq -r '.baseRefName'        <<<"$pr_json")"
  merge_sha="$(jq -r '.mergeCommit.oid'    <<<"$pr_json")"
  title="$(    jq -r '.title'              <<<"$pr_json")"
  orig_body="$(jq -r '.body // ""'         <<<"$pr_json")"

  if [[ "$state" != "MERGED" || -z "$merged_at" ]]; then
    echo "PR #$pr_num is not merged (state=$state)."; exit 1
  fi
  if [[ "$base" != "$SRC_BRANCH" ]]; then
    echo "Warning: PR #$pr_num base is '$base', not '$SRC_BRANCH'."
    read -r -p "Continue anyway? [y/N] " ans
    [[ "$ans" =~ ^[Yy]$ ]] || exit 1
  fi
  if [[ -z "$merge_sha" || "$merge_sha" == "null" ]]; then
    echo "No merge commit SHA for PR #$pr_num."; exit 1
  fi

  # Already reachable from DST_BRANCH? Nothing to do.
  if is_in_dst "$merge_sha"; then
    echo "PR #$pr_num ($merge_sha) is already reachable from $DST_BRANCH. Skipping."
    exit 0
  fi
  if already_backported_shas | grep -qx "$merge_sha" \
     || already_backported_prs | grep -qx "$pr_num"; then
    echo "PR #$pr_num appears to be already backported to $DST_BRANCH. Skipping."
    exit 0
  fi

  local backport_branch="backport/${pr_num}-to-${DST_BRANCH}"
  echo ">>> Backporting PR #$pr_num ($merge_sha) -> $DST_BRANCH"

  git checkout -B "$DST_BRANCH" "$REMOTE/$DST_BRANCH"
  git checkout -B "$backport_branch"

  local parents
  parents="$(git rev-list --parents -n1 "$merge_sha" | awk '{print NF-1}')"
  local cp_args=(-x)
  [[ "$parents" -gt 1 ]] && cp_args+=(-m 1)

  if ! git cherry-pick "${cp_args[@]}" "$merge_sha"; then
    echo
    echo "!! cherry-pick conflict. Resolve, then run:"
    echo "   git add -A && git cherry-pick --continue"
    echo "   $0 resume $pr_num"
    exit 1
  fi

  finish_backport "$pr_num" "$merge_sha" "$title" "$backport_branch" "$orig_body"
}

# Resume after a manual conflict resolution: push + open PR.
cmd_resume() {
  local pr_num="${1:-}"
  [[ -z "$pr_num" ]] && { echo "Usage: $0 resume <PR_NUMBER>"; exit 1; }

  local backport_branch="backport/${pr_num}-to-${DST_BRANCH}"
  local current
  current="$(git rev-parse --abbrev-ref HEAD)"
  if [[ "$current" != "$backport_branch" ]]; then
    echo "You are on '$current', expected '$backport_branch'. Switch first:"
    echo "   git checkout $backport_branch"
    exit 1
  fi

  if [[ -e .git/CHERRY_PICK_HEAD ]]; then
    echo "A cherry-pick is still in progress. Run 'git cherry-pick --continue' first."
    exit 1
  fi

  local pr_json title merge_sha orig_body
  pr_json="$(gh pr view "$pr_num" --repo "$REPO" --json title,mergeCommit,body)"
  title="$(    jq -r '.title'           <<<"$pr_json")"
  merge_sha="$(jq -r '.mergeCommit.oid' <<<"$pr_json")"
  orig_body="$(jq -r '.body // ""'      <<<"$pr_json")"

  finish_backport "$pr_num" "$merge_sha" "$title" "$backport_branch" "$orig_body"
}

finish_backport() {
  local pr_num="$1" merge_sha="$2" title="$3" backport_branch="$4" orig_body="${5:-}"

  git push -u "$REMOTE" "$backport_branch"

  # Determine the head ref for the PR. If the remote is a fork, qualify with owner.
  local remote_owner
  remote_owner="$(gh repo view --json owner -q '.owner.login' \
                    "$(git remote get-url "$REMOTE" | sed -E 's/\.git$//')")"
  local repo_owner="${REPO%%/*}"
  local head_ref="$backport_branch"
  if [[ "$remote_owner" != "$repo_owner" ]]; then
    head_ref="${remote_owner}:${backport_branch}"
  fi

  # Strip any trailing "(#NNN)" or "(backport #NNN)" segments to avoid duplication.
  local clean_title="$title"
  for _ in 1 2; do
    clean_title="$(sed -E 's/[[:space:]]*\((backport[[:space:]]+)?#[0-9]+\)[[:space:]]*$//' <<<"$clean_title")"
  done

  local body
  body="$(printf '%s\n' \
    "Backport of #$pr_num to \`$DST_BRANCH\`." \
    "" \
    "Original commit: $merge_sha" \
    "Original PR: https://github.com/$REPO/pull/$pr_num" \
    "" \
    "---" \
    "" \
    "<!-- Original PR description below -->" \
    "" \
    "$orig_body")"

  gh pr create --repo "$REPO" \
     --base "$DST_BRANCH" --head "$head_ref" \
     --title "$clean_title (backport #$pr_num)" \
     --body  "$body"

  echo ">>> Backport PR for #$pr_num created."
}

main() {
  local sub="${1:-}"; shift || true
  case "$sub" in
    diff)     cmd_diff ;;
    list-prs) cmd_list_prs "${1:-}" ;;
    pending)  cmd_pending  "${1:-}" ;;
    backport) cmd_backport "${1:-}" ;;
    resume)   cmd_resume   "${1:-}" ;;
    *)
      cat <<EOF
Usage:
  $0 diff                         Compare $SRC_BRANCH with $DST_BRANCH
  $0 list-prs [YYYY-MM-DD]        List PRs merged into $SRC_BRANCH (exact base)
  $0 pending  [YYYY-MM-DD]        List PRs not yet present in $DST_BRANCH
  $0 backport <PR_NUMBER>         Backport one PR to $DST_BRANCH
  $0 resume   <PR_NUMBER>         After resolving conflicts: push + open PR
EOF
    ;;
  esac
}

main "$@"