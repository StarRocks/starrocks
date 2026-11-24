#!/usr/bin/env bash
#
# collect-minor-prs.sh — list the PRs that are NEW in a minor release (X.Y.0), in tiers.
#
# A minor's branch is cut from main, so "prev_tag...new_tag" is wrong (it mixes histories
# and includes everything already shipped in the previous minor's patches). Instead:
#   candidates = main commits between the two fork points
#              + commits that landed only on the new branch after its fork (backports)
#   minus      = PRs already backported to the previous minor (up to <prev_ref>, normally its
#                branch tip, so fixes queued for its next patch are not counted as new) and to
#                any OLDER_BRANCHES (e.g. branch-4.0 branch-3.5).
# Enhancements and bug fixes are normally backported, so what is left of them is mostly work on
# features that only exist in the new minor; treat the bugfix tier as a short list, not a log.
# Every commit is resolved to its ORIGINAL main PR number: the leftmost "backport #N" in the
# subject, else the trailing "(#N)".
#
# Works on a local clone (a treeless bare clone is enough: git clone --bare --filter=tree:0).
# Read-only, no GitHub API calls, no labels (labels such as behavior_changed must be fetched
# separately for the shortlisted PRs).
#
# Usage:
#   collect-minor-prs.sh <prev_branch> <new_branch> <prev_ref> <out_dir>
#   e.g. OLDER_BRANCHES='branch-4.0 branch-3.5' collect-minor-prs.sh branch-4.1 branch-4.2 branch-4.1 /tmp/notes-4.2
#   <prev_ref> is the previous minor's branch (or its newest tag, if the branch holds unreleased work to ignore).
# Env: REPO_DIR (default: current dir), MAIN (default: main), OLDER_BRANCHES (space-separated), NEW_REF (default: <new_branch>; set
#      to the X.Y.0 tag once it exists).
#
# Output (in out_dir): summary.txt plus one TSV per tier: number<TAB>sha<TAB>subject
#   feature.tsv  enhancement.tsv  bugfix.tsv  security.tsv  noprefix.tsv
# Excluded as non-user-facing (counted in summary.txt only): [Refactor] [Tool] [Doc] [UT].

set -euo pipefail

PREV_BRANCH="${1:?usage: collect-minor-prs.sh <prev_branch> <new_branch> <prev_ref> <out_dir>}"
NEW_BRANCH="${2:?}"
PREV_REF="${3:?}"
OLDER_BRANCHES="${OLDER_BRANCHES:-}"
OUT="${4:?}"
MAIN="${MAIN:-main}"
NEW_REF="${NEW_REF:-$NEW_BRANCH}"
cd "${REPO_DIR:-.}"
mkdir -p "$OUT"

F_PREV="$(git merge-base "$MAIN" "$PREV_BRANCH")"
F_NEW="$(git merge-base "$MAIN" "$NEW_BRANCH")"

# stdin: "<sha>\t<subject>" -> "<pr>\t<sha>\t<subject>" with the original main PR number.
resolve() {
  awk -F'\t' '{
    s=$2; n=""
    if (match(tolower(s), /backport #[0-9]+/)) { n=substr(s, RSTART+10, RLENGTH-10) }
    else if (match(s, /\(#[0-9]+\)$/))         { n=substr(s, RSTART+2, RLENGTH-3) }
    if (n != "") print n "\t" $1 "\t" s
  }'
}

{
  git log --format='%H%x09%s' "$F_PREV..$PREV_REF"
  for b in $OLDER_BRANCHES; do git log --format='%H%x09%s' "$F_PREV..$b"; done
} | resolve | cut -f1 | sort -u > "$OUT/.shipped_in_prev"
{
  git log --format='%H%x09%s' "$F_PREV..$F_NEW"               # main between the two fork points
  git log --format='%H%x09%s' "$F_NEW..$NEW_REF"               # only on the new branch since its fork
} | resolve | sort -t$'\t' -k1,1 -u > "$OUT/.all"

awk -F'\t' 'NR==FNR { shipped[$1]=1; next } !($1 in shipped)' "$OUT/.shipped_in_prev" "$OUT/.all" > "$OUT/.new"

: > "$OUT/feature.tsv"; : > "$OUT/enhancement.tsv"; : > "$OUT/bugfix.tsv"
: > "$OUT/security.tsv"; : > "$OUT/noprefix.tsv"
awk -F'\t' -v out="$OUT" '
  { s=$3; t=tolower(s); p="" ; if (match(s, /^\[[A-Za-z]+\]/)) p=tolower(substr(s, RSTART+1, RLENGTH-2))
    if (t ~ /cve-[0-9]+-[0-9]+/ || (t ~ /(bump|upgrade)/ && t ~ /(netty|jackson|hadoop|spark|zookeeper|jdk|bouncycastle|parquet|zstd|guava|log4j|tomcat|jetty|libcurl|openssl)/)) { print > (out "/security.tsv"); sec++; next }
    if      (p=="feature")     { print > (out "/feature.tsv");     f++ }
    else if (p=="enhancement") { print > (out "/enhancement.tsv"); e++ }
    else if (p=="bugfix")      { print > (out "/bugfix.tsv");      b++ }
    else if (p=="refactor"||p=="tool"||p=="doc"||p=="ut") { x[p]++ }
    else                       { print > (out "/noprefix.tsv");    o++ }
  }
  END { printf "feature=%d enhancement=%d bugfix=%d security=%d noprefix=%d | excluded refactor=%d tool=%d doc=%d ut=%d\n", f,e,b,sec,o,x["refactor"],x["tool"],x["doc"],x["ut"] > (out "/.counts") }
' "$OUT/.new"

{
  echo "prev fork point : ${F_PREV:0:10} ($PREV_BRANCH)"
  echo "new fork point  : ${F_NEW:0:10} ($NEW_BRANCH), new ref: $NEW_REF"
  echo "all PRs in range            : $(wc -l < "$OUT/.all" | tr -d ' ')"
  echo "already in $PREV_REF/older  : $(( $(wc -l < "$OUT/.all") - $(wc -l < "$OUT/.new") ))"
  echo "new in this minor           : $(wc -l < "$OUT/.new" | tr -d ' ')"
  cat "$OUT/.counts"
} | tee "$OUT/summary.txt"
