#!/usr/bin/env bash
# Discover the StarRocks supported-version set (major.minor) for SQL-doc
# verification.
#
# Source of truth: GitHub Releases (non-prerelease, non-draft), reduced to
# distinct major.minor, semver-sorted descending (numeric, so 4.10 > 4.9),
# clamped to <= --max, then the top K. This keeps the sql-doc-autofix skill
# correct across releases (e.g. it proposes {4.2,4.1,4.0} once 4.2 ships) with
# no per-release edits.
#
# Usage:
#   docs/scripts/supported_versions.sh [--max X.Y] [-k N] [--repo OWNER/REPO]
#
#   --max X.Y   Highest version to include (default: newest discovered). Use to
#               pin the run to the release whose docs you are working on.
#   -k N        How many distinct minors to return (default: 3).
#   --repo R    GitHub repo (default: StarRocks/starrocks).
#
# Output: space-separated list on stdout, newest first, e.g. "4.1 4.0 3.5".
# On failure (gh missing, network/API error, no releases) it prints a message to
# stderr and exits non-zero, so the caller can fall back to an operator-supplied
# list. The proposed set is meant to be confirmed/edited by a human, not used
# blindly.
set -euo pipefail

REPO="StarRocks/starrocks"
K=3
MAX=""

while [ $# -gt 0 ]; do
  case "$1" in
    --max)     MAX="${2:-}"; shift 2;;
    --max=*)   MAX="${1#*=}"; shift;;
    -k|--top)  K="${2:-}"; shift 2;;
    -k=*)      K="${1#*=}"; shift;;
    --top=*)   K="${1#*=}"; shift;;
    --repo)    REPO="${2:-}"; shift 2;;
    --repo=*)  REPO="${1#*=}"; shift;;
    -h|--help) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'; exit 0;;
    *) echo "supported_versions: unknown arg: $1" >&2; exit 2;;
  esac
done

command -v gh >/dev/null 2>&1 || {
  echo "supported_versions: gh CLI not found; supply the version list manually" >&2; exit 3; }

# Non-prerelease, non-draft release tags -> major.minor, unique, numeric-desc.
# -u dedups on the sort keys (both fields), i.e. on the whole major.minor.
mm=$(gh api "repos/${REPO}/releases?per_page=100" --paginate \
       --jq '.[] | select(.prerelease==false and .draft==false) | .tag_name' 2>/dev/null \
     | sed -nE 's/^v?([0-9]+\.[0-9]+).*/\1/p' \
     | sort -t. -k1,1nr -k2,2nr -u) || {
  echo "supported_versions: gh api call failed; supply the version list manually" >&2; exit 4; }

[ -n "$mm" ] || { echo "supported_versions: no releases found" >&2; exit 5; }

# Clamp to <= MAX (default: keep the newest discovered).
if [ -n "$MAX" ]; then
  MAX_MM=$(printf '%s\n' "$MAX" | sed -nE 's/^v?([0-9]+\.[0-9]+).*/\1/p')
  [ -n "$MAX_MM" ] || { echo "supported_versions: bad --max '$MAX'" >&2; exit 2; }
  mm=$(printf '%s\n' "$mm" | awk -F. -v M="$MAX_MM" '
    BEGIN { split(M, a, ".") }
    { if ($1 < a[1] || ($1 == a[1] && $2 <= a[2])) print }')
  [ -n "$mm" ] || { echo "supported_versions: nothing <= $MAX_MM" >&2; exit 5; }
fi

# Top-K, newest first, space-joined.
printf '%s\n' "$mm" | head -n "$K" | paste -sd' ' -
