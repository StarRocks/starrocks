#!/usr/bin/env bash
#
# ci_case_files.sh -- the CASE FILES check, driven from CI.
#
# Called from .github/workflows/ci-sql-tester-case-files.yml, which supplies only the flow
# (trigger, permissions, checkout, token) and leaves every step here.
#
# WHY the workflow carries no `paths:` filter, and this decides scope instead:
# CASE FILES is a required status check. A workflow a filter keeps from STARTING reports
# nothing at all -- not a skipped check, nothing -- so GitHub waits forever for a context
# that never arrives, and every PR outside the filter is blocked with no error to explain
# it. (A job a filter skips INSIDE a started workflow is the opposite: it reports `skipped`,
# which counts as passing.) So the workflow starts for every PR, and this reports success
# whether or not there was anything to check.
#
# Inputs (env):
#   GITHUB_REPOSITORY  owner/repo
#   PR_NUMBER          the pull request being checked
#   BASE_REF           the PR's base branch, e.g. main
#   GH_TOKEN           token that can read the PR's files

set -euo pipefail

: "${GITHUB_REPOSITORY:?GITHUB_REPOSITORY is empty}"
: "${PR_NUMBER:?PR_NUMBER is empty}"
: "${BASE_REF:?BASE_REF is empty}"

# The same set the `paths:` filter used to name: the case tree, the checker, and its tests.
# A PR that changes only the checker selects no case file, so the check below exercises none
# of the rules -- the unit tests are what cover it there.
IN_SCOPE_RE='^test/sql/|^build-support/(test_)?check_sql_tester_case_files\.py$|^build-support/ci_case_files\.sh$|^\.github/workflows/ci-sql-tester-case-files\.yml$'

# No `|| true`: a failed API call cannot be told apart from "nothing to check", and the two
# must not lead to the same green check.
changed="$(gh api "repos/${GITHUB_REPOSITORY}/pulls/${PR_NUMBER}/files" --paginate -q '.[].filename')"

if ! grep -qE "${IN_SCOPE_RE}" <<<"${changed}"; then
    echo "no case file, checker or workflow change in this PR; nothing to check"
    exit 0
fi

# Only now pay for the history. The checker compares against the merge base, which a
# depth-1 checkout cannot reach; fetching it on every PR is what this ordering avoids.
echo "-------------------- fetching history for the merge base --------------------"
git fetch --unshallow --no-tags origin 2>/dev/null || git fetch --no-tags origin
git fetch --no-tags origin "+refs/heads/${BASE_REF}:refs/remotes/origin/${BASE_REF}"

echo "-------------------- unit tests --------------------"
python3 -m unittest build-support/test_check_sql_tester_case_files.py -v

echo "-------------------- check --------------------"
python3 build-support/check_sql_tester_case_files.py --base "origin/${BASE_REF}"
