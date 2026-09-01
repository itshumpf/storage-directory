#!/usr/bin/env bash
#
# push_with_retry.sh — stage the given paths, commit, and push, surviving a
# remote that moved while the job was running.
#
#   .github/push_with_retry.sh "<commit message>" path [path...]
#
# WHY THIS EXISTS
# ---------------
# The daily scrape runs for over an hour. Anything pushed to main during that
# window — a local code change, another workflow — makes a plain `git push`
# fail as a non-fast-forward, and the commit dies with the runner.
#
# That is not hypothetical. On 2026-09-01 a completed scrape was committed
# (11 files, 37,275 insertions) and then thrown away for exactly this reason.
# The collection had succeeded in full. Nothing was wrong with the data.
#
# Rebasing is safe for this job because it only ever adds generated files and
# rewrites its own outputs; it never touches source.
#
# Requires a full checkout (fetch-depth: 0). You cannot rebase onto history
# the runner does not have.
set -uo pipefail

MSG="${1:?usage: push_with_retry.sh <message> <path> [path...]}"
shift
[ "$#" -gt 0 ] || { echo "no paths given" >&2; exit 2; }

git config user.name  "github-actions[bot]"
git config user.email "github-actions[bot]@users.noreply.github.com"

# Explicit paths only, never -A. Anything not named here is not this job's to
# commit, and a stray file swept up by a wildcard is a change nobody reviewed.
for p in "$@"; do
    [ -e "$p" ] && git add "$p"
done

if git diff --staged --quiet; then
    echo "nothing staged for: $MSG"
    exit 0
fi

git commit -m "$MSG"

for attempt in 1 2 3; do
    if git pull --rebase --autostash origin main && git push; then
        echo "pushed on attempt $attempt: $MSG"
        exit 0
    fi
    echo "push attempt $attempt failed; refetching and retrying"
    sleep 15
done

echo "::error::Could not push after 3 attempts. This commit exists only on"\
     "this runner and will be lost when it is torn down. Re-run the workflow."
exit 1
