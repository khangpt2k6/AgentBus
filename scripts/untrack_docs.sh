#!/bin/sh
# Untrack docs and playwright tests so they won't be pushed to GitHub.
# Run this from the repo root. It will stage the removal; you must commit manually.

set -e

echo "This will untrack the following paths if they are currently tracked: docs/ tests/playwright/"

git rm --cached -r docs tests/playwright || true

echo "Files removed from the index. Review 'git status', then commit with a message that does not include any co-author lines. Example:"
echo "  git commit -m \"Remove docs and playwright tests from repo\""
echo "Finally push to your branch:"
echo "  git push origin <branch>"

echo "If you prefer to keep local copies but stop tracking, the above is sufficient."
