<#
Untrack docs and playwright tests so they won't be pushed to GitHub.
Run this from the repo root (PowerShell).
It will stage the removals; commit manually to finalize.
#>

try {
    Write-Host "Untracking docs/ and tests/playwright/ (if tracked)"
    git rm --cached -r docs tests/playwright -ErrorAction SilentlyContinue
    Write-Host "Removed from index. Review 'git status', then commit without co-author lines. Example:`n    git commit -m 'Remove docs and playwright tests from repo'`
    Write-Host "Then push: git push origin <branch>"
} catch {
    Write-Host "An error occurred: $_"
}
