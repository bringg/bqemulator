@AGENTS.md

# Bringg fork rules (override AGENTS.md where they conflict)

This repo is `bringg/bqemulator`, a fork of `jjviscomi/bqemulator`.

## Remotes

- `origin` = `bringg/bqemulator` (where all work goes)
- `upstream` = `jjviscomi/bqemulator` (read-only; fetch only)

One-time setup per clone:

```bash
git remote add upstream git@github.com:jjviscomi/bqemulator.git
git remote set-url --push upstream no_push
gh repo set-default bringg/bqemulator
```

## Pull requests

- Every PR targets `bringg/bqemulator`, base `main`. Never open a PR to `jjviscomi/bqemulator`.
- Always pass the repo explicitly:
  `gh pr create --repo bringg/bqemulator --base main --draft`
- Never push to `upstream`.

## Syncing upstream

- One direction only: upstream -> `bringg/main`.
- `git fetch upstream`, merge `upstream/main` into a `sync/upstream-<date>` branch from `origin/main`, open a PR to `bringg/bqemulator:main`.
- Merge sync PRs with "Create a merge commit", never squash.
- Never use GitHub "Sync fork" -> "Discard commits".
