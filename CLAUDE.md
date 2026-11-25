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
- Before pushing the sync branch, remove upstream workflows and Dependabot config again (new files and modify/delete conflicts alike):
  `git ls-files .github/workflows .github/dependabot.yml | grep -v bringg-docker.yml | xargs git rm -q`

## Upstream automation

- Upstream workflows and `.github/dependabot.yml` are deleted on the fork, so nothing from upstream runs here. `bringg-docker.yml` is the only workflow.
- Dependency bumps come through upstream syncs. Vulnerability alerts stay on.

## Releasing the Docker image

- `.github/workflows/bringg-docker.yml` builds the image and pushes it (amd64 only). Registry login and image name come from repo secrets, not from files in this repo.
- To release: create a GitHub Release on `main` with tag `v<x.y.z>-<n>`, e.g. `v1.4.7-1` -> image tags `1.4.7-1` and `latest`. Next release bumps `<n>`; bump `<x.y.z>` only for bigger changes.
- Never reuse a tag. The workflow fails if the image tag already exists.
- A manual run (`workflow_dispatch`) publishes image tag `sha-<short sha>` only (`latest` is not moved).
