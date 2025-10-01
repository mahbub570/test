# PR Maintainer Checklist Workflow

This GitHub Action automatically comments on new pull requests to provide maintainers with a checklist and onboarding information.  

## Features
- Posts a **maintainer checklist** on every new PR targeting `master`.
- Encourages contributors to join the [Scribe community on Matrix](https://matrix.to/#/#scribe_community:matrix.org).
- Provides **commit message checks** for first-time contributors.
- Ensures maintainers verify:
  - Linting and formatting checks.
  - `CHANGELOG.md` updates.

## Trigger
- Runs on **pull_request_target** when a PR is **opened** against `master`.

## Permissions
- Requires `pull-requests: write` to post comments.

## Example Use
Copy the file into `.github/workflows/pr_maintainer_checklist.yml` in your repository.

---
✨ Maintainers get a consistent checklist, and contributors get helpful guidance automatically.
