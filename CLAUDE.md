# moleculer-files-s3

`moleculer-files-s3`: S3 storage driver for [`moleculer-files`](https://github.com/keletes/moleculer-files), on the MinIO JS client (v8, works with any S3-compatible endpoint). Used by configurator-cache. Design and usage are in `README.md`.

## Shape of this repo (like `@keletes/moleculer-db-adapter-mongo`)

- **No npm registry.** Consumers install from a git tag: `"moleculer-files-s3": "git+https://github.com/keletes/moleculer-files-s3.git#vX.Y.Z"`. Public repo, so no `PACKAGES_READ_PAT` anywhere.
- **`dist/` is built and committed by the release commit only** (`@semantic-release/exec` runs `yarn build`, `@semantic-release/git` force-adds `dist/**`). `dist/` is gitignored on purpose: do NOT commit it in PRs.
- `yarn.lock` is committed (CI installs with `--frozen-lockfile`).
- `moleculer` and `moleculer-files` are peer dependencies; `moleculer-files` is also a devDependency pinned to a tag of the base repo (`git+https://github.com/keletes/moleculer-files#vX.Y.Z`), bump it when the base releases something the driver needs.

## Commands

```
yarn verify   # typecheck + lint
yarn test     # jest against an in-process fake S3 (test/fake-s3.ts); no containers, no network
yarn build    # tsc -> dist/
```

CI (`verify.yml`): `verify` (typecheck, lint, build, audit) and `test` are the required checks on `main`.

## Gotchas

- **Tests use `test/fake-s3.ts`**, a small HTTP server speaking just the S3 calls the MinIO client makes (bucket exists/create/location, object put/get/head/delete, listing v1/v2, multipart upload incl. list/abort). It checks the access key in the SigV4 `Authorization` header but not the signature. If a MinIO client upgrade starts calling something else, extend the fake rather than mocking the client.
- Moleculer copies the adapter instance when it merges the service schema: in tests use `service.adapter`, not the instance you passed in.
- `require("moleculer-files-s3")` returns the class itself (`export =`), as before the TypeScript rewrite: configurator-cache loads it with `require`.
- `save` goes through `stream.pipeline` into the upload because the MinIO client ignores errors of the source stream (the upload would hang); on failure the incomplete multipart upload is removed.
- The bucket is the service's `collection`. Objects are addressed by key (`meta.id`, else `meta.filename`, else a UUID); there is no versioning.

## Releases: trunk-based, Conventional Commits

**Every PR title must follow [Conventional Commits](https://www.conventionalcommits.org/)** (`.github/workflows/pr-title.yml`). Squash-merge uses the PR title as the `main` commit message, which `tag.yml` feeds to `semantic-release`:

| Prefix | Effect |
|---|---|
| `fix:` | patch |
| `feat:` | minor |
| `feat!:` or `BREAKING CHANGE:` footer | major |
| `chore:`, `docs:`, `style:`, `refactor:`, `perf:`, `test:`, `ci:`, `build:` | no release |

On a release, semantic-release builds `dist/`, commits `package.json` + `dist/` as `chore(release): X.Y.Z [skip ci]`, tags `vX.Y.Z` and publishes a GitHub Release. **Never bump the version or push a `v*` tag by hand.**

`tag.yml` pushes through the branch ruleset with the GitHub App token (org secrets `DEPLOY_APP_ID`, `DEPLOY_APP_PRIVATE_KEY`; the app is a bypass actor of the ruleset and must be installed on this repo). Node 22 is required for `yarn install`.

## Branching: check merge status first

*Always* check whether the current branch has already been merged into `main` before starting work: `git fetch origin`, `git branch -r --merged origin/main`, `git cherry origin/main`, and (squash merges) `gh pr list --head <branch> --state merged`. If merged, branch from an up-to-date `origin/main`. Every change goes through a feature branch and PR; `main` is protected. After a PR merges, check `main` out and fast-forward it before starting anything else.
