# @keletes/moleculer-files-s3

`@keletes/moleculer-files-s3`: S3 storage driver for [`@keletes/moleculer-files`](https://github.com/keletes/moleculer-files), on the MinIO JS client (v8, works with any S3-compatible endpoint). Used by configurator-cache. Design and usage are in `README.md`.

## Shape of this repo (like `@keletes/moleculer-db-adapter-mongo`)

- **Published to GitHub Packages** as `@keletes/moleculer-files-s3` (registry `https://npm.pkg.github.com`, `.npmrc` maps the `@keletes` scope). Consumers: `"@keletes/moleculer-files-s3": "^X.Y.Z"` plus a token with `read:packages`. CI installs authenticate with the org secret `PACKAGES_READ_TOKEN`.
- **`dist/` is not tracked.** The release builds it (`@semantic-release/exec` runs `yarn build`) and publishes it in the package; `package.json` `version` stays `0.0.0-development`. Do NOT commit `dist/`.
- `yarn.lock` is committed (CI installs with `--frozen-lockfile`).
- `moleculer` and `@keletes/moleculer-files` are peer dependencies; `@keletes/moleculer-files` is also a devDependency (`^0.3.0`, from GitHub Packages), bump it when the base releases something the driver needs.

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
- `require("@keletes/moleculer-files-s3")` returns the class itself (`export =`), as before the TypeScript rewrite: configurator-cache loads it with `require`.
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

On a release, semantic-release builds `dist/`, tags `vX.Y.Z`, publishes a GitHub Release and publishes the package to GitHub Packages. No release commit is pushed back. **Never bump the version or push a `v*` tag by hand.**

`tag.yml` creates the tag/release with the GitHub App token (org secrets `DEPLOY_APP_ID`, `DEPLOY_APP_PRIVATE_KEY`) and publishes with the workflow `GITHUB_TOKEN` (`packages: write`). Node 22 is required for `yarn install`.

## Branching: check merge status first

*Always* check whether the current branch has already been merged into `main` before starting work: `git fetch origin`, `git branch -r --merged origin/main`, `git cherry origin/main`, and (squash merges) `gh pr list --head <branch> --state merged`. If merged, branch from an up-to-date `origin/main`. Every change goes through a feature branch and PR; `main` is protected. After a PR merges, check `main` out and fast-forward it before starting anything else.
