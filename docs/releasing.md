# Releasing

The `minipaw` library and the `minipaw-cli` crate are released together from
`.github/workflows/release.yml`, with [release-plz](https://release-plz.dev)
configured in `release-plz.toml`.

## How a release happens

1. Every push to `main` updates a release PR (branch `release-plz-…`). It
   bumps the workspace version from the conventional commits since the last
   release (below 1.0, a breaking change, `feat!:`, bumps the minor version
   and anything else the patch) and adds their entries to `CHANGELOG.md`.
   Edit the PR to reword the changelog.
2. Merging it publishes `minipaw`, then `minipaw-cli`, to crates.io, tags
   `vX.Y.Z`, and creates the GitHub release with the changelog entry as its
   notes (the site's changelog page reads them).
3. The same run builds the CLI for `linux-x64`, `linux-arm64` (static,
   musl), `darwin-x64` and `darwin-arm64`, attaches
   `minipaw-<platform>.tar.gz` and `SHA256SUMS` to the release, and only then
   marks it latest, which is what `web/public/install.sh` downloads.

The two crates share a version group: a change to the library bumps both. A
change only to the CLI bumps only the CLI, and the library catches up at its
next release.

The release PR is opened with `GITHUB_TOKEN`, so CI doesn't run on it; it
only changes versions and changelogs, and CI runs on `main` once it merges.
If `main` ever requires checks, give release-plz a GitHub App token instead.

To rebuild the binaries for a release, run the workflow by hand (Actions →
Release → Run workflow) with its tag.

## One-time setup

- Settings → Actions → General: allow GitHub Actions to create pull
  requests.
- Before this workflow first runs on `main` (merging it): crates.io
  publishing is trusted publishing (no token in the repo), which
  can't create a crate, so publish the first version by hand:

  ```sh
  cargo publish -p minipaw
  cargo publish -p minipaw-cli
  ```

  Then, on crates.io, add a trusted publisher to both crates: repository
  `deepso7/minipaw`, workflow `release.yml`.
- That first version has no tag or GitHub release yet. Run the Release
  workflow by hand on `main` with tag `v0.1.0`: it creates both from that
  commit and attaches the binaries.
