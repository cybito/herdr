# Custom release provenance

- GitHub fork: `https://github.com/cybito/herdr` (upstream: `https://github.com/herdrdev/herdr.git`). `custom` is the custom build and release source.
- Historical Forgejo source: `https://git.cybit.top/cybit/herdr`; its custom ref was `85da0bf55d2d56fce868b422c079e97cd4260141` at migration. This is historical provenance only.
- Historical Forgejo OCI native artifact `git.cybit.top/cybit/ias-herdr`: Darwin digest `sha256:2c211e67a7969ec21342760f7e27808cd1f24e937dacf6e2ff85d8ed43aa8930`; Linux digest `sha256:2b557fc553c0e84f30a21c30747699c7b253ea8cc8a23ca59bd7013a019e8af4`. Its receipt records source commit `cf6fb543fe88f6ab568794ac01b0cd8c66cba4ef`, not the current custom tip; do not rewrite that provenance.

## Release contract

Only a published GitHub Release in `cybito/herdr` triggers `.github/workflows/custom-release.yml`. Ordinary pushes, tag pushes, and pull requests do not publish. Keep the fork's default branch `custom`; disable inherited upstream workflows, leaving only Custom release enabled.

Tags are `v<major>.<minor>.<patch>-custom.<positive integer>`, and their dereferenced commit must be an ancestor of `origin/custom`. The base must equal `Cargo.toml`'s package version. This migration's first fully verified asset release uses `v0.9.3-custom.4`; existing tags/releases are never moved. Include the workflow and both scripts in the tagged commit. Validation resolves the tag's exact SHA; both builds checkout that SHA even if `custom` advances.

Publish the GitHub Release at that exact pushed custom SHA. The macOS ARM64 job uses `macos-26`; Omarchy ARM64 GNU/Linux uses `ubuntu-24.04-arm`. Both use Rust **1.96.1**, Zig **0.16.0**, checksum-verified downloads, `cargo build --release --locked`, and the existing Herdr build identity. Native `herdr --version` must print `herdr <tag without v>`.

The platform package layout, receipt, checksums, native architecture validation, isolated install-prefix smoke, conflict rejection, idempotence, and Linux shared-library check remain enforced. Build outputs and fixtures live on the ephemeral runner. Product packages are GitHub Release assets, not OCI artifacts. The workflow uses `GITHUB_TOKEN`/`GH_TOKEN` with `contents: write` only for asset and managed-note operations; no Forgejo PAT, package environment, ORAS, or external registry is needed.

Each package file is uploaded under the deterministic name `<tag>-<platform>-<original-filename>`, including `release.json` and `SHA256SUMS`. Existing matching assets are downloaded and compared byte-for-byte; mismatches fail and are never overwritten. Missing files in a partial set may be added safely. The full set is downloaded again and checked for source tag/commit/platform, receipt fields, payload hash/size, and checksums. Files must be smaller than 2 GiB, and a release may contain at most 1000 assets.

After both platform sets independently verify, the summary job updates only the `<!-- custom-builds:start -->` / `<!-- custom-builds:end -->` block in existing Release notes, preserving user-authored notes outside that block. It links the GitHub Release assets and installation instructions.

## Download and install

Use the assets linked from the GitHub Release for the chosen ARM64 platform. Every file is prefixed `<tag>-<platform>-`; restore the original filename before extracting or validating checksums. For example:

```sh
tag=v0.9.3-custom.4
platform=linux
mkdir -p /absolute/empty/download-dir
cd /absolute/empty/download-dir
gh release download "$tag" --repo cybito/herdr --pattern "$tag-$platform-*"
for asset in "$tag-$platform-"*; do mv "$asset" "${asset#"$tag-$platform-"}"; done
sha256sum -c SHA256SUMS
tar -xzf "herdr-$tag-$platform-arm64.tar.gz"
./install.sh --prefix /absolute/isolated/prefix
/absolute/isolated/prefix/bin/herdr --version
```

For macOS, use `platform=darwin` and `shasum -a 256 -c SHA256SUMS`. Requirements are GitHub CLI, Python 3 for the installer, and a matching ARM64 host. The archive contains `bin/herdr`, upstream README, LICENSE/NOTICE when present, and `install.sh`; installation only copies to `<prefix>/bin` and `<prefix>/share/herdr`. The default prefix is `$HOME/.local`. Different existing files and symlink destinations are rejected. Installation does not alter configuration, uninstall system packages, restart services, or launch a server. macOS binaries are not Apple Developer signed/notarized; Gatekeeper manual approval may be required.

## Maintainer helper interface and regression seam

From the exact release checkout, all directories must be absolute:

```sh
bash .github/scripts/custom-release.sh build linux "$TAG" "$SHA" "$BUILD_DIR"
python3 .github/scripts/package-release.py check --tag "$TAG" --commit "$SHA" --platform linux --output-dir "$CHECK_DIR"
python3 .github/scripts/package-release.py pack --tag "$TAG" --commit "$SHA" --platform linux --input-dir "$BUILD_DIR" --output-dir "$PACKAGE_DIR"
bash .github/scripts/custom-release.sh smoke linux "$TAG" "$SHA" "$PACKAGE_DIR"
GH_TOKEN="$GITHUB_TOKEN" python3 .github/scripts/package-release.py publish --directory "$PACKAGE_DIR"
python3 .github/scripts/package-release.py verify --tag "$TAG" --commit "$SHA" --platform linux --output-dir "$VERIFY_DIR"
python3 .github/scripts/package-release.py self-test
```

`check` returns `exists:true` after download/readback verification or `exists:false` for an absent platform set; failures are nonzero. `pack` returns `directory`; `publish` returns the Release link and asset names; `verify` returns the validated receipt. Tests cover absence, partial/mismatching assets and release limits, alongside event/source identity boundaries and receipt/checksum/payload validation. The integration owner should run the test seam, shell/YAML checks, and two hosted release jobs; no test/build/formatter was run for this change.
