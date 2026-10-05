# Custom release provenance

- GitHub fork: `https://github.com/cybito/herdr` (upstream: `https://github.com/herdrdev/herdr.git`). `custom` is the custom build and release source.
- Historical Forgejo source: `https://git.cybit.top/cybit/herdr`; its custom ref was `85da0bf55d2d56fce868b422c079e97cd4260141` at migration. The historical repository remains intact.
- Historical OCI native artifact `git.cybit.top/cybit/ias-herdr`: Darwin digest `sha256:2c211e67a7969ec21342760f7e27808cd1f24e937dacf6e2ff85d8ed43aa8930`; Linux digest `sha256:2b557fc553c0e84f30a21c30747699c7b253ea8cc8a23ca59bd7013a019e8af4`. Its receipt records source commit `cf6fb543fe88f6ab568794ac01b0cd8c66cba4ef`, not the current custom tip; do not rewrite that provenance.
- Custom install packages share OCI namespace `ias-herdr` as a distinct artifact type. Immutable download: `oras pull git.cybit.top/cybit/ias-herdr@sha256:<digest>`.

## Release contract

Only a published GitHub Release in `cybito/herdr` triggers
`.github/workflows/custom-release.yml`. Ordinary pushes, tag pushes, and pull
requests do not publish. Keep the fork's default branch `custom`; disable all
inherited upstream workflows in the fork's Actions settings, leaving only
Custom release enabled. No upstream release workflow is called.

Tags must be `v<major>.<minor>.<patch>-custom.<positive integer>` and their
dereferenced commit must be an ancestor of `origin/custom`. The base must equal
`Cargo.toml`'s actual package version. The planned first tag is
`v0.9.3-custom.1`; if already occupied, use the next unused custom integer for
the current base. Do not move an existing release tag. Include the workflow and
both scripts in the tagged commit. Release `target_commitish` alone is not
trusted; validation resolves the tag's exact SHA and both builds checkout that
SHA even if `custom` advances.

Publish the GitHub Release with that exact pushed custom SHA as its target and
no attachments. The macOS ARM64 job uses `macos-26`; the Omarchy ARM64 native
GNU/Linux job uses `ubuntu-24.04-arm`. Both use Rust **1.96.1** and Zig
**0.16.0**, checksum-verified downloads, `cargo build --release --locked`,
`LIBGHOSTTY_VT_OPTIMIZE=ReleaseFast`, and `LIBGHOSTTY_VT_SIMD=true`. The build
identity is `HERDR_BUILD_CHANNEL=custom`, `HERDR_BUILD_ID=<custom integer>`,
`HERDR_BUILD_COMMIT=<exact SHA>`. Cargo's native base version is not changed
to the release tag, and historical IME receipt identities are not injected.
Native `herdr --version` must print `herdr <tag without v>`.

Build output, dependency downloads, install fixtures, and auth files live only
on the ephemeral hosted runner. No GitHub Release assets, GitHub Artifacts,
Actions cache, GHCR, taps, or production installation are used.

## Forgejo credentials and publication prerequisites

Before the first release, create the GitHub environment `forgejo-registry`.
Its deployment policy allows tags `v*-custom.*`, not branch deployments.
Human must create a dedicated Forgejo PAT for account `cybit`, preferably
public-only with package-write permission, and supply it through hidden input
as environment secret `FORGEJO_REGISTRY_TOKEN`. Do not export existing Forgejo
OAuth keys or Docker credential-helper secrets. Package permissions are owner
scoped: the PAT may affect other packages owned by `cybit`; it is not honestly
restricted to this single repository.

The token is injected only into the upload step, after native version/help and
archive install smoke pass. Login uses stdin, a private mode-0700 auth directory,
and a mode-0600 registry config. Registry commands explicitly select a config;
anonymous checks use a separate empty config. Cleanup runs after upload and in
an `always()` step. TLS verification is never disabled. Missing credentials or
registry auth/network failures fail the job, never masquerade as a missing
package. Initial real publication still requires this human provisioning and
the actual hosted build runs; implementation alone is not upload evidence.

The package is `git.cybit.top/cybit/ias-herdr`, linked in Forgejo's package
settings to `cybit/herdr`. Source annotations remain the actual GitHub source
URL, not a fabricated Forgejo source URL; old native receipts remain intact.
New tags are `<release-tag>-{darwin,linux}-arm64`, with artifact type
`application/vnd.cybito.install-package.v1`, distinct from historical
`application/vnd.ias.native.v1`.

`release.json` has schema 1 and exactly `schema`, `project`, `source_repo`,
`source_commit`, `release_tag`, `platform`, `architecture`, `toolchains`,
`files`. It records actual compiler/build-tool versions and the archive's
size/SHA-256. It is an OCI sidecar, not embedded in the archive it hashes.
`SHA256SUMS` covers both the archive and `release.json`; OCI descriptors cover
every layer, including the sums file. Media types are `application/json`,
`text/plain`, and `application/gzip`, never default container filesystem layers.

Publishing uses a local OCI layout with a source-commit UTC created annotation,
determines its digest before copying, then verifies the exact remote manifest
bytes, config/layer descriptors, and a separate immutable pull. Existing tags
are reused only after complete byte/hash and source/tag/platform verification.
Conflicting identities or payloads fail rather than overwrite. Failed jobs do
not delete registry content; re-running fills only the missing platform.
Both successful platform jobs are required before an anonymous summary job
updates the `<!-- custom-builds:start -->` / `<!-- custom-builds:end -->` block
in the existing Release notes. User-written notes outside that block remain.
That summary has GitHub notes-write permission but no Forgejo write token.

## Download and install

Requirements: ORAS 1.3.3 for OCI downloads, Python 3 for the installer, and an
ARM64 host matching the chosen platform. Use the immutable reference in the
GitHub Release notes rather than a mutable tag:

```sh
mkdir -p /absolute/empty/download-dir
oras pull git.cybit.top/cybit/ias-herdr@sha256:<digest> \
  --output /absolute/empty/download-dir
cd /absolute/empty/download-dir
# Linux:
sha256sum -c SHA256SUMS
# macOS alternative:
shasum -a 256 -c SHA256SUMS
tar -xzf herdr-v0.9.3-custom.1-linux-arm64.tar.gz
# On macOS choose the matching ...-darwin-arm64.tar.gz instead.
./install.sh --prefix /absolute/isolated/prefix
/absolute/isolated/prefix/bin/herdr --version
```

The archive contains `bin/herdr`, upstream `README.md`, the corresponding
`LICENSE`/`NOTICE` when present, and `install.sh`. It copies only to
`<prefix>/bin` and `<prefix>/share/herdr`. The default prefix is `$HOME/.local`;
CI always supplies a temporary absolute prefix. Identical existing files are
left unchanged, including their timestamps. Different existing files and
symlink destination paths are rejected before any copy. Installation does not
overwrite configuration, uninstall packages, restart services, or launch a
server. To replace a different previously installed binary, explicitly choose
a new prefix rather than relying on silent overwrite.

Do not run bare `herdr`, `herdr server`, attach/update commands, or production
socket commands as release smoke. The runner uses `env -i`, isolated HOME/XDG
directories, native `--version` and `--help` only; it confirms help lists the
CLI commands. It repeats fixture installation to check byte/mode/timestamp
idempotence, checks conflicting-file rejection, and checks Linux `ldd` for
missing runtime libraries. No real user data or production session is accessed.
macOS binaries are not Apple Developer signed/notarized; Gatekeeper manual
approval may be required. This fork does not claim official signed identity.

## Maintainer helper interface and verification

From the exact clean release checkout, all directories below must be absolute:

```sh
bash .github/scripts/custom-release.sh build linux "$TAG" "$SHA" "$BUILD_DIR"
python3 .github/scripts/package-release.py check \
  --tag "$TAG" --commit "$SHA" --platform linux --output-dir "$CHECK_DIR"
python3 .github/scripts/package-release.py pack \
  --tag "$TAG" --commit "$SHA" --platform linux \
  --input-dir "$BUILD_DIR" --output-dir "$PACKAGE_DIR"
bash .github/scripts/custom-release.sh smoke linux "$TAG" "$SHA" "$PACKAGE_DIR"
python3 .github/scripts/package-release.py publish \
  --directory "$PACKAGE_DIR" --registry-config "$AUTH_CONFIG"
python3 .github/scripts/package-release.py verify \
  --reference "git.cybit.top/cybit/ias-herdr@sha256:$DIGEST" \
  --output-dir "$VERIFY_DIR"
```

Replace `linux` with `darwin` on native macOS ARM64. `check` emits JSON with
`exists:true` and the verified digest reference, or `exists:false` only for
explicit OCI `manifest_unknown`/`name_unknown`. Other failures are nonzero.
`pack` returns `directory`; `publish` returns `reference` and `digest`;
`verify` returns the validated `release.json`. Verification/output directories
must be empty to avoid mixing releases. Project/owner/package are constants,
not caller-supplied values.

The helper includes actual Git/event boundary regression tests; they create
an isolated repository, accept a custom-ancestor tag, reject an upstream-only
commit and invalid/malicious tags, reject corrupt receipts/payloads and source
identity reuse, and fail closed on ambiguous 404/network/auth failures:

```sh
python3 - <<'PY'
import runpy, unittest
helper = runpy.run_path('.github/scripts/package-release.py')
suite = unittest.defaultTestLoader.loadTestsFromTestCase(helper['ReleaseBoundaryTests'])
result = unittest.TextTestRunner(verbosity=2).run(suite)
raise SystemExit(not result.wasSuccessful())
PY
```

The integration owner should run these tests and shell/YAML checks, then the
two real hosted release jobs and immutable download/install checks. Also
confirm ordinary pushes create no custom release run, successful reruns keep
the same digests, GitHub assets/artifacts/cache remain unused, inherited
workflows remain disabled, and the two historical digests above are still
anonymously readable. This workflow does not modify or deploy IaC pins.
