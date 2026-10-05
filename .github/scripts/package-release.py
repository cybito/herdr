#!/usr/bin/env python3
"""Herdr install-package OCI contract. No native-v1 receipts or credentials reused."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import unittest
from unittest.mock import patch

PROJECT = "herdr"
SOURCE = "https://github.com/cybito/herdr.git"
PACKAGE = "git.cybit.top/cybit/ias-herdr"
ARTIFACT_TYPE = "application/vnd.cybito.install-package.v1"
TAG_RE = re.compile(r"v([0-9]+\.[0-9]+\.[0-9]+)-custom\.([1-9][0-9]*)\Z")
SHA_RE = re.compile(r"[0-9a-f]{40}\Z")
DIGEST_RE = re.compile(r"sha256:[0-9a-f]{64}\Z")
FIELDS = {"schema", "project", "source_repo", "source_commit", "release_tag", "platform", "architecture", "toolchains", "files"}
MEDIA = {"release.json": "application/json", "SHA256SUMS": "text/plain"}
ROOT = Path(__file__).resolve().parents[2]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def run(args, cwd=None):
    result = subprocess.run([str(a) for a in args], cwd=cwd, capture_output=True)
    if result.returncode:
        raise RuntimeError(result.stderr.decode(errors="replace") or result.stdout.decode(errors="replace"))
    return result.stdout


def git(*args, cwd=ROOT):
    return run(["git", *args], cwd=cwd).decode().strip()


def absolute(value):
    path = Path(value)
    require(path.is_absolute(), "directory must be absolute")
    return path


def empty_directory(path):
    require(not path.is_symlink(), "directory must not be a symlink")
    path.mkdir(parents=True, exist_ok=True)
    require(not any(path.iterdir()), f"directory must be empty: {path}")


def identity(tag, commit, platform):
    require(TAG_RE.fullmatch(tag) is not None, "invalid custom release tag")
    require(SHA_RE.fullmatch(commit) is not None, "invalid source commit")
    require(platform in ("darwin", "linux"), "invalid platform")


def validate_event(event, repository, cwd=ROOT):
    require(repository == "cybito/herdr", "wrong repository")
    require(event.get("action") == "published", "only release.published is accepted")
    release = event["release"]
    require(release.get("draft") is False, "draft releases are forbidden")
    require(not release.get("assets"), "GitHub Release assets must be empty")
    tag = release["tag_name"]
    require(isinstance(tag, str) and TAG_RE.fullmatch(tag), "invalid custom release tag")
    commit = git("rev-parse", "--verify", f"refs/tags/{tag}^{{commit}}", cwd=cwd)
    identity(tag, commit, "linux")
    require(git("rev-parse", "HEAD", cwd=cwd) == commit, "validation checkout must be the exact release tag commit")
    run(["git", "merge-base", "--is-ancestor", commit, "refs/remotes/origin/custom"], cwd=cwd)
    for name in (".github/workflows/custom-release.yml", ".github/scripts/custom-release.sh", ".github/scripts/package-release.py"):
        git("cat-file", "-e", f"{commit}:{name}", cwd=cwd)
    manifest = git("show", f"{commit}:Cargo.toml", cwd=cwd)
    version = re.search(r'^version\s*=\s*"([^"]+)"', manifest, re.MULTILINE)
    require(version and version.group(1) == TAG_RE.fullmatch(tag).group(1), "tag base differs from Cargo package version")
    return {"tag": tag, "commit": commit, "version": tag[1:]}


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def file_record(path):
    return {"name": path.name, "sha256": sha(path), "size": path.stat().st_size}


def filename(name):
    require(isinstance(name, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", name), "unsafe filename")
    require(name not in (".", ".."), "unsafe filename")
    return name


def media(name):
    return MEDIA.get(name, "application/gzip" if name.endswith(".tar.gz") else "application/octet-stream")


def receipt(directory):
    result = json.loads((directory / "release.json").read_text())
    require(set(result) == FIELDS and type(result["schema"]) is int and result["schema"] == 1, "invalid receipt schema")
    require(result["project"] == PROJECT and result["source_repo"] == SOURCE, "wrong receipt source")
    identity(result["release_tag"], result["source_commit"], result["platform"])
    require(result["architecture"] == "arm64", "wrong architecture")
    require(isinstance(result["toolchains"], dict) and result["toolchains"] and all(isinstance(k, str) and isinstance(v, str) and v for k, v in result["toolchains"].items()), "invalid toolchains")
    require(result["toolchains"].get("rustc", "").startswith("rustc 1.96.1 ") and result["toolchains"].get("zig") == "0.16.0", "wrong receipt toolchains")
    files = result["files"]
    expected_archive = f"herdr-{result['release_tag']}-{result['platform']}-arm64.tar.gz"
    require(isinstance(files, list) and len(files) == 1, "Herdr requires exactly one installation archive")
    record = files[0]
    require(set(record) == {"name", "sha256", "size"}, "invalid file record")
    require(filename(record["name"]) == expected_archive, "wrong installation archive name")
    require(isinstance(record["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", record["sha256"]), "invalid file hash")
    require(type(record["size"]) is int and record["size"] > 0, "invalid file size")
    path = directory / record["name"]
    require(path.is_file() and not path.is_symlink(), "payload is not a regular file")
    require(file_record(path) == record, "payload hash or size mismatch")
    names = [record["name"], "release.json"]
    expected_sums = "".join(f"{sha(directory / name)}  {name}\n" for name in names)
    require((directory / "SHA256SUMS").read_text() == expected_sums, "SHA256SUMS mismatch")
    return result


def oras(args, config, cwd=None):
    flag = "--to-registry-config" if args[0] == "cp" else "--registry-config"
    return run(["oras", *args, flag, config], cwd=cwd)


def anonymous_config(directory):
    path = directory / "anonymous-auth.json"
    path.write_text('{"auths":{}}\n')
    path.chmod(0o600)
    return path


def fetch_manifest(reference, config):
    result = subprocess.run(["oras", "manifest", "fetch", reference, "--registry-config", str(config)], capture_output=True)
    if result.returncode:
        error = result.stderr.decode(errors="replace")
        # Only explicit OCI Distribution error codes mean absence; never generic 404,
        # permission denial, or network failures. Any ambiguity fails closed.
        if re.search(r"\b(?:MANIFEST_UNKNOWN|NAME_UNKNOWN|manifest_unknown|name_unknown)\b", error):
            return None
        raise RuntimeError(error or "manifest fetch failed")
    return result.stdout


def verify_reference(reference, output, config, manifest_bytes=None):
    require(reference.startswith(PACKAGE + "@") and DIGEST_RE.fullmatch(reference.split("@")[-1]), "verification requires this package's immutable digest reference")
    empty_directory(output)
    raw = manifest_bytes if manifest_bytes is not None else fetch_manifest(reference, config)
    require(raw is not None, "immutable manifest missing")
    require("sha256:" + hashlib.sha256(raw).hexdigest() == reference.split("@")[-1], "manifest digest mismatch")
    manifest = json.loads(raw)
    require(manifest.get("schemaVersion") == 2 and manifest.get("artifactType") == ARTIFACT_TYPE, "wrong OCI artifact type")
    require(manifest.get("mediaType") == "application/vnd.oci.image.manifest.v1+json", "wrong manifest media type")
    descriptors = [manifest["config"], *manifest["layers"]]
    names = set()
    with tempfile.TemporaryDirectory() as tmp:
        temp = Path(tmp)
        for index, desc in enumerate(descriptors):
            digest = desc["digest"]
            require(DIGEST_RE.fullmatch(digest), "invalid layer digest")
            blob = temp / f"blob-{index}"
            oras(["blob", "fetch", f"{PACKAGE}@{digest}", "--output", blob], config)
            require(blob.stat().st_size == desc["size"] and "sha256:" + sha(blob) == digest, "OCI blob descriptor mismatch")
            if index:
                name = filename(desc.get("annotations", {}).get("org.opencontainers.image.title"))
                require(name not in names, "duplicate OCI layer filename")
                names.add(name)
                require(desc["mediaType"] == media(name), "wrong layer media type")
                shutil.copyfile(blob, output / name)
            else:
                require(desc["mediaType"] == "application/vnd.oci.empty.v1+json" and blob.read_bytes() == b"{}", "unexpected OCI config")
        checked = receipt(output)
        expected = {"release.json", "SHA256SUMS", *(f["name"] for f in checked["files"])}
        require(names == expected, "unexpected OCI layers")
        annotation = manifest.get("annotations", {})
        require(annotation.get("org.opencontainers.image.source") == SOURCE and annotation.get("org.opencontainers.image.revision") == checked["source_commit"] and annotation.get("org.opencontainers.image.version") == checked["release_tag"], "manifest provenance mismatch")
        pulled = temp / "pulled"
        pulled.mkdir()
        oras(["pull", reference, "--output", pulled], config)
        require({p.name for p in pulled.iterdir()} == expected, "unexpected pulled files")
        require(receipt(pulled) == checked, "independent pull receipt mismatch")
        for name in expected:
            require(sha(pulled / name) == sha(output / name), "independent pull bytes mismatch")
    return checked


def check_release(tag, commit, platform, output, config):
    identity(tag, commit, platform)
    tag_ref = f"{PACKAGE}:{tag}-{platform}-arm64"
    raw = fetch_manifest(tag_ref, config)
    if raw is None:
        return {"exists": False}
    reference = PACKAGE + "@sha256:" + hashlib.sha256(raw).hexdigest()
    checked = verify_reference(reference, output, config, raw)
    require((checked["source_commit"], checked["release_tag"], checked["platform"]) == (commit, tag, platform), "published tag identity differs; refusing overwrite")
    return {"exists": True, "reference": reference}


INSTALLER = '''#!/bin/sh
set -eu
prefix="$HOME/.local"
if [ "$#" -ne 0 ]; then
  [ "$#" -eq 2 ] && [ "$1" = --prefix ] || { echo "Usage: install.sh [--prefix /absolute/directory]" >&2; exit 2; }
  prefix=$2
fi
case "$prefix" in /*) ;; *) echo "prefix must be absolute" >&2; exit 2;; esac
root=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
exec python3 - "$root" "$prefix" <<'PY'
import hashlib, os, pathlib, shutil, sys
root, prefix = map(pathlib.Path, sys.argv[1:])
if '..' in prefix.parts:
    raise SystemExit('prefix must not contain ..')
def digest(p):
    h = hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda: f.read(1024 * 1024), b''):
            h.update(b)
    return h.digest()
copies = []
for base in ('bin', 'share'):
    for source in sorted((root / base).rglob('*')):
        if source.is_symlink():
            raise SystemExit('package symlinks are forbidden')
        if not source.is_file():
            continue
        target = prefix / source.relative_to(root)
        for parent in (target.parent, *target.parents):
            if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
                raise SystemExit('unsafe destination parent: ' + str(parent))
        if target.is_symlink() or (target.exists() and (not target.is_file() or digest(source) != digest(target))):
            raise SystemExit('refusing to overwrite different existing file: ' + str(target))
        copies.append((source, target))
# All conflicts are checked before creating or copying anything.
for source, target in copies:
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        with target.open('xb') as out, source.open('rb') as inp:
            shutil.copyfileobj(inp, out)
        target.chmod(source.stat().st_mode & 0o777)
print('Installed herdr into ' + str(prefix) + '; no config or server modified.')
PY
'''


def assert_arm64(binary, platform):
    data = binary.read_bytes()[:64]
    if platform == "linux":
        require(data[:4] == b"\x7fELF" and data[4] == 2 and data[5] == 1 and struct.unpack_from("<H", data, 18)[0] == 183, "expected ARM64 ELF")
    else:
        require(data[:4] == b"\xcf\xfa\xed\xfe" and struct.unpack_from("<I", data, 4)[0] == 0x0100000C, "expected ARM64 Mach-O")


def pack_release(tag, commit, platform, input_dir, output):
    identity(tag, commit, platform)
    require(git("rev-parse", "HEAD") == commit, "source commit differs from checkout HEAD")
    empty_directory(output)
    binary = input_dir / "bin" / "herdr"
    require(binary.is_file() and not binary.is_symlink(), "missing built herdr")
    assert_arm64(binary, platform)
    toolchains = json.loads((input_dir / "toolchains.json").read_text())
    require(toolchains.get("rustc", "").startswith("rustc 1.96.1 ") and toolchains.get("zig") == "0.16.0", "wrong compiler versions")
    timestamp = int(git("show", "-s", "--format=%ct", commit))
    archive = output / f"herdr-{tag}-{platform}-arm64.tar.gz"
    with tempfile.TemporaryDirectory() as tmp:
        stage = Path(tmp)
        (stage / "bin").mkdir()
        shutil.copy2(binary, stage / "bin/herdr")
        (stage / "bin/herdr").chmod(0o755)
        docs = stage / "share/herdr"
        docs.mkdir(parents=True)
        shutil.copy2(ROOT / "README.md", docs / "README.md")
        for name in ("LICENSE", "NOTICE"):
            if (ROOT / name).is_file():
                shutil.copy2(ROOT / name, docs / name)
        (stage / "README.md").write_text("Herdr custom ARM64 package. Requires Python 3 for installation.\nRun ./install.sh --prefix /absolute/path (default: $HOME/.local).\nConflicting existing files are never overwritten. No server or configuration changes.\n")
        (stage / "install.sh").write_text(INSTALLER)
        (stage / "install.sh").chmod(0o755)
        with archive.open("wb") as raw, gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=timestamp) as gz, tarfile.open(fileobj=gz, mode="w") as tar:
            for path in sorted(stage.rglob("*")):
                info = tar.gettarinfo(str(path), arcname=path.relative_to(stage).as_posix())
                info.uid = info.gid = 0
                info.uname = info.gname = ""
                info.mtime = timestamp
                if path.is_file():
                    with path.open("rb") as stream:
                        tar.addfile(info, stream)
                else:
                    tar.addfile(info)
    result = {"schema": 1, "project": PROJECT, "source_repo": SOURCE, "source_commit": commit, "release_tag": tag, "platform": platform, "architecture": "arm64", "toolchains": toolchains, "files": [file_record(archive)]}
    (output / "release.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    (output / "SHA256SUMS").write_text("".join(f"{sha(output / name)}  {name}\n" for name in [archive.name, "release.json"]))
    receipt(output)
    return {"directory": str(output)}


def publish_release(directory, config):
    checked = receipt(directory)
    tag, commit, platform = checked["release_tag"], checked["source_commit"], checked["platform"]
    with tempfile.TemporaryDirectory() as tmp:
        temp = Path(tmp)
        existing = check_release(tag, commit, platform, temp / "existing", config)
        if existing["exists"]:
            require(receipt(temp / "existing") == checked, "existing receipt or payload differs")
            return {"reference": existing["reference"], "digest": existing["reference"].split("@")[-1]}
        layout = temp / "layout"
        # %ct converted to UTC avoids locale/offset-dependent OCI manifests.
        import datetime
        created = datetime.datetime.fromtimestamp(int(git("show", "-s", "--format=%ct", commit)), datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        names = [f["name"] for f in checked["files"]] + ["release.json", "SHA256SUMS"]
        run(["oras", "push", "--oci-layout", f"{layout}:release", "--artifact-type", ARTIFACT_TYPE,
             "--annotation", f"org.opencontainers.image.created={created}",
             "--annotation", f"org.opencontainers.image.source={SOURCE}",
             "--annotation", f"org.opencontainers.image.revision={commit}",
             "--annotation", f"org.opencontainers.image.version={tag}",
             *[f"{name}:{media(name)}" for name in names]], cwd=directory)
        index = json.loads((layout / "index.json").read_text())
        require(len(index["manifests"]) == 1, "unexpected local OCI index")
        digest = index["manifests"][0]["digest"]
        raw = (layout / "blobs/sha256" / digest.split(":")[1]).read_bytes()
        require("sha256:" + hashlib.sha256(raw).hexdigest() == digest, "local manifest digest mismatch")
        # Per-tag/platform Actions concurrency prevents publication races within this fork.
        oras(["cp", "--from-oci-layout", f"{layout}@{digest}", f"{PACKAGE}:{tag}-{platform}-arm64"], config)
        reference = f"{PACKAGE}@{digest}"
        remote = fetch_manifest(reference, config)
        require(remote == raw, "remote manifest differs from local OCI bytes")
        require(verify_reference(reference, temp / "verified", config, remote) == checked, "published receipt differs")
        return {"reference": reference, "digest": digest}


class ReleaseBoundaryTests(unittest.TestCase):
    """Load with runpy + unittest; the documented command avoids filename discovery."""
    def test_validation_boundaries(self):
        with tempfile.TemporaryDirectory() as tmp:
            cwd = Path(tmp)
            run(["git", "init", "-q", cwd])
            run(["git", "config", "user.name", "fixture"], cwd)
            run(["git", "config", "user.email", "fixture@example.invalid"], cwd)
            for name in (".github/workflows/custom-release.yml", ".github/scripts/custom-release.sh", ".github/scripts/package-release.py"):
                path = cwd / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("fixture\n")
            (cwd / "Cargo.toml").write_text('[package]\nversion = "0.9.3"\n')
            run(["git", "add", "."], cwd)
            run(["git", "commit", "-qm", "custom"], cwd)
            custom = git("rev-parse", "HEAD", cwd=cwd)
            run(["git", "update-ref", "refs/remotes/origin/custom", custom], cwd)
            run(["git", "tag", "v0.9.3-custom.1"], cwd)
            event = {"action": "published", "release": {"draft": False, "assets": [], "tag_name": "v0.9.3-custom.1"}}
            self.assertEqual(validate_event(event, "cybito/herdr", cwd)["commit"], custom)
            (cwd / "newer-custom").write_text("custom advanced")
            run(["git", "add", "."], cwd)
            run(["git", "commit", "-qm", "newer custom"], cwd)
            run(["git", "update-ref", "refs/remotes/origin/custom", git("rev-parse", "HEAD", cwd=cwd)], cwd)
            with self.assertRaises(ValueError):
                validate_event(event, "cybito/herdr", cwd)
            run(["git", "checkout", "-q", "--detach", custom], cwd)
            self.assertEqual(validate_event(event, "cybito/herdr", cwd)["commit"], custom)
            for tag in ("v0.9.3", "v0.9.3-custom.0", "v0.9.3-custom.2;touch sentinel"):
                event["release"]["tag_name"] = tag
                with self.assertRaises(ValueError):
                    validate_event(event, "cybito/herdr", cwd)
            self.assertFalse((cwd / "sentinel").exists())
            (cwd / "upstream-only").write_text("not on custom")
            run(["git", "add", "."], cwd)
            run(["git", "commit", "-qm", "upstream-only"], cwd)
            run(["git", "tag", "v0.9.3-custom.2"], cwd)
            event["release"]["tag_name"] = "v0.9.3-custom.2"
            with self.assertRaises(RuntimeError):
                validate_event(event, "cybito/herdr", cwd)

    def test_notfound_is_explicit(self):
        for message in (b"unauthorized", b"connection reset", b"404 Not Found"):
            result = subprocess.CompletedProcess([], 1, b"", message)
            with patch.object(subprocess, "run", return_value=result), self.assertRaises(RuntimeError):
                fetch_manifest(PACKAGE + ":fixture", Path("/tmp/config"))
        for message in (b"MANIFEST_UNKNOWN", b"name_unknown"):
            with patch.object(subprocess, "run", return_value=subprocess.CompletedProcess([], 1, b"", message)):
                self.assertIsNone(fetch_manifest(PACKAGE + ":fixture", Path("/tmp/config")))

    def test_identity_mismatch_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(globals(), {"fetch_manifest": lambda *a: b"{}", "verify_reference": lambda *a: {"source_commit": "b" * 40, "release_tag": "v0.9.3-custom.1", "platform": "linux"}}):
            with self.assertRaises(ValueError):
                check_release("v0.9.3-custom.1", "a" * 40, "linux", Path(tmp), Path("/tmp/config"))

    def test_corrupt_receipt_and_payload_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            payload = directory / "herdr-v0.9.3-custom.1-linux-arm64.tar.gz"
            payload.write_bytes(b"fixture-install-payload")
            original = {"schema": 1, "project": PROJECT, "source_repo": SOURCE, "source_commit": "a" * 40, "release_tag": "v0.9.3-custom.1", "platform": "linux", "architecture": "arm64", "toolchains": {"rustc": "rustc 1.96.1 (fixture)", "zig": "0.16.0"}, "files": [file_record(payload)]}
            def save(value):
                (directory / "release.json").write_text(json.dumps(value))
                (directory / "SHA256SUMS").write_text("".join(f"{sha(directory / name)}  {name}\n" for name in (payload.name, "release.json")))
            save(original)
            self.assertEqual(receipt(directory), original)
            wrong_source = {**original, "source_repo": "https://git.cybit.top/cybit/herdr"}
            save(wrong_source)
            with self.assertRaises(ValueError):
                receipt(directory)
            save(original)
            payload.write_bytes(b"corrupted-payload")
            with self.assertRaises(ValueError):
                receipt(directory)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("check", "pack"):
        command = commands.add_parser(name)
        command.add_argument("--tag", required=True)
        command.add_argument("--commit", required=True)
        command.add_argument("--platform", choices=("darwin", "linux"), required=True)
        command.add_argument("--output-dir", type=absolute, required=True)
        if name == "pack":
            command.add_argument("--input-dir", type=absolute, required=True)
    command = commands.add_parser("publish")
    command.add_argument("--directory", type=absolute, required=True)
    command.add_argument("--registry-config", type=absolute, required=True)
    command = commands.add_parser("verify")
    command.add_argument("--reference", required=True)
    command.add_argument("--output-dir", type=absolute, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        config = anonymous_config(Path(tmp))
        if args.command == "check":
            result = check_release(args.tag, args.commit, args.platform, args.output_dir, config)
        elif args.command == "pack":
            result = pack_release(args.tag, args.commit, args.platform, args.input_dir, args.output_dir)
        elif args.command == "publish":
            require(args.registry_config.is_file(), "missing registry configuration")
            result = publish_release(args.directory, args.registry_config)
        else:
            result = verify_reference(args.reference, args.output_dir, config)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    try:
        main()
    except (ValueError, RuntimeError, OSError, KeyError, TypeError) as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
