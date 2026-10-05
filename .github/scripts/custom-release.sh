#!/usr/bin/env bash
set -euo pipefail

usage() { echo 'Usage: custom-release.sh build|smoke <darwin|linux> <release-tag> <source-sha> <absolute-output-dir>' >&2; exit 2; }
[[ $# == 5 ]] || usage
action=$1 platform=$2 tag=$3 source=$4 out=$5
[[ $action == build || $action == smoke ]] || usage
[[ $platform == darwin || $platform == linux ]] || usage
[[ $tag =~ ^v([0-9]+\.[0-9]+\.[0-9]+)-custom\.([1-9][0-9]*)$ ]] || usage
base=${BASH_REMATCH[1]} build_id=${BASH_REMATCH[2]}
[[ $source =~ ^[0-9a-f]{40}$ && $out == /* ]] || usage
root=$(git rev-parse --show-toplevel)
cd "$root"
[[ $(git rev-parse HEAD) == "$source" ]] || { echo 'source SHA must equal HEAD' >&2; exit 1; }
[[ $(uname -m) == arm64 || $(uname -m) == aarch64 ]] || { echo 'native ARM64 runner required' >&2; exit 1; }
if [[ $platform == darwin ]]; then
  [[ $(uname -s) == Darwin ]] || usage
  target=aarch64-apple-darwin
else
  [[ $(uname -s) == Linux ]] || usage
  target=aarch64-unknown-linux-gnu
fi
python3 - "$base" <<'PY'
import re, pathlib, sys
text = pathlib.Path('Cargo.toml').read_text()
match = re.search(r'^version\s*=\s*"([^"]+)"', text, re.M)
assert match and match.group(1) == sys.argv[1], 'release base must equal Cargo package version'
PY

smoke_binary() {
  local binary=$1 fixture=$2
  mkdir -p "$fixture/home" "$fixture/config" "$fixture/data" "$fixture/state" "$fixture/cache" "$fixture/tmp"
  env -i PATH="$PATH" HOME="$fixture/home" XDG_CONFIG_HOME="$fixture/config" XDG_DATA_HOME="$fixture/data" XDG_STATE_HOME="$fixture/state" XDG_CACHE_HOME="$fixture/cache" TMPDIR="$fixture/tmp" "$binary" --version > "$fixture/version.txt"
  [[ $(cat "$fixture/version.txt") == "herdr ${tag#v}" ]] || { cat "$fixture/version.txt" >&2; exit 1; }
  env -i PATH="$PATH" HOME="$fixture/home" XDG_CONFIG_HOME="$fixture/config" XDG_DATA_HOME="$fixture/data" XDG_STATE_HOME="$fixture/state" XDG_CACHE_HOME="$fixture/cache" TMPDIR="$fixture/tmp" "$binary" --help > "$fixture/help.txt"
  python3 - "$fixture/help.txt" <<'PY'
from pathlib import Path
import sys
text=Path(sys.argv[1]).read_text().lower()
assert all(command in text for command in ("server", "attach", "update")), "help must expose CLI commands"
PY
}

if [[ $action == build ]]; then
  [[ -z $(git status --porcelain --untracked-files=no) ]] || { echo 'tracked checkout must be clean' >&2; exit 1; }
  mkdir -p "$out/bin"
  export LIBGHOSTTY_VT_OPTIMIZE=ReleaseFast LIBGHOSTTY_VT_SIMD=true
  export HERDR_BUILD_CHANNEL=custom HERDR_BUILD_ID="$build_id" HERDR_BUILD_COMMIT="$source"
  export CARGO_TARGET_DIR="$out/cargo-target"
  export RUSTC="$(rustup which --toolchain 1.96.1 rustc)"
  export RUSTDOC="$(rustup which --toolchain 1.96.1 rustdoc)"
  [[ $(zig version) == 0.16.0 ]] || { echo 'Zig 0.16.0 required' >&2; exit 1; }
  "$RUSTC" --version | python3 -c 'import sys; assert sys.stdin.read().startswith("rustc 1.96.1 ")'
  cargo +1.96.1 build --release --locked --target "$target"
  cp "$CARGO_TARGET_DIR/$target/release/herdr" "$out/bin/herdr"
  chmod 755 "$out/bin/herdr"
  python3 - "$out/toolchains.json" <<'PY'
import json, os, subprocess, sys
json.dump({"rustc":subprocess.check_output([os.environ["RUSTC"],"--version"],text=True).strip(),"zig":subprocess.check_output(["zig","version"],text=True).strip()},open(sys.argv[1],"w"),indent=2)
PY
  smoke_binary "$out/bin/herdr" "$out/smoke-binary"
else
  fixture=$(mktemp -d "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/herdr-install-smoke.XXXXXX")
  trap 'rm -rf "$fixture"' EXIT
  tar -xzf "$out/herdr-$tag-$platform-arm64.tar.gz" -C "$fixture"
  prefix="$fixture/install-prefix"
  "$fixture/install.sh" --prefix "$prefix"
  smoke_binary "$prefix/bin/herdr" "$fixture/runtime"
  python3 - "$prefix" "$fixture/before.json" <<'PY'
import hashlib,json,pathlib,sys
p=pathlib.Path(sys.argv[1]); json.dump({str(x.relative_to(p)):hashlib.sha256(x.read_bytes()).hexdigest() for x in p.rglob('*') if x.is_file()},open(sys.argv[2],'w'),sort_keys=True)
PY
  "$fixture/install.sh" --prefix "$prefix"
  python3 - "$prefix" "$fixture/before.json" <<'PY'
import hashlib,json,pathlib,sys
p=pathlib.Path(sys.argv[1]); actual={str(x.relative_to(p)):hashlib.sha256(x.read_bytes()).hexdigest() for x in p.rglob('*') if x.is_file()}; assert actual==json.load(open(sys.argv[2])), 'repeat install changed files'
PY
  conflict="$fixture/conflict-prefix"
  mkdir -p "$conflict/bin"
  printf '%s\n' unrelated-user-file > "$conflict/bin/herdr"
  if "$fixture/install.sh" --prefix "$conflict"; then echo 'installer overwrote a conflicting file' >&2; exit 1; fi
  [[ $(cat "$conflict/bin/herdr") == unrelated-user-file ]] || exit 1
  if [[ $platform == linux ]]; then
    if ldd "$prefix/bin/herdr" | grep -q 'not found'; then echo 'missing runtime library' >&2; exit 1; fi
  fi
fi
