#!/usr/bin/env python3
"""Herdr GitHub Release asset contract."""
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

PROJECT="herdr"
SOURCE="https://github.com/cybito/herdr.git"
TAG_RE=re.compile(r"v([0-9]+\.[0-9]+\.[0-9]+)-custom\.([1-9][0-9]*)\Z")
SHA_RE=re.compile(r"[0-9a-f]{40}\Z")
FIELDS={"schema","project","source_repo","source_commit","release_tag","platform","architecture","toolchains","files"}
ROOT=Path(__file__).resolve().parents[2]
ASSET_MAX_BYTES=2*1024**3
RELEASE_MAX_ASSETS=1000


def require(condition,message):
    if not condition: raise ValueError(message)


def run(args,cwd=None):
    result=subprocess.run([str(a) for a in args],cwd=cwd,capture_output=True)
    if result.returncode: raise RuntimeError(result.stderr.decode(errors="replace") or result.stdout.decode(errors="replace"))
    return result.stdout


def git(*args,cwd=ROOT): return run(["git",*args],cwd=cwd).decode().strip()


def absolute(value):
    path=Path(value); require(path.is_absolute(),"directory must be absolute"); return path


def empty_directory(path):
    require(not path.is_symlink(),"directory must not be a symlink"); path.mkdir(parents=True,exist_ok=True)
    require(not any(path.iterdir()),f"directory must be empty: {path}")


def identity(tag,commit,platform):
    require(TAG_RE.fullmatch(tag) is not None,"invalid custom release tag"); require(SHA_RE.fullmatch(commit) is not None,"invalid source commit")
    require(platform in ("darwin","linux"),"invalid platform")


def validate_event(event,repository,cwd=ROOT):
    require(repository=="cybito/herdr","wrong repository"); require(event.get("action")=="published","only release.published is accepted")
    release=event["release"]; require(release.get("draft") is False,"draft releases are forbidden")
    tag=release["tag_name"]; require(isinstance(tag,str) and TAG_RE.fullmatch(tag),"invalid custom release tag")
    commit=git("rev-parse","--verify",f"refs/tags/{tag}^{{commit}}",cwd=cwd); identity(tag,commit,"linux")
    require(git("rev-parse","HEAD",cwd=cwd)==commit,"validation checkout must be the exact release tag commit")
    run(["git","merge-base","--is-ancestor",commit,"refs/remotes/origin/custom"],cwd=cwd)
    for name in (".github/workflows/custom-release.yml",".github/scripts/custom-release.sh",".github/scripts/package-release.py"): git("cat-file","-e",f"{commit}:{name}",cwd=cwd)
    manifest=git("show",f"{commit}:Cargo.toml",cwd=cwd); version=re.search(r'^version\s*=\s*"([^"]+)"',manifest,re.MULTILINE)
    require(version and version.group(1)==TAG_RE.fullmatch(tag).group(1),"tag base differs from Cargo package version")
    return {"tag":tag,"commit":commit,"version":tag[1:]}


def sha(path):
    digest=hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda:stream.read(1024*1024),b""): digest.update(block)
    return digest.hexdigest()


def file_record(path): return {"name":path.name,"sha256":sha(path),"size":path.stat().st_size}


def filename(name):
    require(isinstance(name,str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*",name),"unsafe filename"); require(name not in (".",".."),"unsafe filename"); return name


def receipt(directory):
    result=json.loads((directory/"release.json").read_text())
    require(set(result)==FIELDS and type(result["schema"]) is int and result["schema"]==1,"invalid receipt schema")
    require(result["project"]==PROJECT and result["source_repo"]==SOURCE,"wrong receipt source"); identity(result["release_tag"],result["source_commit"],result["platform"])
    require(result["architecture"]=="arm64","wrong architecture")
    require(isinstance(result["toolchains"],dict) and result["toolchains"] and all(isinstance(k,str) and isinstance(v,str) and v for k,v in result["toolchains"].items()),"invalid toolchains")
    require(result["toolchains"].get("rustc","").startswith("rustc 1.96.1 ") and result["toolchains"].get("zig")=="0.16.0","wrong receipt toolchains")
    files=result["files"]; archive=f"herdr-{result['release_tag']}-{result['platform']}-arm64.tar.gz"
    require(isinstance(files,list) and len(files)==1,"Herdr requires exactly one installation archive")
    record=files[0]; require(set(record)=={"name","sha256","size"} and filename(record["name"])==archive,"wrong file record")
    require(isinstance(record["sha256"],str) and re.fullmatch(r"[0-9a-f]{64}",record["sha256"]),"invalid file hash"); require(type(record["size"]) is int and record["size"]>0,"invalid file size")
    path=directory/record["name"]; require(path.is_file() and not path.is_symlink(),"payload is not a regular file"); require(file_record(path)==record,"payload hash or size mismatch")
    names=[record["name"],"release.json"]; require((directory/"SHA256SUMS").read_text()=="".join(f"{sha(directory/n)}  {n}\n" for n in names),"SHA256SUMS mismatch"); return result


def asset_name(tag,platform,name): return f"{tag}-{platform}-{filename(name)}"


def asset_files(directory):
    checked=receipt(directory); names=[r["name"] for r in checked["files"]]+["release.json","SHA256SUMS"]; result={n:directory/n for n in names}
    for name,path in result.items(): require(path.stat().st_size<ASSET_MAX_BYTES,f"GitHub asset must be smaller than 2 GiB: {name}")
    return result


def gh(args): return run(["gh",*args])


def release_assets(tag):
    data=json.loads(gh(["release","view",tag,"--repo","cybito/herdr","--json","assets"])); return {a["name"]:a for a in data["assets"]}


def download_assets(tag,names,output):
    empty_directory(output)
    if names: gh(["release","download",tag,"--repo","cybito/herdr","--dir",output,*[arg for n in names for arg in ("--pattern",n)]])
    return output


def verify_asset_set(directory,tag,commit,platform):
    checked=receipt(directory); require((checked["release_tag"],checked["source_commit"],checked["platform"])==(tag,commit,platform),"downloaded release asset identity mismatch"); return checked


def expected_names(tag,platform): return {asset_name(tag,platform,n) for n in (f"herdr-{tag}-{platform}-arm64.tar.gz","release.json","SHA256SUMS")}


def check_release(tag,commit,platform,output):
    identity(tag,commit,platform); assets=release_assets(tag); required=expected_names(tag,platform); require(len(assets)<=RELEASE_MAX_ASSETS,"release exceeds GitHub's 1000 asset limit")
    existing=required.intersection(assets); require(not any(n.startswith(f"{tag}-{platform}-") and n not in required for n in assets),"unexpected platform asset names")
    if existing!=required: return {"exists":False}
    download_assets(tag,sorted(required),output)
    for name in required:
        local=output/name; require(local.is_file() and not local.is_symlink(),"downloaded asset missing or not a file"); require(f"sha256:{sha(local)}"==assets[name].get("digest"),"GitHub asset digest differs after download"); shutil.move(local,output/name[len(f"{tag}-{platform}-"):])
    checked=verify_asset_set(output,tag,commit,platform); return {"exists":True,"reference":f"https://github.com/cybito/herdr/releases/tag/{tag}","assets":sorted(required),"receipt":checked}


INSTALLER='''#!/usr/bin/env python3
import argparse, hashlib, os, pathlib, shutil, tempfile
p=argparse.ArgumentParser(); p.add_argument("--prefix", default=os.path.expanduser("~/.local")); a=p.parse_args(); prefix=pathlib.Path(a.prefix)
if not prefix.is_absolute(): raise SystemExit("prefix must be absolute")
root=pathlib.Path(__file__).resolve().parent
files={"bin/herdr":prefix/"bin/herdr", "share/herdr/README.md":prefix/"share/herdr/README.md"}
for name in ("LICENSE", "NOTICE"):
 if (root/"share/herdr"/name).is_file(): files[f"share/herdr/{name}"]=prefix/"share/herdr"/name
for src,dst in files.items():
 source=root/src
 if not source.is_file(): raise SystemExit(f"missing package file: {src}")
 if dst.exists() and (not dst.is_file() or hashlib.sha256(dst.read_bytes()).digest()!=hashlib.sha256(source.read_bytes()).digest()): raise SystemExit(f"refusing to overwrite differing file: {dst}")
for src,dst in files.items():
 dst.parent.mkdir(parents=True,exist_ok=True)
 if not dst.exists():
  fd,tmp=tempfile.mkstemp(dir=dst.parent); os.close(fd); shutil.copy2(root/src,tmp); os.replace(tmp,dst)
'''


def assert_arm64(binary,platform):
    data=binary.read_bytes()[:64]
    if platform=="linux": require(data[:4]==b"\x7fELF" and data[4]==2 and data[5]==1 and struct.unpack_from("<H",data,18)[0]==183,"expected ARM64 ELF")
    else: require(data[:4]==b"\xcf\xfa\xed\xfe" and struct.unpack_from("<I",data,4)[0]==0x0100000C,"expected ARM64 Mach-O")


def pack_release(tag,commit,platform,input_dir,output):
    identity(tag,commit,platform); require(git("rev-parse","HEAD")==commit,"source commit differs from checkout HEAD"); empty_directory(output)
    binary=input_dir/"bin"/"herdr"; require(binary.is_file() and not binary.is_symlink(),"missing built herdr"); assert_arm64(binary,platform)
    toolchains=json.loads((input_dir/"toolchains.json").read_text()); require(toolchains.get("rustc","").startswith("rustc 1.96.1 ") and toolchains.get("zig")=="0.16.0","wrong compiler versions")
    timestamp=int(git("show","-s","--format=%ct",commit)); archive=output/f"herdr-{tag}-{platform}-arm64.tar.gz"
    with tempfile.TemporaryDirectory() as tmp:
        stage=Path(tmp); (stage/"bin").mkdir(); shutil.copy2(binary,stage/"bin/herdr"); (stage/"bin/herdr").chmod(0o755); docs=stage/"share/herdr"; docs.mkdir(parents=True); shutil.copy2(ROOT/"README.md",docs/"README.md")
        for name in ("LICENSE","NOTICE"):
            if (ROOT/name).is_file(): shutil.copy2(ROOT/name,docs/name)
        (stage/"README.md").write_text("Herdr custom ARM64 package. Requires Python 3 for installation.\nRun ./install.sh --prefix /absolute/path (default: $HOME/.local).\nConflicting existing files are never overwritten. No server or configuration changes.\n"); (stage/"install.sh").write_text(INSTALLER); (stage/"install.sh").chmod(0o755)
        with archive.open("wb") as raw,gzip.GzipFile(filename="",mode="wb",fileobj=raw,mtime=timestamp) as gz,tarfile.open(fileobj=gz,mode="w") as tar:
            for path in sorted(stage.rglob("*")):
                info=tar.gettarinfo(str(path),arcname=path.relative_to(stage).as_posix()); info.uid=info.gid=0; info.uname=info.gname=""; info.mtime=timestamp
                if path.is_file():
                    with path.open("rb") as stream: tar.addfile(info,stream)
                else: tar.addfile(info)
    result={"schema":1,"project":PROJECT,"source_repo":SOURCE,"source_commit":commit,"release_tag":tag,"platform":platform,"architecture":"arm64","toolchains":toolchains,"files":[file_record(archive)]}; (output/"release.json").write_text(json.dumps(result,indent=2,sort_keys=True)+"\n"); (output/"SHA256SUMS").write_text("".join(f"{sha(output/n)}  {n}\n" for n in [archive.name,"release.json"])); receipt(output); return {"directory":str(output)}


def publish_release(directory):
    checked=receipt(directory); tag,commit,platform=checked["release_tag"],checked["source_commit"],checked["platform"]; files=asset_files(directory); assets=release_assets(tag); required={asset_name(tag,platform,n) for n in files}
    unexpected={n for n in assets if n.startswith(f"{tag}-{platform}-") and n not in required}; require(not unexpected,"unexpected platform assets prevent safe reconciliation"); missing=required-assets.keys(); require(len(assets)+len(missing)<=RELEASE_MAX_ASSETS,"GitHub release would exceed 1000 assets")
    with tempfile.TemporaryDirectory() as tmp:
        temp=Path(tmp); existing=required-missing
        if existing:
            download_assets(tag,sorted(existing),temp)
            for name in existing:
                original=name[len(f"{tag}-{platform}-"):]; require(sha(temp/name)==sha(files[original]),f"existing asset bytes mismatch: {name}")
        for name in missing: shutil.copyfile(files[name[len(f"{tag}-{platform}-"):]],temp/name)
        if missing: gh(["release","upload",tag,"--repo","cybito/herdr",*[str(temp/n) for n in sorted(missing)]])
        verified=temp/"verified"; result=check_release(tag,commit,platform,verified); require(result["exists"],"uploaded asset set did not verify")
    return {"reference":result["reference"],"assets":sorted(required)}


def verify_download(tag,commit,platform,output):
    result=check_release(tag,commit,platform,output); require(result["exists"],"release asset set missing"); return result["receipt"]


class ReleaseBoundaryTests(unittest.TestCase):
    """Seam tests for immutable GitHub Release asset reconciliation."""
    def package_fixture(self,root,tag="v0.9.3-custom.1",commit="a"*40,platform="linux"):
        package=root/"package"; package.mkdir(); archive=package/f"herdr-{tag}-{platform}-arm64.tar.gz"; archive.write_bytes(b"archive")
        value={"schema":1,"project":PROJECT,"source_repo":SOURCE,"source_commit":commit,"release_tag":tag,"platform":platform,"architecture":"arm64","toolchains":{"rustc":"rustc 1.96.1 fixture","zig":"0.16.0"},"files":[file_record(archive)]}
        (package/"release.json").write_text(json.dumps(value)); (package/"SHA256SUMS").write_text("".join(f"{sha(package/n)}  {n}\n" for n in (archive.name,"release.json"))); return package

    def test_missing_asset_set_is_explicit(self):
        with tempfile.TemporaryDirectory() as tmp,patch.dict(globals(),{"release_assets":lambda tag:{}}): self.assertFalse(check_release("v0.9.3-custom.1","a"*40,"linux",Path(tmp))["exists"])

    def test_partial_asset_set_is_incomplete(self):
        name=next(iter(expected_names("v0.9.3-custom.1","linux")))
        with tempfile.TemporaryDirectory() as tmp,patch.dict(globals(),{"release_assets":lambda tag:{name:{}}}): self.assertFalse(check_release("v0.9.3-custom.1","a"*40,"linux",Path(tmp))["exists"])

    def test_publish_fills_partial_set_without_clobber_and_verifies_download(self):
        tag,commit,platform="v0.9.3-custom.1","a"*40,"linux"
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp); package=self.package_fixture(root); required=expected_names(tag,platform); present=next(iter(required)); state={present:{"name":present,"digest":"sha256:"+sha(package/present[len(f"{tag}-{platform}-"):])}}; uploads=[]; reads=[]
            def fake_gh(args):
                if args[:2]==["release","view"]: return json.dumps({"assets":list(state.values())}).encode()
                if args[:2]==["release","upload"]:
                    uploads.append(args); self.assertEqual(args[:4],["release","upload",tag,"--repo"]); self.assertEqual(args[4],"cybito/herdr")
                    for path in args[5:]:
                        asset=Path(path); state[asset.name]={"name":asset.name,"digest":"sha256:"+sha(asset)}
                    return b""
                if args[:2]==["release","download"]:
                    reads.append(args); out=Path(args[args.index("--dir")+1]); patterns=[args[i+1] for i,a in enumerate(args) if a=="--pattern"]
                    for name in patterns: shutil.copyfile(package/name[len(f"{tag}-{platform}-"):],out/name)
                    return b""
                raise AssertionError(args)
            with patch.dict(globals(),{"gh":fake_gh}): result=publish_release(package)
            self.assertEqual(set(result["assets"]),required); self.assertEqual(set(state),required); self.assertTrue(uploads); self.assertNotIn("--clobber",uploads[0]); self.assertGreaterEqual(len(reads),2)

    def test_publish_refuses_existing_mismatched_asset_without_upload(self):
        tag,commit,platform="v0.9.3-custom.1","a"*40,"linux"
        with tempfile.TemporaryDirectory() as tmp:
            package=self.package_fixture(Path(tmp)); names=expected_names(tag,platform); existing={n:{"name":n,"digest":"sha256:"+"0"*64} for n in names}; uploads=[]
            def fake_gh(args):
                if args[:2]==["release","view"]: return json.dumps({"assets":list(existing.values())}).encode()
                if args[:2]==["release","download"]:
                    out=Path(args[args.index("--dir")+1]); patterns=[args[i+1] for i,a in enumerate(args) if a=="--pattern"]
                    for name in patterns: shutil.copyfile(package/name[len(f"{tag}-{platform}-"):],out/name)
                    return b""
                if args[:2]==["release","upload"]: uploads.append(args); return b""
                raise AssertionError(args)
            with patch.dict(globals(),{"gh":fake_gh}):
                with self.assertRaises(ValueError): publish_release(package)
            self.assertFalse(uploads)

    def test_asset_limit_and_name_contract(self):
        self.assertEqual(asset_name("v0.9.3-custom.1","linux","release.json"),"v0.9.3-custom.1-linux-release.json")
        with tempfile.TemporaryDirectory() as tmp,patch.dict(globals(),{"release_assets":lambda tag:{str(n):{} for n in range(1001)}}):
            with self.assertRaises(ValueError): check_release("v0.9.3-custom.1","a"*40,"linux",Path(tmp))


def main():
    parser=argparse.ArgumentParser(description=__doc__); commands=parser.add_subparsers(dest="command",required=True)
    for name in ("check","pack"):
        command=commands.add_parser(name); command.add_argument("--tag",required=True); command.add_argument("--commit",required=True); command.add_argument("--platform",choices=("darwin","linux"),required=True); command.add_argument("--output-dir",type=absolute,required=True)
        if name=="pack": command.add_argument("--input-dir",type=absolute,required=True)
    command=commands.add_parser("publish"); command.add_argument("--directory",type=absolute,required=True)
    command=commands.add_parser("verify"); command.add_argument("--tag",required=True); command.add_argument("--commit",required=True); command.add_argument("--platform",choices=("darwin","linux"),required=True); command.add_argument("--output-dir",type=absolute,required=True)
    commands.add_parser("self-test",help="run release asset seam tests")
    args=parser.parse_args()
    if args.command=="self-test":
        suite=unittest.defaultTestLoader.loadTestsFromTestCase(ReleaseBoundaryTests)
        result=unittest.TextTestRunner(verbosity=2).run(suite)
        if not result.wasSuccessful(): raise SystemExit(1)
        return
    if args.command=="check": result=check_release(args.tag,args.commit,args.platform,args.output_dir)
    elif args.command=="pack": result=pack_release(args.tag,args.commit,args.platform,args.input_dir,args.output_dir)
    elif args.command=="publish": result=publish_release(args.directory)
    else: result=verify_download(args.tag,args.commit,args.platform,args.output_dir)
    print(json.dumps(result,sort_keys=True))


if __name__=="__main__":
    try: main()
    except (ValueError,RuntimeError,OSError,KeyError,TypeError) as error: print(str(error),file=sys.stderr); sys.exit(1)
