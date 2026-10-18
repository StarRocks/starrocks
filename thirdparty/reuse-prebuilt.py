#!/usr/bin/env python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Check whether the pinned dev-env dependencies match this checkout's inputs."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys


# Lance is built separately. Never exclude vars.sh, package-manifest.sh or
# build-thirdparty.sh: changes to the common recipes must invalidate reuse.
# The standalone pprof executable is refreshed on every reuse, so only its
# literal download URL/checksum are normalized; all library inputs remain hashed.
EXCLUDED = {
    "thirdparty/src", "thirdparty/installed", "thirdparty/lance",
    "thirdparty/build-lance.sh", "thirdparty/reuse-prebuilt.py",
    "thirdparty/test_reuse_prebuilt.py", "thirdparty/prebuilt-baseline.json",
}


def input_fingerprint(root):
    paths = [root / "env.sh"]
    for directory in ("thirdparty", "docker/dockerfiles/toolchains"):
        for parent, dirs, files in os.walk(str(root / directory)):
            dirs[:] = sorted(name for name in dirs
                             if name != "__pycache__"
                             and str((Path(parent) / name).relative_to(root)) not in EXCLUDED)
            paths.extend(Path(parent) / name for name in dirs if (Path(parent) / name).is_symlink())
            paths.extend(Path(parent) / name for name in files)
    digest = hashlib.sha256()
    for path in sorted(paths):
        name = str(path.relative_to(root))
        if name in EXCLUDED or path.suffix == ".md":
            continue
        # Include names, executable bits and symlink targets, not mtimes or uid.
        digest.update(name.encode() + b"\0")
        digest.update(str(path.lstat().st_mode & 0o111).encode() + b"\0")
        digest.update(b"symlink\0" if path.is_symlink() else b"file\0")
        data = os.readlink(str(path)).encode() if path.is_symlink() else path.read_bytes()
        if not path.is_symlink() and name in ("thirdparty/vars-aarch64.sh", "thirdparty/vars-x86_64.sh"):
            # Do not normalize expansions, commands, or arbitrary shell code.
            data = re.sub(rb'(?m)^(PPROF_(?:DOWNLOAD|MD5SUM))="[A-Za-z0-9:/%._-]+"$',
                          rb'\1="<refreshed>"', data)
        digest.update(hashlib.sha256(data).digest())
    return digest.hexdigest()


def reuse_reason(root, prefix, distro, toolchain_tag, source, baseline):
    if source != "prebuilt":
        return "a full source build was requested"
    if distro not in baseline["images"]:
        return "no verified prebuilt image for " + distro
    if toolchain_tag != baseline["toolchain_tag"]:
        return "the toolchain version changed"
    if (root / "custom_env.sh").exists():
        return "custom_env.sh may change dependency build flags"
    if input_fingerprint(root) != baseline["inputs_sha256"]:
        return "dependency build inputs differ from the verified baseline"
    for name in ("bin/protoc", "lib/libprotobuf.a", "lib64/libarrow.a"):
        path = prefix / name
        if not path.is_file() or path.stat().st_size == 0:
            return "prebuilt dependency is missing or empty: " + name
    return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--prefix", type=Path)
    parser.add_argument("--distro")
    parser.add_argument("--toolchain-tag")
    parser.add_argument("--source", choices=("prebuilt", "full"), default="prebuilt")
    parser.add_argument("--print-fingerprint", action="store_true")
    args = parser.parse_args()
    try:
        if args.print_fingerprint:
            print(input_fingerprint(args.root))
            return 0
        if args.prefix is None or args.distro is None or args.toolchain_tag is None:
            parser.error("--prefix, --distro and --toolchain-tag are required for reuse checks")
        baseline = json.loads((args.root / "thirdparty/prebuilt-baseline.json").read_text())
        reason = reuse_reason(args.root, args.prefix, args.distro, args.toolchain_tag,
                              args.source, baseline)
        if reason:
            print("Cannot reuse thirdparty: " + reason)
            return 1
        print("Verified thirdparty baseline " + baseline["source_commit"] + " from "
              + baseline["images"][args.distro])
        return 0
    except (OSError, ValueError, KeyError) as error:
        print("Cannot verify thirdparty baseline: " + str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
