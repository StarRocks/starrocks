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

"""Validate and install a prebuilt Lance package; no compiler is invoked."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import shutil
import tempfile


def checksum(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def create_install_parent(path):
    try:
        path.mkdir()
    except FileNotFoundError:
        create_install_parent(path.parent)
        create_install_parent(path)
    except FileExistsError:
        if not path.is_dir():
            raise
    else:
        path.chmod(0o755)


def install(source, prefix):
    manifest = json.loads((source / "share/lance/manifest.json").read_text())
    machine = {"arm64": "aarch64"}.get(platform.machine(), platform.machine())
    if manifest["SYSTEM"] != platform.system() or manifest["MACHINE"] != machine:
        raise RuntimeError("Lance package does not match the host platform")
    library = "libstarrocks_lance" + (".dylib" if platform.system() == "Darwin" else ".so")
    for name, expected in [("lib/" + library, manifest["LIBRARY_SHA256"]),
                           ("include/lance_rs_ffi.h", manifest["HEADER_SHA256"])]:
        if checksum(source / name) != expected:
            raise RuntimeError("Lance package checksum mismatch: " + name)
    for name in ("StarRocksLanceConfig.cmake", "StarRocksLanceConfigVersion.cmake"):
        if not (source / "lib/cmake/StarRocksLance" / name).is_file():
            raise RuntimeError("Lance package is missing its CMake configuration")
    prefix = prefix.absolute()
    if prefix.exists() and any(prefix.iterdir()) and not (prefix / "share/lance/manifest.json").is_file():
        raise RuntimeError("Refusing to replace a non-Lance installation: " + str(prefix))
    create_install_parent(prefix.parent)
    with tempfile.TemporaryDirectory(prefix=".lance-install-", dir=prefix.parent) as temp:
        stage, backup = Path(temp) / "new", Path(temp) / "previous"
        shutil.copytree(source, stage)
        if prefix.exists():
            prefix.rename(backup)
        try:
            stage.rename(prefix)
        except OSError:
            if backup.exists():
                backup.rename(prefix)
            raise
    print("Installed prebuilt Lance package to " + str(prefix))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--prefix", required=True, type=Path)
    args = parser.parse_args()
    install(args.source, args.prefix)
