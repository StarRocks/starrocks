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

"""Build the isolated Lance Rust library with a verified, checkout-local toolchain.

An installed Rust >= 1.96.1 is used when available. Otherwise Linux builds download
an official distribution into the build directory, verify its pinned SHA-256, and
install only cargo/rustc/std there. Never modify the user's rustup or system tools.
"""
import argparse
import hashlib
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile
import urllib.request

VERSION = "1.96.1"
CHECKSUMS = {
    "x86_64": "d29ccb1559a177c4e72291f6e5f629de7fe8885e7521ca47802627544b121e95",
    "aarch64": "3abcb9489d001d95f30e8cfe68118be85afb0adbf0a9b21438909719689c08fb",
}

def toolchain(target):
    cargo = shutil.which("cargo")
    rustc = shutil.which("rustc")
    if cargo and rustc:
        version = subprocess.check_output([rustc, "--version"], text=True).split()[1].split("-")[0]
        if tuple(map(int, version.split("."))) >= (1, 96, 1):
            return Path(cargo).parent, rustc
    arch = platform.machine()
    if platform.system() != "Linux" or arch not in CHECKSUMS:
        raise RuntimeError("Install Rust >= 1.96.1 to build Lance on this platform")
    prefix = target / "toolchain"
    if (prefix / ".complete").exists():
        return prefix / "bin", str(prefix / "bin/rustc")
    triple = arch + "-unknown-linux-gnu"
    name = "rust-" + VERSION + "-" + triple
    archive = target / (name + ".tar.xz")
    urllib.request.urlretrieve("https://static.rust-lang.org/dist/" + archive.name, archive)
    digest = hashlib.sha256()
    with archive.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    if digest.hexdigest() != CHECKSUMS[arch]:
        archive.unlink()
        raise RuntimeError("Rust toolchain SHA-256 mismatch")
    # The archive has been verified against a pinned upstream digest.
    with tarfile.open(archive) as source:
        source.extractall(target)
    subprocess.run([str(target / name / "install.sh"), "--prefix=" + str(prefix),
                    "--components=rustc,rust-std-" + triple + ",cargo", "--disable-ldconfig"], check=True)
    (prefix / ".complete").touch()
    shutil.rmtree(target / name)
    archive.unlink()
    return prefix / "bin", str(prefix / "bin/rustc")

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--target", type=Path, required=True)
    parser.add_argument("--protoc", required=True)
    parser.add_argument("--test", action="store_true")
    args = parser.parse_args()
    args.target.mkdir(parents=True, exist_ok=True)
    binaries, rustc = toolchain(args.target)
    env = dict(os.environ, PATH=str(binaries) + os.pathsep + os.environ["PATH"],
               RUSTC=rustc, CARGO_TARGET_DIR=str(args.target), PROTOC=args.protoc)
    # Bound Cargo independently of the outer C++ build to avoid multiplying parallelism.
    jobs = env.get("LANCE_BUILD_JOBS", "2")
    command = [str(binaries / "cargo"), "build", "--locked", "--release", "--jobs", jobs,
               "--manifest-path", str(args.source / "Cargo.toml")]
    subprocess.run(command, env=env, check=True)
    if args.test:
        env["LANCE_TEST_DATASET"] = str(args.target / "testdata" / "rows.lance")
        command[1] = "test"
        subprocess.run(command, env=env, check=True)

if __name__ == "__main__":
    main()
