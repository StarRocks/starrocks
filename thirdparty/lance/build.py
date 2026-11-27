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

"""Build and install the optional Lance dependency before configuring StarRocks."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import tempfile

VERSION = "0.1.0"
SOURCE = Path(__file__).resolve().parent


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def toolchain(env):
    rustc = shutil.which(env.get("RUSTC", "rustc"), path=env.get("PATH"))
    cargo = shutil.which(env.get("CARGO", "cargo"), path=env.get("PATH"))
    if not rustc or not cargo:
        raise RuntimeError("Install Rust >= 1.96.1 before building the Lance thirdparty dependency")
    version = subprocess.check_output([rustc, "--version"], env=env, text=True)
    match = re.match(r"rustc (\d+)\.(\d+)\.(\d+)", version)
    if not match or tuple(map(int, match.groups())) < (1, 96, 1):
        raise RuntimeError("Lance requires Rust >= 1.96.1")
    info = subprocess.check_output([rustc, "-vV"], env=env, text=True)
    host = next(line[len("host: "):] for line in info.splitlines() if line.startswith("host: "))
    if env.get("CARGO_BUILD_TARGET", host) != host:
        raise RuntimeError("Build Lance natively in the target platform's thirdparty environment")
    return cargo, rustc, host, version.strip()


def create_install_parent(path):
    """Make new install ancestors traversable without changing existing directories."""
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


def install(stage, prefix):
    # Only replace an installation owned by this builder; never erase an arbitrary prefix.
    if prefix.exists() and not (prefix / "share/lance/manifest.json").is_file():
        if any(prefix.iterdir()):
            raise RuntimeError("Refusing to replace a non-Lance installation: " + str(prefix))
    backup = stage.parent / (stage.name + "-previous")
    if prefix.exists():
        prefix.rename(backup)
    try:
        stage.rename(prefix)
    except OSError:
        if backup.exists():
            backup.rename(prefix)
        raise
    if backup.exists():
        shutil.rmtree(backup)


def build(args):
    env = dict(os.environ)
    system = platform.system()
    machine = {"arm64": "aarch64", "AMD64": "x86_64"}.get(platform.machine(), platform.machine())
    if system not in ("Linux", "Darwin") or machine not in ("x86_64", "aarch64"):
        raise RuntimeError("Lance thirdparty builds support native Linux/macOS x86_64 and ARM64")
    cargo, rustc, host, rust_version = toolchain(env)
    expected_host = machine + ("-unknown-linux-gnu" if system == "Linux" else "-apple-darwin")
    if host != expected_host:
        raise RuntimeError("Rust host target does not match the native build platform: " + host)
    protoc = shutil.which(str(args.protoc), path=env.get("PATH"))
    if not protoc:
        raise RuntimeError("Install protobuf/protoc first, or pass --protoc /path/to/protoc")
    prefix = args.prefix.resolve()
    target = args.target_dir.resolve()
    create_install_parent(prefix.parent)
    target.mkdir(parents=True, exist_ok=True)
    env.update(RUSTC=rustc, PROTOC=protoc, CARGO_TARGET_DIR=str(target), CARGO_BUILD_TARGET=host)
    # Preserve profiling frame chains in Rust and its native dependencies.
    flags = env.get("CARGO_ENCODED_RUSTFLAGS")
    if flags is None:
        flags = shlex.split(env.get("RUSTFLAGS", ""))
    else:
        flags = flags.split("\x1f") if flags else []
    env["CARGO_ENCODED_RUSTFLAGS"] = "\x1f".join(flags + ["-Cforce-frame-pointers=yes"])
    for name in ("CFLAGS", "CXXFLAGS"):
        env[name] = env.get(name, "") + " -fno-omit-frame-pointer"
    command = [cargo, "build", "--release", "--locked", "--target", host,
               "--jobs", str(args.jobs), "--manifest-path", str(SOURCE / "Cargo.toml")]
    if args.offline:
        command.append("--offline")
    subprocess.run(command, env=env, check=True)
    stage = Path(tempfile.mkdtemp(prefix=".lance-install-", dir=prefix.parent))
    try:
        fixture = stage / "share/lance/testdata/rows.lance"
        env["LANCE_TEST_DATASET"] = str(fixture)
        command[1] = "test"
        subprocess.run(command, env=env, check=True)
        if not (fixture / "_versions").is_dir():
            raise RuntimeError("Lance tests did not produce the BE integration fixture")
        library_name = "libstarrocks_lance" + (".dylib" if system == "Darwin" else ".so")
        (stage / "lib").mkdir()
        (stage / "include").mkdir()
        shutil.copy2(target / host / "release" / library_name, stage / "lib" / library_name)
        shutil.copy2(SOURCE / "lance_rs_ffi.h", stage / "include/lance_rs_ffi.h")
        if system == "Darwin":
            subprocess.run(["install_name_tool", "-id", "@rpath/" + library_name,
                            str(stage / "lib" / library_name)], check=True)
        cmake = stage / "lib/cmake/StarRocksLance"
        cmake.mkdir(parents=True)
        values = {"VERSION": VERSION, "SYSTEM": system, "MACHINE": machine,
                  "LIBRARY": library_name, "LIBRARY_SHA256": sha256(stage / "lib" / library_name),
                  "HEADER_SHA256": sha256(SOURCE / "lance_rs_ffi.h")}
        for name in ("StarRocksLanceConfig.cmake", "StarRocksLanceConfigVersion.cmake"):
            content = (SOURCE / (name + ".in")).read_text()
            for key, value in values.items():
                content = content.replace("@" + key + "@", value)
            (cmake / name).write_text(content)
        manifest = dict(values, rust_version=rust_version, target=host,
                        cargo_lock_sha256=sha256(SOURCE / "Cargo.lock"),
                        source_sha256={str(path.relative_to(SOURCE)): sha256(path)
                                       for path in sorted(SOURCE.glob("src/*.rs"))})
        (stage / "share/lance/manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        shutil.copy2(SOURCE / "README.md", stage / "share/lance/README.md")
        shutil.copy2(SOURCE.parents[1] / "LICENSE.txt", stage / "share/lance/LICENSE.txt")
        # Dependency images may be built with umask 077 and consumed by another user.
        # Normalize the whole package, including generated fixture and CMake files.
        for path in stage.rglob("*"):
            path.chmod(0o755 if path.is_dir() else 0o644)
        (stage / "lib" / library_name).chmod(0o755)
        stage.chmod(0o755)
        install(stage, prefix)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    print("Installed Lance thirdparty dependency to " + str(prefix))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    thirdparty = SOURCE.parent
    installed = Path(os.environ.get("TP_INSTALL_DIR", str(thirdparty / "installed")))
    parser.add_argument("--prefix", type=Path, default=installed / "lance")
    parser.add_argument("--target-dir", type=Path, default=thirdparty / "src/lance-build")
    parser.add_argument("--protoc", type=Path,
                        default=Path(os.environ.get("PROTOC", str(installed / "bin/protoc"))))
    parser.add_argument("-j", "--jobs", type=int, default=2)
    parser.add_argument("--offline", action="store_true")
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    try:
        build(args)
    except (RuntimeError, subprocess.CalledProcessError) as error:
        parser.exit(1, str(error) + "\n")


if __name__ == "__main__":
    main()
