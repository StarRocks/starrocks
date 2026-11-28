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

"""Generate, verify and relocate a local Paimon fixture using a Java verifier."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


def file_manifest(warehouse):
    files = []
    for path in sorted(warehouse.rglob("*")):
        if path.is_symlink():
            raise ValueError("fixture contains a symlink: %s" % path.relative_to(warehouse))
        if path.is_file():
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(chunk)
            files.append({"path": path.relative_to(warehouse).as_posix(),
                          "size": path.stat().st_size, "sha256": digest.hexdigest()})
    if not files:
        raise ValueError("fixture warehouse is empty")
    return files


def run(command, timeout):
    result = subprocess.run(command, text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=timeout)
    if result.returncode:
        raise RuntimeError("fixture process exited with %s\n%s" % (result.returncode, result.stdout))
    return result.stdout


def verify(java, classpath, java_class, warehouse, timeout, expected_files):
    output = run([java, "-cp", classpath, java_class, "verify", str(warehouse)], timeout)
    markers = [line.split("\t") for line in output.splitlines() if line.startswith("FIXTURE_MANIFEST\t")]
    if len(markers) != 1 or len(markers[0]) != 5:
        raise ValueError("verifier must emit one FIXTURE_MANIFEST with fixture, SDK, snapshot and schema")
    _, fixture, sdk, snapshot, schema = markers[0]
    if not fixture or not sdk or not snapshot.isdigit() or not schema.isdigit():
        raise ValueError("invalid fixture identity from verifier")
    if file_manifest(warehouse) != expected_files:
        raise ValueError("verification changed fixture files")
    return {"fixture": fixture, "paimon_version": sdk,
            "snapshot_id": int(snapshot), "schema_id": int(schema)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--classpath", required=True, help="Paimon SDK and its dependency jars")
    parser.add_argument("--output", required=True, type=Path, help="new local artifact directory")
    parser.add_argument("--java-source", type=Path,
                        default=Path(__file__).with_name("GenerateFullGlobalIndexFixture.java"))
    parser.add_argument("--java-class", default="GenerateFullGlobalIndexFixture")
    parser.add_argument("--java-home", type=Path, help="JDK containing bin/java and bin/javac")
    parser.add_argument("--timeout", type=int, default=180, help="seconds per compiler or JVM process")
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be positive")
    source = args.java_source.resolve(strict=True)
    # Resolve relative classpath entries before launching any subprocess.
    classpath = os.pathsep.join(str(Path(part).absolute()) for part in args.classpath.split(os.pathsep) if part)
    if not classpath:
        parser.error("--classpath must contain at least one entry")
    java = str(args.java_home / "bin/java") if args.java_home else "java"
    javac = str(args.java_home / "bin/javac") if args.java_home else "javac"
    output = args.output.absolute()
    output.mkdir(parents=True, exist_ok=False)
    classes = output / "classes"
    classes.mkdir()
    warehouse = output / "warehouse"
    relocated = output / "relocated-warehouse"
    run([javac, "-cp", classpath, "-d", str(classes), str(source)], args.timeout)
    runtime = os.pathsep.join([str(classes), classpath])
    run([java, "-cp", runtime, args.java_class, "generate", str(warehouse)], args.timeout)
    files = file_manifest(warehouse)
    identity = verify(java, runtime, args.java_class, warehouse, args.timeout, files)
    shutil.copytree(warehouse, relocated)
    if file_manifest(relocated) != files:
        raise ValueError("relocation changed fixture files")
    relocated_identity = verify(java, runtime, args.java_class, relocated, args.timeout, files)
    if relocated_identity != identity or file_manifest(warehouse) != files:
        raise ValueError("fixture identity or source files changed during relocated verification")
    manifest = {"manifest_version": 1, **identity, "files": files}
    manifest_path = output / "fixture-manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print("Verified fixture %s, snapshot %s, schema %s, %s files" %
          (identity["fixture"], identity["snapshot_id"], identity["schema_id"], len(files)))
    print(manifest_path)


if __name__ == "__main__":
    main()
