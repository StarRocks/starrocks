#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT = REPO_ROOT / "nix" / "thirdparty-archives.nix"

SYSTEMS = {
    "x86_64-linux": {
        "machine": "x86_64",
        "kernel": "Linux",
        "override": None,
    },
    "aarch64-linux": {
        "machine": "aarch64",
        "kernel": "Linux",
        "override": None,
    },
    "aarch64-darwin": {
        "machine": "arm64",
        "kernel": "Darwin",
        "override": "thirdparty/vars-darwin-aarch64.sh",
    },
}

# These inputs are optional binary payloads or replaced by Nix-provided tools.
ARCHIVE_EXCLUDES = {
    "GCS_CONNECTOR",
    "JDK",
    "JINDOSDK",
    "PPROF",
    "STARCACHE",
    "TENANN",
}
PACKAGE_EXCLUDES = {
    "aliyun_jindosdk",
    "gcs_connector",
    "jdk",
    "pprof",
    "starcache",
    "tenann",
}
OPTIONAL_PACKAGE_ARCHIVES = {
    "breakpad": "BREAK_PAD",
    "libdeflate": "LIBDEFLATE",
}

AWS_CRT_ARCHIVES = [
    (
        "aws-crt-cpp",
        "e4514b7fb8b1fe67429aa7b0e00f628999722174",
        "sha256-9MdK0nOOyGkkOQ5XH0W2Fo19p8nbJ1S5aL41yoydCNY=",
    ),
    (
        "aws-c-auth",
        "6ba7a0f8688c713dfe137716dbd5be324c2315b0",
        "sha256-2ZbYkDcTbff3C/XZ6O2tvizeXGuMa3GjcQfqsihPSME=",
    ),
    (
        "aws-c-cal",
        "56f0a79ceb10f2efcf92f525ace717f84d8c8a11",
        "sha256-ocVtkdo90qpCYGQJ8l8TQW7dtM9IyMH5yCS9b5KjcIE=",
    ),
    (
        "aws-c-common",
        "8eaa0986ad3cfd46c87432a2e4c8ab81a786085f",
        "sha256-UuCdrkLYRWYkCzc7Me1bP7xw3NMp9Nm5frbTbL2Aei4=",
    ),
    (
        "aws-c-compression",
        "99ec79ee2970f1a045d4ced1501b97ee521f2f85",
        "sha256-7WoVgE97s+K6EH2Tr+lNy0AT3Lforus6ggd14hGHF4w=",
    ),
    (
        "aws-c-event-stream",
        "63d1e1021b04ce3c3b1fc1895078ac85e0430b24",
        "sha256-YebplulHKkFVbYogYGpsxx9vh/nVwlHeqPUCpGys8BQ=",
    ),
    (
        "aws-c-http",
        "6a1c157c20640a607102738909e89561a41e91e9",
        "sha256-qLNpZNjA4c6xbrd6bO2vSCIlnNZvvH1lmWn4MFPTuAA=",
    ),
    (
        "aws-c-io",
        "6225ebb9da28f1023ad5e21694de9d165cd65f3b",
        "sha256-ZnqXN5PwtWvcNCWtvm/BM0QjG7jmBT28ZhGSS8dinAY=",
    ),
    (
        "aws-c-mqtt",
        "17ee24a2177fc64cf9773d430a24e6fa06a89dd0",
        "sha256-mEebEd+3JeEeFSY/mLU9rfUP6+KRASRne6brERsRnlw=",
    ),
    (
        "aws-c-s3",
        "1dd55be83b19a55cd9c155e2da977cdc76112a91",
        "sha256-dQ8tQwR2Xn6oj9n+ExbpEdy29yF51kUVdaUcnlFZ+hw=",
    ),
    (
        "aws-c-sdkutils",
        "fd8c0ba2e233997eaaefe82fb818b8b444b956d3",
        "sha256-deMc7gYjITzRCE0oNuyqOhQiF0nmofVWzapRgHKIhHQ=",
    ),
    (
        "aws-checksums",
        "321b805559c8e911be5bddba13fcbd222a3e2d3a",
        "sha256-HZAxPZWoMNk9hxE71+yA5Z18FbFW+9K1Q7sv3t7WitE=",
    ),
    (
        "aws-lc",
        "dc4e28145ceb6d46b5475e833f2da8def6d583fe",
        "sha256-TeecD45bAZv2tZ9qmq0+M34jeU+Jmzu18cntQVVi79A=",
    ),
    (
        "s2n",
        "0998358a6ef7c4f22295deba088796fe354c5f4c",
        "sha256-4r57nUbltvSxnRgYkGdtnCS7evbQWRdSK0EWea0kAFw=",
    ),
]


@dataclass(frozen=True)
class Archive:
    archive_id: str
    file_name: str
    url: str
    md5: str
    sha256: str


def fake_tool_dir(machine: str, kernel: str) -> tempfile.TemporaryDirectory[str]:
    tmp = tempfile.TemporaryDirectory()
    tool_dir = Path(tmp.name)
    (tool_dir / "uname").write_text(
        f"""#!/bin/sh
case "${{1:-}}" in
  -m) printf '%s\\n' '{machine}' ;;
  -s) printf '%s\\n' '{kernel}' ;;
  *) printf '%s\\n' '{kernel}' ;;
esac
""",
        encoding="utf-8",
    )
    (tool_dir / "nproc").write_text("#!/bin/sh\nprintf '%s\\n' 8\n", encoding="utf-8")
    (tool_dir / "sysctl").write_text(
        """#!/bin/sh
case "$*" in
  "-n hw.physicalcpu") printf '%s\\n' 8 ;;
  *) exit 1 ;;
esac
""",
        encoding="utf-8",
    )
    for tool in ("uname", "nproc", "sysctl"):
        (tool_dir / tool).chmod(0o755)
    return tmp


def shell_inventory(system: str) -> tuple[list[str], list[str], dict[str, dict[str, str]]]:
    config = SYSTEMS[system]
    with fake_tool_dir(config["machine"], config["kernel"]) as tools:
        env = os.environ.copy()
        env["PATH"] = f"{tools}{os.pathsep}{env['PATH']}"
        env["TP_DIR"] = str(REPO_ROOT / "thirdparty")
        env["PARALLEL"] = "1"
        override = config["override"]
        if override is None:
            env.pop("STARROCKS_TP_VARS_OVERRIDE", None)
        else:
            env["STARROCKS_TP_VARS_OVERRIDE"] = str(REPO_ROOT / override)

        script = r"""
set -euo pipefail
. "$TP_DIR/vars.sh"
. "$TP_DIR/package-manifest.sh"
starrocks_set_default_packages "$MACHINE_TYPE"
TP_ARCHIVE_LIST=()
tp_archives_decl="$(declare -p TP_ARCHIVES)"
tp_archives_attrs="${tp_archives_decl%% TP_ARCHIVES=*}"
if [[ "${tp_archives_attrs}" == declare\ -*a* ]]; then
  TP_ARCHIVE_LIST=("${TP_ARCHIVES[@]}")
elif [[ -n "${TP_ARCHIVES:-}" ]]; then
  read -r -a TP_ARCHIVE_LIST <<< "$TP_ARCHIVES"
fi
printf 'TP_ARCHIVES\0%s\0' "${TP_ARCHIVE_LIST[*]}"
printf 'PACKAGES'
for package in "${STARROCKS_THIRDPARTY_ALL_PACKAGES[@]}"; do
  printf '\0%s' "$package"
done
printf '\0END_PACKAGES\0'
for archive in "${TP_ARCHIVE_LIST[@]}"; do
  eval "name=\${${archive}_NAME:-}"
  eval "url=\${${archive}_DOWNLOAD:-}"
  eval "md5=\${${archive}_MD5SUM:-}"
  printf 'ARCHIVE\0%s\0%s\0%s\0%s\0' "$archive" "$name" "$url" "$md5"
done
"""
        result = subprocess.run(
            ["bash", "-c", script],
            cwd=REPO_ROOT,
            env=env,
            check=True,
            stdout=subprocess.PIPE,
        )

    fields = result.stdout.decode("utf-8").split("\0")
    archive_ids: list[str] = []
    packages: list[str] = []
    archive_vars: dict[str, dict[str, str]] = {}
    i = 0
    while i < len(fields):
        tag = fields[i]
        if tag == "TP_ARCHIVES":
            archive_ids = fields[i + 1].split()
            i += 2
        elif tag == "PACKAGES":
            i += 1
            while fields[i] != "END_PACKAGES":
                packages.append(fields[i])
                i += 1
            i += 1
        elif tag == "ARCHIVE":
            archive_id, file_name, url, md5 = fields[i + 1 : i + 5]
            archive_vars[archive_id] = {
                "file_name": file_name,
                "url": url,
                "md5": md5,
            }
            i += 5
        elif tag == "":
            i += 1
        else:
            raise RuntimeError(f"unexpected shell inventory field: {tag!r}")

    return archive_ids, packages, archive_vars


def known_sha256s() -> dict[tuple[str, str], str]:
    if not OUTPUT.exists():
        return {}
    text = OUTPUT.read_text(encoding="utf-8")
    pattern = re.compile(
        r'"([^"]+)" = \{\n'
        r'\s+url = "([^"]+)";\n'
        r'\s+md5 = "([^"]+)";\n'
        r'\s+sha256 = "([^"]+)";\n'
        r'\s+\};'
    )
    return {
        (file_name, md5): sha256
        for file_name, _url, md5, sha256 in pattern.findall(text)
    }


def prefetch_sha256(file_name: str, url: str) -> str:
    result = subprocess.run(
        ["nix-prefetch-url", "--name", file_name, url],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError(f"nix-prefetch-url did not return a hash for {file_name}")
    return lines[-1]


def include_archive(archive_id: str, packages: list[str]) -> bool:
    if archive_id in ARCHIVE_EXCLUDES:
        return False
    for package, optional_archive_id in OPTIONAL_PACKAGE_ARCHIVES.items():
        if archive_id == optional_archive_id:
            return package in packages
    return True


def system_data(
    system: str, hashes: dict[tuple[str, str], str], prefetch_missing: bool
) -> tuple[list[str], list[str], list[Archive]]:
    archive_ids, packages, archive_vars = shell_inventory(system)
    packages = [package for package in packages if package not in PACKAGE_EXCLUDES]

    selected_archive_ids = [
        archive_id
        for archive_id in archive_ids
        if include_archive(archive_id, packages)
    ]
    archives: list[Archive] = []
    for archive_id in selected_archive_ids:
        archive = archive_vars[archive_id]
        file_name = archive["file_name"]
        md5 = archive["md5"]
        sha256 = hashes.get((file_name, md5))
        if sha256 is None:
            if not prefetch_missing:
                raise RuntimeError(
                    f"missing sha256 for {file_name}; rerun with --prefetch-missing"
                )
            sha256 = prefetch_sha256(file_name, archive["url"])
        archives.append(
            Archive(
                archive_id=archive_id,
                file_name=file_name,
                url=archive["url"],
                md5=md5,
                sha256=sha256,
            )
        )

    return selected_archive_ids, packages, archives


def nix_string(value: str) -> str:
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def render_attr_list(name: str, values_by_system: dict[str, list[str]]) -> list[str]:
    lines = [f"  {name} = {{"]
    for system in SYSTEMS:
        lines.append(f"    {nix_string(system)} = [")
        lines.extend(f"      {nix_string(value)}" for value in values_by_system[system])
        lines.append("    ];")
    lines.append("  };")
    return lines


def render(archives_by_system: dict[str, list[Archive]], archive_ids_by_system: dict[str, list[str]], packages_by_system: dict[str, list[str]]) -> str:
    archives = {
        archive.file_name: archive
        for system_archives in archives_by_system.values()
        for archive in system_archives
    }

    lines = [
        "# Generated by ./nix/generate-thirdparty-archives.py from thirdparty/vars*.sh",
        "# and thirdparty/package-manifest.sh. Do not edit by hand.",
        "{ pkgs, lib }:",
        "",
        "let",
        "  archives = {",
    ]
    for file_name in sorted(archives, key=str.casefold):
        archive = archives[file_name]
        lines.extend(
            [
                f"    {nix_string(file_name)} = {{",
                f"      url = {nix_string(archive.url)};",
                f"      md5 = {nix_string(archive.md5)};",
                f"      sha256 = {nix_string(archive.sha256)};",
                "    };",
            ]
        )
    lines.extend(["  };", "", "  awsCrtArchives = ["])
    for name, revision, sha256 in AWS_CRT_ARCHIVES:
        file_name = f"{name}-{revision}.zip"
        url = f"https://codeload.github.com/awslabs/{name}/zip/{revision}"
        lines.extend(
            [
                "    {",
                f"      name = {nix_string(name)};",
                f"      fileName = {nix_string(file_name)};",
                f"      url = {nix_string(url)};",
                f"      sha256 = {nix_string(sha256)};",
                "    }",
            ]
        )
    lines.extend(
        [
            "  ];",
            "",
            *render_attr_list(
                "bySystem",
                {
                    system: [archive.file_name for archive in archives_by_system[system]]
                    for system in SYSTEMS
                },
            ),
            "",
            *render_attr_list("archiveNamesBySystem", archive_ids_by_system),
            "",
            *render_attr_list("packageNamesBySystem", packages_by_system),
            "",
            "in",
            "{",
            "  archiveNamesFor = system: archiveNamesBySystem.${system} or [ ];",
            "",
            "  packageNamesFor = system: packageNamesBySystem.${system} or [ ];",
            "",
            "  fetchedFor =",
            "    system:",
            "    map (",
            "      name:",
            "      let",
            "        archive = archives.${name};",
            "      in",
            "      {",
            "        inherit name;",
            "        path = pkgs.fetchurl {",
            "          inherit name;",
            "          inherit (archive) url sha256;",
            "        };",
            "      }",
            "    ) (bySystem.${system} or [ ]);",
            "",
            "  fetchedAwsCrt = map (archive: {",
            "    inherit (archive) name;",
            "    path = pkgs.fetchurl {",
            "      name = archive.fileName;",
            "      inherit (archive) url sha256;",
            "    };",
            "  }) awsCrtArchives;",
            "}",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--prefetch-missing",
        action="store_true",
        help="use nix-prefetch-url when a new archive hash is missing",
    )
    args = parser.parse_args()

    hashes = known_sha256s()
    archive_ids_by_system: dict[str, list[str]] = {}
    packages_by_system: dict[str, list[str]] = {}
    archives_by_system: dict[str, list[Archive]] = {}
    for system in SYSTEMS:
        archive_ids, packages, archives = system_data(
            system, hashes, args.prefetch_missing
        )
        archive_ids_by_system[system] = archive_ids
        packages_by_system[system] = packages
        archives_by_system[system] = archives

    OUTPUT.write_text(
        render(archives_by_system, archive_ids_by_system, packages_by_system),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
