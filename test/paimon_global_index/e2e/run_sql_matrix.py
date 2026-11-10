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

"""Run the scalar SQL acceptance matrix against an isolated candidate cluster."""

import argparse
import json
import re
import subprocess
import uuid
from pathlib import Path


CASES = [
    ("A01", "evolved_string", "id = '1'", ["1"], False),
    ("A02", "evolved_bigint", "id >= 1", ["1", "2", "2147483648"], False),
    ("A03", "healthy", "id IN (1, 2)", ["1", "2"], True),
    ("A06", "partial", "id IN (1, 3)", ["1", "3"], False),
    ("A07", "disabled", "id = 1", ["1"], False),
    ("C02-empty", "healthy", "id = 42", [], None),
    ("C02-range", "healthy", "id >= 2", ["2"], True),
    ("C02-null", "healthy", "id IS NULL", ["NULL"], True),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mysql", default="mysql")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--warehouse", required=True, help="Shared local warehouse path inside FE and BE containers")
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"(/[A-Za-z0-9_.-]+)+", args.warehouse):
        parser.error("warehouse must be an absolute local path without SQL special characters")
    args.evidence.mkdir(parents=True, exist_ok=False)
    catalog = "gi_e2e_" + uuid.uuid4().hex[:12]
    commands = [
        args.mysql, "--no-defaults", "--protocol=TCP", "--host=127.0.0.1", "--port=" + str(args.port),
        "--user=root", "--batch", "--skip-column-names", "--connect-timeout=5",
    ]

    def execute(label, sql, *, column_names=False):
        (args.evidence / (label + ".sql")).write_text(sql + "\n", encoding="utf-8")
        try:
            command = commands + ["--column-names"] if column_names else commands
            result = subprocess.run(command, input=sql, capture_output=True, text=True, timeout=90)
        except subprocess.TimeoutExpired as exc:
            (args.evidence / (label + ".stderr")).write_text(str(exc), encoding="utf-8")
            raise
        (args.evidence / (label + ".stdout")).write_text(result.stdout, encoding="utf-8")
        (args.evidence / (label + ".stderr")).write_text(result.stderr, encoding="utf-8")
        result.check_returncode()
        return result.stdout.strip().splitlines()

    outcomes = []
    created = False
    try:
        backends = execute("backends", "SHOW BACKENDS;", column_names=True)
        if not backends or "Alive" not in backends[0].split("\t"):
            raise RuntimeError("SHOW BACKENDS did not return an Alive column")
        alive = backends[0].split("\t").index("Alive")
        if sum(line.split("\t")[alive].lower() == "true" for line in backends[1:]) < 2:
            raise RuntimeError("The acceptance matrix requires two live BEs")
        execute("create_catalog", f'CREATE EXTERNAL CATALOG {catalog} PROPERTIES ('
                f'"type"="paimon", "paimon.catalog.type"="filesystem", '
                f'"paimon.catalog.warehouse"="file:{args.warehouse}");')
        created = True
        for case, table, predicate, expected, applied in CASES:
            for stage in range(3):
                label = f"{case}-stage{stage}"
                setup = f"SET paimon_global_index_scan_stage = {stage};"
                query = f"SELECT id FROM {catalog}.gi_e2e.{table} WHERE {predicate} ORDER BY id;"
                try:
                    actual = execute(label, setup + query)
                    explain = execute(label + "-explain", setup + "EXPLAIN VERBOSE " + query)
                    was_applied = any("DISTRIBUTED INDEX: applied" in line for line in explain)
                    path_matches = (applied is None and stage == 2
                                    or was_applied == (stage == 2 and applied is True))
                    unsafe_index_disabled = None
                    if stage > 0 and case in {"A01", "A02"}:
                        trace = execute(label + "-trace", setup + "TRACE VALUES EXTERNAL " + query)
                        unsafe_index_disabled = any(
                            "PaimonGlobalIndex.SdkFallback" in line
                            and "incompatible historical index key encoding" in line for line in trace
                        )
                        path_matches = path_matches and unsafe_index_disabled
                    passed = actual == expected and path_matches
                    outcomes.append(dict(case=case, stage=stage, status="PASS" if passed else "FAIL",
                                         rows=actual, expected=expected, distributed_applied=was_applied,
                                         unsafe_index_disabled=unsafe_index_disabled))
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
                    outcomes.append(dict(case=case, stage=stage, status="FAIL", error=str(exc)))
    finally:
        (args.evidence / "matrix.json").write_text(json.dumps(outcomes, indent=2) + "\n", encoding="utf-8")
        if created:
            execute("drop_catalog", f"DROP CATALOG {catalog};")
    return int(any(outcome["status"] != "PASS" for outcome in outcomes))


if __name__ == "__main__":
    raise SystemExit(main())
