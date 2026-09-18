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

"""Opt-in PostgreSQL text[] / varchar[] JDBC regression test (Python 3.8+)."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import uuid

HERE = Path(__file__).resolve().parent


def quote(value, char):
    return char + value.replace(char, char + char) + char


def run(command, sql, timeout):
    result = subprocess.run(command, input=sql, text=True, encoding="utf-8", stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout, check=False)
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def comparable(output):
    def cell(value):
        if value == "NULL":
            return None
        if value.startswith("["):
            return json.loads(value)
        return value
    return [[cell(value) for value in line.split("\t")] for line in output.splitlines()]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog")
    parser.add_argument("--mysql", default="mysql")
    parser.add_argument("--psql", default="psql")
    parser.add_argument("--mysql-defaults-file")
    parser.add_argument("--sr-host", default=os.environ.get("SR_HOST", "127.0.0.1"))
    parser.add_argument("--sr-port", type=int, default=9030)
    parser.add_argument("--sr-user", default="root")
    parser.add_argument("--pg-reader-role")
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--output", type=Path, default=Path("postgresql-jdbc-array-results.json"))
    parser.add_argument("--list", action="store_true")
    args = parser.parse_args()
    cases = json.loads((HERE / "cases.json").read_text(encoding="utf-8"))
    if args.list:
        print("\n".join(case["name"] for case in cases))
        return 0
    if not args.catalog or args.timeout < 1 or args.output.exists():
        parser.error("Set --catalog and a positive --timeout; --output must not already exist")
    schema = "sr_pg_array_" + uuid.uuid4().hex
    pg_schema = quote(schema, '"')
    table = ".".join(quote(x, "`") for x in (args.catalog, schema, "probe"))
    pg_table = pg_schema + '."probe"'
    mysql = [args.mysql]
    if args.mysql_defaults_file:
        mysql += ["--defaults-extra-file=" + str(Path(args.mysql_defaults_file).resolve())]
    mysql += ["--batch", "--raw", "--skip-column-names", "--default-character-set=utf8mb4",
              "--connect-timeout=10", "--host=" + args.sr_host, "--port=" + str(args.sr_port), "--user=" + args.sr_user]
    psql = [args.psql, "-X", "-w", "-q", "-A", "-t", "-F", "\t", "-P", "null=NULL", "-v", "ON_ERROR_STOP=1"]
    def sr(sql):
        return run(mysql, "SET query_timeout={}; SET chunk_size=1024; ".format(args.timeout) + sql, args.timeout)

    def pg(sql):
        return run(psql, "SET statement_timeout={}; ".format(args.timeout * 1000) + sql, args.timeout)

    # Reconstruct each source array in ordinal order, explicitly discarding PostgreSQL lower bounds.
    normalized = ("WITH p AS (SELECT id, "
                  "CASE WHEN items IS NULL THEN NULL ELSE ARRAY(SELECT v FROM unnest(items) "
                  "WITH ORDINALITY u(v,n) ORDER BY n) END AS items, "
                  "CASE WHEN labels IS NULL THEN NULL ELSE ARRAY(SELECT v FROM unnest(labels) "
                  "WITH ORDINALITY u(v,n) ORDER BY n) END AS labels FROM " + pg_table + ") ")
    report = {"schema": schema, "cases": [], "errors": []}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    created = False
    try:
        pg("CREATE SCHEMA " + pg_schema)
        created = True
        setup = (HERE / "fixtures.sql").read_text(encoding="utf-8").replace("{schema}", pg_schema)
        if args.pg_reader_role:
            role = quote(args.pg_reader_role, '"')
            setup += "GRANT USAGE ON SCHEMA " + pg_schema + " TO " + role + "; "
            setup += "GRANT SELECT ON ALL TABLES IN SCHEMA " + pg_schema + " TO " + role + ";"
        pg(setup)
        description = sr("DESC " + table)
        if len(re.findall(r"array<varchar", description, re.IGNORECASE)) != 2:
            raise AssertionError("Expected two ARRAY<VARCHAR> columns: " + description)
        report["description"] = description
        for aggregate in (False, True):
            for join in (False, True):
                settings = ("SET enable_jdbc_agg_push_down=" + str(aggregate).lower() + "; "
                            "SET enable_jdbc_join_push_down=" + str(join).lower() + "; ")
                for case in cases:
                    entry = {"name": case["name"], "aggregate": aggregate, "join": join}
                    try:
                        query = case["sr"].replace("{table}", table)
                        actual = sr(settings + query)
                        expected = pg(normalized + case["pg"])
                        plan = sr(settings + "EXPLAIN VERBOSE " + query)
                        remote = re.findall(r"^\s*QUERY:\s*(.+)$", plan, re.MULTILINE)
                        entry.update(actual=actual, expected=expected, plan=plan, remote_queries=remote)
                        if comparable(actual) != comparable(expected):
                            raise AssertionError("StarRocks results differ from the ordinal-normalized PostgreSQL oracle")
                        if not remote:
                            raise AssertionError("Missing JDBC QUERY in EXPLAIN")
                        if case["array_operation"]:
                            for sql in remote:
                                if re.search(r"\bGROUP\s+BY\b|\bDISTINCT\b|\bJOIN\b|\bCOUNT\s*\(|\[", sql, re.I):
                                    raise AssertionError("Array operation unexpectedly pushed: " + sql)
                                # Scalar id predicates may push. Array predicates must not appear after WHERE.
                                where = re.split(r"\bWHERE\b", sql, flags=re.I)
                                if len(where) > 1 and re.search(r"\bitems\b|\blabels\b", where[-1], re.I):
                                    raise AssertionError("Array predicate unexpectedly pushed: " + sql)
                        entry["passed"] = True
                    except Exception as error:
                        entry.update(passed=False, error=str(error))
                    report["cases"].append(entry)
        unsupported = table.rsplit(".", 1)[0] + ".`unsupported`"
        description = sr("DESC " + unsupported)
        if description.count("UNKNOWN_TYPE") != 2:
            raise AssertionError("Other array types should remain unsupported: " + description)
        report["unsupported_description"] = description
        try:
            sr("SELECT numbers FROM " + unsupported)
            raise AssertionError("integer[] unexpectedly succeeded")
        except RuntimeError as error:
            if not re.search(r"unsupported|not supported|UNKNOWN_TYPE", str(error), re.I):
                raise
            report["unsupported_error"] = str(error)
        multidimensional = table.rsplit(".", 1)[0] + ".`multidimensional`"
        try:
            sr("SELECT items FROM " + multidimensional + " WHERE id = 1")
            raise AssertionError("Multidimensional array unexpectedly succeeded")
        except RuntimeError as error:
            if "only one-dimensional text[] and varchar[]" not in str(error):
                raise
            report["multidimensional_error"] = str(error)
        healthy = sr("SELECT items FROM " + multidimensional + " WHERE id = 2")
        if comparable(healthy) != [[["valid"]]]:
            raise AssertionError("Query after rejected array did not recover: " + healthy)
        report["recovery_passed"] = True
    except Exception as error:
        report["errors"].append(str(error))
    finally:
        try:
            if created:
                pg("DROP SCHEMA " + pg_schema + " CASCADE")
                report["cleaned_up"] = True
        except Exception as error:
            report["errors"].append("Cleanup failed: " + str(error))
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    passed = sum(case["passed"] for case in report["cases"])
    print("{}/{} cases passed; {} errors; evidence: {}".format(passed, len(report["cases"]),
                                                             len(report["errors"]), args.output))
    return 0 if passed == len(cases) * 4 and not report["errors"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
