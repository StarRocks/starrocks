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

"""Opt-in PostgreSQL JDBC result and pushdown regression test (Python 3.8+)."""

import argparse
import datetime
import decimal
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid


HERE = Path(__file__).resolve().parent
MODES = (("local", False, False), ("aggregate", True, False),
         ("topn", False, True), ("both", True, True))
NUMBER = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z")
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\Z")


def identifier(value, quote):
    return quote + value.replace(quote, quote + quote) + quote


def rows(output):
    return [line.split("\t") for line in output.splitlines()]


def comparable(result):
    # Compare exact numbers, including BIGINT extrema, without float rounding.
    # CLI timestamp renderings may differ only in trailing fractional zeros.
    def value(cell):
        if cell == "NULL":
            return None
        if NUMBER.fullmatch(cell):
            return decimal.Decimal(cell)
        if TIMESTAMP.fullmatch(cell):
            if "." in cell:
                # Python 3.8 accepts only three or six fractional digits.
                whole, fraction = cell.split(".")
                cell = whole + "." + fraction.ljust(6, "0")
            return datetime.datetime.fromisoformat(cell)
        return cell
    return [[value(cell) for cell in row] for row in result]


def run_client(command, sql, timeout):
    try:
        result = subprocess.run(command, input=sql, text=True, encoding="utf-8",
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=timeout, check=False)
    except subprocess.TimeoutExpired as error:
        raise RuntimeError("SQL client exceeded {} seconds".format(timeout)) from error
    except OSError as error:
        raise RuntimeError("Unable to execute SQL client: {}".format(error)) from error
    if result.returncode:
        raise RuntimeError("SQL client exited {}: {}".format(
            result.returncode, result.stderr.strip()))
    return result.stdout


def assert_plan(case, plan, aggregate_enabled, topn_enabled):
    queries = re.findall(r"^\s*QUERY:\s*(.+)$", plan, re.MULTILINE)
    errors = []
    if len(queries) != 1:
        errors.append("Expected exactly one JDBC QUERY, found {}".format(len(queries)))
    remote = "\n".join(queries)
    aggregate_expected = bool(case.get("aggregate")) and aggregate_enabled
    aggregate_actual = bool(re.search(r"\bGROUP\s+BY\b|\b(?:sum|count|min|max|avg)\s*\(",
                                      remote, re.IGNORECASE))
    if aggregate_actual != aggregate_expected:
        errors.append("Remote aggregate expected {}, found {}".format(
            aggregate_expected, aggregate_actual))
    if aggregate_expected and case["aggregate"] == "group" and not re.search(
            r"\bGROUP\s+BY\b", remote, re.IGNORECASE):
        errors.append("Expected remote GROUP BY")
    if aggregate_expected and case.get("remote_having") and not re.search(
            r"\bHAVING\b", remote, re.IGNORECASE):
        errors.append("Expected remote HAVING")
    topn_expected = bool(case.get("topn_limit")) and topn_enabled and (
        not case.get("topn_requires_aggregate") or aggregate_enabled)
    order_actual = bool(re.search(r"\bORDER\s+BY\b", remote, re.IGNORECASE))
    if order_actual != topn_expected:
        errors.append("Remote ORDER BY expected {}, found {}".format(topn_expected, order_actual))
    limits = [int(limit) for limit in re.findall(r"\bLIMIT\s+(\d+)\b", remote, re.IGNORECASE)]
    if topn_expected:
        # The outer JDBC scan may repeat its safe limit around the inline SQL.
        if not limits or any(limit != case["topn_limit"] for limit in limits):
            errors.append("Expected only remote LIMIT {}, found {}".format(case["topn_limit"], limits))
        if not re.search(r"\b(?:ASC|DESC)\s+NULLS\s+(?:FIRST|LAST)\b", remote, re.IGNORECASE):
            errors.append("Expected explicit remote direction and NULL ordering")
        # The remote ORDER BY replaces the local TopN: leaving one behind would mean the rows
        # still needed reordering, which is exactly what the pushdown is supposed to avoid.
        if "TOP-N" in plan:
            errors.append("Unexpected local TOP-N alongside a pushed ORDER BY")
    # A string sort key has to carry COLLATE "C" so PostgreSQL orders it by bytes, the way
    # StarRocks does; a non-string key must not carry one.
    collate_expected = case.get("remote_collate", 0) if topn_expected else 0
    collate_actual = len(re.findall(r'COLLATE\s+"C"', remote, re.IGNORECASE))
    if collate_actual != collate_expected:
        errors.append('Expected {} remote COLLATE "C", found {}'.format(collate_expected, collate_actual))
    if not topn_expected and limits:
        errors.append("Unexpected remote LIMIT during TopN fallback: {}".format(limits))
    return queries, errors


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", help="Existing PostgreSQL JDBC catalog in StarRocks")
    parser.add_argument("--mysql", default="mysql", help="mysql executable path (or forwarding wrapper)")
    parser.add_argument("--psql", default="psql", help="psql executable path (or forwarding wrapper)")
    parser.add_argument("--mysql-defaults-file", help="mysql defaults-extra-file containing client authentication")
    parser.add_argument("--sr-host", default=os.environ.get("SR_HOST", "127.0.0.1"))
    parser.add_argument("--sr-port", type=int, default=int(os.environ.get("SR_PORT", "9030")))
    parser.add_argument("--sr-user", default=os.environ.get("SR_USER", "root"))
    parser.add_argument("--pg-reader-role", help="Grant this existing catalog role access to the new fixture schema")
    parser.add_argument("--timeout", type=int, default=60, help="Timeout in seconds for each client invocation")
    parser.add_argument("--output", type=Path, default=Path("postgresql-jdbc-results.json"))
    parser.add_argument("--list", action="store_true", help="List cases without connecting to either database")
    args = parser.parse_args()
    if not args.list and not args.catalog:
        parser.error("--catalog is required unless --list is used")
    if args.timeout < 1:
        parser.error("--timeout must be positive")
    if not args.list and args.output.exists():
        parser.error("--output already exists; choose a new path to preserve prior evidence")
    return args


def main():
    args = arguments()
    cases = json.loads((HERE / "cases.json").read_text(encoding="utf-8"))
    if args.list:
        for case in cases:
            print(case["name"])
        return 0

    schema = "sr_pg_pushdown_" + uuid.uuid4().hex
    pg_schema = identifier(schema, '"')
    pg_table = pg_schema + '."probe"'
    sr_table = ".".join(identifier(part, "`") for part in (args.catalog, schema, "probe"))
    psql = [args.psql, "-X", "-w", "-q", "-A", "-t", "-F", "\t", "-P", "null=NULL",
            "-v", "ON_ERROR_STOP=1"]
    mysql = [args.mysql]
    if args.mysql_defaults_file:
        mysql.append("--defaults-extra-file=" + str(Path(args.mysql_defaults_file).resolve()))
    mysql.extend(["--batch", "--raw", "--skip-column-names", "--default-character-set=utf8mb4",
                  "--connect-timeout=10", "--host=" + args.sr_host, "--port=" + str(args.sr_port),
                  "--user=" + args.sr_user])

    def pg(sql):
        return run_client(psql, "SET TIME ZONE 'UTC'; SET DateStyle = 'ISO, YMD'; "
                          "SET statement_timeout = '{}s';\n{}\n".format(args.timeout, sql), args.timeout)

    def sr(sql, agg, topn):
        settings = ("SET time_zone = '+00:00'; SET query_timeout = {}; "
                    "SET enable_jdbc_project_push_down = true; "
                    "SET enable_jdbc_agg_push_down = {}; "
                    "SET enable_jdbc_topn_push_down = {};\n").format(
                        args.timeout, str(agg).lower(), str(topn).lower())
        return run_client(mysql, settings + sql + ";\n", args.timeout)

    evidence = {"started_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "schema": schema, "catalog": args.catalog, "cases": [],
                "errors": [], "cleanup_error": None}
    created = False
    try:
        # Prove that this invocation created the schema before allowing cleanup.
        pg("CREATE SCHEMA {};".format(pg_schema))
        created = True
        pg((HERE / "fixtures.sql").read_text(encoding="utf-8").format(table=pg_table))
        if args.pg_reader_role:
            role = identifier(args.pg_reader_role, '"')
            pg("GRANT USAGE ON SCHEMA {} TO {}; GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {};".format(
                pg_schema, role, pg_schema, role))
        for case in cases:
            entry = {"name": case["name"], "sql_template": case["sql"], "expectations": {
                key: value for key, value in case.items()
                if key not in ("name", "sql", "pg_sql")}, "modes": []}
            evidence["cases"].append(entry)
            # StarRocks compares strings by bytes while PostgreSQL uses the column's collation,
            # so a text case supplies its own reference query with an explicit COLLATE "C".
            # Without that the two disagree before any pushdown, which is precisely the
            # difference the pushed ORDER BY has to cancel out.
            pg_sql = case.get("pg_sql", case["sql"]).format(table=pg_table)
            entry["postgresql_sql"] = pg_sql
            try:
                expected = rows(pg(pg_sql + ";"))
                entry["postgresql_rows"] = expected
            except Exception as error:
                entry["error"] = str(error)
                evidence["errors"].append(case["name"] + ": PostgreSQL reference failed: " + str(error))
                print("FAIL {}: PostgreSQL reference failed".format(case["name"]), flush=True)
                continue
            for mode, agg, topn in MODES:
                sql = case["sql"].format(table=sr_table)
                result = {"name": mode, "aggregate_enabled": agg, "topn_enabled": topn,
                          "starrocks_sql": sql, "errors": [], "passed": False}
                entry["modes"].append(result)
                try:
                    actual = rows(sr(sql, agg, topn))
                    result["starrocks_rows"] = actual
                    if comparable(actual) != comparable(expected):
                        result["errors"].append("Ordered result differs from PostgreSQL reference")
                    plan = sr("EXPLAIN VERBOSE " + sql, agg, topn)
                    result["plan"] = plan
                    result["jdbc_queries"], plan_errors = assert_plan(case, plan, agg, topn)
                    result["errors"].extend(plan_errors)
                except Exception as error:
                    result["errors"].append(str(error))
                result["passed"] = not result["errors"]
                print("{} {} [{}]".format("PASS" if result["passed"] else "FAIL", case["name"], mode),
                      flush=True)
    except Exception as error:
        evidence["errors"].append(str(error))
    except KeyboardInterrupt:
        evidence["errors"].append("Interrupted by operator")
    finally:
        if created:
            try:
                pg("DROP SCHEMA {} CASCADE;".format(pg_schema))
            except Exception as error:
                # Preserve original failures; cleanup failure is a separate failure.
                evidence["cleanup_error"] = str(error)
        mode_results = [mode for case in evidence["cases"] for mode in case["modes"]]
        passed = sum(result["passed"] for result in mode_results)
        evidence["summary"] = {"expected_executions": len(cases) * len(MODES),
                               "completed_executions": len(mode_results), "passed": passed,
                               "failed": len(mode_results) - passed}
        evidence["passed"] = (passed == len(cases) * len(MODES)
                              and not evidence["errors"] and not evidence["cleanup_error"])
        evidence["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print("{}: {}/{} executions passed; evidence: {}".format(
            "PASS" if evidence["passed"] else "FAIL", passed, len(cases) * len(MODES), args.output), flush=True)
        for error in evidence["errors"]:
            print(error, file=sys.stderr)
        if evidence["cleanup_error"]:
            print("Cleanup failed for schema {}: {}".format(schema, evidence["cleanup_error"]), file=sys.stderr)
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
