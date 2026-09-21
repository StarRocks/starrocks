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
import time
import uuid


HERE = Path(__file__).resolve().parent
MODES = (("local", False, False), ("aggregate", True, False),
         ("topn", False, True), ("both", True, True))
# A runtime filter case is compared against PostgreSQL in both switch positions instead: the
# filter is a pure optimization, so the two have to agree with each other and with PostgreSQL.
# The agg/TopN switches a runtime filter case needs are fixed per case, in its "session" block,
# because the shape it is testing (a filter landing outside an already pushed GROUP BY, say)
# only exists in one of the four combinations.
RF_MODES = (("rf_off", False), ("rf_on", True))
# StarRocks-only join hint. A runtime filter is an in-filter built by a broadcast hash join, so
# every runtime filter case has to ask for one; PostgreSQL gets the same text without the hint.
BROADCAST = "[broadcast] "
# The scan waits this long for a filter to arrive before starting to read. The default is 20ms,
# which is not enough for a JDBC scan to reliably see one: without this the ON executions would
# pass or fail depending on how fast the build side finished.
RF_SCAN_WAIT_MS = 3000
QUERY_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z")
NUMBER = re.compile(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z")
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?\Z")


def identifier(value, quote):
    return quote + value.replace(quote, quote + quote) + quote


SESSION_VARIABLE = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def setting(name, value):
    """Renders one per-case ``SET`` statement, refusing anything but a plain variable name."""
    if not SESSION_VARIABLE.fullmatch(name):
        raise RuntimeError("Not a session variable name: {!r}".format(name))
    if isinstance(value, bool):
        literal = str(value).lower()
    elif isinstance(value, int) or isinstance(value, float):
        literal = repr(value)
    else:
        literal = "'" + str(value).replace("'", "''") + "'"
    return "SET {} = {};".format(name, literal)


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
    # An aggregate sitting above a subquery's own LIMIT can only be pushed once that LIMIT has
    # itself been pushed into the scan, so such a case declares that its aggregate depends on the
    # TopN switch as well; the reverse dependency already exists as topn_requires_aggregate.
    aggregate_expected = bool(case.get("aggregate")) and aggregate_enabled and (
        not case.get("aggregate_requires_topn") or topn_enabled)
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
        offsets = [int(offset) for offset in re.findall(r"\bOFFSET\s+(\d+)\b", remote, re.IGNORECASE)]
        expected_offset = case.get("remote_offset")
        if expected_offset is None:
            if offsets:
                errors.append("Unexpected remote OFFSET: {}".format(offsets))
        elif offsets != [expected_offset]:
            errors.append("Expected one remote OFFSET {}, found {}".format(expected_offset, offsets))
        # The remote ORDER BY replaces the local TopN: leaving one behind would mean the rows
        # still needed reordering, which is exactly what the pushdown is supposed to avoid. A query
        # written with a second TopN the rule cannot take -- one above a local aggregate, say --
        # says so, and then that one is required to still be there rather than merely tolerated.
        if case.get("local_topn_retained"):
            if "TOP-N" not in plan:
                errors.append("Expected the unpushable local TOP-N to remain")
        elif "TOP-N" in plan:
            errors.append("Unexpected local TOP-N alongside a pushed ORDER BY")
    # A pushed string comparison has to carry COLLATE "C" so PostgreSQL evaluates it by bytes, the
    # way StarRocks does; a non-string one must not carry one. A sort key gets it from the pushed
    # ORDER BY and a MIN/MAX argument from the pushed aggregate, so each is counted against the
    # switch that put it there.
    collate_expected = (case.get("remote_collate", 0) if topn_expected else 0) + (
        case.get("aggregate_collate", 0) if aggregate_expected else 0)
    collate_actual = len(re.findall(r'COLLATE\s+"C"', remote, re.IGNORECASE))
    if collate_actual != collate_expected:
        errors.append('Expected {} remote COLLATE "C", found {}'.format(collate_expected, collate_actual))
    if not topn_expected:
        # A runtime filter case may push an ordinary row limit into the scan, which is not a TopN
        # pushdown and carries no ORDER BY. It is declared per case so an unexpected limit is still
        # a failure; cases without the key keep rejecting every remote LIMIT.
        allowed = case.get("remote_limit")
        unexpected = [limit for limit in limits if limit != allowed]
        if unexpected:
            errors.append("Unexpected remote LIMIT during TopN fallback: {}".format(unexpected))
        if allowed and not limits:
            errors.append("Expected remote LIMIT {}, found none".format(allowed))
        if re.search(r"\bOFFSET\b", remote, re.IGNORECASE):
            errors.append("Unexpected remote OFFSET during TopN fallback")
    # Direct assertions on the remote statement itself. Rows alone cannot tell a subscript that
    # PostgreSQL evaluated from one StarRocks evaluated locally, nor say which columns were asked
    # for; both are visible here and nowhere else. Substrings are matched case-sensitively against
    # the QUERY line, quoting included, so an assertion cannot pass on a differently quoted name.
    for fragment in case.get("remote_sql_contains", []):
        if fragment not in remote:
            errors.append("Expected remote SQL to contain {!r}".format(fragment))
    for fragment in case.get("remote_sql_excludes", []):
        if fragment in remote:
            errors.append("Expected remote SQL not to contain {!r}".format(fragment))
    return queries, errors


IN_LIST = re.compile(r" IN \(")


def in_list_values(text, start):
    """Splits the rendered ``IN`` list that begins at ``start`` into its values.

    Returns ``(values, index_of_closing_bracket)``, or ``(None, None)`` when the list does not
    terminate. A value is a SQL literal that may hold a comma or a bracket, so the scan tracks
    quoting; ``''`` inside a literal is an escaped quote, which this sees as a close immediately
    followed by an open and which therefore needs no case of its own.
    """
    values = []
    current = []
    quoted = False
    index = start
    while index < len(text):
        char = text[index]
        if char == "'":
            quoted = not quoted
        elif not quoted and char == ")":
            values.append("".join(current))
            return values, index
        elif not quoted and char == ",":
            values.append("".join(current))
            current = []
            index += 1
            continue
        current.append(char)
        index += 1
    return None, None


def canonical_in_lists(sql):
    """Sorts the values inside every rendered ``IN (...)`` list of ``sql``.

    A runtime filter's values are rendered into the statement in the iteration order of the BE's
    hash set, which nothing defines and nothing keeps stable across executions. Sorting the
    expected fragment and the finished statement the same way keeps the assertion on the exact
    literal text -- quoting and escaping included, which is the half the remote engine's behaviour
    turns on -- without pinning an order that would make the case flaky.
    """
    out = []
    position = 0
    while True:
        match = IN_LIST.search(sql, position)
        if not match:
            out.append(sql[position:])
            return "".join(out)
        values, end = in_list_values(sql, match.end())
        if values is None:
            out.append(sql[position:])
            return "".join(out)
        out.append(sql[position:match.end()])
        out.append(",".join(sorted(values)))
        position = end


def profile_fields(profile, name):
    return [value.strip() for value in
            re.findall(r"^\s*-\s*" + name + r":\s*(.*?)\s*$", profile, re.MULTILINE)]


def profile_counter(profile, name, errors):
    values = profile_fields(profile, name)
    if not values:
        errors.append("Profile has no {} counter".format(name))
        return None
    if len(set(values)) != 1:
        errors.append("Profile reports conflicting {}: {}".format(name, values))
        return None
    try:
        return int(values[0])
    except ValueError:
        errors.append("Profile {} is not a plain count: {}".format(name, values[0]))
        return None


def profile_info(profile, name, expected, errors):
    values = profile_fields(profile, name)
    if not values:
        errors.append("Profile has no {} entry".format(name))
    elif any(value != expected for value in values):
        errors.append("Expected {} {!r}, found {!r}".format(name, expected, values))


def assert_runtime_filter(spec, plan, profile, rf_enabled):
    """Check what the remote SQL actually carried, not merely what the plan allowed.

    The FE half comes from EXPLAIN VERBOSE, which says whether the FE authorized the push down at
    all. The BE half comes from the scan's own profile, which records the finished remote SQL plus
    how many filters and values went into it -- the only place the runtime shape is visible, since
    a plan is printed before any filter exists. Reading it here rather than PostgreSQL's statement
    log keeps the assertion independent of the remote server's logging configuration.
    """
    errors = []
    allowed = re.findall(r"^\s*RUNTIME FILTER PUSH DOWN: allowed on (\d+) column\(s\)$",
                         plan, re.MULTILINE)
    expected_allowed = spec.get("fe_allowed_columns") if rf_enabled else None
    if expected_allowed is None:
        if allowed:
            errors.append("Expected no FE runtime filter authorization, found {}".format(allowed))
    elif allowed != [str(expected_allowed)]:
        errors.append("Expected FE to authorize {} column(s), found {}".format(
            expected_allowed, allowed or "none"))

    expected_filters = spec.get("filters", 0) if rf_enabled else 0
    expected_values = spec.get("values", 0) if rf_enabled else 0
    filters = profile_counter(profile, "PushdownRuntimeFilters", errors)
    values = profile_counter(profile, "PushdownRuntimeFilterValues", errors)
    if filters is not None and filters != expected_filters:
        errors.append("Expected PushdownRuntimeFilters {}, found {}".format(expected_filters, filters))
    if values is not None and values != expected_values:
        errors.append("Expected PushdownRuntimeFilterValues {}, found {}".format(expected_values, values))
    profile_info(profile, "PushdownRuntimeFilterColumns",
                 spec.get("columns", "none") if rf_enabled else "none", errors)
    profile_info(profile, "PushdownRuntimeFilterSkipped",
                 spec.get("skipped", "none") if rf_enabled else spec.get("off_skipped",
                                                                         "not_authorized_by_fe"),
                 errors)

    remote = profile_fields(profile, "Query")
    if len(remote) != 1:
        errors.append("Expected exactly one remote query in the profile, found {}".format(len(remote)))
    else:
        predicate = spec.get("remote_predicate") if rf_enabled else None
        if predicate and canonical_in_lists(predicate) not in canonical_in_lists(remote[0]):
            errors.append("Expected remote SQL to contain {!r}".format(predicate))
        # The values are rendered into the statement, so nothing is bound and no placeholder may
        # appear whether or not a filter was pushed. A `?` that reaches the driver is a parameter
        # the bridge has no value for, which fails the whole statement.
        if "?" in remote[0]:
            errors.append("Expected no placeholder in the remote SQL")
    return remote, errors


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

    def sr_settings(agg, topn, session=None):
        settings = ("SET time_zone = '+00:00'; SET query_timeout = {}; "
                    "SET enable_jdbc_project_push_down = true; "
                    "SET enable_jdbc_agg_push_down = {}; "
                    "SET enable_jdbc_topn_push_down = {};\n").format(
                        args.timeout, str(agg).lower(), str(topn).lower())
        # A case may pin further session variables, in every mode, after the five above. A case
        # without the field appends nothing, so what the older cases send is unchanged.
        for name in sorted(session or ()):
            settings += setting(name, session[name]) + "\n"
        return settings

    def sr(sql, agg, topn, session=None):
        return run_client(mysql, sr_settings(agg, topn, session) + sql + ";\n", args.timeout)

    def sr_profiled(sql, agg, topn, session):
        """Run one statement with profiling on and return its rows and its query id.

        Both have to come out of a single client invocation: session variables and
        last_query_id() do not survive a new connection, and picking the statement back out of
        SHOW PROFILELIST would race every other session on a shared cluster.
        """
        settings = sr_settings(agg, topn, session)
        settings += "SET enable_profile = true;\n"
        settings += "SET runtime_filter_scan_wait_time = {};\n".format(RF_SCAN_WAIT_MS)
        output = run_client(mysql, settings + sql + ";\nSELECT last_query_id();\n", args.timeout)
        lines = output.splitlines()
        if not lines or not QUERY_ID.fullmatch(lines[-1]):
            raise RuntimeError("Statement did not report a query id; last line: {!r}".format(
                lines[-1] if lines else ""))
        return [line.split("\t") for line in lines[:-1]], lines[-1]

    def sr_profile(query_id):
        # The BE reports its profile to the FE after the statement has already returned, so the
        # first read can legitimately come back empty.
        deadline = time.monotonic() + args.timeout
        profile = ""
        while True:
            profile = run_client(
                mysql, "SELECT get_query_profile('{}');\n".format(query_id), args.timeout)
            if "PushdownRuntimeFilters" in profile:
                return profile
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    "Profile for {} never reported a JDBC scan within {} seconds".format(
                        query_id, args.timeout))
            time.sleep(1)

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
            session = case.get("session")
            # A case whose subject is a refusal has no reference rows to compare: reading a
            # multidimensional PostgreSQL value raises in the JDBC bridge, and an error is not a
            # row. Such a case states the message it requires instead, and still asserts its plan.
            expect_error = case.get("expect_error")
            expected = None
            if expect_error is None:
                pg_sql = case.get("pg_sql", case["sql"]).format(table=pg_table, bc="")
                entry["postgresql_sql"] = pg_sql
                try:
                    expected = rows(pg(pg_sql + ";"))
                    entry["postgresql_rows"] = expected
                except Exception as error:
                    entry["error"] = str(error)
                    evidence["errors"].append(case["name"] + ": PostgreSQL reference failed: " + str(error))
                    print("FAIL {}: PostgreSQL reference failed".format(case["name"]), flush=True)
                    continue
            sql = case["sql"].format(table=sr_table, bc=BROADCAST)
            spec = case.get("runtime_filter")
            if spec is None:
                modes = [(mode, agg, topn, session, None) for mode, agg, topn in MODES]
            else:
                # A runtime filter case fixes the other two switches itself, because the shape it
                # exercises exists in only one of their combinations. Its own session block is
                # layered over whatever the case already pinned for every mode.
                rf_session = dict(session or {}, **spec.get("session", {}))
                agg = bool(rf_session.get("enable_jdbc_agg_push_down", False))
                topn = bool(rf_session.get("enable_jdbc_topn_push_down", False))
                modes = [(mode, agg, topn,
                          dict(rf_session, enable_jdbc_runtime_filter_push_down=rf), rf)
                         for mode, rf in RF_MODES]
            for mode, agg, topn, extra, rf in modes:
                result = {"name": mode, "aggregate_enabled": agg, "topn_enabled": topn,
                          "starrocks_sql": sql, "session": extra, "errors": [], "passed": False}
                entry["modes"].append(result)
                try:
                    if expect_error is None:
                        if spec is None:
                            actual = rows(sr(sql, agg, topn, extra))
                        else:
                            actual, query_id = sr_profiled(sql, agg, topn, extra)
                            result["query_id"] = query_id
                        result["starrocks_rows"] = actual
                        if comparable(actual) != comparable(expected):
                            result["errors"].append("Ordered result differs from PostgreSQL reference")
                    else:
                        try:
                            result["starrocks_rows"] = rows(sr(sql, agg, topn, extra))
                            result["errors"].append(
                                "Expected StarRocks to fail with {!r}".format(expect_error))
                        except RuntimeError as error:
                            result["starrocks_error"] = str(error)
                            if expect_error not in str(error):
                                result["errors"].append(
                                    "Expected a StarRocks failure containing {!r}".format(expect_error))
                    plan = None
                    try:
                        plan = sr("EXPLAIN VERBOSE " + sql, agg, topn, extra)
                    except RuntimeError as error:
                        # A column whose type the catalog cannot map is refused while the
                        # statement is analysed, so EXPLAIN refuses it too and there is no plan to
                        # assert. That is the same refusal the case is pinning, not a second
                        # failure -- but only when it carries the same message.
                        if expect_error is None or expect_error not in str(error):
                            raise
                        result["plan_error"] = str(error)
                    if plan is not None:
                        result["plan"] = plan
                        result["jdbc_queries"], plan_errors = assert_plan(case, plan, agg, topn)
                        result["errors"].extend(plan_errors)
                        # Only a case that ran to completion has a profile to read: an
                        # expect_error case never reaches sr_profiled and carries no query id.
                        if spec is not None and result.get("query_id"):
                            profile = sr_profile(result["query_id"])
                            result["profile"] = profile
                            remote, rf_errors = assert_runtime_filter(spec, plan, profile, rf)
                            result["remote_sql"] = remote
                            result["errors"].extend(rf_errors)
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
        expected_executions = sum(
            len(RF_MODES) if "runtime_filter" in case else len(MODES) for case in cases)
        evidence["summary"] = {"expected_executions": expected_executions,
                               "completed_executions": len(mode_results), "passed": passed,
                               "failed": len(mode_results) - passed}
        evidence["passed"] = (passed == expected_executions
                              and not evidence["errors"] and not evidence["cleanup_error"])
        evidence["finished_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print("{}: {}/{} executions passed; evidence: {}".format(
            "PASS" if evidence["passed"] else "FAIL", passed, expected_executions, args.output), flush=True)
        for error in evidence["errors"]:
            print(error, file=sys.stderr)
        if evidence["cleanup_error"]:
            print("Cleanup failed for schema {}: {}".format(schema, evidence["cleanup_error"]), file=sys.stderr)
    return 0 if evidence["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
