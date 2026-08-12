#!/usr/bin/env python3
"""Verify exact standalone and bundle tablet-metadata diagnostic reads."""

import base64
import csv
import io
import json
import shlex
import subprocess
import sys
import urllib.error
import urllib.request


def query_rows(mysql_cmd, sql):
    result = subprocess.run(
        shlex.split(mysql_cmd) + ["--batch", "--raw", "-e", sql],
        check=True,
        capture_output=True,
        text=True,
    )
    return list(csv.DictReader(io.StringIO(result.stdout), delimiter="\t"))


def first_value(rows, field, description):
    if not rows or not rows[0].get(field):
        raise RuntimeError("missing {} in {}".format(field, description))
    return rows[0][field]


def find_compute_node(mysql_cmd):
    for row in query_rows(mysql_cmd, "SHOW COMPUTE NODES"):
        alive = row.get("Alive", "").lower()
        if alive in ("true", "1"):
            host = row.get("IP") or row.get("Host")
            port = row.get("HttpPort")
            if host and port:
                return host, port
    raise RuntimeError("no alive compute node with HTTP endpoint")


def table_info(mysql_cmd, database, table):
    tablet = first_value(
        query_rows(mysql_cmd, "SHOW TABLETS FROM {}.{} LIMIT 1".format(database, table)),
        "TabletId",
        "SHOW TABLETS",
    )
    version = first_value(
        query_rows(mysql_cmd, "SHOW PARTITIONS FROM {}.{}".format(database, table)),
        "VisibleVersion",
        "SHOW PARTITIONS",
    )
    if int(version) < 2:
        raise RuntimeError("visible version is below 2")
    return int(tablet), int(version)


def has_encryption_meta(value):
    if isinstance(value, dict):
        return any("encryption_meta" in key or has_encryption_meta(item) for key, item in value.items())
    if isinstance(value, list):
        return any(has_encryption_meta(item) for item in value)
    return False


def request(url):
    credential = base64.b64encode(b"root:").decode("ascii")
    http_request = urllib.request.Request(url, headers={"Authorization": "Basic " + credential})
    try:
        with urllib.request.urlopen(http_request, timeout=30) as response:
            return response.status, response.headers, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.headers, error.read()


def verify_exact(base_url, tablet_id, version, is_bundle):
    status, headers, body = request(
        "{}/api/cloudnative/dump_tablet_metadata/{}?version={}&is_bundle={}".format(
            base_url, tablet_id, version, str(is_bundle).lower()
        )
    )
    if status != 200:
        raise RuntimeError("exact metadata read failed")
    if headers.get("Content-Type") != "application/json":
        raise RuntimeError("unexpected content type")
    if headers.get("Cache-Control") != "no-store":
        raise RuntimeError("unexpected cache control")
    if headers.get("X-Content-Type-Options") != "nosniff":
        raise RuntimeError("unexpected content type options")
    document = json.loads(body)
    if set(document) != {"metadata"}:
        raise RuntimeError("unexpected metadata envelope")
    metadata = document["metadata"]
    if metadata.get("id") != tablet_id or metadata.get("version") != version:
        raise RuntimeError("metadata id or version mismatch")
    if has_encryption_meta(document):
        raise RuntimeError("metadata response contains encryption material")


def require_status(url, expected):
    status, _, _ = request(url)
    if status != expected:
        raise RuntimeError("unexpected diagnostic status")
    return status


def main():
    mysql_cmd, database, standalone_table, bundled_table = sys.argv[1:5]
    host, port = find_compute_node(mysql_cmd)
    base_url = "http://{}:{}".format(host, port)
    standalone_id, standalone_version = table_info(mysql_cmd, database, standalone_table)
    bundled_id, bundled_version = table_info(mysql_cmd, database, bundled_table)

    verify_exact(base_url, standalone_id, standalone_version, False)
    verify_exact(base_url, bundled_id, bundled_version, True)
    wrong_format = require_status(
        "{}/api/cloudnative/dump_tablet_metadata/{}?version={}&is_bundle=true".format(
            base_url, standalone_id, standalone_version
        ),
        404,
    )
    unknown_parameter = require_status(
        "{}/api/cloudnative/dump_tablet_metadata/{}?version={}&is_bundle=false&pretty=true".format(
            base_url, standalone_id, standalone_version
        ),
        400,
    )
    missing_version = require_status(
        "{}/api/cloudnative/dump_tablet_metadata/{}?is_bundle=false".format(base_url, standalone_id),
        400,
    )

    print("standalone_exact=PASS")
    print("bundle_exact=PASS")
    print("wrong_format_status={}".format(wrong_format))
    print("unknown_parameter_status={}".format(unknown_parameter))
    print("missing_version_status={}".format(missing_version))


if __name__ == "__main__":
    main()
