#!/usr/bin/env python3
"""Verify exact standalone and bundle tablet-metadata diagnostic reads."""

import base64
import configparser
import csv
import functools
import io
import json
import os
import subprocess
import sys
import urllib.error
import urllib.request


@functools.lru_cache(maxsize=1)
def read_cluster_config():
    config_path = os.environ.get("config_path")
    if not config_path:
        raise RuntimeError("SQL test config path is not set")

    parser = configparser.ConfigParser(interpolation=None)
    try:
        with open(config_path, encoding="utf-8") as config_file:
            parser.read_file(config_file)
    except (OSError, configparser.Error):
        raise RuntimeError("unable to read SQL test config") from None

    host = parser.get("cluster", "host", fallback="").strip()
    port = parser.get("cluster", "port", fallback="").strip()
    user = parser.get("cluster", "user", fallback="").strip()
    password = parser.get("cluster", "password", fallback="")
    try:
        valid_port = 0 < int(port) <= 65535
    except ValueError:
        valid_port = False
    if not host or not user or not valid_port:
        raise RuntimeError("invalid SQL test cluster config")
    return host, port, user, password


def mysql_argv():
    host, port, user, _ = read_cluster_config()
    argv = ["mysql", "--host={}".format(host), "--port={}".format(port), "--user={}".format(user)]
    if any(argument == "-p" or argument.startswith("--password") for argument in argv):
        raise RuntimeError("MySQL password option must not be passed in argv")
    return argv


def query_rows(sql):
    _, _, _, password = read_cluster_config()
    mysql_env = os.environ.copy()
    if password:
        mysql_env["MYSQL_PWD"] = password
    else:
        mysql_env.pop("MYSQL_PWD", None)
    result = subprocess.run(
        mysql_argv() + ["--batch", "--raw", "--execute", sql],
        check=True,
        capture_output=True,
        text=True,
        env=mysql_env,
    )
    return list(csv.DictReader(io.StringIO(result.stdout), delimiter="\t"))


def first_value(rows, field, description):
    if not rows or not rows[0].get(field):
        raise RuntimeError("missing {} in {}".format(field, description))
    return rows[0][field]


def find_compute_node():
    for row in query_rows("SHOW COMPUTE NODES"):
        alive = row.get("Alive", "").lower()
        if alive in ("true", "1"):
            host = row.get("IP") or row.get("Host")
            port = row.get("HttpPort")
            if host and port:
                return host, port
    raise RuntimeError("no alive compute node with HTTP endpoint")


def table_info(database, table):
    tablet = first_value(
        query_rows("SHOW TABLETS FROM {}.{} LIMIT 1".format(database, table)),
        "TabletId",
        "SHOW TABLETS",
    )
    version = first_value(
        query_rows("SHOW PARTITIONS FROM {}.{}".format(database, table)),
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


def request(url, read_body):
    credential = base64.b64encode(b"root:").decode("ascii")
    http_request = urllib.request.Request(url, headers={"Authorization": "Basic " + credential})
    try:
        with urllib.request.urlopen(http_request, timeout=30) as response:
            body = response.read() if read_body else None
            return response.status, response.headers, body
    except urllib.error.HTTPError as error:
        try:
            body = error.read() if read_body else None
            return error.code, error.headers, body
        finally:
            error.close()


def verify_exact(base_url, tablet_id, version, is_bundle):
    status, headers, body = request(
        "{}/api/cloudnative/dump_tablet_metadata/{}?version={}&is_bundle={}".format(
            base_url, tablet_id, version, str(is_bundle).lower()
        ),
        True,
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
    document_fields = set(document)
    if "metadata" not in document_fields or not document_fields.issubset({"metadata", "redacted_fields"}):
        raise RuntimeError("unexpected metadata envelope")
    if "redacted_fields" in document:
        redacted_fields = document["redacted_fields"]
        if (
            not isinstance(redacted_fields, list)
            or not redacted_fields
            or any(not isinstance(field, str) or "encryption_meta" not in field for field in redacted_fields)
        ):
            raise RuntimeError("unexpected redacted fields")
    metadata = document["metadata"]
    if metadata.get("id") != tablet_id or metadata.get("version") != version:
        raise RuntimeError("metadata id or version mismatch")
    if has_encryption_meta(document):
        raise RuntimeError("metadata response contains encryption material")


def require_status(url, expected):
    status, _, _ = request(url, False)
    if status != expected:
        raise RuntimeError("unexpected diagnostic status")
    return status


def main():
    if len(sys.argv) != 4:
        print(
            "usage: verify_dump_tablet_metadata.py DATABASE STANDALONE_TABLE BUNDLED_TABLE",
            file=sys.stderr,
        )
        return 2

    database, standalone_table, bundled_table = sys.argv[1:]
    host, port = find_compute_node()
    base_url = "http://{}:{}".format(host, port)
    standalone_id, standalone_version = table_info(database, standalone_table)
    bundled_id, bundled_version = table_info(database, bundled_table)

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
    return 0


if __name__ == "__main__":
    sys.exit(main())
