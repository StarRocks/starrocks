#!/usr/bin/env python3
"""Verify cache-local standalone and bundled tablet-metadata diagnostic reads."""

import base64
import configparser
import csv
import functools
import ipaddress
import io
import json
import os
import subprocess
import sys
import urllib.error
import urllib.parse
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


def find_alive_compute_nodes():
    nodes = []
    for row in query_rows("SHOW COMPUTE NODES"):
        alive = row.get("Alive", "").lower()
        if alive in ("true", "1"):
            host = row.get("IP") or row.get("Host")
            port = row.get("HttpPort")
            if not host or not port:
                raise RuntimeError("alive compute node is missing an HTTP endpoint")
            try:
                valid_port = 0 < int(port) <= 65535
            except ValueError:
                valid_port = False
            if not valid_port:
                raise RuntimeError("alive compute node has an invalid HTTP endpoint")
            nodes.append((host, port))
    if not nodes:
        raise RuntimeError("no alive compute node with HTTP endpoint")
    return nodes


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


def verify_metadata_envelope(headers, body, tablet_id, version):
    if headers.get("Content-Type") != "application/json":
        raise RuntimeError("unexpected content type")
    if headers.get("Cache-Control") != "no-store":
        raise RuntimeError("unexpected cache control")
    if headers.get("X-Content-Type-Options") != "nosniff":
        raise RuntimeError("unexpected content type options")
    try:
        document = json.loads(body)
    except (TypeError, ValueError):
        raise RuntimeError("metadata response was not valid JSON") from None
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
    if not isinstance(metadata, dict) or metadata.get("id") != tablet_id or metadata.get("version") != version:
        raise RuntimeError("metadata id or version mismatch")
    if has_encryption_meta(document):
        raise RuntimeError("metadata response contains encryption material")


def verify_not_cached_response(headers, body):
    if headers.get("Content-Type") != "application/json":
        raise RuntimeError("cache miss had unexpected content type")
    if headers.get("Cache-Control") != "no-store":
        raise RuntimeError("cache miss had unexpected cache control")
    if headers.get("X-Content-Type-Options") != "nosniff":
        raise RuntimeError("cache miss had unexpected content type options")
    try:
        document = json.loads(body)
    except (TypeError, ValueError):
        raise RuntimeError("cache miss response was not valid JSON") from None
    if set(document) != {"code", "message"} or document.get("code") != "METADATA_NOT_CACHED":
        raise RuntimeError("cache miss response had an unexpected error envelope")
    message = document.get("message")
    if not isinstance(message, str) or not all(
        fragment in message
        for fragment in ("current compute node", "in-memory metadata cache", "AWS CLI", "meta_tool")
    ):
        raise RuntimeError("cache miss response was not actionable")


def format_authority_host(host):
    address, separator, zone = host.partition("%")
    try:
        ipaddress.IPv6Address(address)
    except ValueError:
        return host
    if separator:
        return "[{}%25{}]".format(address, urllib.parse.quote(zone, safe=""))
    return "[{}]".format(address)


def endpoint_url(host, port, tablet_id, query):
    path = "http://{}:{}/api/cloudnative/dump_tablet_metadata/{}".format(
        format_authority_host(host), port, tablet_id
    )
    return path if not query else path + "?" + query


def verify_exact_fixture_on_all_nodes(nodes, tablet_id, version):
    cache_hits = 0
    for host, port in nodes:
        status, headers, body = request(endpoint_url(host, port, tablet_id, "version={}".format(version)), True)
        if status == 200:
            verify_metadata_envelope(headers, body, tablet_id, version)
            cache_hits += 1
        elif status == 404:
            verify_not_cached_response(headers, body)
        else:
            raise RuntimeError("exact metadata request returned status={}".format(status))
    if cache_hits == 0:
        raise RuntimeError("metadata was not cached on any alive compute node")


def verify_impossible_version_on_all_nodes(nodes, tablet_id):
    for host, port in nodes:
        status, headers, body = request(
            endpoint_url(host, port, tablet_id, "version=9223372036854775807"), True
        )
        if status != 404:
            raise RuntimeError("impossible-version request returned status={}".format(status))
        verify_not_cached_response(headers, body)


def require_status_on_all_nodes(nodes, tablet_id, query, expected):
    for host, port in nodes:
        status, _, _ = request(endpoint_url(host, port, tablet_id, query), False)
        if status != expected:
            raise RuntimeError("diagnostic request returned status={}".format(status))


def main():
    if len(sys.argv) != 4:
        print(
            "usage: verify_dump_tablet_metadata.py DATABASE STANDALONE_TABLE BUNDLED_TABLE",
            file=sys.stderr,
        )
        return 2

    database, standalone_table, bundled_table = sys.argv[1:]
    nodes = find_alive_compute_nodes()
    standalone_id, standalone_version = table_info(database, standalone_table)
    bundled_id, bundled_version = table_info(database, bundled_table)

    verify_exact_fixture_on_all_nodes(nodes, standalone_id, standalone_version)
    verify_exact_fixture_on_all_nodes(nodes, bundled_id, bundled_version)
    verify_impossible_version_on_all_nodes(nodes, standalone_id)
    require_status_on_all_nodes(nodes, standalone_id, "version={}&is_bundle=false".format(standalone_version), 400)
    require_status_on_all_nodes(nodes, standalone_id, "", 400)

    print("standalone_cache_local=PASS")
    print("bundle_cache_local=PASS")
    print("impossible_version_all_nodes=PASS")
    print("removed_is_bundle_all_nodes=PASS")
    print("missing_version_all_nodes=PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
