#!/usr/bin/env bash

# Shared-data CI workers may appear in SHOW BACKENDS, SHOW COMPUTE NODES, or both.
# Match test/lib/sr_sql_lib.py::_get_backend_http_endpoints and identify columns by
# header name so added SHOW columns do not shift IP/HttpPort/Alive.
collect_endpoints() {
    {
        ${mysql_cmd} -e "SHOW BACKENDS"
        ${mysql_cmd} -e "SHOW COMPUTE NODES"
    } | awk -F '\t' '
        $1 == "BackendId" || $1 == "ComputeNodeId" {
            ip = http = alive = 0
            for (i = 1; i <= NF; i++) {
                if ($i == "IP") ip = i
                if ($i == "HttpPort") http = i
                if ($i == "Alive") alive = i
            }
            next
        }
        ip && http && alive && $alive == "true" && $ip != "" && $http != "" {
            print $ip ":" $http
        }
    ' | sort -u
}

check_table() {
    local table=$1
    local tablet_id version endpoints endpoint attempt body

    tablet_id=$(${mysql_cmd} -D"${database}" -e "SHOW TABLETS FROM ${table} LIMIT 1" | awk -F '\t' '
        NR == 1 {
            for (i = 1; i <= NF; i++) {
                if ($i == "TabletId") column = i
            }
            if (column == 0) {
                print "TabletId column not found" > "/dev/stderr"
                exit 1
            }
            next
        }
        NR == 2 {
            print $column
            exit
        }
    ')
    version=$(${mysql_cmd} -Ne "SELECT VISIBLE_VERSION FROM information_schema.partitions_meta WHERE DB_NAME='${database}' AND TABLE_NAME='${table}' LIMIT 1")
    tablet_id=${tablet_id//$'\r'/}
    version=${version//$'\r'/}
    if [ -z "${tablet_id}" ] || [ -z "${version}" ]; then
        echo "failed to resolve tablet metadata identity: table=${table} tablet_id='${tablet_id}' version='${version}'" >&2
        return 1
    fi

    for attempt in $(seq 1 10); do
        # A real predicate scan loads lake tablet metadata into the CN metacache.
        # COUNT(*) can be rewritten to a meta-scan/FE stats path and may not cache
        # the versioned key this API looks up. Parallel CI can also evict the cache
        # left behind by INSERT, so refresh it immediately before the dump.
        ${mysql_cmd} -D"${database}" -Ne "SELECT k FROM ${table} WHERE k = 1" >/dev/null || return 1
        endpoints=$(collect_endpoints)
        for endpoint in ${endpoints}; do
            body=$(curl -sS --connect-timeout 2 --max-time 10 -u root: \
                "http://${endpoint}/api/cloudnative/dump_tablet_metadata/${tablet_id}?version=${version}" || true)
            echo "${body}" | jq -e '.status == "OK"' >/dev/null && return 0
        done
        [ "${attempt}" -eq 10 ] || sleep 1
    done

    echo "dump_tablet_metadata cache miss: table=${table} tablet_id=${tablet_id} version=${version} endpoints=${endpoints:-<none>}" >&2
    for endpoint in ${endpoints}; do
        echo "response from ${endpoint}:" >&2
        curl -sS --connect-timeout 2 --max-time 10 -u root: \
            "http://${endpoint}/api/cloudnative/dump_tablet_metadata/${tablet_id}?version=${version}" >&2 || true
        echo >&2
    done
    return 1
}

check_table standalone_meta && echo non_bundled=PASS &&
    check_table bundled_meta && echo bundled=PASS
