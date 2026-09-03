#!/usr/bin/env bash

check_table() {
    local table=$1
    local tablet_id version endpoints endpoint attempt

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
    [ -n "${tablet_id}" ] && [ -n "${version}" ] || return 1

    for attempt in $(seq 1 10); do
        ${mysql_cmd} -D"${database}" -Ne "SELECT COUNT(*) FROM ${table}" >/dev/null || return 1
        # A shared-data cluster serves queries from nodes registered either as backends or as
        # compute nodes, and each statement lists only its own kind, so probe the union of both.
        # Their first nine columns agree (id, IP, HeartbeatPort, BePort, HttpPort, BrpcPort,
        # LastStartTime, LastHeartbeat, Alive), which lets one awk program read either output.
        endpoints=$( { ${mysql_cmd} -Ne "SHOW BACKENDS"; ${mysql_cmd} -Ne "SHOW COMPUTE NODES"; } |
            awk -F '\t' '$9 == "true" { print $2 ":" $5 }')
        for endpoint in ${endpoints}; do
            curl -fsS --connect-timeout 1 --max-time 3 -u root: "http://${endpoint}/api/cloudnative/dump_tablet_metadata/${tablet_id}?version=${version}" |
                jq -e '.status == "OK"' >/dev/null && return 0
        done
        [ "${attempt}" -eq 10 ] || sleep 1
    done
    return 1
}

check_table standalone_meta && echo non_bundled=PASS &&
    check_table bundled_meta && echo bundled=PASS
