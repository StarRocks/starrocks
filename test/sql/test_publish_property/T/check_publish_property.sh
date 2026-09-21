#!/usr/bin/env bash

# Reads back, from a node that publishes them, what the named tables are running with. SHOW CREATE
# TABLE says only what the frontend stored; the values reach a node with a publish request and live
# in that node's memory, so this endpoint is the only place the two can be compared.
#
# Usage: env mysql_cmd=... database=... bash check_publish_property.sh <table> [<table>...]

alive_endpoints() {
    # A shared-data cluster serves a tablet from a node registered either as a backend or as a
    # compute node, and each statement lists only its own kind, so probe the union of both. Their
    # first nine columns agree (id, IP, HeartbeatPort, BePort, HttpPort, BrpcPort, LastStartTime,
    # LastHeartbeat, Alive), which lets one awk program read either output.
    { ${mysql_cmd} -Ne "SHOW BACKENDS"; ${mysql_cmd} -Ne "SHOW COMPUTE NODES"; } |
        awk -F '\t' '$9 == "true" { print $2 ":" $5 }'
}

tablets_of() {
    ${mysql_cmd} -D"${database}" -e "SHOW TABLETS FROM $1" | awk -F '\t' '
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
        { print $column }
    '
}

# An index is held only while it is in the cache, and it is put there by a publish. Re-inserting a
# primary key table's own rows upserts every one of them to the value it already has, which leaves
# the data alone and publishes a version.
warm_up() {
    ${mysql_cmd} -D"${database}" -e "INSERT INTO $1 SELECT * FROM $1" >/dev/null 2>&1
}

ask() {
    curl -fsS --connect-timeout 1 --max-time 3 -u root: "http://$1/api/publish_property?$2"
}

# What one tablet holds, or nothing if no node has an index for it.
one_tablet() {
    local tablet_id=$1 endpoint body
    for endpoint in ${endpoints}; do
        body=$(ask "${endpoint}" "type=pk&tablet_id=${tablet_id}") || continue
        if [ "$(jq -r '.status' <<<"${body}")" = "OK" ]; then
            jq -c '{revision, properties}' <<<"${body}"
            return
        fi
    done
}

# One line per table: the revision its tablets are on and the properties they hold. Every tablet of a
# table is published with the same set, so a table whose tablets disagree is a bug worth seeing --
# hence reading all of them and folding identical answers together rather than trusting the first.
#
# Retried, and warmed up again each round, because neither half of the read is guaranteed on the
# first try: a publish may not have finished by the time the load statement returns, and an index
# that was cached a moment ago may have been evicted since. Both are answered by publishing again.
report() {
    local table=$1 tablets expected tablet_id answers attempt
    tablets=$(tablets_of "${table}")
    expected=$(printf '%s\n' "${tablets}" | grep -c .)
    if [ "${expected}" -eq 0 ]; then
        echo "${table}=NO_TABLET"
        return
    fi

    for attempt in $(seq 1 10); do
        warm_up "${table}"
        answers=""
        for tablet_id in ${tablets}; do
            answers="${answers}$(one_tablet "${tablet_id}")
"
        done
        # Every tablet has to have answered; a partial reading would hide the one that did not.
        if [ "$(printf '%s' "${answers}" | grep -c .)" -eq "${expected}" ]; then
            echo "${table}=$(printf '%s' "${answers}" | sort -u | paste -sd '|' -)"
            return
        fi
        sleep 1
    done

    # Deliberately loud rather than a bare marker: name the tablets, so a failure says which one has
    # no index rather than only that the table could not be read.
    echo "${table}=UNAVAILABLE tablets=$(printf '%s' "${tablets}" | paste -sd ',' -)"
}

endpoints=$(alive_endpoints)
for table in "$@"; do
    report "${table}"
done
