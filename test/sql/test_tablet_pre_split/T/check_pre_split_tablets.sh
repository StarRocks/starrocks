#!/usr/bin/env bash
# Prints whether ${table} was widened past one tablet and how many of its tablets hold no rows.
#
# Pre-split cuts a range-distributed table at sampled sort-key values, so when the sampler read the
# right column every cut lies inside the loaded key range and every tablet gets rows. A cut planned
# from the wrong column falls outside that range, and the tablet past it is left empty -- which is
# the only visible trace of a wrong binding, since the load itself still succeeds.

tablet_ids=$(${mysql_cmd} -D"${database}" -e "SHOW TABLETS FROM ${table}" | awk -F '\t' '
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
    { print $column }')

# SHOW TABLETS lists every layout the partition still retains, including the single parent tablet a
# split retires but keeps until vacuum reclaims it. A query only reaches the current layout and
# rejects a retired tablet as dropped, so those are skipped rather than counted as empty.
tablets=0
empty=0
for tablet_id in ${tablet_ids}; do
    if ! rows=$(${mysql_cmd} -D"${database}" -Ne "SELECT count(*) FROM ${table} TABLET(${tablet_id})" 2>&1); then
        case "${rows}" in
            *"The tablet may have been dropped"*) continue ;;
            *) echo "${rows}" >&2; exit 1 ;;
        esac
    fi
    tablets=$((tablets + 1))
    [ "${rows}" -gt 0 ] || empty=$((empty + 1))
done

echo "widened=$([ "${tablets}" -gt 1 ] && echo 1 || echo 0) empty_tablets=${empty}"
