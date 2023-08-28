#!/bin/bash

original_value=$(${mysql_cmd} -e "ADMIN SHOW FRONTEND CONFIG LIKE 'enable_collect_query_detail_info'\G" | grep 'Value:' | awk -F ': ' '{print $2}')

if [ "${original_value}" == "true" ] || [ "${original_value}" == "false" ]; then
    echo "get enable_collect_query_detail_info value successfully"
else
    echo "failed to get enable_collect_query_detail_info value"
    exit 1
fi

check_details() {
    local content=$1
    local db=$2
    local sql=$3

    echo "${content}" | jq -c '.[]' | while IFS= read -r query_detail; do
        cur_database=$(echo "$query_detail" | jq -r '.database')
        cur_sql=$(echo "$query_detail" | jq -r '.sql')
        cur_state=$(echo "$query_detail" | jq -r '.state')
        cur_cpuCostNs=$(echo "$query_detail" | jq -r '.cpuCostNs')

        if [ "${cur_database}" != "${db}" ]; then
            continue
        fi

        if [ "${cur_sql}" != "${sql}" ]; then
            continue
        fi

        if [ "${cur_state}" != "FINISHED" ]; then
            continue
        fi

        if [ ${cur_cpuCostNs} -gt 0 ]; then
            echo "get positive cpuCostNs successfully for '${sql}'"
        else
            echo "failed to get positive cpuCostNs for '${sql}'"
        fi
    done
}

# enable query details
${mysql_cmd} -e "ADMIN SET FRONTEND CONFIG ('enable_collect_query_detail_info' = 'true');"

# test select
${mysql_cmd} -e "SELECT COUNT(*) FROM t_audit_log;" > /dev/null 2>&1
query_details=$(curl -s ${url}/api/query_detail\?event_time=0)
check_details "${query_details}" ${db} "SELECT COUNT(*) FROM t_audit_log"

# test insert into
${mysql_cmd} -e "INSERT INTO t_audit_log SELECT * FROM t_audit_log;" > /dev/null 2>&1
query_details=$(curl -s ${url}/api/query_detail\?event_time=0)
check_details "${query_details}" ${db} "INSERT INTO t_audit_log SELECT * FROM t_audit_log"

# reset query details
${mysql_cmd} -e "ADMIN SET FRONTEND CONFIG ('enable_collect_query_detail_info' = '${original_value}');"
