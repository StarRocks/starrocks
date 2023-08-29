#!/bin/bash

execute_and_check_details() {
    local db=$1
    local sql=$2

    # The whole sql-test are executed concurrently, and the maximum of query details is 300.
    # So it may be flushed out by other sql-test, so we need to retry.
    for((i=0;i<30;i++)) 
    do
        ${mysql_cmd} -e "${sql}" > /dev/null 2>&1
        query_details=$(curl -s ${url}/api/query_detail\?event_time=0)

        while IFS= read -r query_detail; do
            local cur_database=$(echo "$query_detail" | jq -r '.database')
            local cur_sql=$(echo "$query_detail" | jq -r '.sql')
            local cur_state=$(echo "$query_detail" | jq -r '.state')
            local cur_cpuCostNs=$(echo "$query_detail" | jq -r '.cpuCostNs')

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
                return
            fi
        done < <(echo "${query_details}" | jq -c '.[]')
    done
    
    echo "failed to get positive cpuCostNs for '${sql}'"
}

# backup the original value
original_value=$(${mysql_cmd} -e "ADMIN SHOW FRONTEND CONFIG LIKE 'enable_collect_query_detail_info'\G" | grep 'Value:' | awk -F ': ' '{print $2}')

if [ "${original_value}" == "true" ] || [ "${original_value}" == "false" ]; then
    echo "get enable_collect_query_detail_info value successfully"
else
    echo "failed to get enable_collect_query_detail_info value"
    exit 1
fi

# enable query details
${mysql_cmd} -e "ADMIN SET FRONTEND CONFIG ('enable_collect_query_detail_info' = 'true');"

# test select
execute_and_check_details ${db} "SELECT COUNT(*) FROM t_audit_log"

# test insert into
execute_and_check_details ${db} "EXPLAIN ANALYZE INSERT INTO t_audit_log SELECT * FROM t_audit_log"

# reset query details
${mysql_cmd} -e "ADMIN SET FRONTEND CONFIG ('enable_collect_query_detail_info' = '${original_value}');"
