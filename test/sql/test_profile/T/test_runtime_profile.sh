#!/bin/bash

${mysql_cmd} -e '
set enable_profile=true;
set runtime_profile_report_interval=2;
SELECT SUM(wv), AVG(wv) FROM (
    SELECT max(c1) OVER (ORDER BY c2 ROWS BETWEEN UNBOUNDED PRECEDING AND 2 PRECEDING) AS wv FROM t_runtime_profile
) t
' &

query_pid=$!
trap "kill -9 ${query_pid};" SIGINT SIGTERM EXIT

sleep 10

query_id=$(${mysql_cmd} -e 'show profilelist limit 5' | grep 'Running' | awk '{print $1}' | head -n 1)
analyze_output=$(${mysql_cmd} -e "analyze profile from '${query_id}'")

if grep -q 'Summary' <<< "${analyze_output}"; then
    echo "Analyze runtime profile succeeded"
else
    echo "Analyze runtime profile failed"
    exit 1
fi
