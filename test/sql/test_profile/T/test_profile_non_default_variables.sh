#!/bin/bash

function test_non_default_variables() {
    analyze_output=$(${mysql_cmd} -e "$1")
    if grep -q "pipeline_dop: 0 -> 2" <<< "${analyze_output}"; then
        echo "NonDefaultSessionVariables contains 'pipeline_dop'"
    else
        echo "NonDefaultSessionVariables not contains 'pipeline_dop'"
        exit 1
    fi
}

sql=$(cat << EOF
explain analyze
select count(*) from t0;
EOF
)

test_non_default_variables "set pipeline_dop=2; ${sql}"

sql=$(cat << EOF
explain analyze
insert into t0 select * from t0;
EOF
)

test_non_default_variables "set pipeline_dop=2; ${sql}"
