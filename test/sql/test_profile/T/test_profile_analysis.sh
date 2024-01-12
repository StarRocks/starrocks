#!/bin/bash

function test_explain_analyze() {
    analyze_output=$(${mysql_cmd} -e "$1")
    if grep -q "Summary" <<< "${analyze_output}"; then
        echo "Analyze profile succeeded"
    else
        echo "Analyze profile failed"
        exit 1
    fi
}

sql=$(cat << EOF
explain analyze
select count(*) from t0;
EOF
)

test_explain_analyze "${sql}"
test_explain_analyze "set enable_runtime_adaptive_dop = true; ${sql}"
test_explain_analyze "set enable_spill = true; set spill_mode = 'force'; ${sql}"

sql=$(cat << EOF
explain analyze
insert into t0 select * from t0;
EOF
)

test_explain_analyze "${sql}"
test_explain_analyze "set enable_runtime_adaptive_dop = true; ${sql}"
test_explain_analyze "set enable_spill = true; set spill_mode = 'force'; ${sql}"
