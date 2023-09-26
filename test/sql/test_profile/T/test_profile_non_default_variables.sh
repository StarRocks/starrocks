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
with customer_total_return as
(select sr_customer_sk as ctr_customer_sk
,sr_store_sk as ctr_store_sk
,sum(SR_RETURN_AMT) as ctr_total_return
from store_returns
,date_dim
where sr_returned_date_sk = d_date_sk
and d_year =2000
group by sr_customer_sk
,sr_store_sk)
 select  c_customer_id
from customer_total_return ctr1
,store
,customer
where ctr1.ctr_total_return > (select avg(ctr_total_return)*1.2
from customer_total_return ctr2
where ctr1.ctr_store_sk = ctr2.ctr_store_sk)
and s_store_sk = ctr1.ctr_store_sk
and s_state = 'TN'
and ctr1.ctr_customer_sk = c_customer_sk
order by c_customer_id
limit 100;
EOF
)

test_non_default_variables "set pipeline_dop=2; ${sql}"

sql="explain analyze insert into reason select * from reason;"

test_non_default_variables "set pipeline_dop=2; ${sql}"
