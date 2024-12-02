#!/bin/bash

# It's used to generate sqls for benchmarking TABLE SAMPLE functionality

# Define mysql connection parameters
mysql_user=${MYSQL_USER:-"root"}
mysql_host=${MYSQL_HOST:-"127.0.0.1"}
mysql_port=${MYSQL_PORT:-"9030"}
mysql_database=tpcds_100g

query_columns="select table_name, column_name from information_schema.columns where table_schema='tpcds_100g' \
  and table_name in ('catalog_sales', 'store_returns', 'store_sales', 'web_sales') "


percents=(
1
4
16
32
)

# select count(lo_orderkey), count(distinct lo_orderkey) from lineorder
# --------------
# +--------------------+-----------------------------+
# | count(lo_orderkey) | count(DISTINCT lo_orderkey) |
# +--------------------+-----------------------------+
# | 600037902          | 150000000                   |
# +--------------------+-----------------------------+
# 1 row in set (2.06 sec)
# Bye"

execute_sql() {
  local variables=$1
  local query="$2"

  result=$(mysql -u ${mysql_user} -P ${mysql_port} -h ${mysql_host} ${mysql_database} -vvv -e "$query" 2>&1)
  if [ $? -ne 0 ]; then
    echo "MySQL execution failed: ${result}"
    exit 1;
  fi

  execution_time=$(echo "$result" | grep -oP '\(\K[0-9]+\.[0-9]+(?= sec\))')
  count=$(echo "$result" | grep -A2 "count" | tail -n1 | awk '{print $2}')
  ndv=$(echo "$result" | grep -A2 "count" | tail -n1 | awk '{print $4}')


  echo "$variables,count=${count},ndv=${ndv},elapsed=${execution_time},query=${query}"
}


while read table col; do
    full_column="column=$table.$col"
    method="method=full"
    percent="percent=100"

    prefix="${full_column},$method,$percent"
    execute_sql "$prefix" "select count($col), ndv($col) from $table; "
    execute_sql "$prefix" "select count($col), count(distinct $col) from $table; "
    
    for sample_percent in ${percents[@]}; do
        sample_ratio=$(echo "scale=2; $sample_percent / 100" | bc)
        percent="percent=${sample_percent}"

        method="method=rand"
        prefix="${full_column},$method,$percent"
        execute_sql $prefix "select count($col), ndv($col) from $table where rand() <= ${sample_ratio}; "
        
        method="method=rand_limit"
        prefix="${full_column},$method,$percent"
        execute_sql $prefix "select count($col), ndv($col) from $table where rand() <= ${sample_ratio} limit 10000000; "
        
        method="method=by_block"
        prefix="${full_column},$method,$percent"
        execute_sql $prefix "select count($col), ndv($col) from $table sample('method'='by_block', 'percent'='${sample_percent}', 'seed'='1');"
        
        method="method=by_page"
        prefix="${full_column},$method,$percent"
        execute_sql $prefix "select count($col), ndv($col) from $table sample('method'='by_page', 'percent'='${sample_percent}', 'seed'='1');"
    done
  
done < <(mysql -u "$mysql_user" -h "$mysql_host" -P"$mysql_port" -e "${query_columns}" -N)


exit 0