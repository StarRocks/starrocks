#!/usr/bin/env bash
# Generates the evo_*, evo_noshred_*, prune_rowgroups and prune_int16 VARIANT parquet files in this directory with Spark 4.1 and
# iceberg-spark-runtime-4.1_2.13:1.12.0 on a local Hadoop catalog (rows described in gen_variant_evolution.sql).
# Usage: SPARK_HOME=<spark-4.1.x-bin-hadoop3> WAREHOUSE=<empty scratch dir> ./gen_variant_evolution.sh
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
: "${SPARK_HOME:?set SPARK_HOME to a Spark 4.1 distribution}"
: "${WAREHOUSE:?set WAREHOUSE to a scratch directory for the Iceberg warehouse}"

spark_sql() {
    "$SPARK_HOME/bin/spark-sql" --master 'local[1]' \
        --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0 \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.iceberg_fs=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.iceberg_fs.type=hadoop \
        --conf spark.sql.catalog.iceberg_fs.warehouse="$WAREHOUSE" \
        --conf spark.sql.iceberg.vectorization.enabled=false \
        --conf spark.sql.session.timeZone=UTC \
        "$@" 2>/dev/null
}

spark_sql -f "$HERE/gen_variant_evolution.sql" > /dev/null

# Each data file covers one id range; name it after the batch that wrote it.
label() {
    case $1 in
    1000) echo e1_int ;; 2000) echo e2_bigint ;; 3000) echo e3_double ;; 4000) echo e4_array ;;
    5000) echo e5_string ;; 6000) echo e6_object ;; 7000) echo e7_mixed ;; 0) echo rowgroups ;;
    *) echo "unexpected lower bound $1" >&2; exit 1 ;;
    esac
}

for t in evo evo_noshred prune prune_int16; do
    spark_sql -e "SELECT readable_metrics.id.lower_bound, file_path FROM iceberg_fs.variant_spark_gen.$t.files" |
        grep -E '^[0-9]+' | while read -r lb path; do
        if [ "$t" = prune_int16 ]; then name=prune_int16; else name="${t}_$(label "$lb")"; fi
        cp "${path#file:}" "$HERE/$name.parquet"
    done
done
