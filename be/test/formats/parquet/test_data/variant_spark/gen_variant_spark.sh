#!/usr/bin/env bash
# Generates the VARIANT parquet files in this directory on a local Hadoop catalog:
#   shred_*, noshred_*  written by Spark 4.1 with iceberg-spark-runtime-4.1_2.13:1.12.0
#   api_shred_*         the same rows rewritten by the plain Iceberg 1.12 Java API (GenVariantIcebergApi.java)
# Both writers infer the shredding layout per file (write.parquet.shred-variants=true).
# Spark's vectorized Parquet reader is turned off because it fails on some shredded
# variants (apache/iceberg#18430, #18601, #18669).
# Usage: SPARK_HOME=<spark-4.1.x-bin-hadoop3> WAREHOUSE=<empty scratch dir> ./gen_variant_spark.sh
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

spark_sql -f "$HERE/gen_variant_spark.sql" > /dev/null

# Each data file covers one id range; name it after the batch that wrote it.
label() {
    case $1 in
    1) echo 01_typed ;; 7) echo 02_conflict ;; 11) echo 03_nulls ;; 17) echo 04_scalars ;; 101) echo 05_inference ;; 301) echo 06_top_int ;;
    *) echo "unexpected lower bound $1" >&2; exit 1 ;;
    esac
}

for t in shred noshred; do
    spark_sql -e "SELECT readable_metrics.id.lower_bound, file_path FROM iceberg_fs.variant_spark_gen.$t.files" |
        grep -E '^[0-9]+' | while read -r lb path; do
        cp "${path#file:}" "$HERE/${t}_$(label "$lb").parquet"
    done
done

# The same rows through the Iceberg Java API writer; needs JDK 17+ (single-file source launch).
ICEBERG_JAR=$(find "${IVY_HOME:-$HOME/.ivy2}"* -name 'iceberg-spark-runtime-4.1_2.13-1.12.0.jar' 2>/dev/null | head -1)
java -cp "$SPARK_HOME/jars/*:$ICEBERG_JAR" "$HERE/GenVariantIcebergApi.java" "$WAREHOUSE" 2>/dev/null |
    while IFS=$'\t' read -r lb path _; do
    cp "${path#file:}" "$HERE/api_shred_$(label "$lb").parquet"
done

# Spark's answers, used to check the recorded StarRocks results.
spark_sql -f "$HERE/expected_variant_spark.sql" | grep -E '^(shred|noshred)\b' > "$HERE/expected_variant_spark.tsv"
