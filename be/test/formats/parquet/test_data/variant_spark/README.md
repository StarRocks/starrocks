# Iceberg 1.12 VARIANT data files written by Spark and the Iceberg Java API

Test data for `test/sql/test_iceberg/T/test_iceberg_variant_spark_shredding`, which checks that StarRocks reads
VARIANT columns written by real writers correctly
([StarRocks#82480](https://github.com/StarRocks/starrocks/issues/82480),
[StarRocks#82482](https://github.com/StarRocks/starrocks/issues/82482)).

The files are generated offline and committed; CI only reads them.

## Writers

The same rows are written three times, into Iceberg v3 tables (`id INT, v VARIANT`) on a local Hadoop catalog:

| Files | Writer | Shredding |
|---|---|---|
| `shred_*.parquet` | Spark 4.1 + `iceberg-spark-runtime-4.1_2.13:1.12.0` | `write.parquet.shred-variants=true` |
| `noshred_*.parquet` | Spark 4.1 + `iceberg-spark-runtime-4.1_2.13:1.12.0` | off |
| `api_shred_*.parquet` | plain Iceberg 1.12 Java API (`GenericFileWriterFactory`), see `GenVariantIcebergApi.java` | `write.parquet.shred-variants=true` |

With shredding on, Iceberg 1.12 infers the shredded layout of each data file from its first buffered rows
(`write.parquet.variant-inference-buffer-size`, default 100), see the
[Iceberg table properties](https://iceberg.apache.org/docs/nightly/configuration/#write-properties).
Spark (`SparkVariantShreddingAnalyzer`) and the generic Java API (`RecordVariantShreddingAnalyzer`) do this
inference with two different implementations, which is why both are covered.
`GenVariantIcebergApi.java` reads the rows back from the Spark-written unshredded table, so the three sets of files
hold exactly the same values.

## Files

Every INSERT writes exactly one data file (`REPARTITION(1)`) over a disjoint id range; the suffix names the batch.
The rows are in `gen_variant_spark.sql`.

| Suffix | ids | Rows | Shredded layout inferred by Iceberg 1.12 |
|---|---|---|---|
| `01_typed` | 1-6 | objects with typed scalars (int8/int64, decimal, double, bool, string, date, timestamp, timestamp_ntz), nested objects, arrays, arrays of objects; a pre-1970 timestamp and min/max int64 | every field, including arrays of objects and timestamp/date |
| `02_conflict` | 7-10 | the same paths with other types, mixed inside the file | almost nothing; conflicting paths stay in the residual `value` |
| `03_nulls` | 11-16 | variant null, SQL NULL, empty object, missing fields, nulls inside objects and arrays | `i`, `arr`, `ao` |
| `04_scalars` | 17-21 | top-level values that are not objects (int, string, array, decimal, timestamp) | none (mixed root types) |
| `05_inference` | 101-250 | 100 rows `{"i": int, "s": string}`, then 50 rows with `i` as a string and a new field `x` | `i` (int16), `s`; the later strings and `x` only in the residual `value` |
| `06_top_int` | 301-405 | 100 top-level ints, then strings and a variant null | the root itself (int16); the rest in the residual `value` |

`expected_variant_spark.tsv` holds Spark's answers for the queries in `expected_variant_spark.sql` (whole document as
JSON and a few paths); Spark returns the same rows for the shredded and the unshredded table.

## Regenerating

Run on a machine with Spark 4.1 and JDK 17+ (the Iceberg runtime jar is fetched by `--packages`):

```bash
SPARK_HOME=<spark-4.1.x-bin-hadoop3> WAREHOUSE=<empty scratch dir> ./gen_variant_spark.sh
```

The script runs `gen_variant_spark.sql` with `spark-sql`, copies the data files here named after their id range,
runs `GenVariantIcebergApi.java` for the `api_shred_*` files, and writes `expected_variant_spark.tsv`.
Notes:
- `spark.sql.session.timeZone=UTC`, so timestamps do not depend on the machine.
- `spark.sql.iceberg.vectorization.enabled=false`: Spark's vectorized Parquet reader fails on some shredded variants
  ([apache/iceberg#18430](https://github.com/apache/iceberg/issues/18430),
  [#18601](https://github.com/apache/iceberg/issues/18601),
  [#18669](https://github.com/apache/iceberg/issues/18669)).
- Regenerated files may differ byte for byte but hold the same rows; re-record the sql-test after regenerating.
