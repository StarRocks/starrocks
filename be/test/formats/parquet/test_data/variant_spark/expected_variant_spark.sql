-- Spark's answers for the generated tables; StarRocks results are checked against these.
SET spark.sql.session.timeZone=UTC;
SET spark.sql.iceberg.vectorization.enabled=false;
SELECT 'shred' AS t, id, to_json(v),
       to_json(variant_get(v, '$.i')), to_json(variant_get(v, '$.s')), to_json(variant_get(v, '$.dec')),
       to_json(variant_get(v, '$.t')), to_json(variant_get(v, '$.o.x')), to_json(variant_get(v, '$.o.z.w')),
       to_json(variant_get(v, '$.arr[1]')), to_json(variant_get(v, '$.ao[0].k')), to_json(variant_get(v, '$.x'))
FROM iceberg_fs.variant_spark_gen.shred
UNION ALL
SELECT 'noshred', id, to_json(v),
       to_json(variant_get(v, '$.i')), to_json(variant_get(v, '$.s')), to_json(variant_get(v, '$.dec')),
       to_json(variant_get(v, '$.t')), to_json(variant_get(v, '$.o.x')), to_json(variant_get(v, '$.o.z.w')),
       to_json(variant_get(v, '$.arr[1]')), to_json(variant_get(v, '$.ao[0].k')), to_json(variant_get(v, '$.x'))
FROM iceberg_fs.variant_spark_gen.noshred
ORDER BY 1, 2;
