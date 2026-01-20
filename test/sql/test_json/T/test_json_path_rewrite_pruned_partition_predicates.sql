-- name: test_json_path_rewrite_pruned_partition_predicates
drop database if exists test_json_path_rewrite_pruned_partition_predicates;
CREATE DATABASE test_json_path_rewrite_pruned_partition_predicates;
USE test_json_path_rewrite_pruned_partition_predicates;

CREATE TABLE json_partition_prune (
  `company_id` varchar(256) NULL,
  `ts` datetime NULL,
  `company_metadata` json NULL
) ENGINE=OLAP
DUPLICATE KEY(`company_id`)
PARTITION BY RANGE(`ts`)
(PARTITION p20260108 VALUES [('2026-01-08 00:00:00'), ('2026-01-09 00:00:00')))
DISTRIBUTED BY HASH(`company_id`) BUCKETS 1
PROPERTIES("replication_num" = "1");

INSERT INTO json_partition_prune VALUES
('a', '2026-01-08 12:00:00', parse_json('{"path":"/profile"}')),
('b', '2026-01-08 13:00:00', parse_json('{"path":"/products"}')),
('c', '2026-01-08 14:00:00', parse_json('{"path":"/products"}'));

SELECT COALESCE(COALESCE(json_query(company_metadata, '$.path'), ''), '') AS k, COUNT(*) AS c
FROM json_partition_prune
WHERE ts >= TIMESTAMP('2026-01-08 00:00:00') AND ts < TIMESTAMP('2026-01-09 00:00:00')
GROUP BY 1
ORDER BY 1;

