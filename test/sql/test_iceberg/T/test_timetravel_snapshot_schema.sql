-- name: test_timetravel_snapshot_schema @sequential @no_arrow_flight_sql
-- Test Point:
--   1. VERSION AS OF a tag/branch created before a column rename resolves columns by the
--      snapshot schema: the old column name resolves, the renamed name does not exist yet.
--   2. Predicates on the snapshot-schema column name are planned and evaluated correctly.
--   3. The same holds for a partitioned table whose partition source column was renamed.
-- Method: rename a column between two snapshots anchored by a tag and a branch; assert
--         column resolution (positive and negative) and row results for time-travel reads
--         vs latest-schema reads, with and without predicates.
-- Scope: Iceberg time travel per-snapshot schema (analyzer read schema / BE descriptor / scan planning)

create external catalog iceberg_sql_test_${uuid0} PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="hive", "iceberg.catalog.hive.metastore.uris"="${iceberg_catalog_hive_metastore_uris}","aws.s3.access_key" = "${oss_ak}","aws.s3.secret_key" = "${oss_sk}","aws.s3.endpoint" = "${oss_endpoint}", "enable_iceberg_metadata_cache"="false");

create database iceberg_sql_test_${uuid0}.iceberg_db_${uuid0} properties (
    "location" = "oss://${oss_bucket}/iceberg_tt_schema_${uuid0}/iceberg_db/${uuid0}"
);

-- 1) unpartitioned table: rename column between snapshots
function: retry_execute_sql("create external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} (id int, c int)", False, 5, 1000)

insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} values (1, 100);
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} create tag tag_before_rename;
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} create branch branch_before_rename;
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} rename column c to c_renamed;
insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} values (2, 200);

refresh external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0};

-- latest read uses the renamed column
select id, c_renamed from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} order by id;
-- the old name no longer exists in the latest schema
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} order by id;

-- time travel to the tag resolves the snapshot schema: old name works
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "tag_before_rename" order by id;
-- the renamed name does not exist in the snapshot schema
select id, c_renamed from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "tag_before_rename" order by id;
-- predicate on the snapshot-schema column name
select * from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "tag_before_rename" where c = 100;
-- branch reference behaves the same as the tag
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "branch_before_rename" where c >= 100 order by id;

-- 2) partitioned table: partition source column renamed between snapshots
function: retry_execute_sql("create external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} (id int, c int) partition by c", False, 5, 1000)

insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} values (1, 100);
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} create tag tag_part_before_rename;
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} rename column c to c_renamed;
insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} values (2, 200);

refresh external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0};

select id, c_renamed from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} order by id;
-- time travel on the partitioned table resolves the snapshot schema and plans the scan
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} version as of "tag_part_before_rename" order by id;
-- predicate on the renamed partition source column under time travel
select * from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} version as of "tag_part_before_rename" where c = 100;

drop table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} force;
drop table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} force;
drop database iceberg_sql_test_${uuid0}.iceberg_db_${uuid0};
drop catalog iceberg_sql_test_${uuid0};

shell: ossutil64 rm -rf oss://${oss_bucket}/iceberg_tt_schema_${uuid0}/iceberg_db/${uuid0} > /dev/null || echo "exit 0" >/dev/null