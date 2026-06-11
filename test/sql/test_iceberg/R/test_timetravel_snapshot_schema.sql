-- name: test_timetravel_snapshot_schema @sequential @no_arrow_flight_sql
create external catalog iceberg_sql_test_${uuid0} PROPERTIES ("type"="iceberg", "iceberg.catalog.type"="hive", "iceberg.catalog.hive.metastore.uris"="${iceberg_catalog_hive_metastore_uris}","aws.s3.access_key" = "${oss_ak}","aws.s3.secret_key" = "${oss_sk}","aws.s3.endpoint" = "${oss_endpoint}", "enable_iceberg_metadata_cache"="false");
-- result:
-- !result
create database iceberg_sql_test_${uuid0}.iceberg_db_${uuid0} properties (
    "location" = "oss://${oss_bucket}/iceberg_tt_schema_${uuid0}/iceberg_db/${uuid0}"
);
-- result:
-- !result
function: retry_execute_sql("create external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} (id int, c int)", False, 5, 1000)
-- result:
{'status': True, 'result': '', 'msg': b''}
-- !result
insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} values (1, 100);
-- result:
-- !result
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} create tag tag_before_rename;
-- result:
-- !result
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} create branch branch_before_rename;
-- result:
-- !result
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} rename column c to c_renamed;
-- result:
-- !result
insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} values (2, 200);
-- result:
-- !result
refresh external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0};
-- result:
-- !result
select id, c_renamed from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} order by id;
-- result:
1	100
2	200
-- !result
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} order by id;
-- result:
E: (1064, "Getting analyzing error. Detail message: Column 'c' cannot be resolved.")
-- !result
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "tag_before_rename" order by id;
-- result:
1	100
-- !result
select id, c_renamed from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "tag_before_rename" order by id;
-- result:
E: (1064, "Getting analyzing error. Detail message: Column 'c_renamed' cannot be resolved.")
-- !result
select * from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "tag_before_rename" where c = 100;
-- result:
1	100
-- !result
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} version as of "branch_before_rename" where c >= 100 order by id;
-- result:
1	100
-- !result
function: retry_execute_sql("create external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} (id int, c int) partition by c", False, 5, 1000)
-- result:
{'status': True, 'result': '', 'msg': b''}
-- !result
insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} values (1, 100);
-- result:
-- !result
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} create tag tag_part_before_rename;
-- result:
-- !result
alter table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} rename column c to c_renamed;
-- result:
-- !result
insert into iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} values (2, 200);
-- result:
-- !result
refresh external table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0};
-- result:
-- !result
select id, c_renamed from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} order by id;
-- result:
1	100
2	200
-- !result
select id, c from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} version as of "tag_part_before_rename" order by id;
-- result:
1	100
-- !result
select * from iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} version as of "tag_part_before_rename" where c = 100;
-- result:
1	100
-- !result
drop table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_tbl_${uuid0} force;
-- result:
-- !result
drop table iceberg_sql_test_${uuid0}.iceberg_db_${uuid0}.ice_part_tbl_${uuid0} force;
-- result:
-- !result
drop database iceberg_sql_test_${uuid0}.iceberg_db_${uuid0};
-- result:
-- !result
drop catalog iceberg_sql_test_${uuid0};
-- result:
-- !result
shell: ossutil64 rm -rf oss://${oss_bucket}/iceberg_tt_schema_${uuid0}/iceberg_db/${uuid0} > /dev/null || echo "exit 0" >/dev/null
-- result:
0

-- !result