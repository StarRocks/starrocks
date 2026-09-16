-- name: test_iceberg_mv_base_table_refresh_version_times
create external catalog mv_iceberg_${uuid0}
properties
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "${iceberg_catalog_hive_metastore_uris}"
);
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
create database ice_db_${uuid0};
-- result:
-- !result
use ice_db_${uuid0};
-- result:
-- !result
create table ice_base (k int, v int) properties('format-version'='2');
-- result:
-- !result
insert into ice_base values (1, 10), (2, 20);
-- result:
-- !result
create table ice_base2 (k int, w int) properties('format-version'='2');
-- result:
-- !result
insert into ice_base2 values (1, 100), (2, 200);
-- result:
-- !result
set catalog default_catalog;
-- result:
-- !result
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
create materialized view mv_ice
refresh deferred manual
as select k, v from mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base;
-- result:
-- !result
refresh materialized view mv_ice with sync mode;
select BASE_TABLE_REFRESH_VERSION_TIMES from information_schema.materialized_views
where TABLE_SCHEMA = 'db_${uuid0}' and TABLE_NAME = 'mv_ice';
-- result:
[REGEX]^\{"mv_iceberg_\w+\.ice_db_\w+\.ice_base":"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}"\}$
-- !result
create materialized view mv_ice_multi
refresh deferred manual
as select a.k, a.v, b.w
   from mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base a
   join mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base2 b on a.k = b.k;
-- result:
-- !result
refresh materialized view mv_ice_multi with sync mode;
select
  get_json_string(BASE_TABLE_REFRESH_VERSION_TIMES, '$."mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base"')
    regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' as base_ok,
  get_json_string(BASE_TABLE_REFRESH_VERSION_TIMES, '$."mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base2"')
    regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' as base2_ok
from information_schema.materialized_views
where TABLE_SCHEMA = 'db_${uuid0}' and TABLE_NAME = 'mv_ice_multi';
-- result:
1	1
-- !result
drop materialized view mv_ice;
-- result:
-- !result
drop materialized view mv_ice_multi;
-- result:
-- !result
drop database db_${uuid0} force;
-- result:
-- !result
set catalog mv_iceberg_${uuid0};
-- result:
-- !result
drop table ice_db_${uuid0}.ice_base force;
-- result:
-- !result
drop table ice_db_${uuid0}.ice_base2 force;
-- result:
-- !result
drop database ice_db_${uuid0} force;
-- result:
-- !result
drop catalog mv_iceberg_${uuid0};
-- result:
-- !result