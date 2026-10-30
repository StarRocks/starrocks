-- name: test_iceberg_mv_base_table_refresh_version_times

create external catalog mv_iceberg_${uuid0}
properties
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "${iceberg_catalog_hive_metastore_uris}"
);

set catalog mv_iceberg_${uuid0};
create database ice_db_${uuid0};
use ice_db_${uuid0};
create table ice_base (k int, v int) properties('format-version'='2');
insert into ice_base values (1, 10), (2, 20);
create table ice_base2 (k int, w int) properties('format-version'='2');
insert into ice_base2 values (1, 100), (2, 200);

set catalog default_catalog;
create database db_${uuid0};
use db_${uuid0};

create materialized view mv_ice
refresh deferred manual
as select k, v from mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base;

refresh materialized view mv_ice with sync mode;

-- A single iceberg base table reports a real data version time, so BASE_TABLE_REFRESH_VERSION_TIMES holds one
-- entry keyed by catalog.db.table. The value is the iceberg snapshot time (non-deterministic), so match shape only.
select BASE_TABLE_REFRESH_VERSION_TIMES from information_schema.materialized_views
where TABLE_SCHEMA = 'db_${uuid0}' and TABLE_NAME = 'mv_ice';

create materialized view mv_ice_multi
refresh deferred manual
as select a.k, a.v, b.w
   from mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base a
   join mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base2 b on a.k = b.k;

refresh materialized view mv_ice_multi with sync mode;

-- Two iceberg base tables -> two entries. Map order is non-deterministic, so extract each key individually and
-- assert its value is a datetime (avoids depending on JSON key ordering).
select
  get_json_string(BASE_TABLE_REFRESH_VERSION_TIMES, '$."mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base"')
    regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' as base_ok,
  get_json_string(BASE_TABLE_REFRESH_VERSION_TIMES, '$."mv_iceberg_${uuid0}.ice_db_${uuid0}.ice_base2"')
    regexp '^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$' as base2_ok
from information_schema.materialized_views
where TABLE_SCHEMA = 'db_${uuid0}' and TABLE_NAME = 'mv_ice_multi';

drop materialized view mv_ice;
drop materialized view mv_ice_multi;
drop database db_${uuid0} force;
set catalog mv_iceberg_${uuid0};
drop table ice_db_${uuid0}.ice_base force;
drop table ice_db_${uuid0}.ice_base2 force;
drop database ice_db_${uuid0} force;
drop catalog mv_iceberg_${uuid0};
