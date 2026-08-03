-- name: test_materialized_view_refresh_jobs_submit_user
create database db_${uuid0};
-- result:
-- !result
use db_${uuid0};
-- result:
-- !result
[UC]shell: ip=hostname -I | awk '{print $1}';
-- result:
0
172.26.92.227
-- !result
CREATE TABLE base (dt date, val int) PARTITION BY date_trunc('day', dt);
-- result:
-- !result
INSERT INTO base VALUES
  ('2023-12-01', 100),
  ('2023-12-02', 200);
-- result:
-- !result
CREATE USER owner_${uuid0}@'${ip[1]}';
-- result:
-- !result
GRANT CREATE MATERIALIZED VIEW ON DATABASE db_${uuid0} TO owner_${uuid0}@'${ip[1]}';
-- result:
-- !result
GRANT SELECT ON ALL TABLES IN DATABASE db_${uuid0} TO owner_${uuid0}@'${ip[1]}';
-- result:
-- !result
GRANT SELECT, ALTER, REFRESH, DROP ON ALL MATERIALIZED VIEWS IN DATABASE db_${uuid0} TO owner_${uuid0}@'${ip[1]}';
-- result:
-- !result
GRANT IMPERSONATE ON USER root TO owner_${uuid0}@'${ip[1]}';
-- result:
-- !result
EXECUTE AS owner_${uuid0}@'${ip[1]}' with no revert;
-- result:
-- !result
CREATE MATERIALIZED VIEW mv_owned PARTITION BY dt
REFRESH DEFERRED MANUAL
AS SELECT dt, sum(val) AS s FROM base GROUP BY dt;
-- result:
-- !result
execute as root with no revert;
-- result:
-- !result
REFRESH MATERIALIZED VIEW mv_owned WITH SYNC MODE;
-- result:
-- !result
function: assert_query_contains("SELECT SUBMIT_USER FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "root")
-- result:
None
-- !result
function: assert_query_contains("SELECT CREATOR FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "owner_${uuid0}")
-- result:
None
-- !result
function: assert_query_contains("SELECT RUN_AS_USER FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "owner_${uuid0}")
-- result:
None
-- !result
function: assert_query_contains("SELECT (SUBMIT_USER != CREATOR AND SUBMIT_USER NOT LIKE '%owner_${uuid0}%' AND RUN_AS_USER LIKE '%owner_${uuid0}%') FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "1")
-- result:
None
-- !result
DROP MATERIALIZED VIEW mv_owned;
-- result:
-- !result
DROP USER owner_${uuid0}@'${ip[1]}';
-- result:
-- !result
drop database db_${uuid0};
-- result:
-- !result
