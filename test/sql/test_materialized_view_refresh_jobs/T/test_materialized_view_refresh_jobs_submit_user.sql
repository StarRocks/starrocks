-- name: test_materialized_view_refresh_jobs_submit_user
create database db_${uuid0};
use db_${uuid0};

[UC]shell: ip=hostname -I | awk '{print $1}';

CREATE TABLE base (dt date, val int) PARTITION BY date_trunc('day', dt);

INSERT INTO base VALUES
  ('2023-12-01', 100),
  ('2023-12-02', 200);

-- EXECUTE AS swaps currentUserIdentity (read by CREATOR/RUN_AS_USER at CREATE) but not
-- qualifiedUser (read by SUBMIT_USER at refresh), so creating as owner then refreshing as
-- root yields a submitter distinct from the creator/run-as identity on one job row.
CREATE USER owner_${uuid0}@'${ip[1]}';
GRANT CREATE MATERIALIZED VIEW ON DATABASE db_${uuid0} TO owner_${uuid0}@'${ip[1]}';
GRANT SELECT ON ALL TABLES IN DATABASE db_${uuid0} TO owner_${uuid0}@'${ip[1]}';
GRANT SELECT, ALTER, REFRESH, DROP ON ALL MATERIALIZED VIEWS IN DATABASE db_${uuid0} TO owner_${uuid0}@'${ip[1]}';
GRANT IMPERSONATE ON USER root TO owner_${uuid0}@'${ip[1]}';

EXECUTE AS owner_${uuid0}@'${ip[1]}' with no revert;
CREATE MATERIALIZED VIEW mv_owned PARTITION BY dt
REFRESH DEFERRED MANUAL
AS SELECT dt, sum(val) AS s FROM base GROUP BY dt;
execute as root with no revert;

REFRESH MATERIALIZED VIEW mv_owned WITH SYNC MODE;

function: assert_query_contains("SELECT SUBMIT_USER FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "root")
function: assert_query_contains("SELECT CREATOR FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "owner_${uuid0}")
function: assert_query_contains("SELECT RUN_AS_USER FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "owner_${uuid0}")
function: assert_query_contains("SELECT (SUBMIT_USER != CREATOR AND SUBMIT_USER NOT LIKE '%owner_${uuid0}%' AND RUN_AS_USER LIKE '%owner_${uuid0}%') FROM information_schema.materialized_view_refresh_jobs WHERE TABLE_NAME='mv_owned'", "1")

DROP MATERIALIZED VIEW mv_owned;
DROP USER owner_${uuid0}@'${ip[1]}';
drop database db_${uuid0};
