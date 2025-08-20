-- name: test_explain_mv_refresh
CREATE TABLE t1 (dt date, val int) PARTITION BY date_trunc('day', dt);
-- result:
-- !result
CREATE MATERIALIZED VIEW test_mv1 PARTITION BY dt
REFRESH DEFERRED MANUAL 
AS SELECT dt, sum(val) from t1 group by dt;
-- result:
-- !result
explain refresh materialized view test_mv1;
explain refresh materialized view test_mv1 force;
explain refresh materialized view test_mv1 with sync mode;
explain refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02');
explain costs refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02');
explain logical refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02');
explain verbose refresh materialized view test_mv1;
explain verbose refresh materialized view test_mv1 force;
explain verbose refresh materialized view test_mv1 with sync mode;
explain verbose refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02');
explain verbose refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02');
explain verbose refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02');
trace times optimizer refresh materialized view test_mv1;
-- result:
NOT AVAILABLE
-- !result
trace times optimizer refresh materialized view test_mv1 force;
-- result:
NOT AVAILABLE
-- !result
trace times optimizer refresh materialized view test_mv1 with sync mode;
-- result:
NOT AVAILABLE
-- !result
trace logs optimizer refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02');
-- result:
NOT AVAILABLE
-- !result
INSERT INTO t1 VALUES 
  ('2023-12-01', 100),
  ('2023-12-01', 200),
  ('2023-12-02', 300),
  ('2023-12-02', 400),
  ('2023-12-03', 500);
-- result:
-- !result
function: assert_query_contains("explain refresh materialized view test_mv1", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain refresh materialized view test_mv1 force", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain refresh materialized view test_mv1 with sync mode", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02')", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain costs refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02')", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain logical refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02')", "t1")
-- result:
None
-- !result
function: assert_query_contains("explain verbose refresh materialized view test_mv1", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain verbose refresh materialized view test_mv1 force", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain verbose refresh materialized view test_mv1 with sync mode", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain verbose refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02')", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("explain verbose refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02')", "t1")
-- result:
None
-- !result
function: assert_query_contains("explain verbose refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02')", "test_mv1")
-- result:
None
-- !result
function: assert_query_contains("trace times optimizer refresh materialized view test_mv1", "Analyzer")
-- result:
None
-- !result
function: assert_query_contains("trace times optimizer refresh materialized view test_mv1 force", "Analyzer")
-- result:
None
-- !result
function: assert_query_contains("trace times optimizer refresh materialized view test_mv1 with sync mode", "Analyzer")
-- result:
None
-- !result
function: assert_query_contains("trace logs optimizer refresh materialized view test_mv1 partition start ('2023-12-01') end ('2023-12-02')", "t1")
-- result:
None
-- !result