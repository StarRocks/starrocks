-- name: test_kill_analyze



CREATE TABLE test_kill_analyze (k1 int) properties("replication_num"="1");
INSERT INTO test_kill_analyze VALUES(1);

ADMIN ENABLE FAILPOINT 'statistic_executor_collect_statistics' WITH 1 TIMES ON FRONTEND;
[UC]ANALYZE TABLE test_kill_analyze WITH ASYNC MODE;

SET @analyze_id = (SELECT cast(Id as int) FROM information_schema.analyze_status WHERE `Table` = 'test_kill_analyze' LIMIT 1);
[UC]KILL ANALYZE @analyze_id;