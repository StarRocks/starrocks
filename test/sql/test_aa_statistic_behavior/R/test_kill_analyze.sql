-- name: test_kill_analyze
CREATE TABLE test_kill_analyze (k1 int) properties("replication_num"="1");
-- result:
-- !result
INSERT INTO test_kill_analyze VALUES(1);
-- result:
-- !result
ADMIN ENABLE FAILPOINT 'statistic_executor_collect_statistics' WITH 1 TIMES ON FRONTEND;
-- result:
-- !result
[UC]ANALYZE TABLE test_kill_analyze WITH ASYNC MODE;
-- result:
test_db_d97971d779144714a700ba1e4658d721.test_kill_analyze	analyze	status	OK
-- !result
SET @analyze_id = (SELECT cast(Id as int) FROM information_schema.analyze_status WHERE `Table` = 'test_kill_analyze' LIMIT 1);
-- result:
-- !result
[UC]KILL ANALYZE @analyze_id;
-- result:
E: (1064, 'Getting analyzing error. Detail message: There is no running task with analyzeId 12234897.')
-- !result