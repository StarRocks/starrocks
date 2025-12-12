-- name: test_kill_analyze


ADMIN ENABLE FAILPOINT 'statistic_executor_collect_statistics' WITH 3 TIMES ON FRONTEND;

CREATE TABLE t1 (k1 int) properties("replication_num"="1");
INSERT INTO t1 VALUES(1);

ANLAYZE TABLE t1 WITH ASYNC MODE;
KILL ANALYZE <id>;