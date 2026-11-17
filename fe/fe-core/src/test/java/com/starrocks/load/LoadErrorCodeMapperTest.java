// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.load;

import com.starrocks.common.DataQualityException;
import com.starrocks.common.ErrorCode;
import com.starrocks.transaction.TransactionCommitFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class LoadErrorCodeMapperTest {

    private static void assertCode(LoadErrorCode expected, String message) {
        Assertions.assertEquals(expected, LoadErrorCodeMapper.fromMessage(message), message);
    }

    // What StmtExecutor records on the insert load job when the statement throws.
    private static String insertFailure(String reason) {
        return "Cancelled, msg: " + reason;
    }

    // What the coordinator's status carries when the INSERT was cancelled with the given reason.
    private static String insertCancelledBy(String reason) {
        return insertFailure(ErrorCode.ERR_FAILED_WHEN_INSERT.formatErrorMsg(reason));
    }

    @Test
    public void testStructuredCancelTypeWins() {
        // The cancel type is trusted even when the message would match another family.
        Assertions.assertEquals(LoadErrorCode.USER_CANCELLED, LoadErrorCodeMapper.fromFailMsg(
                new FailMsg(FailMsg.CancelType.USER_CANCEL, "Failed to allocate resource to query: pending timeout")));
        Assertions.assertEquals(LoadErrorCode.FILTER_RATIO_EXCEEDED, LoadErrorCodeMapper.fromFailMsg(
                new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED, DataQualityException.QUALITY_FAIL_MSG)));
        Assertions.assertEquals(LoadErrorCode.TIMEOUT, LoadErrorCodeMapper.fromFailMsg(
                new FailMsg(FailMsg.CancelType.TIMEOUT, "loading timeout to cancel")));
        Assertions.assertEquals(LoadErrorCode.INTERNAL_ERROR, LoadErrorCodeMapper.fromFailMsg(
                new FailMsg(FailMsg.CancelType.TXN_UNKNOWN, "")));
    }

    @Test
    public void testUnstructuredCancelTypeFallsBackToMessage() {
        Assertions.assertEquals(LoadErrorCode.BE_UNAVAILABLE, LoadErrorCodeMapper.fromFailMsg(
                new FailMsg(FailMsg.CancelType.LOAD_RUN_FAIL, "[E1008]Reached timeout=7200000ms @127.0.0.1:8060")));
        Assertions.assertEquals(LoadErrorCode.SOURCE_READ_FAILED, LoadErrorCodeMapper.fromFailMsg(
                new FailMsg(FailMsg.CancelType.ETL_RUN_FAIL,
                        "Broker list path failed. path=hdfs://ns/data/*.csv, broker=TNetworkAddress(hostname:b1, port:8000)")));
        Assertions.assertEquals(LoadErrorCode.UNKNOWN, LoadErrorCodeMapper.fromFailMsg(
                new FailMsg(FailMsg.CancelType.ETL_SUBMIT_FAIL, "spark app state: FAILED")));
        Assertions.assertEquals(LoadErrorCode.UNKNOWN, LoadErrorCodeMapper.fromFailMsg(new FailMsg()));
        Assertions.assertEquals(LoadErrorCode.UNKNOWN, LoadErrorCodeMapper.fromFailMsg(null));
    }

    @Test
    public void testFilterRatioExceeded() {
        // Routine load FILTERED_ROWS txn reason and the BE stream load status.
        assertCode(LoadErrorCode.FILTER_RATIO_EXCEEDED, "too many filtered rows");
        // Broker and spark load, as the txn abort reason replayed on a follower.
        assertCode(LoadErrorCode.FILTER_RATIO_EXCEEDED, DataQualityException.QUALITY_FAIL_MSG);
        assertCode(LoadErrorCode.FILTER_RATIO_EXCEEDED,
                DataQualityException.QUALITY_FAIL_MSG + ". tracking sql: select tracking_log from "
                        + "information_schema.load_tracking_logs where job_id=10");
        // INSERT: the reason recorded on the job and the txn abort reason.
        assertCode(LoadErrorCode.FILTER_RATIO_EXCEEDED,
                insertFailure(TransactionCommitFailedException.FILTER_DATA_ERR));
        assertCode(LoadErrorCode.FILTER_RATIO_EXCEEDED,
                TransactionCommitFailedException.FILTER_DATA_ERR + ", tracking sql = select tracking_log from "
                        + "information_schema.load_tracking_logs where job_id=10");
        assertCode(LoadErrorCode.FILTER_RATIO_EXCEEDED, ErrorCode.ERR_LOAD_HAS_FILTERED_DATA.formatErrorMsg("txn_id = 7"));
        // Merge commit quality check.
        assertCode(LoadErrorCode.FILTER_RATIO_EXCEEDED,
                "There is a data quality issue. Please check the tracking URL or SQL for details. "
                        + "Tracking URL: N/A. Tracking SQL: select tracking_log from "
                        + "information_schema.load_tracking_logs where job_id=7");
        // An INSERT that was rejected without a recorded reason stays unclassified.
        assertCode(LoadErrorCode.UNKNOWN, "Cancelled");
        // A parse error alone does not say the filter ratio was exceeded.
        assertCode(LoadErrorCode.UNKNOWN, "parse error");
    }

    @Test
    public void testMemLimitExceeded() {
        assertCode(LoadErrorCode.MEM_LIMIT_EXCEEDED,
                "Memory of process exceed limit. Used: 61977776464, Limit: 61977776128. "
                        + "Mem usage has exceed the limit of BE");
        assertCode(LoadErrorCode.MEM_LIMIT_EXCEEDED,
                "Memory of load exceed limit. Mem usage has exceed the limit of load");
        assertCode(LoadErrorCode.MEM_LIMIT_EXCEEDED,
                "memory limit exceeded, please reduce load frequency or increase config "
                        + "`mem_limit` or `load_process_max_memory_limit_percent`");
    }

    @Test
    public void testCompactionScoreExceeded() {
        assertCode(LoadErrorCode.COMPACTION_SCORE_EXCEEDED,
                "Failed to load data into partition 10012, because of too large compaction score, "
                        + "current/limit: 2100.0/2000. You can reduce the loading job concurrency, "
                        + "or increase compaction concurrency");
        assertCode(LoadErrorCode.COMPACTION_SCORE_EXCEEDED,
                "Partition's compaction score is larger than 100, delay commit for 3000ms. "
                        + "You can try to increase compaction concurrency.");
        // Timed out while being slowed down: the slowdown, not the timeout, is the reason.
        assertCode(LoadErrorCode.COMPACTION_SCORE_EXCEEDED, "Txn 42 timed out due to ingestion slowdown");
    }

    @Test
    public void testTooManyRunningTxns() {
        assertCode(LoadErrorCode.TOO_MANY_RUNNING_TXNS,
                "current running txns on db 10001 is 1000, larger than limit 1000");
    }

    @Test
    public void testLabelAlreadyExists() {
        assertCode(LoadErrorCode.LABEL_ALREADY_EXISTS, "Label [abc] has already been used.");
        assertCode(LoadErrorCode.LABEL_ALREADY_EXISTS, "Transaction already started with label [abc], txnId 7");
    }

    @Test
    public void testTimeout() {
        assertCode(LoadErrorCode.TIMEOUT, "Load timeout. Increase the timeout and retry");
        assertCode(LoadErrorCode.TIMEOUT, "timeout by txn manager");
        assertCode(LoadErrorCode.TIMEOUT, "publish timeout: 60000, transactionId=123");
        assertCode(LoadErrorCode.TIMEOUT, "coordinator could not finished before job timeout");
        assertCode(LoadErrorCode.TIMEOUT, "transaction timeout after 600000ms");
        assertCode(LoadErrorCode.TIMEOUT, "publish timed out after 10000ms");
        assertCode(LoadErrorCode.TIMEOUT, "wait memtable flush timeout for tablet 1001");
        assertCode(LoadErrorCode.TIMEOUT, "Failed to allocate resource to query: pending timeout");
        assertCode(LoadErrorCode.TIMEOUT,
                "get database/table lock timeout, transactionId=7, database=db1, timeoutMillis=5000");
        // ERR_TIMEOUT for a DML statement: thrown by StmtExecutor, or set by the timeout checker's
        // kill and returned through the coordinator.
        for (String execType : new String[] {"Insert", "Update", "Delete", "MergeInto", "Query"}) {
            assertCode(LoadErrorCode.TIMEOUT, insertFailure(ErrorCode.ERR_TIMEOUT.formatErrorMsg(execType, 3600,
                    "please increase the 'insert_timeout' session variable or the 'timeout' property for "
                            + "insert statement and retry")));
            assertCode(LoadErrorCode.TIMEOUT, insertCancelledBy(ErrorCode.ERR_TIMEOUT.formatErrorMsg(execType, 3600,
                    "please increase the 'insert_timeout' session variable, pending time:0")));
        }
        assertCode(LoadErrorCode.TIMEOUT, insertCancelledBy(ErrorCode.ERR_TIMEOUT.formatErrorMsg("Insert", 10,
                "the table t1 table_query_timeout is 10s, pending time:0")));
        // The bare word is not enough.
        assertCode(LoadErrorCode.UNKNOWN, "timeout");
        assertCode(LoadErrorCode.UNKNOWN, "table t_timeout_events does not support write");
    }

    @Test
    public void testUserCancelled() {
        assertCode(LoadErrorCode.USER_CANCELLED, "user cancel");
        assertCode(LoadErrorCode.USER_CANCELLED, "manual abort");
        assertCode(LoadErrorCode.USER_CANCELLED, "cancelled by user");
        assertCode(LoadErrorCode.USER_CANCELLED, "User Aborted");
        assertCode(LoadErrorCode.USER_CANCELLED, "User Cancelled");
        assertCode(LoadErrorCode.USER_CANCELLED, "rollback transaction by user");
        assertCode(LoadErrorCode.USER_CANCELLED, "routine load job has been paused by user");
        assertCode(LoadErrorCode.USER_CANCELLED, "killed by DROP TASK or manual cancel");
        // KILL of the connection or query running the INSERT, and a client that went away.
        assertCode(LoadErrorCode.USER_CANCELLED, insertCancelledBy("killed manually: KILL 42"));
        assertCode(LoadErrorCode.USER_CANCELLED, insertCancelledBy("killed manually: kill query 42"));
        assertCode(LoadErrorCode.USER_CANCELLED, insertCancelledBy("client closed"));
    }

    @Test
    public void testSourceReadFailed() {
        assertCode(LoadErrorCode.SOURCE_READ_FAILED,
                "Broker read failed. path=hdfs://ns/data/a.csv, broker=TNetworkAddress(hostname:b1, port:8000)");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED, "fail to connect hdfs namenode, namenode=ns1, err=timeout");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED,
                "Failed to open file: s3://bucket/data/a.parquet, error: NoSuchKey");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED, "AccessDenied: Access Denied (Service: S3, Status Code: 403)");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED, "get kafka info timeout. used time(ms) 5000");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED, "Offset out of range");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED,
                "too busy to get kafka info, please check the kafka broker status, timeout ms: 5000");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED, "the broker / HDFS may be unreachable. id: 7");
        // A connection failure that names the source is a source failure, not a backend failure.
        assertCode(LoadErrorCode.SOURCE_READ_FAILED,
                "Broker list path exception. path=hdfs://ns/data/*.csv, broker=TNetworkAddress(hostname:b1, port:8000)"
                        + " exception: java.net.ConnectException: Connection refused");
        assertCode(LoadErrorCode.SOURCE_READ_FAILED,
                "Failed to open file: s3://bucket/data/a.parquet, error: Connection reset by peer");
        // BE routine load consumer: a topic without partitions is not a table partition problem.
        assertCode(LoadErrorCode.SOURCE_READ_FAILED, "there is no partition in topic: orders");
        // A path or a topic name quoted in another kind of message does not make it a source error.
        assertCode(LoadErrorCode.UNKNOWN, "loaded 10 rows from s3://bucket/a.csv into kafka_events");
    }

    @Test
    public void testSchemaMismatch() {
        assertCode(LoadErrorCode.SCHEMA_MISMATCH, "Unknown column 'c9' in 't1'");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH, "Slot descriptor and column count mismatch");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH,
                "Tablet is deleted, perhaps this table is doing schema change, or it has already been deleted.");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH, "Partition p20240101 does not exist");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH,
                "partition p1 of table 't1' no longer exists. Check whether it was dropped by partition TTL");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH, "partition p1 of table t1 was dropped concurrently");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH, "Column [c1] does not exist in table.");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH,
                "Value count does not match column count. Expect 3, but got 2. file: s3://bucket/a.csv");
        assertCode(LoadErrorCode.SCHEMA_MISMATCH,
                "parse error. Value count does not match column count. Expect 3, but got 2. Row: a,b");
    }

    @Test
    public void testBeUnavailable() {
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "Backend node not found. Check if any backend node is down.");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "Compute node not found. Check if any compute node is down.");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "backend 10001 is down");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "coordinate BE is down");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "backend [10.0.0.1] does not alive");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "[E1008]Reached timeout=7200000ms @127.0.0.1:8060");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "Connect 10.0.0.1:8060 failed.");
        assertCode(LoadErrorCode.BE_UNAVAILABLE,
                "publish on tablet 100 failed. succeed replica num 1 less than quorum 2."
                        + " table: 1, partition: 2, publish version: 3");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "Tablet lost replicas. Check if any backend is down or not. tablet_id: 1");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "10086 have no alive replicas");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "Failed to find backend to execute");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, "java.net.ConnectException: Connection refused");
        assertCode(LoadErrorCode.BE_UNAVAILABLE, insertFailure(ErrorCode.ERR_QUERY_CANCELLED_BY_CRASH.formatErrorMsg()));
    }

    @Test
    public void testTxnConflict() {
        assertCode(LoadErrorCode.TXN_CONFLICT,
                "Fail to publish version for tablets [1, 2] Status(error=VERSION_ALREADY_EXIST)");
        assertCode(LoadErrorCode.TXN_CONFLICT, "clone version conflict with local version");
        assertCode(LoadErrorCode.TXN_CONFLICT, "publish version failed for transaction 5 on tablet 7");
        assertCode(LoadErrorCode.TXN_CONFLICT,
                "Conflict operation detected. Database db1 storage volume has changed, please retry again");
        assertCode(LoadErrorCode.TXN_CONFLICT, "transaction 12 is already aborted");
        assertCode(LoadErrorCode.TXN_CONFLICT,
                "partition 10 of table t1 received other writes while this INSERT was planned as a "
                        + "plain insert on an empty partition, retry the statement");
        // The wrapper added by LoadMgr keeps the transaction's own reason.
        assertCode(LoadErrorCode.TXN_CONFLICT, "Cancelled since transaction aborted: clone version conflict");
        // The bare word is not enough.
        assertCode(LoadErrorCode.UNKNOWN, "conflict");
        assertCode(LoadErrorCode.UNKNOWN, "table conflict_log does not support write");
    }

    @Test
    public void testInternalError() {
        assertCode(LoadErrorCode.INTERNAL_ERROR, "Load execution failed. errorCode=INTERNAL_ERROR, message=boom");
        assertCode(LoadErrorCode.INTERNAL_ERROR, "Cannot transition state from PLANNING to COMMITTED");
        assertCode(LoadErrorCode.INTERNAL_ERROR,
                "refuse to write incomplete combined txn log: missing 1 of 3 tablets [101]");
        assertCode(LoadErrorCode.INTERNAL_ERROR, "rowset_commit failed, tablet updates is in error state: tablet:1 x");
        assertCode(LoadErrorCode.INTERNAL_ERROR,
                "task not in durable state (PREPARED|CANCELLED|COMMITTED|FINISHED) will be cancelled after fe restart");
        assertCode(LoadErrorCode.INTERNAL_ERROR, "cancelled after FE restart");
        assertCode(LoadErrorCode.INTERNAL_ERROR, "Cancelled since transaction status unknown");
        assertCode(LoadErrorCode.INTERNAL_ERROR,
                "lake tablet metadata version not found, tablet_id=1001, partition_id=1002, version=144847");
        assertCode(LoadErrorCode.INTERNAL_ERROR,
                "its txn logs cannot be found and its publish fails. ADMIN SKIP COMMITTED TRANSACTION 5");
    }

    @Test
    public void testUnknown() {
        assertCode(LoadErrorCode.UNKNOWN, null);
        assertCode(LoadErrorCode.UNKNOWN, "");
        assertCode(LoadErrorCode.UNKNOWN, "all partitions have no load data");
        assertCode(LoadErrorCode.UNKNOWN, "spark app state: FAILED");
    }
}
