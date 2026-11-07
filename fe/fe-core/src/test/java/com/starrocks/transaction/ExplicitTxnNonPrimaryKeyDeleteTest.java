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

package com.starrocks.transaction;

import com.starrocks.common.ErrorCode;
import com.starrocks.load.DeleteMgr;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.DeleteStmt;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

// A DELETE on a table without a primary key is executed by DeleteMgr as a delete job that begins and
// commits a transaction of its own. Inside BEGIN ... ROLLBACK it used to reach DeleteMgr all the same,
// so the rows were gone at once and ROLLBACK could not bring them back (StarRocksTest#12634). The whole
// statement path runs here, on a shared-data cluster as in the report.
public class ExplicitTxnNonPrimaryKeyDeleteTest {
    private static final String DB = "explicit_txn_non_pk_delete";
    private static ConnectContext ctx;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        ctx = UtFrameUtils.createDefaultCtx();
        StarRocksAssert starRocksAssert = new StarRocksAssert(ctx);
        starRocksAssert.withDatabase(DB).useDatabase(DB);
        starRocksAssert.withTable("CREATE TABLE t_dup (k BIGINT NOT NULL, v BIGINT) DUPLICATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1");
        starRocksAssert.withTable("CREATE TABLE t_uniq (k BIGINT NOT NULL, v BIGINT) UNIQUE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1");
        starRocksAssert.withTable("CREATE TABLE t_agg (k BIGINT NOT NULL, v BIGINT SUM) AGGREGATE KEY(k) " +
                "DISTRIBUTED BY HASH(k) BUCKETS 1");
    }

    private static void execute(String sql) throws Exception {
        ctx.setThreadLocalInfo();
        ctx.getState().reset();
        new StmtExecutor(ctx, SqlParser.parseSingleStatement(sql, ctx.getSessionVariable().getSqlMode())).execute();
    }

    @Test
    public void testNonPrimaryKeyDeleteDoesNotEscapeExplicitTxn() throws Exception {
        // Reaching DeleteMgr at all means the DELETE runs and commits outside the explicit transaction.
        AtomicInteger deleteJobs = new AtomicInteger();
        new MockUp<DeleteMgr>() {
            @Mock
            public void process(DeleteStmt stmt) {
                deleteJobs.incrementAndGet();
            }
        };

        // Outside a transaction the DELETE does go to DeleteMgr, which shows the mock is in the path.
        execute("delete from t_dup where k in (1, 2)");
        Assertions.assertFalse(ctx.getState().isError(), ctx.getState().getErrorMessage());
        Assertions.assertEquals(1, deleteJobs.get());

        execute("begin");
        Assertions.assertFalse(ctx.getState().isError(), ctx.getState().getErrorMessage());
        long txnId = ctx.getTxnId();
        Assertions.assertNotEquals(0, txnId);

        for (String table : List.of("t_dup", "t_uniq", "t_agg")) {
            execute("delete from " + table + " where k in (1, 2)");
            Assertions.assertTrue(ctx.getState().isError(), table + ": the DELETE ran outside the transaction");
            Assertions.assertEquals(ErrorCode.ERR_EXPLICIT_TXN_DELETE_ON_NON_PRIMARY_KEY_TABLE,
                    ctx.getState().getErrorCode(), table);
            // The rejected statement leaves the transaction open, so the user can go on or roll back.
            Assertions.assertEquals(txnId, ctx.getTxnId(), table);
            Assertions.assertNotNull(
                    GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getExplicitTxnState(txnId), table);
        }
        Assertions.assertEquals(1, deleteJobs.get(), "a DELETE in the transaction reached DeleteMgr");

        execute("rollback");
        Assertions.assertFalse(ctx.getState().isError(), ctx.getState().getErrorMessage());
        Assertions.assertEquals(0, ctx.getTxnId());
        Assertions.assertNull(GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().getExplicitTxnState(txnId));
    }
}
