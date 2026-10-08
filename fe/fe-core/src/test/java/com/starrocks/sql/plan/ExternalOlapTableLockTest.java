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

package com.starrocks.sql.plan;

import com.starrocks.catalog.ExternalOlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Pair;
import com.starrocks.external.starrocks.TableMetaSyncer;
import com.starrocks.leader.LeaderImpl;
import com.starrocks.planner.DataSink;
import com.starrocks.planner.OlapTableSink;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.StatementPlanner;
import com.starrocks.sql.analyzer.AnalyzerUtils;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.TAuthenticateParams;
import com.starrocks.thrift.TGetTableMetaRequest;
import com.starrocks.thrift.TGetTableMetaResponse;
import com.starrocks.transaction.RemoteTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Invocation;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * An ExternalOlapTable (ENGINE=OLAP pointing at another cluster) lives in an internal database, but its published
 * object is never written: an INSERT syncs a private copy from the other cluster before the meta lock is taken
 * and plans on that copy. So it is no meta lock target, and an INSERT into it plans on the lock-free path.
 */
public class ExternalOlapTableLockTest extends PlanTestBase {

    private final AtomicInteger syncs = new AtomicInteger();

    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        // The "remote" table: the same FE answers the meta request for it, as in TableMetaSyncerTest.
        starRocksAssert.withTable("CREATE TABLE test.ext_src (k1 BIGINT, k2 BIGINT) DUPLICATE KEY(k1) "
                + "PARTITION BY RANGE(k1) (PARTITION p1 VALUES LESS THAN ('10'), PARTITION p2 VALUES LESS THAN ('20')) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES ('replication_num' = '1')");
        starRocksAssert.withTable("CREATE EXTERNAL TABLE test.ext_olap (k1 BIGINT, k2 BIGINT) DUPLICATE KEY(k1) "
                + "DISTRIBUTED BY HASH(k1) BUCKETS 2 PROPERTIES ('host' = '127.0.0.2', 'port' = '9020', "
                + "'user' = 'root', 'password' = '', 'database' = 'test', 'table' = 'ext_src')");
    }

    @BeforeEach
    public void mockTheRemoteCluster() {
        syncs.set(0);
        new MockUp<TableMetaSyncer>() {
            @Mock
            public void syncTable(ExternalOlapTable table) throws Exception {
                syncs.incrementAndGet();
                TGetTableMetaRequest request = new TGetTableMetaRequest();
                request.setDb_name("test");
                request.setTable_name("ext_src");
                TGetTableMetaResponse response = new LeaderImpl().getTableMeta(request);
                table.updateMeta("test", response.getTable_meta(), response.getBackends());
            }
        };
        new MockUp<RemoteTransactionMgr>() {
            @Mock
            public long beginTransaction(long dbId, List<Long> tableIdList, String label,
                                         TransactionState.LoadJobSourceType sourceType, long timeoutSecond,
                                         String host, int port, TAuthenticateParams authenticateParams) {
                return 10086L;
            }
        };
    }

    private static ExternalOlapTable published() {
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "ext_olap");
        Assertions.assertInstanceOf(ExternalOlapTable.class, table);
        return (ExternalOlapTable) table;
    }

    private static ExternalOlapTable sinkTable(String sql) throws Exception {
        Pair<String, ExecPlan> plan = UtFrameUtils.getPlanAndFragment(connectContext, sql);
        DataSink sink = plan.second.getFragments().get(0).getSink();
        Assertions.assertInstanceOf(OlapTableSink.class, sink);
        // Not a plain OlapTable: a shadow copy taken on the lock-free path would have lost the remote side.
        Assertions.assertInstanceOf(ExternalOlapTable.class, ((OlapTableSink) sink).getDstTable());
        return (ExternalOlapTable) ((OlapTableSink) sink).getDstTable();
    }

    /**
     * The INSERT takes the lock-free path -- the statement is judged copy-safe -- and the sink still writes an
     * ExternalOlapTable: the lock-free path shadow copies the OLAP tables it plans on, and used to turn the
     * synced target into a plain OlapTable that lost the remote cluster.
     */
    @Test
    public void testInsertPlansOnTheLockFreePath() throws Exception {
        Assertions.assertFalse(published().isMetaLockTarget());
        List<Boolean> verdicts = new ArrayList<>();
        new MockUp<AnalyzerUtils>() {
            @Mock
            public boolean areTablesCopySafe(Invocation invocation, StatementBase statement) {
                boolean verdict = invocation.proceed();
                verdicts.add(verdict);
                return verdict;
            }
        };
        sinkTable("insert into test.ext_olap select v1, v2 from test.t0");
        Assertions.assertFalse(verdicts.isEmpty(), "the copy-safety verdict was never taken");
        Assertions.assertFalse(verdicts.contains(false), "the INSERT was planned under the lock: " + verdicts);
    }

    /**
     * The sync used to write through the shallow copy into the published table: the remote ids into its
     * persisted ExternalTableInfo, and every synced physical partition into its partition map -- from any number
     * of concurrent INSERTs, without a lock. The copy now has its own of both.
     */
    @Test
    public void testSyncNeverWritesThePublishedTable() throws Exception {
        ExternalOlapTable published = published();
        long sourceDbId = published.getSourceTableDbId();
        long sourceTableId = published.getSourceTableId();
        int physicalPartitions = published.getPhysicalPartitionIdToPartitionId().size();

        ExternalOlapTable target = sinkTable("insert into test.ext_olap select v1, v2 from test.t0");
        Assertions.assertEquals(1, syncs.get());
        Assertions.assertNotSame(published, target);
        Assertions.assertEquals(2, target.getPartitions().size());
        Assertions.assertEquals(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "ext_src")
                .getId(), target.getSourceTableId());

        Assertions.assertEquals(sourceDbId, published.getSourceTableDbId());
        Assertions.assertEquals(sourceTableId, published.getSourceTableId());
        Assertions.assertEquals(physicalPartitions, published.getPhysicalPartitionIdToPartitionId().size());
        Assertions.assertTrue(published.getPartitions().isEmpty());
    }

    /**
     * An EXPLAIN begins no transaction, but it plans the INSERT all the same, so it gets a synced private copy
     * too -- it used to plan on the published table, which has no partitions, tablets or backends.
     */
    @Test
    public void testExplainPlansOnASyncedCopy() throws Exception {
        String plan = getFragmentPlan("explain insert into test.ext_olap select v1, v2 from test.t0");
        Assertions.assertEquals(1, syncs.get());
        assertContains(plan, "OLAP TABLE SINK");
        Assertions.assertTrue(published().getPartitions().isEmpty());
    }

    /**
     * With an unqualified name the EXPLAIN path has to hand the normalized table reference back too. A SELECT
     * that reads no internal table takes the deferred-lock path, which goes straight to InsertAnalyzer without
     * DMLStmtAnalyzer normalizing the reference; with the target already set, InsertAnalyzer skips its own
     * resolution, and the privilege check is then left with no database.
     */
    @Test
    public void testExplainWithAnUnqualifiedTableName() throws Exception {
        connectContext.changeCatalogDb("default_catalog.test");
        InsertStmt stmt = (InsertStmt) UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(
                "explain insert into ext_olap select 1, 2", connectContext);
        Assertions.assertNull(stmt.getTableRef().getDbName(), "the fixture is not an unqualified name");
        StatementPlanner.plan(stmt, connectContext);
        Assertions.assertEquals(1, syncs.get());
        Assertions.assertEquals("test", stmt.getTableRef().getDbName());
        Assertions.assertEquals("default_catalog", stmt.getTableRef().getCatalogName());
    }

    /** The two paths that skip the sync cannot work on an external OLAP table, so they are refused up front. */
    @Test
    public void testOverwriteAndExplicitTransactionAreRefused() throws Exception {
        Exception overwrite = Assertions.assertThrows(Exception.class, () -> UtFrameUtils.getPlanAndFragment(
                connectContext, "insert overwrite test.ext_olap select v1, v2 from test.t0"));
        Assertions.assertTrue(overwrite.getMessage().contains("INSERT OVERWRITE is not supported on an external"),
                overwrite.getMessage());
        // EXPLAIN syncs the target before analysis, which then skips resolving it; the check must not go with it.
        Exception explainOverwrite = Assertions.assertThrows(Exception.class, () -> UtFrameUtils.getPlanAndFragment(
                connectContext, "explain insert overwrite test.ext_olap select v1, v2 from test.t0"));
        Assertions.assertTrue(
                explainOverwrite.getMessage().contains("INSERT OVERWRITE is not supported on an external"),
                explainOverwrite.getMessage());

        connectContext.setTxnId(10010L);
        try {
            Exception explicitTxn = Assertions.assertThrows(Exception.class, () -> UtFrameUtils.getPlanAndFragment(
                    connectContext, "insert into test.ext_olap select v1, v2 from test.t0"));
            Assertions.assertTrue(explicitTxn.getMessage().contains("explicit transaction"),
                    explicitTxn.getMessage());
        } finally {
            connectContext.setTxnId(0);
        }
        Assertions.assertEquals(0, syncs.get());
    }
}
