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

package com.starrocks.sql;

import com.starrocks.catalog.Table;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.utframe.LockProbe;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Test;

/**
 * The privilege check runs between {@code analyzeStatement} and the point where the planner drops the
 * PlannerMetaLock, so everything it does happens inside the critical section. That is fine for the
 * privilege lookups themselves -- they read in-memory authorization state -- but
 * {@code Authorizer.checkTableAction} used to re-resolve the table through {@code MetadataMgr#getTable}
 * on the way in, and for a table that lives in an external catalog that resolution is a connector
 * round trip. The statement's internal tables are locked at that moment, so a slow HMS/JDBC/REST
 * endpoint stalls every DDL waiting on the database intention lock.
 *
 * <p>These tests sample {@link LockHoldDepth#isUnderLock()} inside the mocked connector and assert the
 * connector is never entered while a lock is held. The sampling is OR-accumulated: the same entry point
 * is reached from several paths within one statement, and a plain assignment would let the last
 * (lock-free) call overwrite an earlier violation and turn the test green for the wrong reason.
 */
public class AuthorizerLockConnectorIOTest extends ConnectorPlanTestBase {

    /**
     * Records whether the connector was entered while the calling thread held an FE metadata lock.
     */
    private static class LockProbeHiveMetadata extends MockedHiveMetadata {
        private static final String GET_TABLE = "getTable";
        private final LockProbe lock = LockProbe.onAnyThread();

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            lock.record(GET_TABLE);
            return super.getTable(context, dbName, tblName);
        }
    }

    private void assertConnectorStayedOutOfTheLock(String sql) throws Exception {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        ConnectorMetadata original = metadataMgr.getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME)
                .orElseThrow(() -> new IllegalStateException("hive0 catalog is not registered"));
        LockProbeHiveMetadata probe = new LockProbeHiveMetadata();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, probe);
        try {
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParserNotIncludeAnalyzer(sql, connectContext);
            StatementPlanner.plan(stmt, connectContext);
            probe.lock.assertReachedOutsideTheLock(LockProbeHiveMetadata.GET_TABLE,
                    "connector metadata was fetched while an FE metadata lock was held");
        } finally {
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, original);
        }
    }

    @Test
    public void testPrivilegeCheckDoesNotResolveAnExternalTableUnderTheLock() throws Exception {
        // t0 is internal, so the PlannerMetaLock is really taken and is still held when the privilege
        // check runs. The hive table is the one that check used to re-resolve.
        assertConnectorStayedOutOfTheLock("select * from t0 join hive0.tpch.customer on t0.v1 = hive0.tpch.customer.c_custkey");
    }

    @Test
    public void testPrivilegeCheckStaysOffTheConnectorForAnInsertReadingAnExternalTable() throws Exception {
        // INSERT adds a second privilege check (INSERT on the target) on top of the SELECT checks over
        // everything the query reads, and it reaches the planner through a different visitor.
        assertConnectorStayedOutOfTheLock("insert into t0 select c_custkey, c_custkey, c_custkey from hive0.tpch.customer");
    }

    @Test
    public void testPrivilegeCheckStaysOffTheConnectorForANestedExternalReference() throws Exception {
        // A CTE and a derived table reach the privilege check through a different part of the AST than
        // a plain join does, and each external reference used to cost its own resolution.
        assertConnectorStayedOutOfTheLock("with c as (select c_custkey from hive0.tpch.customer) "
                        + "select * from t0 join c on t0.v1 = c.c_custkey "
                        + "where t0.v2 in (select n_nationkey from hive0.tpch.nation)");
    }

    @Test
    public void testPrivilegeCheckStaysOffTheConnectorForTwoCatalogsInOneStatement() throws Exception {
        assertConnectorStayedOutOfTheLock("select * from t0 "
                        + "join hive0.tpch.customer on t0.v1 = hive0.tpch.customer.c_custkey "
                        + "join jdbc0.partitioned_db0.tbl0 on t0.v2 = jdbc0.partitioned_db0.tbl0.a");
    }
}
