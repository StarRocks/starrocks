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

package com.starrocks.qe;

import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.sys.SysObjectDependencies;
import com.starrocks.common.util.concurrent.lock.LockHoldDepth;
import com.starrocks.connector.ConnectorMetadata;
import com.starrocks.connector.MockedMetadataMgr;
import com.starrocks.connector.hive.MockedHiveMetadata;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.ShowStmt;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TObjectDependencyItem;
import com.starrocks.thrift.TObjectDependencyReq;
import com.starrocks.thrift.TObjectDependencyRes;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * SHOW MATERIALIZED VIEWS and sys.object_dependencies walk a database's materialized views under that database's
 * READ lock, and both used to resolve every base table from inside it -- for an external base table that is a
 * round trip to its catalog with the lock held, which the dynamic sql-test run (blocking-call validation in
 * error mode) reported at {@code ShowExecutor} and {@code SysObjectDependencies}.
 *
 * <p>Only the thread that installed the probe is sampled: the async MV plan cache resolves the same hive table
 * from its own executor and would otherwise poison the counters.
 */
public class MVMetadataReadLockConnectorIOTest extends MVTestBase {

    private static final String MV_NAME = "mv_meta_read_over_hive";

    private LockProbeHiveMetadata probe;
    private ConnectorMetadata originalHiveMetadata;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockHiveCatalog(connectContext);
        starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW " + MV_NAME + "\n"
                + "DISTRIBUTED BY RANDOM\n"
                + "REFRESH DEFERRED MANUAL\n"
                + "AS SELECT l_orderkey, l_suppkey FROM hive0.partitioned_db.lineitem_par;");
    }

    @AfterAll
    public static void afterClass() throws Exception {
        starRocksAssert.dropMaterializedView(MV_NAME);
    }

    private static class LockProbeHiveMetadata extends MockedHiveMetadata {
        private final Thread owner = Thread.currentThread();
        private final AtomicBoolean underLock = new AtomicBoolean(false);
        private final AtomicInteger calls = new AtomicInteger();

        @Override
        public Table getTable(ConnectContext context, String dbName, String tblName) {
            if (Thread.currentThread() == owner) {
                calls.incrementAndGet();
                if (LockHoldDepth.isUnderLock()) {
                    underLock.set(true);
                }
            }
            return super.getTable(context, dbName, tblName);
        }
    }

    private void installProbe() {
        MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
        if (originalHiveMetadata == null) {
            originalHiveMetadata = metadataMgr.getOptionalMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME)
                    .orElseThrow(() -> new IllegalStateException("hive0 catalog is not registered"));
        }
        probe = new LockProbeHiveMetadata();
        metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, probe);
    }

    @AfterEach
    public void removeProbe() {
        if (originalHiveMetadata != null) {
            MockedMetadataMgr metadataMgr = (MockedMetadataMgr) GlobalStateMgr.getCurrentState().getMetadataMgr();
            metadataMgr.registerMockedMetadata(MockedHiveMetadata.MOCKED_HIVE_CATALOG_NAME, originalHiveMetadata);
            originalHiveMetadata = null;
        }
    }

    /**
     * The base-table lookup there only decides whether a native base table needs a SELECT check, so an external
     * base table is not looked up at all -- which is why this test asserts no lookup under the lock rather than
     * a lookup outside it.
     */
    @Test
    public void testShowMaterializedViewsDoesNotResolveAnExternalBaseTableUnderTheLock() throws Exception {
        installProbe();
        ShowStmt stmt = (ShowStmt) UtFrameUtils.parseStmtWithNewParser(
                "SHOW MATERIALIZED VIEWS FROM " + DB_NAME + " WHERE NAME = '" + MV_NAME + "'", connectContext);
        ShowResultSet result = ShowExecutor.execute(stmt, connectContext);
        Assertions.assertEquals(1, result.getResultRows().size(), "the MV over hive must still be listed");
        Assertions.assertFalse(probe.underLock.get(),
                "SHOW MATERIALIZED VIEWS contacted the external catalog while holding the database lock");
    }

    @Test
    public void testObjectDependenciesResolveAnExternalBaseTableOutsideTheLock() {
        installProbe();
        TObjectDependencyRes response = SysObjectDependencies.listObjectDependencies(rootRequest());
        TObjectDependencyItem item = response.getItems().stream()
                .filter(x -> MV_NAME.equals(x.getObject_name()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("the MV over hive is missing from the dependencies"));
        // The external half is filled in after the lock is released; it must still be filled in.
        Assertions.assertEquals("HIVE", item.getRef_object_type());
        Assertions.assertEquals("lineitem_par", item.getRef_object_name());
        Assertions.assertTrue(probe.calls.get() > 0,
                "the probe never saw the connector, so this test proves nothing");
        Assertions.assertFalse(probe.underLock.get(),
                "sys.object_dependencies contacted the external catalog while holding the database lock");
    }

    private static TObjectDependencyReq rootRequest() {
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("root");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setCurrent_user_ident(userIdentity);
        authInfo.setUser("root");
        authInfo.setUser_ip("127.0.0.1");
        TObjectDependencyReq request = new TObjectDependencyReq();
        request.setAuth_info(authInfo);
        return request;
    }
}
