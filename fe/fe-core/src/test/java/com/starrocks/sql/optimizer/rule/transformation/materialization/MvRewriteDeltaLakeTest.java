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

package com.starrocks.sql.optimizer.rule.transformation.materialization;

import com.google.common.base.Joiner;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.qe.ConnectContext;
import com.starrocks.scheduler.mv.pct.PCTTableSnapshotInfo;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import io.delta.kernel.Snapshot;
import io.delta.kernel.internal.SnapshotImpl;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Optional;

public class MvRewriteDeltaLakeTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, "deltalake_catalog");
    }

    private static MvUpdateInfo.MvToRefreshType timeliness(MaterializedView mv) {
        return MvRefreshArbiter.getMVTimelinessUpdateInfo(mv, MVTimelinessArbiter.QueryRewriteParams.ofRefresh())
                .getMVToRefreshType();
    }

    /**
     * In the default CHECKED consistency mode an MV over a Delta Lake table is fresh only while the table is at the
     * Delta version the MV was last refreshed from.
     */
    @Test
    public void testDeltaLakeMvIsFreshOnlyAtTheRefreshedVersion() throws Exception {
        // The mocked Delta Lake table has no snapshot, so stub a stable table identifier (used for base-table
        // matching) and, once the MV exists, a snapshot at a controllable version.
        new MockUp<DeltaLakeTable>() {
            @Mock
            public String getTableIdentifier() {
                return Joiner.on(":").join("tbl", "mocked-delta-uuid");
            }
        };
        Table deltaTable = GlobalStateMgr.getCurrentState().getMetadataMgr()
                .getTable(connectContext, "deltalake_catalog", "deltalake_db", "tbl");
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(ConnectContext context, BaseTableInfo baseTableInfo) {
                return Optional.of(deltaTable);
            }
        };

        String mvName = "mv_delta_checked";
        starRocksAssert.withMaterializedView("create materialized view " + mvName +
                " refresh deferred manual " +
                " as select col1, col2 from deltalake_catalog.deltalake_db.tbl");
        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName);

        long[] version = {7};
        SnapshotImpl snapshot = Mockito.mock(SnapshotImpl.class);
        Mockito.when(snapshot.getVersion()).thenAnswer(invocation -> version[0]);
        new MockUp<DeltaLakeTable>() {
            @Mock
            public Snapshot getDeltaSnapshot() {
                return snapshot;
            }
        };

        // never refreshed
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.FULL, timeliness(mv));

        // refreshed at version 7
        BaseTableInfo baseTableInfo = mv.getBaseTableInfos().get(0);
        PCTTableSnapshotInfo refreshed = new PCTTableSnapshotInfo(baseTableInfo, deltaTable);
        refreshed.updatePartitionInfos(List.of("tbl"));
        Assertions.assertEquals(7, refreshed.getRefreshedPartitionInfos().get("tbl").getVersion());
        mv.getRefreshScheme().getAsyncRefreshContext().getBaseTableRefreshInfo(baseTableInfo)
                .putAll(refreshed.getRefreshedPartitionInfos());
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, timeliness(mv));

        // a new Delta commit
        version[0] = 8;
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.FULL, timeliness(mv));

        starRocksAssert.dropMaterializedView(mvName);
    }
}
