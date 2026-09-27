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
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class MvRewriteDeltaLakeTest extends MVTestBase {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, "deltalake_catalog");
    }

    /**
     * Regression guard for the Delta Lake un-partitioned MV query rewrite bug (issue #11409).
     *
     * <p>{@code DeltaLakePartitionTraits#getUpdatedPartitionNames} used to return {@code null}. The MV timeliness
     * arbiter interprets a null base-table update info as "unknown", returns {@link MvUpdateInfo.MvToRefreshType#FULL}
     * and therefore treats the MV as stale, disabling query rewrite in the default CHECKED consistency mode.
     *
     * <p>After the fix it returns an empty set ("no updated partitions detected"), so the arbiter returns
     * {@link MvUpdateInfo.MvToRefreshType#NO_REFRESH} and the MV stays eligible for rewrite. This test asserts that
     * timeliness decision directly, which is exactly the signal that gates the rewrite (a full end-to-end EXPLAIN
     * assertion is covered by the SQL test {@code test_mv_deltalake_rewrite}).
     */
    @Test
    public void testDeltaLakeUnPartitionedMvIsNotStaleInCheckedMode() throws Exception {
        // The mocked Delta Lake table has no snapshot, so stub a stable table identifier (used for base-table
        // matching) to avoid an NPE and keep the query/MV base-table identifiers consistent.
        new MockUp<DeltaLakeTable>() {
            @Mock
            public String getTableIdentifier() {
                return Joiner.on(":").join("tbl", "mocked-delta-uuid");
            }
        };
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(ConnectContext context, BaseTableInfo baseTableInfo) {
                return Optional.of(new DeltaLakeTable());
            }
        };

        String mvName = "mv_delta_checked";
        // No query_rewrite_consistency property => defaults to CHECKED, the mode in which the bug manifested.
        starRocksAssert.withMaterializedView("create materialized view " + mvName +
                " refresh deferred manual " +
                " as select col1, col2 from deltalake_catalog.deltalake_db.tbl");

        Database testDb = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        MaterializedView mv = (MaterializedView) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(testDb.getFullName(), mvName);

        MvUpdateInfo info = MvRefreshArbiter.getMVTimelinessUpdateInfo(
                mv, MVTimelinessArbiter.QueryRewriteParams.ofRefresh());
        // Before the fix this was FULL (stale => no rewrite); after the fix it must be NO_REFRESH (rewriteable).
        Assertions.assertEquals(MvUpdateInfo.MvToRefreshType.NO_REFRESH, info.getMVToRefreshType());
        Assertions.assertTrue(info.isValidRewrite());

        starRocksAssert.dropMaterializedView(mvName);
    }
}