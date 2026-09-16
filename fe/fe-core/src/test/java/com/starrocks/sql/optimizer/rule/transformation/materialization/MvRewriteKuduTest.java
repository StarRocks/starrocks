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

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KuduTable;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvRefreshArbiter;
import com.starrocks.catalog.MvUpdateInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.mv.MVTimelinessArbiter;
import com.starrocks.connector.kudu.KuduMetadata;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.plan.ConnectorPlanTestBase;
import com.starrocks.type.IntegerType;
import com.starrocks.type.StringType;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class MvRewriteKuduTest extends MVTestBase {

    private static KuduTable kuduTable;

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        ConnectorPlanTestBase.mockCatalog(connectContext, ConnectorPlanTestBase.MOCK_KUDU_CATALOG_NAME);

        List<Column> columns = ImmutableList.of(
                new Column("col1", IntegerType.INT),
                new Column("col2", StringType.STRING));
        kuduTable = new KuduTable("localhost:7051", ConnectorPlanTestBase.MOCK_KUDU_CATALOG_NAME,
                "kudu_db", "kudu_tbl", "kudu_tbl", columns, new ArrayList<>());

        // The mocked kudu0 catalog is backed by a real KuduMetadata pointing at a fake master, so stub the
        // metadata lookups that MV creation / rewrite need to a canned unpartitioned table.
        new MockUp<KuduMetadata>() {
            @Mock
            public Table getTable(ConnectContext context, String dbName, String tblName) {
                return kuduTable;
            }

            @Mock
            public Database getDb(ConnectContext context, String dbName) {
                return new Database(GlobalStateMgr.getCurrentState().getNextId(), dbName);
            }
        };
        new MockUp<MetadataMgr>() {
            @Mock
            public Optional<Table> getTableWithIdentifier(ConnectContext context, BaseTableInfo baseTableInfo) {
                return Optional.of(kuduTable);
            }
        };
    }

    /**
     * Regression guard for the Kudu external-table MV query rewrite bug (same root cause as the Delta Lake case,
     * issue #11409): {@code KuduPartitionTraits#getUpdatedPartitionNames} used to return {@code null}, so the MV
     * timeliness arbiter returned {@link MvUpdateInfo.MvToRefreshType#FULL} and treated the MV as stale, disabling
     * query rewrite in the default CHECKED consistency mode. After the fix it returns an empty set, so the arbiter
     * returns {@link MvUpdateInfo.MvToRefreshType#NO_REFRESH} and the MV stays eligible for rewrite.
     */
    @Test
    public void testKuduUnPartitionedMvIsNotStaleInCheckedMode() throws Exception {
        String mvName = "mv_kudu_checked";
        // No query_rewrite_consistency property => defaults to CHECKED, the mode in which the bug manifested.
        starRocksAssert.withMaterializedView("create materialized view " + mvName +
                " refresh deferred manual " +
                " as select col1, col2 from kudu0.kudu_db.kudu_tbl");

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
