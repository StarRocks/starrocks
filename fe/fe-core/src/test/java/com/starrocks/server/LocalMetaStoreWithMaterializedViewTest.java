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

package com.starrocks.server;

import com.starrocks.catalog.DataProperty;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.MvPlanContext;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.persist.EditLog;
import com.starrocks.persist.ModifyPartitionInfo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.optimizer.CachingMvPlanContextBuilder;
import com.starrocks.sql.optimizer.MaterializedViewOptimizer;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import com.starrocks.thrift.TStorageMedium;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class LocalMetaStoreWithMaterializedViewTest extends MVTestBase  {

    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
    }

    @Test
    public void testGetPartitionIdToStorageMediumMap() throws Exception {
        new MockUp<MaterializedViewOptimizer>() {
            @Mock
            public MvPlanContext optimize(MaterializedView mv,
                                          ConnectContext connectContext) {
                return null;
            }
        };

        // Mock the cacheMaterializedView method to avoid actual caching in the background
        new MockUp<CachingMvPlanContextBuilder>() {
            @Mock
            public void cacheMaterializedView(MaterializedView mv) {
                // Do nothing to avoid actual caching
            }
        };
        starRocksAssert.withTable(
                "CREATE TABLE test.t1(k1 int, k2 int, k3 int)" +
                        " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
        starRocksAssert.withMaterializedView(
                "CREATE MATERIALIZED VIEW test.mv1\n" +
                        "distributed by hash(k1) buckets 3\n" +
                        "refresh async\n" +
                        "properties(\n" +
                        "'replication_num' = '1'\n" +
                        ")\n" +
                        "as\n" +
                        "select k1, k2 from test.t1;");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        new MockUp<PartitionInfo>() {
            @Mock
            public DataProperty getDataProperty(long partitionId) {
                return new DataProperty(TStorageMedium.SSD, 0);
            }
        };
        new MockUp<EditLog>() {
            @Mock
            public void logModifyPartition(ModifyPartitionInfo info) {
                Assertions.assertNotNull(info);
                Assertions.assertTrue(GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getId(), info.getTableId())
                        .isOlapTableOrMaterializedView());
                Assertions.assertEquals(TStorageMedium.HDD, info.getDataProperty().getStorageMedium());
                Assertions.assertEquals(DataProperty.MAX_COOLDOWN_TIME_MS, info.getDataProperty().getCooldownTimeMs());
            }
        };
        LocalMetastore localMetastore = connectContext.getGlobalStateMgr().getLocalMetastore();
        localMetastore.getPartitionIdToStorageMediumMap();
        // Clean test.mv1, avoid its refreshment affecting other cases in this testsuite.
        starRocksAssert.dropMaterializedView("test.mv1");
    }
}
