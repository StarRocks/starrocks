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
package com.starrocks.planner.mv;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.mv.MVMetaVersionRepairer;
import com.starrocks.mv.MVRepairHandler;
import com.starrocks.schema.MTable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.optimizer.rule.transformation.materialization.MVTestBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

public class MVMetaVersionRepairerTest extends MVTestBase {
    private static MTable m1;
    @BeforeAll
    public static void beforeClass() throws Exception {
        MVTestBase.beforeClass();
        m1 = new MTable("m1", "k1",
                ImmutableList.of(
                        "k1 INT",
                        "k2 string",
                        "v1 INT",
                        "v2 INT",
                        "v3 string"
                ),
                "k1",
                ImmutableList.of(
                        "PARTITION `p1` VALUES LESS THAN ('3')",
                        "PARTITION `p2` VALUES LESS THAN ('6')",
                        "PARTITION `p3` VALUES LESS THAN ('9')"
                )
        );
    }

    private MVRepairHandler.PartitionRepairInfo toPartitionInfo(Partition partition, long version,
                                                                long versionTime) {
        return new MVRepairHandler.PartitionRepairInfo(partition.getId(), partition.getName(),
                partition.getDefaultPhysicalPartition().getVisibleVersion(), version, versionTime);
    }

    @Test
    public void testRepairBaseTableVersionChanges1() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, v1, v2 from m1;",
                    (obj) -> {
                        String mvName = (String) obj;
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        Table m1 = getTable("test", "m1");
                        Assertions.assertTrue(m1 != null);
                        Partition curPartition = m1.getPartition("p1");

                        MaterializedView mv1 = getMv("test", mvName);
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assertions.assertEquals("[p1]", mvNames.toString());
                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                mv1.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        logSysInfo(baseTableVisibleVersionMap);

                        // check mv version map before
                        Assertions.assertEquals(1, baseTableVisibleVersionMap.size());
                        Long baseTableId = baseTableVisibleVersionMap.keySet().iterator().next();
                        Map<String, MaterializedView.BasePartitionInfo> value =
                                baseTableVisibleVersionMap.values().iterator().next();
                        Assertions.assertEquals(1, value.size());
                        String baseTablePartitionName = value.keySet().iterator().next();
                        MaterializedView.BasePartitionInfo basePartitionInfo = value.get(baseTablePartitionName);

                        // repair base table version changes
                        long currentTs = System.currentTimeMillis();
                        // p1 has been refreshed, use curPartition as its partition
                        MVRepairHandler.PartitionRepairInfo partitionRepairInfo =
                                toPartitionInfo(curPartition, 100L, currentTs);

                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        Table baseTable = getTable("test", "m1");
                        MVMetaVersionRepairer.repairBaseTableVersionChanges(baseTable, ImmutableList.of(partitionRepairInfo));

                        // check mv version map after
                        baseTableVisibleVersionMap = asyncRefreshContext.getBaseTableVisibleVersionMap();
                        Assertions.assertEquals(1, baseTableVisibleVersionMap.size());
                        Assertions.assertEquals(baseTableId, baseTableVisibleVersionMap.keySet().iterator().next());
                        Map<String, MaterializedView.BasePartitionInfo> newValue =
                                baseTableVisibleVersionMap.values().iterator().next();
                        Assertions.assertEquals(1, newValue.size());
                        Assertions.assertEquals(baseTablePartitionName, newValue.keySet().iterator().next());
                        MaterializedView.BasePartitionInfo newBasePartitionInfo = newValue.get(baseTablePartitionName);
                        Assertions.assertEquals(100L, newBasePartitionInfo.getVersion());
                        Assertions.assertEquals(currentTs, newBasePartitionInfo.getLastRefreshTime());
                        Assertions.assertEquals(basePartitionInfo.getId(), newBasePartitionInfo.getId());
                    });
        });
    }

    @Test
    public void testRepairBaseTableVersionChanges2() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, v1, v2 from m1;",
                    (obj) -> {
                        String mvName = (String) obj;
                        MaterializedView mv1 = getMv("test", mvName);
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assertions.assertEquals("[]", mvNames.toString());
                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                mv1.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        logSysInfo(baseTableVisibleVersionMap);

                        // check mv version map before
                        Assertions.assertEquals(0, baseTableVisibleVersionMap.size());

                        // repair base table version changes
                        Table m1 = getTable("test", "m1");
                        Assertions.assertTrue(m1 != null);
                        Partition curPartition = m1.getPartition("p1");
                        long currentTs = System.currentTimeMillis();
                        MVRepairHandler.PartitionRepairInfo partitionRepairInfo =
                                toPartitionInfo(curPartition, 100L, currentTs);

                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        Table baseTable = getTable("test", "m1");
                        MVMetaVersionRepairer.repairBaseTableVersionChanges(baseTable, ImmutableList.of(partitionRepairInfo));

                        // Since mv has not refreshed, not repair it since mv's version map has not contained the old version
                        baseTableVisibleVersionMap = asyncRefreshContext.getBaseTableVisibleVersionMap();
                        Assertions.assertEquals(0, baseTableVisibleVersionMap.size());
                    });
        });
    }

    @Test
    public void testRepairBaseTableVersionChanges3() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, v1, v2 from m1;",
                    (obj) -> {
                        String mvName = (String) obj;
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", mvName);
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assertions.assertEquals("[p1]", mvNames.toString());
                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                mv1.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        logSysInfo(baseTableVisibleVersionMap);

                        // repair base table version changes
                        Table m1 = getTable("test", "m1");
                        Assertions.assertTrue(m1 != null);

                        // check mv version map before
                        Assertions.assertEquals(1, baseTableVisibleVersionMap.size());
                        Long baseTableId = baseTableVisibleVersionMap.keySet().iterator().next();
                        Map<String, MaterializedView.BasePartitionInfo> value =
                                baseTableVisibleVersionMap.values().iterator().next();
                        Assertions.assertEquals(1, value.size());
                        String baseTablePartitionName = value.keySet().iterator().next();
                        MaterializedView.BasePartitionInfo basePartitionInfo = value.get(baseTablePartitionName);
                        Partition p1 = m1.getPartition("p1");
                        Assertions.assertEquals(basePartitionInfo.getVersion(), p1.getDefaultPhysicalPartition().getVisibleVersion());
                        Assertions.assertEquals(basePartitionInfo.getLastRefreshTime(),
                                p1.getDefaultPhysicalPartition().getVisibleVersionTime());

                        Partition p2 = m1.getPartition("p2");
                        long currentTs = System.currentTimeMillis();
                        // p1 has been refreshed, but p2 has been compaction or fast schema changed, use curPartition as its
                        // partition
                        MVRepairHandler.PartitionRepairInfo partitionRepairInfo =
                                toPartitionInfo(p2, 100L, currentTs);

                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        Table baseTable = getTable("test", "m1");
                        MVMetaVersionRepairer.repairBaseTableVersionChanges(baseTable, ImmutableList.of(partitionRepairInfo));

                        baseTableVisibleVersionMap = asyncRefreshContext.getBaseTableVisibleVersionMap();
                        Assertions.assertEquals(1, baseTableVisibleVersionMap.size());
                        Assertions.assertEquals(baseTableId, baseTableVisibleVersionMap.keySet().iterator().next());
                        Map<String, MaterializedView.BasePartitionInfo> basePartitionInfoMap = baseTableVisibleVersionMap.get(m1.getId());
                        Assertions.assertEquals(1, basePartitionInfoMap.size());
                        logSysInfo(basePartitionInfoMap);
                        // p2 should not be in the version map
                        Assertions.assertFalse(basePartitionInfoMap.containsKey("p2"));
                    });
        });
    }

    @Test
    public void testRepairBaseTableVersionChanges4() {
        starRocksAssert.withTable(m1, () -> {
            cluster.runSql("test", "insert into m1 values (1,1,1,1,1), (4,2,1,1,1);");
            starRocksAssert.withMaterializedView("CREATE MATERIALIZED VIEW mv0 " +
                            " PARTITION BY (k1) " +
                            " DISTRIBUTED BY HASH(k1) " +
                            " REFRESH DEFERRED MANUAL " +
                            " PROPERTIES (\n" +
                            " 'transparent_mv_rewrite_mode' = 'true'" +
                            " ) " +
                            " AS SELECT k1, k2, v1, v2 from m1;",
                    (obj) -> {
                        String mvName = (String) obj;
                        starRocksAssert.refreshMvPartition(String.format("REFRESH MATERIALIZED VIEW mv0 \n" +
                                "PARTITION START ('%s') END ('%s')", "1", "3"));
                        MaterializedView mv1 = getMv("test", mvName);
                        Set<String> mvNames = mv1.getPartitionNames();
                        Assertions.assertEquals("[p1]", mvNames.toString());
                        MaterializedView.AsyncRefreshContext asyncRefreshContext =
                                mv1.getRefreshScheme().getAsyncRefreshContext();
                        Map<Long, Map<String, MaterializedView.BasePartitionInfo>> baseTableVisibleVersionMap =
                                asyncRefreshContext.getBaseTableVisibleVersionMap();
                        logSysInfo(baseTableVisibleVersionMap);

                        // repair base table version changes
                        Table m1 = getTable("test", "m1");
                        Assertions.assertTrue(m1 != null);

                        // check mv version map before
                        Assertions.assertEquals(1, baseTableVisibleVersionMap.size());
                        Long baseTableId = baseTableVisibleVersionMap.keySet().iterator().next();
                        Map<String, MaterializedView.BasePartitionInfo> value =
                                baseTableVisibleVersionMap.values().iterator().next();
                        Assertions.assertEquals(1, value.size());
                        String baseTablePartitionName = value.keySet().iterator().next();
                        MaterializedView.BasePartitionInfo basePartitionInfo = value.get(baseTablePartitionName);
                        Partition p1 = m1.getPartition("p1");
                        long lastRefreshVersion = p1.getDefaultPhysicalPartition().getVisibleVersion();
                        long lastRefreshVersionTime = p1.getDefaultPhysicalPartition().getVisibleVersionTime();
                        Assertions.assertEquals(basePartitionInfo.getVersion(), lastRefreshVersion);
                        Assertions.assertEquals(basePartitionInfo.getLastRefreshTime(), lastRefreshVersionTime);

                        long currentTs = System.currentTimeMillis();
                        // p1 has been refreshed, but p2 has been compaction or fast schema changed, use curPartition as its
                        // partition
                        // p1 has been updated, so the version of p1 should be updated
                        p1.getDefaultPhysicalPartition()
                                .setVisibleVersion(lastRefreshVersion + 1, lastRefreshVersionTime + 1);
                        MVRepairHandler.PartitionRepairInfo partitionRepairInfo =
                                toPartitionInfo(p1, 100L, currentTs);

                        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
                        Table baseTable = getTable("test", "m1");
                        MVMetaVersionRepairer.repairBaseTableVersionChanges(baseTable, ImmutableList.of(partitionRepairInfo));

                        baseTableVisibleVersionMap = asyncRefreshContext.getBaseTableVisibleVersionMap();
                        Assertions.assertEquals(1, baseTableVisibleVersionMap.size());
                        Assertions.assertEquals(baseTableId, baseTableVisibleVersionMap.keySet().iterator().next());
                        Map<String, MaterializedView.BasePartitionInfo> basePartitionInfoMap = baseTableVisibleVersionMap.get(m1.getId());
                        Assertions.assertEquals(1, basePartitionInfoMap.size());
                        logSysInfo(basePartitionInfoMap);
                        basePartitionInfo = basePartitionInfoMap.get("p1");
                        // p1 should not been updated since it has been updated
                        Assertions.assertEquals(basePartitionInfo.getVersion(), lastRefreshVersion);
                        Assertions.assertEquals(basePartitionInfo.getLastRefreshTime(), lastRefreshVersionTime);
                        Assertions.assertFalse(basePartitionInfoMap.containsKey("p2"));
                    });
        });
    }
}
