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

package com.starrocks.alter.reshard;

import com.starrocks.catalog.ColocateGroupSchema;
import com.starrocks.catalog.ColocateRange;
import com.starrocks.catalog.ColocateRangeMgr;
import com.starrocks.catalog.ColocateTableIndex;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RangeDistributionInfo;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.Config;
import com.starrocks.lake.LakeTablet;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

public class RangeColocateTableTest {
    protected static ConnectContext connectContext;
    protected static StarRocksAssert starRocksAssert;
    private static Database db;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        Config.enable_range_distribution = true;

        starRocksAssert.withDatabase("test").useDatabase("test");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
    }

    @Test
    public void testCreateRangeColocateTable() throws Exception {
        String sql = "create table t_colocate_range (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg1:k1');";
        starRocksAssert.withTable(sql);

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_colocate_range");
        Assertions.assertNotNull(table);
        Assertions.assertTrue(table.isCloudNativeTableOrMaterializedView());
        Assertions.assertInstanceOf(RangeDistributionInfo.class, table.getDefaultDistributionInfo());

        // Verify colocate group
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        Assertions.assertTrue(colocateTableIndex.isColocateTable(table.getId()));
        ColocateTableIndex.GroupId groupId = colocateTableIndex.getGroup(table.getId());
        Assertions.assertNotNull(groupId);

        // Verify group schema is RANGE type
        ColocateGroupSchema schema = colocateTableIndex.getGroupSchema(groupId);
        Assertions.assertNotNull(schema);
        Assertions.assertTrue(schema.isRangeColocate());
        Assertions.assertEquals(DistributionInfo.DistributionInfoType.RANGE, schema.getDistributionType());
        Assertions.assertEquals(1, schema.getColocateColumnCount());

        // Verify range colocate uses PACK shard group, not MetaGroup
        Assertions.assertFalse(colocateTableIndex.isMetaGroupColocateTable(table.getId()));

        // Verify ColocateRangeMgr is initialized with ALL range
        ColocateRangeMgr rangeMgr = colocateTableIndex.getColocateRangeMgr();
        Assertions.assertTrue(rangeMgr.containsColocateGroup(groupId.grpId));
        List<ColocateRange> ranges = rangeMgr.getColocateRanges(groupId.grpId);
        Assertions.assertEquals(1, ranges.size());
        Assertions.assertTrue(ranges.get(0).getRange().isAll());

        // Verify tablets were created as LakeTablet
        PhysicalPartition partition = table.getPartitions().iterator().next()
                .getDefaultPhysicalPartition();
        MaterializedIndex index = partition.getLatestBaseIndex();
        List<Tablet> tablets = index.getTablets();
        Assertions.assertEquals(1, tablets.size());
        Assertions.assertInstanceOf(LakeTablet.class, tablets.get(0));
    }

    @Test
    public void testSecondTableJoinsExistingRangeColocateGroup() throws Exception {
        String sql1 = "create table t_colocate_join1 (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg_join:k1');";
        starRocksAssert.withTable(sql1);

        OlapTable table1 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_colocate_join1");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId1 = colocateTableIndex.getGroup(table1.getId());

        String sql2 = "create table t_colocate_join2 (k1 int, k2 int, v2 varchar(10))\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg_join:k1');";
        starRocksAssert.withTable(sql2);

        OlapTable table2 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_colocate_join2");

        // Both tables should be in the same colocate group
        ColocateTableIndex.GroupId groupId2 = colocateTableIndex.getGroup(table2.getId());
        Assertions.assertEquals(groupId1.grpId, groupId2.grpId);

        // Both should NOT be in metaGroups
        Assertions.assertFalse(colocateTableIndex.isMetaGroupColocateTable(table1.getId()));
        Assertions.assertFalse(colocateTableIndex.isMetaGroupColocateTable(table2.getId()));
    }

    @Test
    public void testRangeColocateSchemaMismatchRejected() throws Exception {
        String sql1 = "create table t_colocate_mismatch1 (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg_mismatch:k1');";
        starRocksAssert.withTable(sql1);

        // Different colocate column count should fail
        String sql2 = "create table t_colocate_mismatch2 (k1 int, k2 int, v2 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg_mismatch:k1,k2');";
        Assertions.assertThrows(Exception.class, () -> starRocksAssert.withTable(sql2));
    }

    @Test
    public void testUpdateLakeTableColocationInfoSkipsRangeColocate() throws Exception {
        String sql = "create table t_colocate_update (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg_update:k1');";
        starRocksAssert.withTable(sql);

        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_colocate_update");
        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();

        // Should NOT throw — skips MetaGroup operations for range colocate
        Assertions.assertDoesNotThrow(() ->
                colocateTableIndex.updateLakeTableColocationInfo(table, true /* isJoin */, null));
    }

    @Test
    public void testCreateMultipleIndependentRangeColocateGroups() throws Exception {
        String sql1 = "create table t_colocate_grp_a (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg_a:k1');";
        starRocksAssert.withTable(sql1);

        String sql2 = "create table t_colocate_grp_b (k1 int, k2 int, v1 int)\n" +
                "order by(k1, k2)\n" +
                "properties('replication_num' = '1', 'colocate_with' = 'rg_b:k1,k2');";
        starRocksAssert.withTable(sql2);

        OlapTable table1 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_colocate_grp_a");
        OlapTable table2 = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "t_colocate_grp_b");

        ColocateTableIndex colocateTableIndex = GlobalStateMgr.getCurrentState().getColocateTableIndex();
        ColocateTableIndex.GroupId groupId1 = colocateTableIndex.getGroup(table1.getId());
        ColocateTableIndex.GroupId groupId2 = colocateTableIndex.getGroup(table2.getId());

        Assertions.assertNotEquals(groupId1.grpId, groupId2.grpId);
        Assertions.assertEquals(1, colocateTableIndex.getGroupSchema(groupId1).getColocateColumnCount());
        Assertions.assertEquals(2, colocateTableIndex.getGroupSchema(groupId2).getColocateColumnCount());

        Assertions.assertFalse(colocateTableIndex.isMetaGroupColocateTable(table1.getId()));
        Assertions.assertFalse(colocateTableIndex.isMetaGroupColocateTable(table2.getId()));
    }
}
