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
package com.starrocks.sql.common;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.StructField;
import com.starrocks.catalog.StructType;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MetaUtilTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        String dbName = "test";
        starRocksAssert.withDatabase(dbName).useDatabase(dbName);

        connectContext.getSessionVariable().setMaxTransformReorderJoins(8);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setEnableReplicationJoin(false);

        starRocksAssert.withTable("CREATE TABLE `t0` (\n" +
                "  `v1` bigint NULL COMMENT \"\",\n" +
                "  `v2` bigint NULL COMMENT \"\",\n" +
                "  `v3` bigint NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`v1`, `v2`, v3)\n" +
                "DISTRIBUTED BY HASH(`v1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\"\n" +
                ");");
    }

    @Test
    public void testIsPartitionExist() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t0");
        List<Partition> partitionList = new ArrayList<>(table.getPartitions());
        Assert.assertFalse(MetaUtils.isPartitionExist(GlobalStateMgr.getCurrentState(),
                -1, -1, -1));
        Assert.assertFalse(MetaUtils.isPartitionExist(GlobalStateMgr.getCurrentState(),
                database.getId(), -1, -1));
        Assert.assertFalse(MetaUtils.isPartitionExist(GlobalStateMgr.getCurrentState(),
                database.getId(), table.getId(), -1));
        Assert.assertTrue(MetaUtils.isPartitionExist(GlobalStateMgr.getCurrentState(),
                database.getId(), table.getId(), partitionList.get(0).getId()));
    }

    @Test
    public void testIsPhysicalPartitionExist() {
        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(database.getFullName(), "t0");
        List<PhysicalPartition> partitionList = new ArrayList<>(table.getPhysicalPartitions());
        Assert.assertFalse(MetaUtils.isPhysicalPartitionExist(GlobalStateMgr.getCurrentState(),
                -1, -1, -1));
        Assert.assertFalse(MetaUtils.isPhysicalPartitionExist(GlobalStateMgr.getCurrentState(),
                database.getId(), -1, -1));
        Assert.assertFalse(MetaUtils.isPhysicalPartitionExist(GlobalStateMgr.getCurrentState(),
                database.getId(), table.getId(), -1));
        Assert.assertTrue(MetaUtils.isPhysicalPartitionExist(GlobalStateMgr.getCurrentState(),
                database.getId(), table.getId(), partitionList.get(0).getId()));
    }

    @Test
    public void testGetColumnsByColumnIds() {
        Column columnA = new Column("a", Type.INT);
        Column columnB = new Column("b", Type.STRING);
        Column columnC = new Column("c", new StructType(Lists.newArrayList(new StructField("f1", Type.INT))));
        Map<ColumnId, Column> schema = MetaUtils.buildIdToColumn(Lists.newArrayList(columnA, columnB, columnC));

        Assert.assertEquals(columnA,
                MetaUtils.getColumnsByColumnIds(schema, Lists.newArrayList(ColumnId.create("a"))).get(0));
        Assert.assertEquals(columnB,
                MetaUtils.getColumnsByColumnIds(schema, Lists.newArrayList(ColumnId.create("b"))).get(0));
        Assert.assertEquals(columnC,
                MetaUtils.getColumnsByColumnIds(schema, Lists.newArrayList(ColumnId.create("c"))).get(0));
    }

    @Test
    public void testGetColumnNamesByColumnIds() {
        Column columnA = new Column("a", Type.INT);
        Column columnB = new Column("b", Type.STRING);
        Column columnC = new Column("c", new StructType(Lists.newArrayList(new StructField("f1", Type.INT))));
        Map<ColumnId, Column> schema = MetaUtils.buildIdToColumn(Lists.newArrayList(columnA, columnB, columnC));

        Assert.assertEquals("a",
                MetaUtils.getColumnNamesByColumnIds(schema, Lists.newArrayList(ColumnId.create("a"))).get(0));
        Assert.assertEquals("b",
                MetaUtils.getColumnNamesByColumnIds(schema, Lists.newArrayList(ColumnId.create("b"))).get(0));
        Assert.assertEquals("c",
                MetaUtils.getColumnNamesByColumnIds(schema, Lists.newArrayList(ColumnId.create("c"))).get(0));
    }

    @Test
    public void testGetColumnIdsByColumnNames() {
        Column columnA = new Column("a", Type.INT);
        Column columnB = new Column("b", Type.STRING);
        Column columnC = new Column("c", new StructType(Lists.newArrayList(new StructField("f1", Type.INT))));

        OlapTable olapTable = new OlapTable(1111L, "t1", Lists.newArrayList(columnA, columnB, columnC),
                KeysType.AGG_KEYS, null, null);

        Assert.assertEquals(ColumnId.create("a"),
                MetaUtils.getColumnIdsByColumnNames(olapTable, Lists.newArrayList("a")).get(0));
        Assert.assertEquals(ColumnId.create("b"),
                MetaUtils.getColumnIdsByColumnNames(olapTable, Lists.newArrayList("b")).get(0));
        Assert.assertEquals(ColumnId.create("c"),
                MetaUtils.getColumnIdsByColumnNames(olapTable, Lists.newArrayList("c")).get(0));
    }
}
