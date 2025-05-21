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

package com.starrocks.lake;

import com.google.common.collect.Lists;
import com.staros.proto.ShardGroupInfo;
import com.starrocks.catalog.AggregateType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartitionImpl;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.TabletMeta;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
import com.starrocks.thrift.TStorageMedium;
import com.starrocks.thrift.TStorageType;
import com.starrocks.transaction.TransactionState;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LakeTableHelperTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_table_helper";

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getMetadata().createDb(createDbStmt.getFullDbName());
    }

    @AfterClass
    public static void afterClass() {
    }

    private static LakeTable createTable(String sql) throws Exception {
        CreateTableStmt createTableStmt = (CreateTableStmt) UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createTable(createTableStmt);
        Table table = testDb().getTable(createTableStmt.getTableName());
        return (LakeTable) table;
    }

    private static Database testDb() {
        return GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);
    }

    @Test
    public void testSupportCombinedTxnLog() throws Exception {
        new MockUp<RunMode>() {
            @Mock
            public boolean isSharedDataMode() {
                return true;
            }
        };
        Config.lake_use_combined_txn_log = true;

        Database db = testDb();
        LakeTable table = createTable(
                String.format("CREATE TABLE %s.t0(c0 INT) DUPLICATE KEY(c0) DISTRIBUTED BY HASH(c0) BUCKETS 1",
                        DB_NAME));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.FRONTEND_STREAMING));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.INSERT_STREAMING));

        Config.lake_use_combined_txn_log = false;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));

        Config.lake_use_combined_txn_log = true;
        long invalidDbId = db.getId() + 1;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(invalidDbId, Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));

        long invalidTableId = table.getId() + 1;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(invalidTableId),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));

        table.setState(OlapTable.OlapTableState.SCHEMA_CHANGE);
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(db.getId(), Collections.singletonList(table.getId()),
                TransactionState.LoadJobSourceType.BACKEND_STREAMING));
    }

    @Test
    public void testDeleteShardGroupMeta(@Mocked StarOSAgent starOSAgent) {

        new MockUp<GlobalStateMgr>() {
            @Mock
            public StarOSAgent getStarOSAgent() {
                return starOSAgent;
            }
        };

        long tableId = 1001L;
        long partitionId = 1000L;
        long physicalPartitionId = 2000L;
        long groupIdToClear = 5100L;

        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList());
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(1000L, (short) 3);
        Partition partition =
                new Partition(partitionId, "p1", new MaterializedIndex(), distributionInfo);
        partition.addSubPartition(
                new PhysicalPartitionImpl(physicalPartitionId, "p10", partitionId, groupIdToClear, null));

        // build shardGroupInfos
        List<Long> allShardIds = Stream.of(1000L, 1001L, 1002L, 1003L).collect(Collectors.toList());
        List<ShardGroupInfo> shardGroupInfos = new ArrayList<>();
        ShardGroupInfo info = ShardGroupInfo.newBuilder()
                .setGroupId(groupIdToClear)
                .putLabels("tableId", String.valueOf(tableId))
                .putProperties("createTime", String.valueOf(System.currentTimeMillis() - 86400 * 1000))
                .addAllShardIds(allShardIds)
                .build();
        shardGroupInfos.add(info);
        new MockUp<StarOSAgent>() {
            @Mock
            public void deleteShardGroup(List<Long> groupIds) {
                for (long groupId : groupIds) {
                    shardGroupInfos.removeIf(item -> item.getGroupId() == groupId);
                }
            }
        };

        LakeTableHelper.deleteShardGroupMeta(partition);
        Assert.assertEquals(0, shardGroupInfos.size());
    }

    @Test
    public void testRestoreColumnUniqueIdIfNeeded() throws Exception {
        String sql = "create table test_lake_table_helper.test_tb (k1 int, k2 int, k3 varchar)";
        LakeTable table = createTable(sql);
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb(DB_NAME);

        // add one more index meta
        long indexId = 1000L;
        long partitionId = 1001L;
        KeysType keysType = KeysType.DUP_KEYS;
        Column c0 = new Column("c0", Type.INT, true, AggregateType.NONE, false, null, null);
        Column c1 = new Column("c1", Type.INT, true, AggregateType.NONE, false, null, null);

        DistributionInfo dist = new HashDistributionInfo(10, Arrays.asList(c0, c1));
        MaterializedIndex index = new MaterializedIndex(indexId, MaterializedIndex.IndexState.NORMAL);
        Partition partition = new Partition(partitionId, "t0", index, dist);
        TStorageMedium storage = TStorageMedium.HDD;
        TabletMeta tabletMeta =
                new TabletMeta(db.getId(), table.getId(), partition.getId(), index.getId(), 0, storage, true);
        for (int i = 0; i < 10; i++) {
            Tablet tablet = new LakeTablet(GlobalStateMgr.getCurrentState().getNextId());
            index.addTablet(tablet, tabletMeta);
        }
        table.addPartition(partition);
        table.setIndexMeta(index.getId(), "t0", Arrays.asList(c0, c1), 0, 0, (short) 1, TStorageType.COLUMN,
                keysType);
        List<Column> newIndexSchema = table.getSchemaByIndexId(indexId);
        List<Column> baseSchema = table.getBaseSchema();

        {
            // reset column unique id to invalid value
            c0.setUniqueId(-1);
            c1.setUniqueId(0);
            Assert.assertEquals(2, table.getIndexIdToSchema().size());

            // base schema is fine
            Assert.assertFalse(LakeTableHelper.restoreColumnUniqueId(baseSchema));
            // index schema needs to be restored
            Assert.assertTrue(LakeTableHelper.restoreColumnUniqueId(newIndexSchema));
            Assert.assertEquals(0, c0.getUniqueId());
            Assert.assertEquals(1, c1.getUniqueId());
            for (int ordinal = 0; ordinal < baseSchema.size(); ordinal++) {
                Column column = baseSchema.get(ordinal);
                Assert.assertEquals(ordinal, column.getUniqueId());
            }
        }

        {
            // reset column unique id to invalid value
            c0.setUniqueId(-1);
            c1.setUniqueId(0);
            // case for restoring table
            LakeTableHelper.restoreColumnUniqueIdIfNeeded(table);
            Assert.assertEquals(0, c0.getUniqueId());
            Assert.assertEquals(1, c1.getUniqueId());
            baseSchema = table.getBaseSchema();
            for (int ordinal = 0; ordinal < baseSchema.size(); ordinal++) {
                Column column = baseSchema.get(ordinal);
                Assert.assertEquals(ordinal, column.getUniqueId());
            }
        }

        {
            // reset column unique id to invalid value
            c0.setUniqueId(-1);
            c1.setUniqueId(0);
            baseSchema.get(2).setUniqueId(-1);
            // case for restoring table
            LakeTableHelper.restoreColumnUniqueIdIfNeeded(table);
            Assert.assertEquals(0, c0.getUniqueId());
            Assert.assertEquals(1, c1.getUniqueId());
            baseSchema = table.getBaseSchema();
            for (int ordinal = 0; ordinal < baseSchema.size(); ordinal++) {
                Column column = baseSchema.get(ordinal);
                Assert.assertEquals(ordinal, column.getUniqueId());
            }
        }
    }
}
