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
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LakeTableHelperTest {
    private static ConnectContext connectContext;
    private static final String DB_NAME = "test_lake_table_helper";

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        // create database
        String createDbStmtStr = "create database " + DB_NAME;
        CreateDbStmt createDbStmt = (CreateDbStmt) UtFrameUtils.parseStmtWithNewParser(createDbStmtStr, connectContext);
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
    }

    @AfterAll
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
        Config.lake_use_combined_txn_log = true;
        Assertions.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
        Assertions.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK));
        Assertions.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.INSERT_STREAMING));
        Assertions.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB));
        Assertions.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.LAKE_COMPACTION));
        Assertions.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.FRONTEND_STREAMING));
        Assertions.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BYPASS_WRITE));
        Assertions.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.DELETE));
        Assertions.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.MV_REFRESH));
        Config.lake_use_combined_txn_log = false;
        Assertions.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
    }

    @Test
    public void testEnablePartitionAggregation() throws Exception {
        List<Long> tableIdList = Lists.newArrayList(1001L, 1002L, 1003L);
        Assertions.assertFalse(LakeTableHelper.fileBundling(1L, tableIdList));
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
        long physicalPartitionId = 1002L;
        long groupIdToClear = 5100L;

        DistributionInfo distributionInfo = new HashDistributionInfo(10, Lists.newArrayList());
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.setReplicationNum(1000L, (short) 3);
        Partition partition =
                new Partition(partitionId, physicalPartitionId, "p1", new MaterializedIndex(), distributionInfo);
        Collection<PhysicalPartition> subPartitions = partition.getSubPartitions();
        subPartitions.forEach(physicalPartition -> {
            MaterializedIndex materializedIndex =
                    physicalPartition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).get(0);
            materializedIndex.setShardGroupId(groupIdToClear);
        });

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
        Assertions.assertEquals(0, shardGroupInfos.size());
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
        Partition partition = new Partition(partitionId, partitionId + 100, "t0", index, dist);
        TStorageMedium storage = TStorageMedium.HDD;
        TabletMeta tabletMeta =
                new TabletMeta(db.getId(), table.getId(), partition.getId(), index.getId(), storage, true);
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
            Assertions.assertEquals(2, table.getIndexIdToSchema().size());

            // base schema is fine
            Assertions.assertFalse(LakeTableHelper.restoreColumnUniqueId(baseSchema));
            // index schema needs to be restored
            Assertions.assertTrue(LakeTableHelper.restoreColumnUniqueId(newIndexSchema));
            Assertions.assertEquals(0, c0.getUniqueId());
            Assertions.assertEquals(1, c1.getUniqueId());
            for (int ordinal = 0; ordinal < baseSchema.size(); ordinal++) {
                Column column = baseSchema.get(ordinal);
                Assertions.assertEquals(ordinal, column.getUniqueId());
            }
        }

        {
            // reset column unique id to invalid value
            c0.setUniqueId(-1);
            c1.setUniqueId(0);
            // case for restoring table
            LakeTableHelper.restoreColumnUniqueIdIfNeeded(table);
            Assertions.assertEquals(0, c0.getUniqueId());
            Assertions.assertEquals(1, c1.getUniqueId());
            baseSchema = table.getBaseSchema();
            for (int ordinal = 0; ordinal < baseSchema.size(); ordinal++) {
                Column column = baseSchema.get(ordinal);
                Assertions.assertEquals(ordinal, column.getUniqueId());
            }
        }

        {
            // reset column unique id to invalid value
            c0.setUniqueId(-1);
            c1.setUniqueId(0);
            baseSchema.get(2).setUniqueId(-1);
            // case for restoring table
            LakeTableHelper.restoreColumnUniqueIdIfNeeded(table);
            Assertions.assertEquals(0, c0.getUniqueId());
            Assertions.assertEquals(1, c1.getUniqueId());
            baseSchema = table.getBaseSchema();
            for (int ordinal = 0; ordinal < baseSchema.size(); ordinal++) {
                Column column = baseSchema.get(ordinal);
                Assertions.assertEquals(ordinal, column.getUniqueId());
            }
        }
    }

    @Test
    public void testExtractIdFromPath() {
        Optional<Long> result = LakeTableHelper.extractIdFromPath(null);
        Assertions.assertFalse(result.isPresent());

        result = LakeTableHelper.extractIdFromPath("12345");
        Assertions.assertFalse(result.isPresent());

        result = LakeTableHelper.extractIdFromPath("s3://bucket/path/12345");
        Assertions.assertTrue(result.isPresent());
        Assertions.assertEquals(12345L, result.get().longValue());
    }

    @Test
    public void testIsTransactionSupportCombinedTxnLog() {
        Assertions.assertTrue(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
        Assertions.assertTrue(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK));
        Assertions.assertTrue(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.INSERT_STREAMING));
        Assertions.assertTrue(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB));
        Assertions.assertTrue(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.LAKE_COMPACTION));
        Assertions.assertFalse(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.FRONTEND_STREAMING));
        Assertions.assertFalse(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.BYPASS_WRITE));
        Assertions.assertFalse(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.DELETE));
        Assertions.assertFalse(LakeTableHelper.isTransactionSupportCombinedTxnLog(TransactionState.LoadJobSourceType.MV_REFRESH));
    }
}
