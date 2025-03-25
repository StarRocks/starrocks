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
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.CreateDbStmt;
import com.starrocks.sql.ast.CreateTableStmt;
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
import java.util.Collection;
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
        GlobalStateMgr.getCurrentState().getLocalMetastore().createDb(createDbStmt.getFullDbName());
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
        Config.lake_use_combined_txn_log = true;
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.ROUTINE_LOAD_TASK));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.INSERT_STREAMING));
        Assert.assertTrue(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BATCH_LOAD_JOB));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.LAKE_COMPACTION));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.FRONTEND_STREAMING));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BYPASS_WRITE));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.DELETE));
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.MV_REFRESH));
        Config.lake_use_combined_txn_log = false;
        Assert.assertFalse(LakeTableHelper.supportCombinedTxnLog(TransactionState.LoadJobSourceType.BACKEND_STREAMING));
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
        Assert.assertEquals(0, shardGroupInfos.size());
    }
}
