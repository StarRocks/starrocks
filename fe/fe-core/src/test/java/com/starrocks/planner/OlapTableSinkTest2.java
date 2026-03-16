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

package com.starrocks.planner;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.StarRocksException;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TOlapTablePartition;
import com.starrocks.thrift.TOlapTablePartitionParam;
import com.starrocks.thrift.TWriteQuorumType;
import com.starrocks.type.IntegerType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

public class OlapTableSinkTest2 {
    private static StarRocksAssert starRocksAssert;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        String createTblStmtStr = "create table db2.tbl1(k1 varchar(32), k2 varchar(32), k3 varchar(32), k4 int) " +
                "AGGREGATE KEY(k1, k2, k3, k4) distributed by hash(k1) buckets 3 properties('replication_num' = '1');";
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        starRocksAssert = new StarRocksAssert(connectContext);
        starRocksAssert.withDatabase("db2");
        starRocksAssert.withTable(createTblStmtStr);

        String createRangeTableStmt = "create table db2.tbl_range(k1 int, k2 int, v1 int) " +
                "DUPLICATE KEY(k1) " +
                "PARTITION BY RANGE(k1) (" +
                "  PARTITION p1 VALUES LESS THAN ('10')," +
                "  PARTITION p2 VALUES LESS THAN ('20')" +
                ") DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES('replication_num' = '1');";
        starRocksAssert.withTable(createRangeTableStmt);

        String createListTableStmt = "create table db2.tbl_list(k1 int, k2 int, v1 int) " +
                "DUPLICATE KEY(k1) " +
                "PARTITION BY LIST(k1) (" +
                "  PARTITION p1 VALUES IN ('1', '2', '3')," +
                "  PARTITION p2 VALUES IN ('4', '5', '6')" +
                ") DISTRIBUTED BY HASH(k1) BUCKETS 3 PROPERTIES('replication_num' = '1');";
        starRocksAssert.withTable(createListTableStmt);
    }

    @Test
    public void testCreateLocationException() {
        new MockUp<PartitionInfo>() {
            @Mock
            public int getQuorumNum(long partitionId, TWriteQuorumType writeQuorum) {
                return 3;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "tbl1");

        TOlapTablePartitionParam partitionParam = new TOlapTablePartitionParam();
        TOlapTablePartition tPartition = new TOlapTablePartition();
        for (Partition partition : olapTable.getPartitions()) {
            for (PhysicalPartition physicalPartition : partition.getSubPartitions()) {
                tPartition.setId(physicalPartition.getId());
                partitionParam.addToPartitions(tPartition);
            }
        }

        try {
            OlapTableSink.createLocation(olapTable, partitionParam, false, null);
        } catch (StarRocksException e) {
            System.out.println(e.getMessage());
            Assertions.assertTrue(e.getMessage().contains("replicas: 10001:1/-1/1/0:NORMAL:ALIVE"));
            return;
        }
        Assertions.fail("must throw UserException");
    }

    @Test
    public void testRangePartitionWithDifferentDistributionColumns() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "tbl_range");

        // Get two partitions
        Partition p1 = olapTable.getPartition("p1");
        Partition p2 = olapTable.getPartition("p2");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);

        // Save original distribution info for p2 to restore later
        DistributionInfo originalDistInfo = p2.getDistributionInfo();

        try {
            // Change p2's distribution columns to k2 (different from p1's k1)
            HashDistributionInfo differentDistInfo = new HashDistributionInfo(
                    3, Lists.newArrayList(new Column("k2", IntegerType.INT)));
            p2.setDistributionInfo(differentDistInfo);

            List<Long> partitionIds = olapTable.getPartitions().stream()
                    .map(Partition::getId).collect(Collectors.toList());

            StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
                OlapTableSink.createPartition(db.getId(), olapTable, null,
                        false, 0, partitionIds, null);
            });
            Assertions.assertTrue(exception.getMessage().contains("different distribute columns"));
        } finally {
            p2.setDistributionInfo(originalDistInfo);
        }
    }

    @Test
    public void testListPartitionWithDifferentDistributionColumns() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db2");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "tbl_list");

        Partition p1 = olapTable.getPartition("p1");
        Partition p2 = olapTable.getPartition("p2");
        Assertions.assertNotNull(p1);
        Assertions.assertNotNull(p2);

        DistributionInfo originalDistInfo = p2.getDistributionInfo();

        try {
            HashDistributionInfo differentDistInfo = new HashDistributionInfo(
                    3, Lists.newArrayList(new Column("k2", IntegerType.INT)));
            p2.setDistributionInfo(differentDistInfo);

            List<Long> partitionIds = olapTable.getPartitions().stream()
                    .map(Partition::getId).collect(Collectors.toList());

            StarRocksException exception = Assertions.assertThrows(StarRocksException.class, () -> {
                OlapTableSink.createPartition(db.getId(), olapTable, null,
                        false, 0, partitionIds, null);
            });
            Assertions.assertTrue(exception.getMessage().contains("different distribute columns"));
        } finally {
            p2.setDistributionInfo(originalDistInfo);
        }
    }
}
