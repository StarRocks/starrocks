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

package com.starrocks.consistency;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.LocalTablet;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Replica;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.proc.BaseProcResult;
import com.starrocks.common.util.TimeUtils;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.transaction.PartitionCommitInfo;
import com.starrocks.transaction.TableCommitInfo;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionStatus;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MetaRecoveryDaemonTest {
    @BeforeClass
    public static void setUp() throws Exception {
        PseudoCluster.getOrCreateWithRandomPort(true, 7);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database test");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testRecover() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        String sql = "CREATE TABLE test.`tbl_recover` (\n" +
                "    k1 int,\n" +
                "    k2 VARCHAR NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 8\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"3\",\n" +
                "    \"in_memory\" = \"false\"\n" +
                ");";
        cluster.runSql("test", sql);
        cluster.runSql("test", "insert into test.tbl_recover values (1, 'a'), (2, 'b')");

        // wait insert to finish
        Thread.sleep(2000L);

        Database database = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(database.getFullName(), "tbl_recover");
        Partition partition = table.getPartition("tbl_recover");
        MaterializedIndex index = partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL).get(0);
        for (Tablet tablet : index.getTablets()) {
            for (Replica replica : tablet.getAllReplicas()) {
                Assert.assertEquals(2L, replica.getVersion());
            }
        }

        Assert.assertEquals(2L, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assert.assertEquals(3L, partition.getDefaultPhysicalPartition().getNextVersion());

        // set partition version to a lower value
        partition.getDefaultPhysicalPartition().setVisibleVersion(1L, System.currentTimeMillis());
        partition.getDefaultPhysicalPartition().setNextVersion(2L);

        for (Backend backend : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            backend.getBackendStatus().lastSuccessReportTabletsTime = TimeUtils
                    .longToTimeString(System.currentTimeMillis());
        }

        // add a committed txn
        TransactionState transactionState = new TransactionState(database.getId(), Lists.newArrayList(table.getId()),
                11111, "xxxx", null, TransactionState.LoadJobSourceType.FRONTEND, null, 2222, 100000);
        TableCommitInfo tableCommitInfo = new TableCommitInfo(table.getId());
        PartitionCommitInfo partitionCommitInfo = new PartitionCommitInfo(partition.getDefaultPhysicalPartition().getId(),
                4, -1L);
        tableCommitInfo.addPartitionCommitInfo(partitionCommitInfo);
        transactionState.putIdToTableCommitInfo(table.getId(), tableCommitInfo);
        transactionState.setTransactionStatus(TransactionStatus.COMMITTED);
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr()
                .getDatabaseTransactionMgr(database.getId()).replayUpsertTransactionState(transactionState);

        // recover will fail, because there is a committed txn on that partition
        MetaRecoveryDaemon recovery = new MetaRecoveryDaemon();
        recovery.recover();
        Assert.assertEquals(1L, partition.getDefaultPhysicalPartition().getVisibleVersion());
        BaseProcResult baseProcResult = new BaseProcResult();
        recovery.fetchProcNodeResult(baseProcResult);
        Assert.assertEquals(1, baseProcResult.getRows().size());

        // change the txn state to visible, recover will succeed
        transactionState.setTransactionStatus(TransactionStatus.VISIBLE);
        recovery.recover();
        Assert.assertEquals(2L, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assert.assertEquals(3L, partition.getDefaultPhysicalPartition().getNextVersion());
        baseProcResult = new BaseProcResult();
        recovery.fetchProcNodeResult(baseProcResult);
        Assert.assertEquals(0, baseProcResult.getRows().size());

        // change replica version
        LocalTablet localTablet = (LocalTablet) partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)
                .get(0).getTablets().get(0);
        long version = 3;
        for (Replica replica : localTablet.getAllReplicas()) {
            replica.updateForRestore(++version, 10, 10);
        }
        LocalTablet localTablet2 = (LocalTablet) partition.getDefaultPhysicalPartition()
                .getMaterializedIndices(MaterializedIndex.IndexExtState.ALL)
                .get(0).getTablets().get(0);
        for (Replica replica : localTablet2.getAllReplicas()) {
            replica.updateForRestore(4, 10, 10);
        }

        // set partition version to a lower value
        partition.getDefaultPhysicalPartition().setVisibleVersion(1L, System.currentTimeMillis());
        partition.getDefaultPhysicalPartition().setNextVersion(2L);

        // recover will fail, because there is no common version on tablets.
        recovery.recover();
        Assert.assertEquals(1L, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assert.assertEquals(2L, partition.getDefaultPhysicalPartition().getNextVersion());
        baseProcResult = new BaseProcResult();
        recovery.fetchProcNodeResult(baseProcResult);
        Assert.assertEquals(1, baseProcResult.getRows().size());

        // set replica version back to 2
        for (Replica replica : localTablet.getAllReplicas()) {
            replica.updateForRestore(2, 10, 10);
        }
        for (Replica replica : localTablet2.getAllReplicas()) {
            replica.updateForRestore(2, 10, 10);
        }
        Replica badReplica = localTablet2.getAllReplicas().get(0);
        badReplica.updateForRestore(5, 10, 10);
        Assert.assertFalse(badReplica.isBad());

        // recover will succeed
        recovery.recover();
        Assert.assertEquals(2L, partition.getDefaultPhysicalPartition().getVisibleVersion());
        Assert.assertEquals(3L, partition.getDefaultPhysicalPartition().getNextVersion());
        baseProcResult = new BaseProcResult();
        recovery.fetchProcNodeResult(baseProcResult);
        Assert.assertEquals(0, baseProcResult.getRows().size());
        // replica with higher version will be set to bad
        Assert.assertTrue(badReplica.isBad());
    }

    @Test
    public void testCheckTabletReportCacheUp() {
        long timeMs = System.currentTimeMillis();
        MetaRecoveryDaemon metaRecoveryDaemon = new MetaRecoveryDaemon();
        for (Backend backend : GlobalStateMgr.getCurrentState().getNodeMgr().getClusterInfo().getBackends()) {
            backend.getBackendStatus().lastSuccessReportTabletsTime = TimeUtils
                    .longToTimeString(timeMs);
        }
        Assert.assertFalse(metaRecoveryDaemon.checkTabletReportCacheUp(timeMs + 1000L));
        Assert.assertTrue(metaRecoveryDaemon.checkTabletReportCacheUp(timeMs - 1000L));
    }
}
