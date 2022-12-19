// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.analysis;

import com.starrocks.alter.AlterJobV2Test;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import org.jetbrains.annotations.TestOnly;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateTableAutoTabletTest {
    @BeforeClass
    public static void setUp() throws Exception {
        // set some parameters to speedup test
        PseudoCluster.getOrCreateWithRandomPort(true, 10);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database db_for_auto_tablets");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testAutoTabletWithoutPartition() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("db_for_auto_tablets",
                "create table test_table1 (pk bigint NOT NULL, v0 string not null) primary KEY (pk) DISTRIBUTED BY HASH(pk) PROPERTIES(\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
        Database db = GlobalStateMgr.getCurrentState().getDb("db_for_auto_tablets");
        if (db == null) {
            return;
        }
        db.readLock();
        int bucketNum = 0;
        try {
            OlapTable table = (OlapTable) db.getTable("test_table1");
            if (table == null) {
                return;
            }
            for (Partition partition : table.getPartitions()) {
                bucketNum += partition.getDistributionInfo().getBucketNum();
            }
        } finally {
            db.readUnlock();
        }
        Assert.assertEquals(bucketNum, 12);
    }

    private static void checkTableStateToNormal(OlapTable tb) throws InterruptedException {
        // waiting table state to normal
        int retryTimes = 5;
        while (tb.getState() != OlapTable.OlapTableState.NORMAL && retryTimes > 0) {
            Thread.sleep(5000);
            retryTimes--;
        }
        Assert.assertEquals(OlapTable.OlapTableState.NORMAL, tb.getState());
    }

    @Test
    public void test1AutoTabletWithPartition() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("db_for_auto_tablets",
                "CREATE TABLE test_table2(" +
                    "   pk1 bigint NOT NULL, " +
                    "   pk2 date NOT NULL, " +
                    "   v0 string NOT NULL" +
                    "  ) ENGINE=OLAP" +
                    " PRIMARY KEY(pk1, pk2)" +
                    " PARTITION BY RANGE(pk2) (START (\"2022-08-01\") END (\"2022-08-10\") EVERY (INTERVAL 1 day))" +
                    " DISTRIBUTED BY HASH(pk1)" +
                    " PROPERTIES (\"replication_num\" = \"3\", \"storage_medium\" = \"SSD\");");
        Database db = GlobalStateMgr.getCurrentState().getDb("db_for_auto_tablets");
        if (db == null) {
            return;
        }

        OlapTable table = (OlapTable) db.getTable("test_table2");
        if (table == null) {
            return;
        }

        cluster.runSql("db_for_auto_tablets", "ALTER TABLE test_table2 add partition p20220811 values less than(\"2022-08-11\")");
        checkTableStateToNormal(table);

        int bucketNum = 0;
        db.readLock();
        try {
            Partition partition = table.getPartition("p20220811");
            bucketNum = partition.getDistributionInfo().getBucketNum();
        } finally {
            db.readUnlock();
        }
        Assert.assertEquals(bucketNum, 12);
    }
}
