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

package com.starrocks.analysis;

import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import java.util.ArrayList;
import java.util.List;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CreateTableAutoTabletTest {
    @BeforeClass
    public static void setUp() throws Exception {
        // set some parameters to speedup test
        Config.enable_auto_tablet_distribution = true;
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
        Assert.assertEquals(bucketNum, 20);
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
        Assert.assertEquals(GlobalStateMgr.getCurrentSystemInfo().getBackendIds().size(), 10);
        Assert.assertEquals(bucketNum, 20);
    }

    @Test
    public void test1AutoTabletWithDynamicPartition() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("db_for_auto_tablets",
                " CREATE TABLE test_auto_tablets_of_dynamic_partition (" +
                     "    k1 date," +
                     "    k2 int(11)," +
                     "    k3 smallint(6)," +
                     "    v1 varchar(2048)," +
                     "    v2 datetime" +
                     "  ) ENGINE=OLAP" +
                     "  DUPLICATE KEY(k1, k2, k3) " +
                     "  PARTITION BY RANGE(k1)" +
                     "  (PARTITION p20230306 VALUES [('2023-03-06'), ('2023-03-07')))" +
                     "  DISTRIBUTED BY HASH(k2) BUCKETS 10" +
                     "  PROPERTIES (" +
                     "   'replication_num' = '1'," +
                     "   'dynamic_partition.enable' = 'true'," +
                     "   'dynamic_partition.time_unit' = 'DAY'," +
                     "   'dynamic_partition.time_zone' = 'Asia/Shanghai'," +
                     "   'dynamic_partition.start' = '-3'," +
                     "   'dynamic_partition.end' = '3'," +
                     "   'dynamic_partition.prefix' = 'p');");
        Thread.sleep(1000); // wait for the dynamic partition created
        Database db = GlobalStateMgr.getCurrentState().getDb("db_for_auto_tablets");
        if (db == null) {
            return;
        }

        OlapTable table = (OlapTable) db.getTable("test_auto_tablets_of_dynamic_partition");
        if (table == null) {
            return;
        }

        int bucketNum = 0;
        db.readLock();
        try {
            List<Partition> partitions = (List<Partition>) table.getRecentPartitions(3);
            bucketNum = partitions.get(0).getDistributionInfo().getBucketNum();
        } finally {
            db.readUnlock();
        }
        Assert.assertEquals(bucketNum, 10);
    }

    @Test
    public void test1AutoTabletWithModifyDynamicPartitionProperty() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("db_for_auto_tablets",
                " CREATE TABLE test_modify_dynamic_partition_property (" +
                     "    k1 date," +
                     "    k2 int(11)," +
                     "    k3 smallint(6)," +
                     "    v1 varchar(2048)," +
                     "    v2 datetime" +
                     "  ) ENGINE=OLAP" +
                     "  DUPLICATE KEY(k1, k2, k3) " +
                     "  PARTITION BY RANGE(k1)" +
                     "  (PARTITION p20230306 VALUES [('2023-03-06'), ('2023-03-07')))" +
                     "  DISTRIBUTED BY HASH(k2) BUCKETS 10" +
                     "  PROPERTIES (" +
                     "   'replication_num' = '1'," +
                     "   'dynamic_partition.enable' = 'true'," +
                     "   'dynamic_partition.time_unit' = 'DAY'," +
                     "   'dynamic_partition.time_zone' = 'Asia/Shanghai'," +
                     "   'dynamic_partition.start' = '-1'," +
                     "   'dynamic_partition.end' = '3'," +
                     "   'dynamic_partition.buckets' = '3'," +
                     "   'dynamic_partition.prefix' = 'p');");
        Thread.sleep(1000); // wait for the dynamic partition created
        Database db = GlobalStateMgr.getCurrentState().getDb("db_for_auto_tablets");
        if (db == null) {
            return;
        }

        OlapTable table = (OlapTable) db.getTable("test_modify_dynamic_partition_property");
        if (table == null) {
            return;
        }

        cluster.runSql("db_for_auto_tablets", "ALTER TABLE test_modify_dynamic_partition_property SET ('dynamic_partition.enable' = 'false')");
        cluster.runSql("db_for_auto_tablets", "ALTER TABLE test_modify_dynamic_partition_property ADD PARTITION p20230306 VALUES [('2023-03-06'), ('2023-03-07'))");
        cluster.runSql("db_for_auto_tablets", "ALTER TABLE test_modify_dynamic_partition_property SET ('dynamic_partition.enable' = 'true')");

        int bucketNum = 0;
        db.readLock();
        try {
            Partition partition = table.getPartition("p20230306");
            bucketNum = partition.getDistributionInfo().getBucketNum();
        } finally {
            db.readUnlock();
        }
        Assert.assertEquals(bucketNum, 10);
    }

    @Test
    public void test1AutoTabletWithColocate() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("db_for_auto_tablets",
                " CREATE TABLE colocate_partition (" +
                     "    k1 date," +
                     "    k2 int(11)," +
                     "    k3 smallint(6)," +
                     "    v1 varchar(2048)," +
                     "    v2 datetime" +
                     "  ) ENGINE=OLAP" +
                     "  DUPLICATE KEY(k1, k2, k3) " +
                     "  PARTITION BY RANGE(k1)" +
                     "  (PARTITION p20230306 VALUES [('2023-03-06'), ('2023-03-07')))" +
                     "  DISTRIBUTED BY HASH(k2) BUCKETS 10" +
                     "  PROPERTIES (" +
                     "   'replication_num' = '1'," +
                     "   'colocate_with' = 'g1');");
        Database db = GlobalStateMgr.getCurrentState().getDb("db_for_auto_tablets");
        if (db == null) {
            return;
        }

        OlapTable table = (OlapTable) db.getTable("colocate_partition");
        if (table == null) {
            return;
        }

        cluster.runSql("db_for_auto_tablets", "ALTER TABLE colocate_partition ADD PARTITION p20230312 VALUES [('2023-03-12'), ('2023-03-13'))");
        checkTableStateToNormal(table);

        int bucketNum = 0;
        db.readLock();
        try {
            Partition partition = table.getPartition("p20230312");
            bucketNum = partition.getDistributionInfo().getBucketNum();
        } finally {
            db.readUnlock();
        }
        Assert.assertEquals(bucketNum, 10);
    }


    @Test
    public void createBadDbName() {
        String longDbName = new String(new char[257]).replace('\0', 'a');
        String sql = "create database " + longDbName;
        try {
            UtFrameUtils.parseStmtWithNewParser(sql, UtFrameUtils.createDefaultCtx());
            Assert.fail(); // should raise Exception
        } catch (Exception e) {
            Assert.assertEquals("Getting analyzing error. Detail message: Incorrect database name '"
                    + longDbName + "'.", e.getMessage());
        }
    }

    @Test
    public void createLongDbName() throws Exception {
        String longDbName = new String(new char[256]).replace('\0', 'a');
        String sql = "create database " + longDbName;
        UtFrameUtils.parseStmtWithNewParser(sql, UtFrameUtils.createDefaultCtx());
    }
}
