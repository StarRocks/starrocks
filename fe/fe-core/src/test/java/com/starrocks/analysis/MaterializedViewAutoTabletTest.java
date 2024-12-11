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

<<<<<<< HEAD
import com.starrocks.alter.AlterJobV2Test;
=======
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.common.Config;
<<<<<<< HEAD
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import org.jetbrains.annotations.TestOnly;
=======
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class MaterializedViewAutoTabletTest {
    @BeforeClass
    public static void setUp() throws Exception {
        // set some parameters to speedup test
        Config.enable_auto_tablet_distribution = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 32);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database db_for_auto_tablets");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testMaterializedViewAutoTablet() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("db_for_auto_tablets",
                "create table test_table1 (k1 bigint, v0 int) DUPLICATE KEY (k1) DISTRIBUTED BY HASH(k1);");
<<<<<<< HEAD
        Database db = GlobalStateMgr.getCurrentState().getDb("db_for_auto_tablets");
=======
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db_for_auto_tablets");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        if (db == null) {
            return;
        }
        cluster.runSql("db_for_auto_tablets", "create materialized view mv1 as select k1, sum(v0) from test_table1 group by k1;");

        int bucketNum1 = 0;
        int bucketNum2 = 0;
<<<<<<< HEAD
        db.readLock();
        try {
            OlapTable table = (OlapTable) db.getTable("test_table1");
=======
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table1");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (table == null) {
                return;
            }
            for (Partition partition : table.getPartitions()) {
                bucketNum1 += partition.getDistributionInfo().getBucketNum();
            }

<<<<<<< HEAD
            table = (OlapTable) db.getTable("mv1");
=======
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "mv1");
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
            if (table == null) {
                return;
            }
            for (Partition partition : table.getPartitions()) {
                bucketNum2 += partition.getDistributionInfo().getBucketNum();
            }
        } finally {
<<<<<<< HEAD
            db.readUnlock();
=======
            locker.unLockDatabase(db.getId(), LockType.READ);
>>>>>>> edd5009ce6 ([Doc] Revise Backup Restore according to feedback (#53738))
        }
        Assert.assertEquals(bucketNum1, 6);
        Assert.assertEquals(bucketNum2, 6);
    }
}
