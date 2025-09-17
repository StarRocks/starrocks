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
import com.starrocks.common.util.concurrent.lock.LockType;
import com.starrocks.common.util.concurrent.lock.Locker;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class CTASAutoTabletTest {
    @BeforeAll
    public static void setUp() throws Exception {
        // set some parameters to speedup test
        Config.enable_auto_tablet_distribution = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 3);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql(null, "create database db_for_auto_tablets");
    }

    @AfterAll
    public static void tearDown() throws Exception {
        PseudoCluster.getInstance().shutdown(true);
    }

    @Test
    public void testCTASAutoTablet() throws Exception {
        PseudoCluster cluster = PseudoCluster.getInstance();
        cluster.runSql("db_for_auto_tablets",
                "create table test_table1 (k1 bigint, k2 bigint, v0 string) DUPLICATE KEY(k1) DISTRIBUTED BY HASH(k1);");
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("db_for_auto_tablets");
        if (db == null) {
            return;
        }
        cluster.runSql("db_for_auto_tablets", "create table ctas1 as select * from test_table1;");
        cluster.runSql("db_for_auto_tablets",
                "create table ctas2 distributed by hash(k1, k2) as select * from test_table1;");

        int bucketNum1 = 0;
        int bucketNum2 = 0;
        int bucketNum3 = 0;
        Locker locker = new Locker();
        locker.lockDatabase(db.getId(), LockType.READ);
        try {
            OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "test_table1");
            if (table == null) {
                return;
            }
            for (Partition partition : table.getPartitions()) {
                bucketNum1 += partition.getDistributionInfo().getBucketNum();
            }

            // ctas1
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "ctas1");
            if (table == null) {
                return;
            }
            for (Partition partition : table.getPartitions()) {
                bucketNum2 += partition.getDistributionInfo().getBucketNum();
            }

            // ctas2
            table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "ctas2");
            if (table == null) {
                return;
            }
            for (Partition partition : table.getPartitions()) {
                bucketNum3 += partition.getDistributionInfo().getBucketNum();
            }
        } finally {
            locker.unLockDatabase(db.getId(), LockType.READ);
        }
        Assertions.assertEquals(bucketNum1, Config.default_unpartitioned_table_bucket_num);
        Assertions.assertEquals(bucketNum2, 3);
        Assertions.assertEquals(bucketNum3, Config.default_unpartitioned_table_bucket_num);
    }
}
