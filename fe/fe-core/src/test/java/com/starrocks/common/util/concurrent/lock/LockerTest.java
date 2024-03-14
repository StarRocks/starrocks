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

package com.starrocks.common.util.concurrent.lock;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class LockerTest {
    public static ConnectContext connectContext;
    public static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        // disable checking tablets
        Config.tablet_sched_max_scheduling_tablets = -1;
        Config.alter_scheduler_interval_millisecond = 1;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
        connectContext.getSessionVariable().setOptimizerExecuteTimeout(30000);
        connectContext.getSessionVariable().setUseLowCardinalityOptimizeV2(false);
        FeConstants.enablePruneEmptyOutputScan = false;
        FeConstants.showJoinLocalShuffleInExplain = false;
        FeConstants.showFragmentCost = false;
        FeConstants.runningUnitTest = true;
    }

    @Test
    public void testDatabaseLocks() throws Exception {
        for (int i = 0; i < 10; i++) {
            String dbName = String.format("tmp_db_%d", i);
            starRocksAssert.withDatabase(dbName);
        }
        {
            Database db1 = GlobalStateMgr.getCurrentState().getDb("tmp_db_1");
            List<Database> dbs = List.of(db1, db1, db1);
            Locker locker = new Locker();
            try {
                locker.lockDatabases(dbs, LockType.READ);
            } finally {
                locker.unlockDatabases(dbs, LockType.READ);
            }
        }
        {
            Database db1 = GlobalStateMgr.getCurrentState().getDb("tmp_db_1");
            Database db2 = GlobalStateMgr.getCurrentState().getDb("tmp_db_2");
            Database db3 = GlobalStateMgr.getCurrentState().getDb("tmp_db_3");
            List<Database> dbs = List.of(db1, db1, db1, db2, db2, db3, db3, db2, db1, db2);
            Locker locker = new Locker();
            try {
                locker.lockDatabases(dbs, LockType.READ);
            } finally {
                locker.unlockDatabases(dbs, LockType.READ);
            }
        }
        {
            List<Database> dbs = Lists.newArrayList();
            for (int i = 0; i < 10; i++) {
                String dbName = String.format("tmp_db_%d", i);
                Database db = GlobalStateMgr.getCurrentState().getDb(dbName);
                dbs.add(db);
                dbs.add(db);
            }
            Locker locker = new Locker();
            try {
                locker.lockDatabases(dbs, LockType.READ);
            } finally {
                locker.unlockDatabases(dbs, LockType.READ);
            }
        }
    }
}
