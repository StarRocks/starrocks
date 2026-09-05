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

package com.starrocks.server;

import com.starrocks.catalog.Database;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.InvalidConfException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DatabaseQuotaRefresherTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        Config.alter_scheduler_interval_millisecond = 1000;
        FeConstants.runningUnitTest = true;

        UtFrameUtils.createMinStarRocksCluster(true, RunMode.SHARED_NOTHING);

        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        connectContext.setQueryId(UUIDUtil.genUUID());
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test1").useDatabase("test1")
                .withTable("create table t1(c1 int, c2 int) properties('replication_num' = '1')");
        starRocksAssert.withDatabase("test2").useDatabase("test2")
                .withTable("create table t2(c1 int, c2 int) properties('replication_num' = '1')");

        Database database1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test1");
        database1.setDataQuota(0);
        database1.setReplicaQuota(0);

        Database database2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test2");
        database2.setDataQuota(100);
        database2.setReplicaQuota(100);
    }

    @Test
    public void test() throws DdlException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        DatabaseQuotaRefresher dbQuotaRefresher = new DatabaseQuotaRefresher();
        Method method = DatabaseQuotaRefresher.class.getDeclaredMethod("updateAllDatabaseUsedDataQuota");
        method.setAccessible(true);
        method.invoke(dbQuotaRefresher);

        Database database1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test1");
        Assertions.assertEquals(0, database1.getDataQuota());
        Assertions.assertEquals(0, database1.getReplicaQuota());

        Assertions.assertThrows(StarRocksException.class,
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().checkDataSizeQuota(database1));
        Assertions.assertThrows(StarRocksException.class,
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().checkReplicaQuota(database1));

        Database database2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test2");
        Assertions.assertEquals(100, database2.getDataQuota());
        Assertions.assertEquals(100, database2.getReplicaQuota());

        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().checkDataSizeQuota(database2);
        } catch (Exception e) {
            Assertions.fail();
        }

        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().checkReplicaQuota(database2);
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testCheckAndMayUpdateInterval_NormalUpdate() throws Exception {
        DatabaseQuotaRefresher dbQuotaRefresher = new DatabaseQuotaRefresher();

        // Initial interval should be from Config (300s = 300000ms)
        Assertions.assertEquals(300000L, dbQuotaRefresher.getInterval());

        // Change the config value
        int originalValue = Config.db_used_data_quota_update_interval_secs;
        try {
            starRocksAssert.ddl("ADMIN SET FRONTEND CONFIG ('db_used_data_quota_update_interval_secs' = '60')");

            // Call checkAndMayUpdateInterval
            dbQuotaRefresher.checkAndMayUpdateInterval();

            // Verify interval was updated
            Assertions.assertEquals(60000L, dbQuotaRefresher.getInterval());
        } finally {
            // Restore original value
            Config.db_used_data_quota_update_interval_secs = originalValue;
            dbQuotaRefresher.checkAndMayUpdateInterval();
        }
    }

    @Test
    public void testCheckAndMayUpdateInterval_MinimumRejected() throws Exception {
        DatabaseQuotaRefresher dbQuotaRefresher = new DatabaseQuotaRefresher();

        int originalValue = Config.db_used_data_quota_update_interval_secs;
        try {
            // Try to set a value below minimum (30s), rejected!
            Assertions.assertThrows(InvalidConfException.class, () ->
                    starRocksAssert.ddl("ADMIN SET FRONTEND CONFIG ('db_used_data_quota_update_interval_secs' = '10')")
            );

            // Call checkAndMayUpdateInterval
            dbQuotaRefresher.checkAndMayUpdateInterval();
            Assertions.assertEquals(originalValue, Config.db_used_data_quota_update_interval_secs);
            Assertions.assertEquals(originalValue * 1000L, dbQuotaRefresher.getInterval());
        } finally {
            // Restore original value
            Config.db_used_data_quota_update_interval_secs = originalValue;
            dbQuotaRefresher.checkAndMayUpdateInterval();
        }
    }

    @Test
    public void testCheckAndMayUpdateInterval_NoChange() throws Exception {
        DatabaseQuotaRefresher dbQuotaRefresher = new DatabaseQuotaRefresher();

        int originalValue = Config.db_used_data_quota_update_interval_secs;
        try {
            starRocksAssert.ddl("ADMIN SET FRONTEND CONFIG ('db_used_data_quota_update_interval_secs' = '120')");
            dbQuotaRefresher.checkAndMayUpdateInterval();

            long intervalAfterFirstUpdate = dbQuotaRefresher.getInterval();
            Assertions.assertEquals(120000L, intervalAfterFirstUpdate);

            // Call again without changing config - should be no-op
            dbQuotaRefresher.checkAndMayUpdateInterval();

            // Interval should remain the same
            Assertions.assertEquals(intervalAfterFirstUpdate, dbQuotaRefresher.getInterval());
        } finally {
            Config.db_used_data_quota_update_interval_secs = originalValue;
            dbQuotaRefresher.checkAndMayUpdateInterval();
        }
    }

    @Test
    public void testCheckAndMayUpdateInterval_BoundaryValue() throws Exception {
        DatabaseQuotaRefresher dbQuotaRefresher = new DatabaseQuotaRefresher();

        int originalValue = Config.db_used_data_quota_update_interval_secs;
        try {
            // Test exactly at minimum boundary
            starRocksAssert.ddl("ADMIN SET FRONTEND CONFIG ('db_used_data_quota_update_interval_secs' = '30')");

            dbQuotaRefresher.checkAndMayUpdateInterval();

            // Should accept exactly 30 seconds
            Assertions.assertEquals(30, Config.db_used_data_quota_update_interval_secs);
            Assertions.assertEquals(30000L, dbQuotaRefresher.getInterval());
        } finally {
            Config.db_used_data_quota_update_interval_secs = originalValue;
            dbQuotaRefresher.checkAndMayUpdateInterval();
        }
    }

    @Test
    public void testCheckAndMayUpdateInterval_VeryLargeValue() throws Exception {
        DatabaseQuotaRefresher dbQuotaRefresher = new DatabaseQuotaRefresher();

        int originalValue = Config.db_used_data_quota_update_interval_secs;
        try {
            // Test a very large value (1 hour)
            starRocksAssert.ddl("ADMIN SET FRONTEND CONFIG ('db_used_data_quota_update_interval_secs' = '3600')");

            dbQuotaRefresher.checkAndMayUpdateInterval();

            // Should accept large values
            Assertions.assertEquals(3600, Config.db_used_data_quota_update_interval_secs);
            Assertions.assertEquals(3600000L, dbQuotaRefresher.getInterval());
        } finally {
            Config.db_used_data_quota_update_interval_secs = originalValue;
            dbQuotaRefresher.checkAndMayUpdateInterval();
        }
    }
}
