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
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class DatabaseQuotaRefresherTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
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
        String result = (String) method.invoke(dbQuotaRefresher);

        Database database1 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test1");
        Assert.assertEquals(0, database1.getDataQuota());
        Assert.assertEquals(0, database1.getReplicaQuota());

        Assert.assertThrows(StarRocksException.class,
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().checkDataSizeQuota(database1));
        Assert.assertThrows(StarRocksException.class,
                () -> GlobalStateMgr.getCurrentState().getLocalMetastore().checkReplicaQuota(database1));


        Database database2 = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test2");
        Assert.assertEquals(100, database2.getDataQuota());
        Assert.assertEquals(100, database2.getReplicaQuota());

        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().checkDataSizeQuota(database2);
        } catch (Exception e) {
            Assert.fail();
        }

        try {
            GlobalStateMgr.getCurrentState().getLocalMetastore().checkReplicaQuota(database2);
        } catch (Exception e) {
            Assert.fail();
        }
    }
}
