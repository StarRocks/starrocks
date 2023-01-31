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


package com.starrocks.service;


import com.clearspring.analytics.util.Lists;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

public class FrontendServiceImplTest {

    @Mocked
    ExecuteEnv exeEnv;
    private TUpdateResourceUsageRequest genUpdateResourceUsageRequest(
            long backendId, int numRunningQueries, long memLimitBytes, long memUsedBytes, int cpuUsedPermille) {
        TResourceUsage usage = new TResourceUsage();
        usage.setNum_running_queries(numRunningQueries);
        usage.setMem_limit_bytes(memLimitBytes);
        usage.setMem_used_bytes(memUsedBytes);
        usage.setCpu_used_permille(cpuUsedPermille);

        TUpdateResourceUsageRequest request = new TUpdateResourceUsageRequest();
        request.setResource_usage(usage);
        request.setBackend_id(backendId);

        return request;
    }

    @Test
    public void testUpdateResourceUsage() throws TException {
        QueryQueueManager queryQueueManager = QueryQueueManager.getInstance();
        Backend backend = new Backend();
        long backendId = 0;
        int numRunningQueries = 1;
        long memLimitBytes = 3;
        long memUsedBytes = 2;
        int cpuUsedPermille = 300;
        new MockUp<SystemInfoService>() {
            @Mock
            public Backend getBackend(long id) {
                if (id == backendId) {
                    return backend;
                }
                return null;
            }
        };
        new Expectations(queryQueueManager) {
            {
                queryQueueManager.maybeNotifyAfterLock();
                times = 1;
            }
        };

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TUpdateResourceUsageRequest request = genUpdateResourceUsageRequest(
                backendId, numRunningQueries, memLimitBytes, memUsedBytes, cpuUsedPermille);

        // Notify pending queries.
        impl.updateResourceUsage(request);
        Assert.assertEquals(numRunningQueries, backend.getNumRunningQueries());
        Assert.assertEquals(memLimitBytes, backend.getMemLimitBytes());
        Assert.assertEquals(memUsedBytes, backend.getMemUsedBytes());
        Assert.assertEquals(cpuUsedPermille, backend.getCpuUsedPermille());
        // Don't notify, because this BE doesn't exist.
        request.setBackend_id(/* Not Exist */ 1);
        impl.updateResourceUsage(request);
    }

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        FeConstants.default_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        Config.enable_expression_partition = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE site_access(\n" +
                        "    event_day DATE,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day) (\n" +
                        "START (\"2015-06-01\") END (\"2022-12-01\") EVERY (INTERVAL 1 month)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE site_access_2(\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY time_slice(event_day, interval 5 day) (\n" +
                        "START (\"2015-06-01\") END (\"2022-12-01\") EVERY (INTERVAL 1 year)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\"replication_num\" = \"1\");");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table site_access";
        String dropSQL2 = "drop table site_access_2";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().dropTable(dropTableStmt);
            DropTableStmt dropTableStmt2 = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL2, ctx);
            GlobalStateMgr.getCurrentState().dropTable(dropTableStmt2);
        } catch (Exception ex) {

        }
    }

    @Test
    public void testCreatePartitionApi() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartitionValues(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Partition p19900424 = table.getPartition("p19900424");
        Assert.assertNotNull(p19900424);
    }

}
