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

package com.starrocks.qe;

import com.starrocks.analysis.RedirectStatus;
import com.starrocks.common.ClientPool;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.pseudocluster.PseudoCluster;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TMasterOpRequest;
import com.starrocks.thrift.TMasterOpResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.utframe.MockGenericPool;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class LeaderOpExecutorTest {
    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;
    private static PseudoCluster cluster;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Config.bdbje_heartbeat_timeout_second = 60;
        Config.bdbje_replica_ack_timeout_second = 60;
        Config.bdbje_lock_timeout_second = 60;
        // set some parameters to speedup test
        Config.tablet_sched_checker_interval_seconds = 1;
        Config.tablet_sched_repair_delay_factor_second = 1;
        Config.enable_new_publish_mechanism = true;
        PseudoCluster.getOrCreateWithRandomPort(true, 1);
        GlobalStateMgr.getCurrentState().getTabletChecker().setInterval(1000);
        cluster = PseudoCluster.getInstance();

        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_experimental_mv = true;
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("d1").useDatabase("d1")
                .withTable(
                        "CREATE TABLE d1.t1(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');")
                .withTable(
                        "CREATE TABLE d1.t2(k1 int, k2 int, k3 int)" +
                                " distributed by hash(k1) buckets 3 properties('replication_num' = '1');");
    }

    @Test
    public void testResourceGroupNameInAuditLog() throws Exception {

        String createGroup = "create resource group rg1\n" +
                "to\n" +
                "    (db='d1')\n" +
                "with (\n" +
                "    'cpu_core_limit' = '1',\n" +
                "    'mem_limit' = '50%',\n" +
                "    'concurrency_limit' = '20',\n" +
                "    'type' = 'normal'\n" +
                ");";
        cluster.runSql("d1", createGroup);

        String sql = "insert into t1 select * from t1";
        StatementBase stmtBase = UtFrameUtils.parseStmtWithNewParser(sql, connectContext);
        LeaderOpExecutor executor =
                new LeaderOpExecutor(stmtBase, stmtBase.getOrigStmt(), connectContext, RedirectStatus.FORWARD_NO_SYNC);

        mockFrontendService(new MockFrontendServiceClient());
        executor.execute();

        Assert.assertEquals("rg1", connectContext.getAuditEventBuilder().build().resourceGroup);
    }

    private static class MockFrontendServiceClient extends FrontendService.Client {
        private final FrontendService.Iface frontendService = new FrontendServiceImpl(null);

        public MockFrontendServiceClient() {
            super(null);
        }

        @Override
        public TMasterOpResult forward(TMasterOpRequest params) throws TException {
            return frontendService.forward(params);
        }
    }

    private static void mockFrontendService(MockFrontendServiceClient client) {
        ClientPool.frontendPool = new MockGenericPool<FrontendService.Client>("leader-op-mocked-pool") {
            @Override
            public FrontendService.Client borrowObject(TNetworkAddress address, int timeoutMs) {
                return client;
            }
        };
    }
}
