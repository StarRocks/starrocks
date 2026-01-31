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

package com.starrocks.qe.scheduler;

import com.google.common.collect.ImmutableList;
import com.starrocks.catalog.ResourceGroup;
import com.starrocks.catalog.ResourceGroupClassifier;
import com.starrocks.catalog.ResourceGroupMgr;
import com.starrocks.common.FeConstants;
import com.starrocks.common.Pair;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.ha.LeaderInfo;
import com.starrocks.metric.MetricRepo;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.CoordinatorPreprocessor;
import com.starrocks.qe.DefaultCoordinator;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.rpc.ThriftConnectionPool;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.service.FrontendServiceImpl;
import com.starrocks.sql.plan.MockTpchStatisticStorage;
import com.starrocks.system.Frontend;
import com.starrocks.system.FrontendHbResponse;
import com.starrocks.thrift.FrontendService;
import com.starrocks.thrift.TFinishSlotRequirementRequest;
import com.starrocks.thrift.TFinishSlotRequirementResponse;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TNotifyForwardDeploymentFinishedRequest;
import com.starrocks.thrift.TNotifyForwardDeploymentFinishedRespone;
import com.starrocks.thrift.TReleaseSlotRequest;
import com.starrocks.thrift.TReleaseSlotResponse;
import com.starrocks.thrift.TRequireSlotRequest;
import com.starrocks.thrift.TRequireSlotResponse;
import com.starrocks.thrift.TWorkGroup;
import com.starrocks.utframe.MockGenericPool;
import mockit.Mock;
import mockit.MockUp;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class SchedulerTestBase extends SchedulerTestNoneDBBase {
    protected static final String DB_NAME = "test";
    protected static final AtomicLong COLOCATE_GROUP_INDEX = new AtomicLong(0L);

    protected static final List<Frontend> FRONTENDS = ImmutableList.of(
            new Frontend(FrontendNodeType.FOLLOWER, "fe-1", "127.0.0.1", 8030),
            new Frontend(FrontendNodeType.FOLLOWER, "fe-2", "127.0.0.2", 8030),
            new Frontend(FrontendNodeType.FOLLOWER, "fe-3", "127.0.0.3", 8030)
    );
    protected static final Frontend LOCAL_FRONTEND = FRONTENDS.get(1);
    protected static final int ABSENT_CONCURRENCY_LIMIT = -1;
    protected static final int ABSENT_MAX_CPU_CORES = -1;
    protected final Map<Long, ResourceGroup> mockedGroups = new ConcurrentHashMap<>();
    protected final QueryQueueManager manager = QueryQueueManager.getInstance();

    @BeforeAll
    public static void beforeClass() throws Exception {
        SchedulerTestNoneDBBase.beforeClass();
        starRocksAssert.getCtx().getSessionVariable().setCboPushDownAggregateMode(-1);
        starRocksAssert.withDatabase(DB_NAME).useDatabase(DB_NAME);
        FeConstants.runningUnitTest = true;
        prepareTables(starRocksAssert.getCtx());
    }

    protected static void prepareTables(ConnectContext connectContext) throws Exception {
        final String tpchGroup = "tpch_group_" + COLOCATE_GROUP_INDEX.getAndIncrement();

        // NOTE: Please do not change the order of the following create table statements.
        // Modifying the order will result in changes to the partition ids and tablet ids of a table,
        // which are relied upon by the scheduler plan.
        starRocksAssert.withTable("CREATE TABLE lineitem (\n" +
                "    L_ORDERKEY INTEGER NOT NULL,\n" +
                "    L_PARTKEY INTEGER NOT NULL,\n" +
                "    L_SUPPKEY INTEGER NOT NULL,\n" +
                "    L_LINENUMBER INTEGER NOT NULL,\n" +
                "    L_QUANTITY double NOT NULL,\n" +
                "    L_EXTENDEDPRICE double NOT NULL,\n" +
                "    L_DISCOUNT double NOT NULL,\n" +
                "    L_TAX double NOT NULL,\n" +
                "    L_RETURNFLAG CHAR(1) NOT NULL,\n" +
                "    L_LINESTATUS CHAR(1) NOT NULL,\n" +
                "    L_SHIPDATE DATE NOT NULL,\n" +
                "    L_COMMITDATE DATE NOT NULL,\n" +
                "    L_RECEIPTDATE DATE NOT NULL,\n" +
                "    L_SHIPINSTRUCT CHAR(25) NOT NULL,\n" +
                "    L_SHIPMODE CHAR(10) NOT NULL,\n" +
                "    L_COMMENT VARCHAR(44) NOT NULL,\n" +
                "    PAD char(1) NOT NULL\n" +
                ") ENGINE = OLAP \n" +
                "DUPLICATE KEY(`l_orderkey`) \n" +
                "DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 20 \n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");\n");

        String lineItemSql = "CREATE TABLE `lineitem_partition` (\n" +
                "  `L_ORDERKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_PARTKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_SUPPKEY` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_LINENUMBER` int(11) NOT NULL COMMENT \"\",\n" +
                "  `L_QUANTITY` double NOT NULL COMMENT \"\",\n" +
                "  `L_EXTENDEDPRICE` double NOT NULL COMMENT \"\",\n" +
                "  `L_DISCOUNT` double NOT NULL COMMENT \"\",\n" +
                "  `L_TAX` double NOT NULL COMMENT \"\",\n" +
                "  `L_RETURNFLAG` char(1) NOT NULL COMMENT \"\",\n" +
                "  `L_LINESTATUS` char(1) NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_COMMITDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_RECEIPTDATE` date NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPINSTRUCT` char(25) NOT NULL COMMENT \"\",\n" +
                "  `L_SHIPMODE` char(10) NOT NULL COMMENT \"\",\n" +
                "  `L_COMMENT` varchar(44) NOT NULL COMMENT \"\",\n" +
                "  `PAD` char(1) NOT NULL COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`L_ORDERKEY`, `L_PARTKEY`, `L_SUPPKEY`)\n" +
                "PARTITION BY RANGE(`L_SHIPDATE`)\n" +
                "(PARTITION p1992 VALUES [('1992-01-01'), ('1993-01-01')),\n" +
                "PARTITION p1993 VALUES [('1993-01-01'), ('1994-01-01')),\n" +
                "PARTITION p1994 VALUES [('1994-01-01'), ('1995-01-01')),\n" +
                "PARTITION p1995 VALUES [('1995-01-01'), ('1996-01-01')),\n" +
                "PARTITION p1996 VALUES [('1996-01-01'), ('1997-01-01')),\n" +
                "PARTITION p1997 VALUES [('1997-01-01'), ('1998-01-01')),\n" +
                "PARTITION p1998 VALUES [('1998-01-01'), ('1999-01-01')))\n" +
                "DISTRIBUTED BY HASH(`L_ORDERKEY`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");";
        starRocksAssert.withTable(lineItemSql);
        starRocksAssert.withTable(lineItemSql.replace("lineitem_partition", "lineitem0"));
        starRocksAssert.withTable(lineItemSql.replace("lineitem_partition", "lineitem1"));
        starRocksAssert.withTable(
                lineItemSql.replace("lineitem_partition", "lineitem2").replace(tpchGroup, tpchGroup + "xx"));
        starRocksAssert.withTable(
                lineItemSql.replace("lineitem_partition", "lineitem3").replace(tpchGroup, tpchGroup + "xx"));

        starRocksAssert.withTable("CREATE TABLE customer (\n" +
                "    c_custkey       INT NOT NULL,\n" +
                "    c_name          VARCHAR(25) NOT NULL,\n" +
                "    c_address       VARCHAR(40) NOT NULL,\n" +
                "    c_nationkey     INT NOT NULL,\n" +
                "    c_phone         VARCHAR(15) NOT NULL,\n" +
                "    c_acctbal       DECIMAL(15, 2) NOT NULL,\n" +
                "    c_mktsegment    VARCHAR(10) NOT NULL,\n" +
                "    c_comment       VARCHAR(117) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`c_custkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`c_custkey`) BUCKETS 24\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE `nation` (\n" +
                "    n_nationkey   INT(11) NOT NULL,\n" +
                "    n_name        VARCHAR(25) NOT NULL,\n" +
                "    n_regionkey   INT(11) NOT NULL,\n" +
                "    n_comment     VARCHAR(152) NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`N_NATIONKEY`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`N_NATIONKEY`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE orders (\n" +
                "    o_orderkey       INT NOT NULL,\n" +
                "    o_orderdate      DATE NOT NULL,\n" +
                "    o_custkey        INT NOT NULL,\n" +
                "    o_orderstatus    VARCHAR(1) NOT NULL,\n" +
                "    o_totalprice     DECIMAL(15, 2) NOT NULL,\n" +
                "    o_orderpriority  VARCHAR(15) NOT NULL,\n" +
                "    o_clerk          VARCHAR(15) NOT NULL,\n" +
                "    o_shippriority   INT NOT NULL,\n" +
                "    o_comment        VARCHAR(79) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`o_orderkey`, `o_orderdate`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE part (\n" +
                "    p_partkey       INT NOT NULL,\n" +
                "    p_name          VARCHAR(55) NOT NULL,\n" +
                "    p_mfgr          VARCHAR(25) NOT NULL,\n" +
                "    p_brand         VARCHAR(10) NOT NULL,\n" +
                "    p_type          VARCHAR(25) NOT NULL,\n" +
                "    p_size          INT NOT NULL,\n" +
                "    p_container     VARCHAR(10) NOT NULL,\n" +
                "    p_retailprice   DECIMAL(15, 2) NOT NULL,\n" +
                "    p_comment       VARCHAR(23) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`p_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`p_partkey`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE partsupp (\n" +
                "    ps_partkey      INT NOT NULL,\n" +
                "    ps_suppkey      INT NOT NULL,\n" +
                "    ps_availqty     INT NOT NULL,\n" +
                "    ps_supplycost   DECIMAL(15, 2) NOT NULL,\n" +
                "    ps_comment      VARCHAR(199) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`ps_partkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`ps_partkey`) BUCKETS 18\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\",\n" +
                "    \"colocate_with\" = \"" + tpchGroup + "\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE region (\n" +
                "    r_regionkey     INT NOT NULL,\n" +
                "    r_name          VARCHAR(25) NOT NULL,\n" +
                "    r_comment       VARCHAR(152)\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`r_regionkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`r_regionkey`) BUCKETS 1\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        starRocksAssert.withTable("CREATE TABLE supplier (\n" +
                "    s_suppkey       INT NOT NULL,\n" +
                "    s_name          VARCHAR(25) NOT NULL,\n" +
                "    s_address       VARCHAR(40) NOT NULL,\n" +
                "    s_nationkey     INT NOT NULL,\n" +
                "    s_phone         VARCHAR(15) NOT NULL,\n" +
                "    s_acctbal       DECIMAL(15, 2) NOT NULL,\n" +
                "    s_comment       VARCHAR(101) NOT NULL\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`s_suppkey`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`s_suppkey`) BUCKETS 12\n" +
                "PROPERTIES (\n" +
                "    \"replication_num\" = \"1\"\n" +
                ");");

        connectContext.getGlobalStateMgr().setStatisticStorage(new MockTpchStatisticStorage(connectContext, 100));
        GlobalStateMgr.getCurrentState().getAnalyzeMgr().getBasicStatsMetaMap().clear();

    }

    @AfterAll
    public static void afterClass() {
        FeConstants.runningUnitTest = false;

        SchedulerTestNoneDBBase.afterClass();
    }

    protected static class MockFrontendServiceClient extends FrontendService.Client {
        private final FrontendService.Iface frontendService = new FrontendServiceImpl(null);

        public MockFrontendServiceClient() {
            super(null);
        }

        @Override
        public TRequireSlotResponse requireSlotAsync(TRequireSlotRequest request) throws org.apache.thrift.TException {
            return frontendService.requireSlotAsync(request);
        }

        @Override
        public TReleaseSlotResponse releaseSlot(TReleaseSlotRequest request) throws org.apache.thrift.TException {
            return frontendService.releaseSlot(request);
        }

        @Override
        public TFinishSlotRequirementResponse finishSlotRequirement(TFinishSlotRequirementRequest request)
                throws org.apache.thrift.TException {
            return frontendService.finishSlotRequirement(request);
        }

        @Override
        public TNotifyForwardDeploymentFinishedRespone notifyForwardDeploymentFinished(
                TNotifyForwardDeploymentFinishedRequest request) throws TException {
            return frontendService.notifyForwardDeploymentFinished(request);
        }
    }

    protected static void mockFrontendService(MockFrontendServiceClient client) {
        ThriftConnectionPool.frontendPool = new MockGenericPool<FrontendService.Client>("query-queue-mocked-pool") {
            @Override
            public FrontendService.Client borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
                return client;
            }
        };
    }

    /**
     * Make the coordinator of every query uses the mocked resource group.
     *
     * <p> Mock methods:
     * <ul>
     *  <li> {@link CoordinatorPreprocessor#prepareResourceGroup(ConnectContext, ResourceGroupClassifier.QueryType)}
     *  <li> {@link ResourceGroupMgr#getResourceGroup(long)}
     *  <li> {@link ResourceGroupMgr#getResourceGroupIds()}
     * </ul>
     *
     * @param group The returned group of the mocked method.
     */
    protected void mockResourceGroup(TWorkGroup group) {
        new MockUp<CoordinatorPreprocessor>() {
            @Mock
            public TWorkGroup prepareResourceGroup(ConnectContext connect,
                                                   ResourceGroupClassifier.QueryType queryType) {
                return group;
            }
        };

        if (group != null && group.getId() != LogicalSlot.ABSENT_GROUP_ID) {
            ResourceGroup resourceGroup = new ResourceGroup();
            if (group.getConcurrency_limit() != ABSENT_CONCURRENCY_LIMIT) {
                resourceGroup.setConcurrencyLimit(group.getConcurrency_limit());
            }
            if (group.getMax_cpu_cores() != ABSENT_MAX_CPU_CORES) {
                resourceGroup.setMaxCpuCores(group.getMax_cpu_cores());
            }
            resourceGroup.setId(group.getId());
            resourceGroup.setName(group.getName());
            mockedGroups.put(group.getId(), resourceGroup);
            new MockUp<ResourceGroupMgr>() {
                @Mock
                public ResourceGroup getResourceGroup(long id) {
                    return mockedGroups.get(id);
                }

                @Mock
                public List<Long> getResourceGroupIds() {
                    return new ArrayList<>(mockedGroups.keySet());
                }
            };
        }
    }

    /**
     * Mock {@link NodeMgr} to make it return the specific RPC endpoint of self and leader.
     * The mocked methods including {@link NodeMgr#getFeByName(String)}, {@link NodeMgr#getFeByName(String)}
     * and {@link NodeMgr#getSelfIpAndRpcPort()}.
     */
    protected static void mockFrontends(List<Frontend> frontends) {
        new MockUp<NodeMgr>() {
            @Mock
            public Frontend getFeByName(String name) {
                return frontends.stream().filter(fe -> name.equals(fe.getNodeName())).findAny().orElse(null);
            }

            @Mock
            public Frontend getFeByHost(String host) {
                return frontends.stream().filter(fe -> host.equals(fe.getHost())).findAny().orElse(null);
            }

            @Mock
            public Pair<String, Integer> getSelfIpAndRpcPort() {
                return Pair.create(LOCAL_FRONTEND.getHost(), LOCAL_FRONTEND.getRpcPort());
            }
        };

        long startTimeMs = System.currentTimeMillis() - 1000L;
        frontends.forEach(fe -> handleHbResponse(fe, startTimeMs, true));

        changeLeader(frontends.get(0));
    }

    protected static void changeLeader(Frontend fe) {
        LeaderInfo leaderInfo = new LeaderInfo(fe.getHost(), 80, fe.getRpcPort());
        GlobalStateMgr.getCurrentState().getNodeMgr().setLeader(leaderInfo);
    }

    protected static void handleHbResponse(Frontend fe, long startTimeMs, boolean isAlive) {
        FrontendHbResponse hbResponse;
        if (isAlive) {
            hbResponse = new FrontendHbResponse(fe.getNodeName(), fe.getQueryPort(), fe.getRpcPort(),
                    fe.getReplayedJournalId(), fe.getLastUpdateTime(), startTimeMs, fe.getFeVersion(), 0.5f, 1, null);
        } else {
            hbResponse = new FrontendHbResponse(fe.getNodeName(), "mock-dead-frontend");
        }
        fe.handleHbResponse(hbResponse, false);
    }

    protected DefaultCoordinator runNoPendingQuery() throws Exception {
        DefaultCoordinator coord = getSchedulerWithQueryId("select count(1) from lineitem");
        manager.maybeWait(connectContext, coord);
        Assertions.assertEquals(0L, MetricRepo.COUNTER_QUERY_QUEUE_PENDING.getValue().longValue());
        Assertions.assertEquals(LogicalSlot.State.ALLOCATED, coord.getSlot().getState());
        return coord;
    }

}
