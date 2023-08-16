// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;


import com.starrocks.common.FeConstants;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TAuthInfo;
<<<<<<< HEAD
=======
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TGetLoadTxnStatusRequest;
import com.starrocks.thrift.TGetLoadTxnStatusResult;
>>>>>>> 519ef2ca13 ([Enhancement] Support sync publish version for primary key table (#27055))
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TTableInfo;
<<<<<<< HEAD
=======
import com.starrocks.thrift.TTableStatus;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TUniqueId;
>>>>>>> 519ef2ca13 ([Enhancement] Support sync publish version for primary key table (#27055))
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
<<<<<<< HEAD

=======
import java.util.UUID;
import java.util.stream.Collectors;
>>>>>>> 519ef2ca13 ([Enhancement] Support sync publish version for primary key table (#27055))

public class FrontendServiceImplTest {

    @Mocked
    ExecuteEnv exeEnv;

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);
    }

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

    @Test
    public void testGetTablesInfo() throws Exception {
        starRocksAssert.withDatabase("test_table").useDatabase("test_table")
                .withTable("CREATE TABLE `t1` (\n" +
                        "  `k1` date NULL COMMENT \"\",\n" +
                        "  `v1` int(11) NULL COMMENT \"\",\n" +
                        "  `v2` int(11) NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`k1`)\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"enable_persistent_index\" = \"false\",\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"compression\" = \"LZ4\"\n" +
                        ")")
                .withTable("CREATE TABLE `t2` (\n" +
                        "  `k1` date NULL COMMENT \"\",\n" +
                        "  `v1` int(11) NULL COMMENT \"\",\n" +
                        "  `v2` int(11) NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`k1`)\n" +
                        "DISTRIBUTED BY HASH(k1) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"enable_persistent_index\" = \"false\",\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"compression\" = \"LZ4\"\n" +
                        ")");

        ConnectContext ctx = starRocksAssert.getCtx();
        String createUserSql = "create user test1";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx), ctx);
        String grantSql = "GRANT SELECT ON TABLE test_table.t1 TO `test1`@`%`;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        TAuthInfo authInfo = new TAuthInfo();
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("test1");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        authInfo.setCurrent_user_ident(userIdentity);
        authInfo.setPattern("test_table");
        request.setAuth_info(authInfo);
        TGetTablesInfoResponse response = impl.getTablesInfo(request);
        List<TTableInfo> tablesInfos = response.getTables_infos();
        Assert.assertEquals(1, tablesInfos.size());
        Assert.assertEquals("t1", tablesInfos.get(0).getTable_name());
    }
<<<<<<< HEAD
=======

    @Test
    public void testDefaultValueMeta() throws Exception {
        starRocksAssert.withDatabase("test_table").useDatabase("test_table")
                .withTable("CREATE TABLE `test_default_value` (\n" +
                        "  `id` datetime NULL DEFAULT CURRENT_TIMESTAMP COMMENT \"\",\n" +
                        "  `value` int(11) NULL DEFAULT \"2\" COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`id`, `value`)\n" +
                        "DISTRIBUTED BY RANDOM\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"enable_persistent_index\" = \"false\",\n" +
                        "\"replicated_storage\" = \"true\",\n" +
                        "\"compression\" = \"LZ4\"\n" +
                        ");");

        ConnectContext ctx = starRocksAssert.getCtx();
        String createUserSql = "create user test2";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx), ctx);
        String grantSql = "GRANT SELECT ON TABLE test_table.test_default_value TO USER `test2`@`%`;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TDescribeTableParams request = new TDescribeTableParams();
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("test2");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        request.setCurrent_user_ident(userIdentity);
        TDescribeTableResult response = impl.describeTable(request);
        List<TColumnDef> columnDefList = response.getColumns();
        List<TColumnDef> testDefaultValue = columnDefList.stream()
                .filter(u -> u.getColumnDesc().getTableName().equalsIgnoreCase("test_default_value"))
                .collect(Collectors.toList());
        Assert.assertEquals(2, testDefaultValue.size());
        Assert.assertEquals("CURRENT_TIMESTAMP", testDefaultValue.get(0).getColumnDesc().getColumnDefault());
        Assert.assertEquals("2", testDefaultValue.get(1).getColumnDesc().getColumnDefault());
    }

    @Test
    public void testGetSpecialColumn() throws Exception {
        starRocksAssert.withDatabase("test_table").useDatabase("test_table")
                .withTable("CREATE TABLE `ye$test` (\n" +
                        "event_day DATE,\n" +
                        "department_id int(11) NOT NULL COMMENT \"\"\n" +
                        ") ENGINE=OLAP\n" +
                        "PRIMARY KEY(event_day, department_id)\n" +
                        "DISTRIBUTED BY HASH(department_id) BUCKETS 1\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\",\n" +
                        "\"in_memory\" = \"false\",\n" +
                        "\"storage_format\" = \"DEFAULT\",\n" +
                        "\"enable_persistent_index\" = \"false\"\n" +
                        ");");

        ConnectContext ctx = starRocksAssert.getCtx();
        String createUserSql = "create user test3";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx), ctx);
        String grantSql = "GRANT SELECT ON TABLE test_table.ye$test TO USER `test3`@`%`;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesParams request = new TGetTablesParams();
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("test3");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        request.setCurrent_user_ident(userIdentity);
        request.setPattern("ye$test");
        request.setDb("test_table");
        TGetTablesResult response = impl.getTableNames(request);
        Assert.assertEquals(1, response.tables.size());
    }

    @Test
    public void testGetLoadTxnStatus() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_day");
        UUID uuid = UUID.randomUUID();
        TUniqueId requestId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        long transactionId = GlobalStateMgr.getCurrentGlobalTransactionMgr().beginTransaction(db.getId(),
                             Lists.newArrayList(table.getId()), "1jdc689-xd232", requestId,
                             new TxnCoordinator(TxnSourceType.BE, "1.1.1.1"),
                             TransactionState.LoadJobSourceType.BACKEND_STREAMING, -1, 600);
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetLoadTxnStatusRequest request = new TGetLoadTxnStatusRequest();
        request.setDb("non-exist-db");
        request.setTbl("non-site_access_day-tbl");
        request.setTxnId(100);
        TGetLoadTxnStatusResult result1 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.UNKNOWN, result1.getStatus());
        request.setDb("test");
        TGetLoadTxnStatusResult result2 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.UNKNOWN, result2.getStatus());
        request.setTxnId(transactionId);
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);
        TGetLoadTxnStatusResult result3 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.UNKNOWN, result3.getStatus());
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        TGetLoadTxnStatusResult result4 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.PREPARE, result4.getStatus());
    }
>>>>>>> 519ef2ca13 ([Enhancement] Support sync publish version for primary key table (#27055))
}
