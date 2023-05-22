// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;


import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.thrift.TUserIdentity;
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
}
