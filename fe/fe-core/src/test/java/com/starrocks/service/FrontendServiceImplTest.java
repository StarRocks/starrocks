// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;

import com.google.gson.Gson;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.RangePartitionInfo;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.View;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.qe.QueryQueueManager;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FrontendServiceImplTest {

    @Mocked
    ExecuteEnv exeEnv;

    @Mocked
    GlobalStateMgr globalStateMgr;

    @Mocked
    Auth auth;

    @Test
    public void testGetTablesConfig() throws TException, NoSuchFieldException,
            SecurityException, IllegalArgumentException, IllegalAccessException {

        Database db = new Database(1, "test_db");

        List<Column> partitionsColumns = new ArrayList<>();
        partitionsColumns.add(new Column("p_c1", Type.ARRAY_BOOLEAN));
        partitionsColumns.add(new Column("p_c2", Type.ARRAY_BOOLEAN));

        List<Column> dColumns = new ArrayList<>();
        dColumns.add(new Column("d_c1", Type.ARRAY_BOOLEAN));
        dColumns.add(new Column("d_c2", Type.ARRAY_BOOLEAN));

        List<Column> keyColumns = new ArrayList<>();
        Column keyC1 = new Column("key_c1", Type.ARRAY_BOOLEAN);
        keyC1.setIsKey(true);
        Column keyC2 = new Column("key_c2", Type.ARRAY_BOOLEAN);
        keyC2.setIsKey(true);
        keyColumns.add(keyC1);
        keyColumns.add(keyC2);

        Map<String, String> properties = new HashMap<>();
        properties.put(PropertyAnalyzer.PROPERTIES_STORAGE_TYPE, "test_type");
        TableProperty tProperties = new TableProperty(properties);

        // OlapTable
        RangePartitionInfo partitionInfo = new RangePartitionInfo(partitionsColumns);
        HashDistributionInfo distributionInfo = new HashDistributionInfo(10, dColumns);

        // PK
        OlapTable tablePk =
                new OlapTable(1, "test_table_pk", keyColumns, KeysType.PRIMARY_KEYS, partitionInfo, distributionInfo);
        tablePk.setTableProperty(tProperties);
        tablePk.setColocateGroup("test_group");

        // AGG
        OlapTable tableAGG =
                new OlapTable(2, "test_table_agg", keyColumns, KeysType.AGG_KEYS, partitionInfo, distributionInfo);
        tableAGG.setTableProperty(tProperties);
        tableAGG.setColocateGroup("test_group");

        // DUP
        OlapTable tableDUP =
                new OlapTable(3, "test_table_dup", keyColumns, KeysType.DUP_KEYS, partitionInfo, distributionInfo);
        tableDUP.setTableProperty(tProperties);
        tableDUP.setColocateGroup("test_group");

        // UNI
        OlapTable tableUNI = new OlapTable(4, "test_table_uni", keyColumns,
                KeysType.UNIQUE_KEYS, partitionInfo, distributionInfo);
        tableUNI.setTableProperty(tProperties);
        tableUNI.setColocateGroup("test_group");

        // View
        View view = new View(2, "test_view", keyColumns);

        db.createTable(tablePk);
        db.createTable(tableAGG);
        db.createTable(tableDUP);
        db.createTable(tableUNI);

        db.createTable(view);

        new MockUp<GlobalStateMgr>() {
            @Mock
            public Database getDb(String name) {
                return db;
            }
        };
        new MockUp<OlapTable>() {
            @Mock
            public List<Column> getBaseSchema() {
                return keyColumns;
            }
        };
        new Expectations() {
            {
                globalStateMgr.getDbNames();
                result = Arrays.asList("test_db");
            }
        };

        Field field = globalStateMgr.getClass().getDeclaredField("auth");
        field.setAccessible(true);
        field.set(globalStateMgr, auth);

        new MockUp<Auth>() {
            @Mock
            public boolean checkDbPriv(UserIdentity currentUser, String db, PrivPredicate wanted) {
                return true;
            }
        };

        new MockUp<PatternMatcher>() {
            @Mock
            public boolean match(String candidate) {
                return true;
            }
        };

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("test parttern");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        response.tables_config_infos.forEach(info -> {
            if (info.getTable_name().equals("test_table_pk") ||
                    info.getTable_name().equals("test_table_uni")) {
                Assert.assertEquals("`key_c1`, `key_c2`", info.getPrimary_key());
                Assert.assertEquals("NULL", info.getSort_key());
                Assert.assertEquals("`d_c1`, `d_c2`", info.getDistribute_key());
                Assert.assertEquals("`p_c1`, `p_c2`", info.getPartition_key());
                Assert.assertTrue(distributionInfo.getBucketNum() == 10);
                Assert.assertEquals("HASH", info.getDistribute_type());
                Map<String, String> propsMap = new HashMap<>();
                propsMap = new Gson().fromJson(info.getProperties(), propsMap.getClass());
                Assert.assertTrue(propsMap.get("enable_persistent_index").equals("LZ4"));
                Assert.assertTrue(propsMap.get("storage_format").equals("DEFAULT"));
                Assert.assertTrue(propsMap.get("colocate_with").equals("test_group"));
                Assert.assertTrue(propsMap.get("replication_num").equals("3"));
                Assert.assertTrue(propsMap.get("in_memory").equals("false"));
            } else if (info.getTable_name().equals("test_table_agg") ||
                    info.getTable_name().equals("test_table_dup")) {
                Assert.assertEquals("`key_c1`, `key_c2`", info.getSort_key());
                Assert.assertEquals("NULL", info.getPrimary_key());
                Assert.assertEquals("`d_c1`, `d_c2`", info.getDistribute_key());
                Assert.assertEquals("`p_c1`, `p_c2`", info.getPartition_key());
                Assert.assertTrue(distributionInfo.getBucketNum() == 10);
                Assert.assertEquals("HASH", info.getDistribute_type());
            }
        });

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
        SystemInfoService systemInfoService = GlobalStateMgr.getCurrentSystemInfo();
        Backend backend = new Backend();
        long backendId = 0;
        int numRunningQueries = 1;
        long memLimitBytes = 3;
        long memUsedBytes = 2;
        int cpuUsedPermille = 300;
        new Expectations(systemInfoService, queryQueueManager) {
            {
                queryQueueManager.maybeNotifyAfterLock();
                times = 1;
            }

            {
                systemInfoService.getBackend(backendId);
                result = backend;
            }

            {
                systemInfoService.getBackend(anyLong);
                result = null;
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
}
