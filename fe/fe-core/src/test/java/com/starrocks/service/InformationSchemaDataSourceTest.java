// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

package com.starrocks.service;

import com.google.gson.Gson;
import com.starrocks.analysis.UserIdentity;
import com.starrocks.catalog.InfoSchemaDb;
import com.starrocks.common.Config;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.mysql.privilege.PrivPredicate;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TTableConfigInfo;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class InformationSchemaDataSourceTest {
    
    @Mocked
    ExecuteEnv exeEnv;

    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        Config.enable_experimental_mv = true;
    }

    @Test
    public void testGetTablesConfig() throws Exception {

        new MockUp<Auth>() {
            @Mock
            public boolean checkDbPriv(UserIdentity currentUser, String db, PrivPredicate wanted) {
                return true;
            }
        };

        starRocksAssert.withEnableMV().withDatabase("db1").useDatabase("db1");
        String createTblStmtStr = "CREATE TABLE db1.tbl1 (`k1` int,`k2` int,`k3` int,`v1` int,`v2` int,`v3` int) " +
                                  "ENGINE=OLAP " + "PRIMARY KEY(`k1`, `k2`, `k3`) " +
                                  "COMMENT \"OLAP\" " +
                                  "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`) BUCKETS 3 " +
                                  "ORDER BY(`v2`, `v3`) " +
                                  "PROPERTIES ('replication_num' = '1');";
        starRocksAssert.withTable(createTblStmtStr);

        String createMvStmtStr = "CREATE MATERIALIZED VIEW db1.mv1 " +
                                 "DISTRIBUTED BY HASH(k1) BUCKETS 10 " +
                                 "REFRESH ASYNC " +
                                 "AS SELECT k1, k2 " +
                                 "FROM db1.tbl1 ";
        
        starRocksAssert.withMaterializedView(createMvStmtStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("root");
        userIdentity.setHost("%");
        authInfo.setCurrent_user_ident(userIdentity);
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().get(0);
        Assert.assertEquals("db1", tableConfig.getTable_schema());
        Assert.assertEquals("tbl1", tableConfig.getTable_name());
        Assert.assertEquals("OLAP", tableConfig.getTable_engine());
        Assert.assertEquals("PRIMARY_KEYS", tableConfig.getTable_model());
        Assert.assertEquals("`k1`, `k2`, `k3`", tableConfig.getPrimary_key());
        Assert.assertEquals("", tableConfig.getPartition_key());
        Assert.assertEquals("`k1`, `k2`, `k3`", tableConfig.getDistribute_key());
        Assert.assertEquals("HASH", tableConfig.getDistribute_type());
        Assert.assertEquals(3, tableConfig.getDistribute_bucket());
        Assert.assertEquals("`v2`, `v3`", tableConfig.getSort_key());

        TTableConfigInfo mvConfig = response.getTables_config_infos().get(1);
        Assert.assertEquals("MATERIALIZED_VIEW", mvConfig.getTable_engine());
        Map<String, String> propsMap = new HashMap<>();
        propsMap = new Gson().fromJson(mvConfig.getProperties(), propsMap.getClass());
        Assert.assertEquals("1", propsMap.get("replication_num"));
        Assert.assertEquals("HDD", propsMap.get("storage_medium"));

    }

    @Test
    public void testGetTablesConfigBasic() throws Exception {

        starRocksAssert.withEnableMV().withDatabase("db2").useDatabase("db2");
        String createTblStmtStr = "CREATE TABLE db2.`unique_table_with_null` (\n" +
                "  `k1` date  COMMENT \"\",\n" +
                "  `k2` datetime  COMMENT \"\",\n" +
                "  `k3` varchar(20)  COMMENT \"\",\n" +
                "  `k4` varchar(20)  COMMENT \"\",\n" +
                "  `k5` boolean  COMMENT \"\",\n" +
                "  `v1` tinyint(4)  COMMENT \"\",\n" +
                "  `v2` smallint(6)  COMMENT \"\",\n" +
                "  `v3` int(11)  COMMENT \"\",\n" +
                "  `v4` bigint(20)  COMMENT \"\",\n" +
                "  `v5` largeint(40)  COMMENT \"\",\n" +
                "  `v6` float  COMMENT \"\",\n" +
                "  `v7` double  COMMENT \"\",\n" +
                "  `v8` decimal128(27, 9)  COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        starRocksAssert.withTable(createTblStmtStr);


        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db2");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().get(0);
        Assert.assertEquals("db2", tableConfig.getTable_schema());
        Assert.assertEquals("unique_table_with_null", tableConfig.getTable_name());
        Assert.assertEquals("OLAP", tableConfig.getTable_engine());
        Assert.assertEquals("UNIQUE_KEYS", tableConfig.getTable_model());
        Assert.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getPrimary_key());
        Assert.assertEquals("", tableConfig.getPartition_key());
        Assert.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getDistribute_key());
        Assert.assertEquals("HASH", tableConfig.getDistribute_type());
        Assert.assertEquals(3, tableConfig.getDistribute_bucket());
        Assert.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getSort_key());
    }

    @Test
    public void testGetInformationSchemaTable() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesInfoRequest request = new TGetTablesInfoRequest();
        TAuthInfo authInfo = new TAuthInfo();
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("root");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        authInfo.setCurrent_user_ident(userIdentity);
        authInfo.setPattern(InfoSchemaDb.DATABASE_NAME);
        request.setAuth_info(authInfo);
        TGetTablesInfoResponse response = impl.getTablesInfo(request);
        boolean checkTables = false;
        for (TTableInfo tablesInfo : response.tables_infos) {
            if (tablesInfo.getTable_name().equalsIgnoreCase("tables")) {
                checkTables = true;
                Assert.assertEquals("SYSTEM VIEW", tablesInfo.getTable_type());
            }
        }
        Assert.assertTrue(checkTables);
    }

    @Test
    public void testGetTablesConfigBasic() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);

        StarRocksAssert starRocksAssert = new StarRocksAssert(UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT));
        starRocksAssert.withEnableMV().withDatabase("db2").useDatabase("db2");
        Config.enable_experimental_mv = true;
        String createTblStmtStr = "CREATE TABLE db2.`unique_table_with_null` (\n" +
                "  `k1` date  COMMENT \"\",\n" +
                "  `k2` datetime  COMMENT \"\",\n" +
                "  `k3` varchar(20)  COMMENT \"\",\n" +
                "  `k4` varchar(20)  COMMENT \"\",\n" +
                "  `k5` boolean  COMMENT \"\",\n" +
                "  `v1` tinyint(4)  COMMENT \"\",\n" +
                "  `v2` smallint(6)  COMMENT \"\",\n" +
                "  `v3` int(11)  COMMENT \"\",\n" +
                "  `v4` bigint(20)  COMMENT \"\",\n" +
                "  `v5` largeint(40)  COMMENT \"\",\n" +
                "  `v6` float  COMMENT \"\",\n" +
                "  `v7` double  COMMENT \"\",\n" +
                "  `v8` decimal128(27, 9)  COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "UNIQUE KEY(`k1`, `k2`, `k3`, `k4`, `k5`)\n" +
                "COMMENT \"OLAP\"\n" +
                "DISTRIBUTED BY HASH(`k1`, `k2`, `k3`, `k4`, `k5`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"storage_format\" = \"V2\",\n" +
                "\"enable_persistent_index\" = \"false\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        starRocksAssert.withTable(createTblStmtStr);


        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db2");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().get(0);
        Assert.assertEquals("db2", tableConfig.getTable_schema());
        Assert.assertEquals("unique_table_with_null", tableConfig.getTable_name());
        Assert.assertEquals("OLAP", tableConfig.getTable_engine());
        Assert.assertEquals("UNIQUE_KEYS", tableConfig.getTable_model());
        Assert.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getPrimary_key());
        Assert.assertEquals("", tableConfig.getPartition_key());
        Assert.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getDistribute_key());
        Assert.assertEquals("HASH", tableConfig.getDistribute_type());
        Assert.assertEquals(3, tableConfig.getDistribute_bucket());
        Assert.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getSort_key());
    }
}
