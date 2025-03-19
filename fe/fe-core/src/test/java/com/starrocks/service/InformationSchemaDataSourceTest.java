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

import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.common.util.DateUtils;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.sql.ast.UserIdentity;
import com.starrocks.thrift.TApplicableRolesInfo;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TGetApplicableRolesRequest;
import com.starrocks.thrift.TGetApplicableRolesResponse;
import com.starrocks.thrift.TGetKeywordsRequest;
import com.starrocks.thrift.TGetKeywordsResponse;
import com.starrocks.thrift.TGetPartitionsMetaRequest;
import com.starrocks.thrift.TGetPartitionsMetaResponse;
import com.starrocks.thrift.TGetTablesConfigRequest;
import com.starrocks.thrift.TGetTablesConfigResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TKeywordInfo;
import com.starrocks.thrift.TPartitionMetaInfo;
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

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
    }

    @Test
    public void testGetTablesConfig() throws Exception {

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
        authInfo.setPattern("db1");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_name().equals("tbl1")).findFirst()
                .orElseGet(null);
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

        TTableConfigInfo mvConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_engine().equals("MATERIALIZED_VIEW")).findFirst()
                .orElseGet(null);
        Assert.assertEquals("MATERIALIZED_VIEW", mvConfig.getTable_engine());
        Map<String, String> propsMap = new Gson().fromJson(mvConfig.getProperties(), Map.class);
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
                "\"enable_persistent_index\" = \"true\",\n" +
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
        TTableConfigInfo tableConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_name().equals("unique_table_with_null")).findFirst().orElseGet(null);
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
    public void testGetPartitionsMeta() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("db3").useDatabase("db3");
        String createTblStmtStr = "CREATE TABLE db3.`duplicate_table_with_null` (\n" +
                "  `k1` date  COMMENT \"\",\n" +
                "  `k2` int COMMENT \"\",\n" +
                "  `k3` varchar(20)  COMMENT \"\",\n" +
                "  `k4` varchar(20)  COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DUPLICATE KEY(`k1`, `k2`)\n" +
                "PARTITION BY RANGE(`k2`)\n" +
                "(PARTITION p1 VALUES [(\"-2147483648\"), (\"19930101\")))\n" +
                "DISTRIBUTED BY HASH(`k1`) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"in_memory\" = \"false\",\n" +
                "\"enable_persistent_index\" = \"true\",\n" +
                "\"compression\" = \"LZ4\"\n" +
                ");";
        starRocksAssert.withTable(createTblStmtStr);

        String ddlStr = "ALTER TABLE db3.`duplicate_table_with_null`\n" +
                "add TEMPORARY partition p2 VALUES [(\"19930101\"), (\"19940101\"))\n" +
                "DISTRIBUTED BY HASH(`k1`);";
        starRocksAssert.ddl(ddlStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetPartitionsMetaRequest req = new TGetPartitionsMetaRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db3");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetPartitionsMetaResponse response = impl.getPartitionsMeta(req);
        TPartitionMetaInfo partitionMeta = response.getPartitions_meta_infos().stream()
                .filter(t -> t.getTable_name().equals("duplicate_table_with_null")).findFirst().orElseGet(null);
        Assert.assertEquals("db3", partitionMeta.getDb_name());
        Assert.assertEquals("duplicate_table_with_null", partitionMeta.getTable_name());
    }

    @Test
    public void testRandomDistribution() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("db4").useDatabase("db4");
        String createTblStmtStr = "CREATE TABLE db4.`duplicate_table_random` (\n" +
                "  `k1` date  COMMENT \"\",\n" +
                "  `k2` datetime  COMMENT \"\",\n" +
                "  `k3` varchar(20)  COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "DISTRIBUTED BY RANDOM BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTblStmtStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db4");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_name().equals("duplicate_table_random")).findFirst().orElseGet(null);
        Assert.assertEquals("RANDOM", tableConfig.getDistribute_type());
        Assert.assertEquals(3, tableConfig.getDistribute_bucket());
        Assert.assertEquals("", tableConfig.getDistribute_key());
    }

    @Test
    public void testDynamicPartition() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("db5").useDatabase("db5");
        String createTblStmtStr = "CREATE TABLE db5.`duplicate_dynamic_table` (\n" +
                "  `k1` date  COMMENT \"\",\n" +
                "  `k2` datetime  COMMENT \"\",\n" +
                "  `k3` varchar(20)  COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(k1)()\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3\n" +
                "PROPERTIES (\n" +
                "\"dynamic_partition.enable\" = \"true\",\n" +
                "\"dynamic_partition.time_unit\" = \"DAY\",\n" +
                "\"dynamic_partition.start\" = \"-3\",\n" +
                "\"dynamic_partition.end\" = \"3\",\n" +
                "\"dynamic_partition.prefix\" = \"p\",\n" +
                "\"replication_num\" = \"1\"\n" +
                ");";
        starRocksAssert.withTable(createTblStmtStr);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesConfigRequest req = new TGetTablesConfigRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("db5");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);
        TGetTablesConfigResponse response = impl.getTablesConfig(req);
        TTableConfigInfo tableConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_name().equals("duplicate_dynamic_table")).findFirst().orElseGet(null);
        Map<String, String> props = new Gson().fromJson(tableConfig.getProperties(), Map.class);
        Assert.assertEquals("true", props.get("dynamic_partition.enable"));
        Assert.assertEquals("DAY", props.get("dynamic_partition.time_unit"));
        Assert.assertEquals("-3", props.get("dynamic_partition.start"));
        Assert.assertEquals("3", props.get("dynamic_partition.end"));
        Assert.assertEquals("p", props.get("dynamic_partition.prefix"));
        Assert.assertEquals("1", props.get("replication_num"));
    }

    public static ZoneOffset offset(ZoneId id) {
        return ZoneOffset.ofTotalSeconds((int) 
            TimeUnit.MILLISECONDS.toSeconds(
                TimeZone.getTimeZone(id).getRawOffset()        // Returns offset in milliseconds 
            )
        );
    }

    @Test
    public void testTaskRunsEvaluation() throws Exception {
        starRocksAssert.withDatabase("d1").useDatabase("d1");
        starRocksAssert.withTable("create table t1 (c1 int, c2 int) properties('replication_num'='1') ");
        starRocksAssert.ddl("submit task t_1024 as insert into t1 select * from t1");

        TaskRunStatus taskRun = new TaskRunStatus();
        taskRun.setTaskName("t_1024");
        taskRun.setState(Constants.TaskRunState.SUCCESS);
        taskRun.setDbName("d1");
        taskRun.setCreateTime(DateUtils.parseDatTimeString("2024-01-02 03:04:05")
                .toEpochSecond(offset(ZoneId.systemDefault())) * 1000);
        taskRun.setFinishTime(DateUtils.parseDatTimeString("2024-01-02 03:04:05")
                .toEpochSecond(offset(ZoneId.systemDefault())) * 1000);
        taskRun.setExpireTime(DateUtils.parseDatTimeString("2024-01-02 03:04:05")
                .toEpochSecond(offset(ZoneId.systemDefault())) * 1000);
        new MockUp<TaskManager>() {
            @Mock
            public List<TaskRunStatus> getMatchedTaskRunStatus(TGetTasksParams params) {
                return Lists.newArrayList(taskRun);
            }
        };

        starRocksAssert.query("select * from information_schema.task_runs where task_name = 't_1024' ")
                .explainContains("     constant exprs: ",
                        "NULL | 't_1024' | '2024-01-02 03:04:05' | '2024-01-02 03:04:05' | 'SUCCESS' | " +
                                "NULL | 'd1' | 'insert into t1 select * from t1' | '2024-01-02 03:04:05' | 0 | " +
                                "NULL | '0%' | '' | NULL");
        starRocksAssert.query("select state, error_message" +
                        " from information_schema.task_runs where task_name = 't_1024' ")
                .explainContains("     constant exprs: ",
                        "'SUCCESS' | NULL");
        starRocksAssert.query("select count(task_name) " +
                        " from information_schema.task_runs where task_name = 't_1024' ")
                .explainContains("     constant exprs: ",
                        "'t_1024'");
        starRocksAssert.query("select count(error_code) " +
                        " from information_schema.task_runs where task_name = 't_1024' ")
                .explainContains("     constant exprs: \n         0\n");
        starRocksAssert.query("select count(*) " +
                        " from information_schema.task_runs where task_name = 't_1024' ")
                .explainContains("     constant exprs: ", "NULL");
        starRocksAssert.query("select count(distinct task_name) " +
                        " from information_schema.task_runs where task_name = 't_1024' ")
                .explainContains("     constant exprs: ", "'t_1024'");
        starRocksAssert.query("select error_code + 1, error_message + 'haha' " +
                        " from information_schema.task_runs where task_name = 't_1024' ")
                .explainContains("     constant exprs: ",
                        "0 | NULL");

        // Not supported
        starRocksAssert.query("select state, error_message" +
                        " from information_schema.task_runs where task_name > 't_1024' ")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select state, error_message" +
                        " from information_schema.task_runs where task_name='t_1024' or task_name='t_1025' ")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select state, error_message" +
                        " from information_schema.task_runs where error_message > 't_1024' ")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select state, error_message" +
                        " from information_schema.task_runs where state = 'SUCCESS' ")
                .explainContains("SCAN SCHEMA");
    }

    @Test
    public void testGetKeywords() throws Exception {
        starRocksAssert.withEnableMV();

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

        TGetKeywordsRequest req = new TGetKeywordsRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("%");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);

        TGetKeywordsResponse response = impl.getKeywords(req);
        List<TKeywordInfo> keywordList = response.getKeywords();

        Assert.assertNotNull("Keywords list should not be null", keywordList);
        Assert.assertFalse("Keywords list should not be empty", keywordList.isEmpty());

        List<String> keywordNames = keywordList.stream()
                .map(TKeywordInfo::getKeyword)
                .collect(Collectors.toList());

        Assert.assertTrue("Keywords should contain 'SELECT'", keywordNames.contains("SELECT"));
        Assert.assertTrue("Keywords should contain 'INSERT'", keywordNames.contains("INSERT"));
        Assert.assertTrue("Keywords should contain 'UPDATE'", keywordNames.contains("UPDATE"));
        Assert.assertTrue("Keywords should contain 'DELETE'", keywordNames.contains("DELETE"));
        Assert.assertTrue("Keywords should contain 'TABLE'", keywordNames.contains("TABLE"));
        Assert.assertTrue("Keywords should contain 'INDEX'", keywordNames.contains("INDEX"));
        Assert.assertTrue("Keywords should contain 'VIEW'", keywordNames.contains("VIEW"));
        Assert.assertTrue("Keywords should contain 'USER'", keywordNames.contains("USER"));
        Assert.assertTrue("Keywords should contain 'PASSWORD'", keywordNames.contains("PASSWORD"));

        TKeywordInfo selectKeyword = keywordList.stream()
                .filter(k -> k.getKeyword().equals("SELECT"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull("SELECT keyword should be present", selectKeyword);
        Assert.assertTrue("SELECT keyword should be reserved", selectKeyword.isReserved());

        TKeywordInfo userKeyword = keywordList.stream()
                .filter(k -> k.getKeyword().equals("USER"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull("USER keyword should be present", userKeyword);
        Assert.assertFalse("USER keyword should not be reserved", userKeyword.isReserved());
    }

    @Test
    public void testGetApplicableRoles() throws Exception {
        starRocksAssert.withEnableMV();

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

        TGetApplicableRolesRequest req = new TGetApplicableRolesRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("%");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);

        TGetApplicableRolesResponse response = impl.getApplicableRoles(req);
        List<TApplicableRolesInfo> rolesList = response.getRoles();

        Assert.assertNotNull("Roles list should not be null", rolesList);
        Assert.assertFalse("Roles list should not be empty", rolesList.isEmpty());

        List<String> roleNames = rolesList.stream()
                .map(TApplicableRolesInfo::getRole_name)
                .collect(Collectors.toList());

        Assert.assertTrue("Roles should contain 'root'", roleNames.contains("root"));

        TApplicableRolesInfo adminRole = rolesList.stream()
                .filter(r -> r.getRole_name().equals("root"))
                .findFirst()
                .orElse(null);
        Assert.assertNotNull("root role should be present", adminRole);
        Assert.assertEquals("User should be root", "root", adminRole.getUser());
        Assert.assertEquals("Host should be %", "%", adminRole.getHost());
        Assert.assertEquals("isGrantable should be NO", "NO", "NO");
        Assert.assertEquals("isDefault should be NO", "NO", "NO");
        Assert.assertEquals("isMandatory should be NO", "NO", "NO");
    }
}
