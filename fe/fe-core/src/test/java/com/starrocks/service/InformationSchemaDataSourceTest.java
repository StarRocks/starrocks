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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.starrocks.authentication.UserIdentityUtils;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.catalog.system.information.InfoSchemaDb;
import com.starrocks.catalog.system.information.TablesSystemTable;
import com.starrocks.catalog.system.information.ViewsSystemTable;
import com.starrocks.common.util.DateUtils;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.scheduler.slot.BaseSlotManager;
import com.starrocks.qe.scheduler.slot.BaseSlotTracker;
import com.starrocks.qe.scheduler.slot.LogicalSlot;
import com.starrocks.qe.scheduler.slot.SlotManager;
import com.starrocks.qe.scheduler.slot.SlotSelectionStrategyV2;
import com.starrocks.qe.scheduler.slot.SlotTracker;
import com.starrocks.scheduler.Constants;
import com.starrocks.scheduler.Task;
import com.starrocks.scheduler.TaskBuilder;
import com.starrocks.scheduler.TaskManager;
import com.starrocks.scheduler.persist.TaskRunStatus;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.WarehouseManager;
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
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTasksParams;
import com.starrocks.thrift.TGetWarehouseMetricsRequest;
import com.starrocks.thrift.TGetWarehouseMetricsRespone;
import com.starrocks.thrift.TGetWarehouseQueriesRequest;
import com.starrocks.thrift.TGetWarehouseQueriesResponse;
import com.starrocks.thrift.TKeywordInfo;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TPartitionMetaInfo;
import com.starrocks.thrift.TTableConfigInfo;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.thrift.TTableStatus;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.StarRocksTestBase;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class InformationSchemaDataSourceTest extends StarRocksTestBase {

    @Mocked
    ExecuteEnv exeEnv;
    private static ConnectContext connectContext;

    @BeforeAll
    public static void beforeClass() throws Exception {
        UtFrameUtils.createMinStarRocksCluster();
        UtFrameUtils.addMockBackend(10002);
        UtFrameUtils.addMockBackend(10003);
        connectContext = UtFrameUtils.initCtxForNewPrivilege(UserIdentity.ROOT);
        connectContext.setThreadLocalInfo();
        starRocksAssert = new StarRocksAssert(connectContext);
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

        String createViewStmtStr = "CREATE VIEW db1.v1 " +
                "AS SELECT k1, k2 " +
                "FROM db1.tbl1 ";
        starRocksAssert.withView(createViewStmtStr);

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
        Assertions.assertEquals("db1", tableConfig.getTable_schema());
        Assertions.assertEquals("tbl1", tableConfig.getTable_name());
        Assertions.assertEquals("OLAP", tableConfig.getTable_engine());
        Assertions.assertEquals("PRIMARY_KEYS", tableConfig.getTable_model());
        Assertions.assertEquals("`k1`, `k2`, `k3`", tableConfig.getPrimary_key());
        Assertions.assertEquals("", tableConfig.getPartition_key());
        Assertions.assertEquals("`k1`, `k2`, `k3`", tableConfig.getDistribute_key());
        Assertions.assertEquals("HASH", tableConfig.getDistribute_type());
        Assertions.assertEquals(3, tableConfig.getDistribute_bucket());
        Assertions.assertEquals("`v2`, `v3`", tableConfig.getSort_key());

        TTableConfigInfo mvConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_engine().equals("MATERIALIZED_VIEW")).findFirst()
                .orElseGet(null);
        Assertions.assertEquals("MATERIALIZED_VIEW", mvConfig.getTable_engine());
        Map<String, String> propsMap = new Gson().fromJson(mvConfig.getProperties(), Map.class);
        Assertions.assertEquals("1", propsMap.get("replication_num"));
        Assertions.assertEquals("HDD", propsMap.get("storage_medium"));

        TTableConfigInfo viewConfig = response.getTables_config_infos().stream()
                .filter(t -> t.getTable_engine().equals("VIEW")).findFirst()
                .orElseGet(null);
        Assertions.assertEquals("VIEW", viewConfig.getTable_engine());
        Assertions.assertEquals("db1", viewConfig.getTable_schema());
        Assertions.assertEquals("v1", viewConfig.getTable_name());

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
        Assertions.assertEquals("db2", tableConfig.getTable_schema());
        Assertions.assertEquals("unique_table_with_null", tableConfig.getTable_name());
        Assertions.assertEquals("OLAP", tableConfig.getTable_engine());
        Assertions.assertEquals("UNIQUE_KEYS", tableConfig.getTable_model());
        Assertions.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getPrimary_key());
        Assertions.assertEquals("", tableConfig.getPartition_key());
        Assertions.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getDistribute_key());
        Assertions.assertEquals("HASH", tableConfig.getDistribute_type());
        Assertions.assertEquals(3, tableConfig.getDistribute_bucket());
        Assertions.assertEquals("`k1`, `k2`, `k3`, `k4`, `k5`", tableConfig.getSort_key());

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
                Assertions.assertEquals("SYSTEM VIEW", tablesInfo.getTable_type());
            }
        }
        Assertions.assertTrue(checkTables);
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
        Assertions.assertEquals("db3", partitionMeta.getDb_name());
        Assertions.assertEquals("duplicate_table_with_null", partitionMeta.getTable_name());
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
        Assertions.assertEquals("RANDOM", tableConfig.getDistribute_type());
        Assertions.assertEquals(3, tableConfig.getDistribute_bucket());
        Assertions.assertEquals("", tableConfig.getDistribute_key());
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
        Assertions.assertEquals("true", props.get("dynamic_partition.enable"));
        Assertions.assertEquals("DAY", props.get("dynamic_partition.time_unit"));
        Assertions.assertEquals("-3", props.get("dynamic_partition.start"));
        Assertions.assertEquals("3", props.get("dynamic_partition.end"));
        Assertions.assertEquals("p", props.get("dynamic_partition.prefix"));
        Assertions.assertEquals("1", props.get("replication_num"));
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

        Assertions.assertNotNull(keywordList, "Keywords list should not be null");
        Assertions.assertFalse(keywordList.isEmpty(), "Keywords list should not be empty");

        List<String> keywordNames = keywordList.stream()
                .map(TKeywordInfo::getKeyword)
                .collect(Collectors.toList());

        Assertions.assertTrue(keywordNames.contains("SELECT"), "Keywords should contain 'SELECT'");
        Assertions.assertTrue(keywordNames.contains("INSERT"), "Keywords should contain 'INSERT'");
        Assertions.assertTrue(keywordNames.contains("UPDATE"), "Keywords should contain 'UPDATE'");
        Assertions.assertTrue(keywordNames.contains("DELETE"), "Keywords should contain 'DELETE'");
        Assertions.assertTrue(keywordNames.contains("TABLE"), "Keywords should contain 'TABLE'");
        Assertions.assertTrue(keywordNames.contains("INDEX"), "Keywords should contain 'INDEX'");
        Assertions.assertTrue(keywordNames.contains("VIEW"), "Keywords should contain 'VIEW'");
        Assertions.assertTrue(keywordNames.contains("USER"), "Keywords should contain 'USER'");
        Assertions.assertTrue(keywordNames.contains("PASSWORD"), "Keywords should contain 'PASSWORD'");

        TKeywordInfo selectKeyword = keywordList.stream()
                .filter(k -> k.getKeyword().equals("SELECT"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(selectKeyword, "SELECT keyword should be present");
        Assertions.assertTrue(selectKeyword.isReserved(), "SELECT keyword should be reserved");

        TKeywordInfo userKeyword = keywordList.stream()
                .filter(k -> k.getKeyword().equals("USER"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(userKeyword, "USER keyword should be present");
        Assertions.assertFalse(userKeyword.isReserved(), "USER keyword should not be reserved");
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

        Assertions.assertNotNull(rolesList, "Roles list should not be null");
        Assertions.assertFalse(rolesList.isEmpty(), "Roles list should not be empty");

        List<String> roleNames = rolesList.stream()
                .map(TApplicableRolesInfo::getRole_name)
                .collect(Collectors.toList());

        Assertions.assertTrue(roleNames.contains("root"), "Roles should contain 'root'");

        TApplicableRolesInfo adminRole = rolesList.stream()
                .filter(r -> r.getRole_name().equals("root"))
                .findFirst()
                .orElse(null);
        Assertions.assertNotNull(adminRole, "root role should be present");
        Assertions.assertEquals("root", adminRole.getUser(), "User should be root");
        Assertions.assertEquals("%", adminRole.getHost(), "Host should be %");
        Assertions.assertEquals("NO", "NO", "isGrantable should be NO");
        Assertions.assertEquals("NO", "NO", "isDefault should be NO");
        Assertions.assertEquals("NO", "NO", "isMandatory should be NO");
    }

    @Test
    public void testWarehouseMetrics() throws Exception {
        starRocksAssert.withEnableMV();
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetWarehouseMetricsRequest req = new TGetWarehouseMetricsRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("%");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);

        TGetWarehouseMetricsRespone response = impl.getWarehouseMetrics(req);
        Assertions.assertNotNull(response.getMetrics());

        starRocksAssert.query("select * from information_schema.warehouse_metrics;")
                .explainContains(" OUTPUT EXPRS:1: WAREHOUSE_ID | 2: WAREHOUSE_NAME | 3: QUEUE_PENDING_LENGTH " +
                        "| 4: QUEUE_RUNNING_LENGTH | 5: MAX_PENDING_LENGTH | 6: MAX_PENDING_TIME_SECOND " +
                        "| 7: EARLIEST_QUERY_WAIT_TIME | 8: MAX_REQUIRED_SLOTS | 9: SUM_REQUIRED_SLOTS | 10: REMAIN_SLOTS " +
                        "| 11: MAX_SLOTS | 12: EXTRA_MESSAGE\n");
    }

    @Test
    public void testWarehouseQueries() throws Exception {
        starRocksAssert.withEnableMV();
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetWarehouseQueriesRequest req = new TGetWarehouseQueriesRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("%");
        authInfo.setUser("root");
        authInfo.setUser_ip("%");
        req.setAuth_info(authInfo);

        TGetWarehouseQueriesResponse response = impl.getWarehouseQueries(req);
        Assertions.assertTrue(response.getQueries().isEmpty());

        starRocksAssert.query("select * from information_schema.warehouse_queries;")
                .explainContains(" OUTPUT EXPRS:1: WAREHOUSE_ID | 2: WAREHOUSE_NAME | 3: QUERY_ID | 4: STATE " +
                        "| 5: EST_COSTS_SLOTS | 6: ALLOCATE_SLOTS | 7: QUEUED_WAIT_SECONDS " +
                        "| 8: QUERY | 9: QUERY_START_TIME | 10: QUERY_END_TIME | 11: QUERY_DURATION | 12: EXTRA_MESSAGE\n");
    }

    @Test
    public void testMaterializedViewsEvaluation() throws Exception {
        starRocksAssert.withDatabase("d1").useDatabase("d1");
        starRocksAssert.withTable("create table t1 (c1 int, c2 int) properties('replication_num'='1') ");
        starRocksAssert.withMaterializedView("create materialized view test_mv1 refresh manual as select * from t1");

        MaterializedView mv = starRocksAssert.getMv("d1", "test_mv1");
        Assertions.assertTrue(mv != null);
        TaskManager taskManager = GlobalStateMgr.getCurrentState().getTaskManager();
        String taskName = TaskBuilder.getMvTaskName(mv.getId());
        Task task = taskManager.getTask(taskName);
        Assertions.assertTrue(task != null);

        TaskRunStatus taskRun = new TaskRunStatus();
        taskRun.setTaskName(taskName);
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
            public Map<String, List<TaskRunStatus>> listMVRefreshedTaskRunStatus(String dbName, Set<String> taskNames) {
                Map<String, List<TaskRunStatus>> result = Maps.newHashMap();
                result.put(taskName, Lists.newArrayList(taskRun));
                return result;
            }
        };
        // supported
        starRocksAssert.query("select TABLE_NAME, LAST_REFRESH_STATE,LAST_REFRESH_ERROR_CODE,IS_ACTIVE,INACTIVE_REASON\n" +
                        "from information_schema.materialized_views where table_name = 'test_mv1")
                .explainContains(" OUTPUT EXPRS:3: TABLE_NAME | 13: LAST_REFRESH_STATE " +
                                "| 19: LAST_REFRESH_ERROR_CODE | 5: IS_ACTIVE | 6: INACTIVE_REASON\n" +
                                "  PARTITION: UNPARTITIONED",
                        "'test_mv1' | 'true' | '' | 'SUCCESS' | '0'");
        starRocksAssert.query("select count(1) from information_schema.materialized_views")
                .explainContains("     constant exprs: ");
        starRocksAssert.query("select * from information_schema.materialized_views")
                .explainContains("     constant exprs: ",
                        "'d1' | 'test_mv1' | 'MANUAL' | 'true' | '' | 'UNPARTITIONED' ");
        starRocksAssert.query("select * from information_schema.materialized_views where table_name = 'test_mv1' ")
                .explainContains("     constant exprs: ",
                        "'d1' | 'test_mv1' | 'MANUAL' | 'true' | '' | 'UNPARTITIONED' ");
        starRocksAssert.query("select * from information_schema.materialized_views " +
                        "where TABLE_SCHEMA = 'd1' and TABLE_NAME = 'test_mv1'")
                .explainContains("     constant exprs: ",
                        "'d1' | 'test_mv1' | 'MANUAL' | 'true' | '' | 'UNPARTITIONED' ");
        starRocksAssert.query("select *, TASK_ID + 1 from information_schema.materialized_views " +
                        "where TABLE_SCHEMA = 'd1' and TABLE_NAME = 'test_mv1'")
                .explainContains("     constant exprs: ",
                        "'d1' | 'test_mv1' | 'MANUAL' | 'true' | '' | 'UNPARTITIONED' ");

        // not supported
        starRocksAssert.query("select * from information_schema.materialized_views where TABLE_NAME != 'test_mv1' ")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select * from information_schema.materialized_views where TABLE_SCHEMA = 'd1' or " +
                        "TABLE_NAME = 'test_mv1' ")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select * from information_schema.materialized_views where TABLE_NAME = 'test_mv1' " +
                        "or TABLE_NAME = 'test_mv2' ")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select * from information_schema.materialized_views where TASK_NAME = 'txxx' ")
                .explainContains("SCAN SCHEMA");
    }

    @Test
    public void testWarehouseMetricsEvaluation() throws Exception {
        starRocksAssert.withDatabase("d1").useDatabase("d1");
        BaseSlotManager slotManager = GlobalStateMgr.getCurrentState().getSlotManager();
        SlotSelectionStrategyV2 strategy = new SlotSelectionStrategyV2(slotManager, WarehouseManager.DEFAULT_WAREHOUSE_ID);
        SlotTracker slotTracker = new SlotTracker(slotManager, ImmutableList.of(strategy));

        new MockUp<SlotManager>() {
            @Mock
            public Map<Long, BaseSlotTracker> getWarehouseIdToSlotTracker() {
                Map<Long, BaseSlotTracker> result = Maps.newHashMap();
                result.put(WarehouseManager.DEFAULT_WAREHOUSE_ID, slotTracker);
                return result;
            }
        };
        // supported
        starRocksAssert.query("select count(1) from information_schema.warehouse_metrics")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select * from information_schema.warehouse_metrics")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select WAREHOUSE_NAME, REMAIN_SLOTS from information_schema.warehouse_metrics")
                .explainContains("constant exprs: \n" +
                        "         'default_warehouse' | '0'");
        starRocksAssert.query("select count(1) from information_schema.warehouse_metrics where warehouse_id = '0'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select * from information_schema.warehouse_metrics where WAREHOUSE_ID = '0'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select WAREHOUSE_NAME, REMAIN_SLOTS from information_schema.warehouse_metrics " +
                        "where WAREHOUSE_ID = 0")
                .explainContains("constant exprs: \n" +
                        "         'default_warehouse' | '0'");
        starRocksAssert.query("select count(1) from information_schema.warehouse_metrics where WAREHOUSE_NAME = " +
                        "'default_warehouse'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select * from information_schema.warehouse_metrics where WAREHOUSE_NAME= 'default_warehouse'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select WAREHOUSE_NAME, REMAIN_SLOTS from information_schema.warehouse_metrics " +
                        "where WAREHOUSE_NAME = 'default_warehouse'")
                .explainContains("constant exprs: \n" +
                        "         'default_warehouse' | '0'");

        // not supported
        starRocksAssert.query("select count(1) from information_schema.warehouse_metrics where WAREHOUSE_ID != '0'")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select * from information_schema.warehouse_metrics where WAREHOUSE_ID = 0 and " +
                        "WAREHOUSE_NAME = 'default_warehouse'")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select * from information_schema.warehouse_metrics where WAREHOUSE_ID = 0 or " +
                        "WAREHOUSE_NAME = 'default_warehouse'")
                .explainContains("SCAN SCHEMA");
    }

    private static LogicalSlot generateSlot(int numSlots) {
        return new LogicalSlot(UUIDUtil.genTUniqueId(), "fe", WarehouseManager.DEFAULT_WAREHOUSE_ID,
                LogicalSlot.ABSENT_GROUP_ID, numSlots, 0, 0, 0,
                0, 0);
    }

    @Test
    public void testWarehouseQueriesEvaluation() throws Exception {
        starRocksAssert.withDatabase("d1").useDatabase("d1");
        LogicalSlot slot1 = generateSlot(1);
        new MockUp<SlotManager>() {
            @Mock
            public List<LogicalSlot> getSlots() {
                List<LogicalSlot> result = Lists.newArrayList();
                result.add(slot1);
                return result;
            }
        };
        // supported
        starRocksAssert.query("select count(1) from information_schema.warehouse_queries")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select * from information_schema.warehouse_queries")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select WAREHOUSE_NAME, EST_COSTS_SLOTS from information_schema.warehouse_queries")
                .explainContains("     constant exprs: \n" +
                        "         'default_warehouse' | '1'");
        starRocksAssert.query("select count(1) from information_schema.warehouse_queries where warehouse_id = '0'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select * from information_schema.warehouse_queries where WAREHOUSE_ID = '0'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select WAREHOUSE_NAME, EST_COSTS_SLOTS from information_schema.warehouse_queries " +
                        "where WAREHOUSE_ID = 0")
                .explainContains("     constant exprs: \n" +
                        "         'default_warehouse' | '1'");
        starRocksAssert.query("select count(1) from information_schema.warehouse_queries where WAREHOUSE_NAME = " +
                        "'default_warehouse'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select * from information_schema.warehouse_queries where WAREHOUSE_NAME= 'default_warehouse'")
                .explainContains("     constant exprs: \n" +
                        "         '0'");
        starRocksAssert.query("select WAREHOUSE_NAME, EST_COSTS_SLOTS from information_schema.warehouse_queries " +
                        "where WAREHOUSE_NAME = 'default_warehouse'")
                .explainContains("     constant exprs: \n" +
                        "         'default_warehouse' | '1'");

        // not supported
        starRocksAssert.query("select count(1) from information_schema.warehouse_queries where WAREHOUSE_ID != '0'")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select * from information_schema.warehouse_queries where WAREHOUSE_ID = 0 and " +
                        "WAREHOUSE_NAME = 'default_warehouse'")
                .explainContains("SCAN SCHEMA");
        starRocksAssert.query("select * from information_schema.warehouse_queries where WAREHOUSE_ID = 0 or " +
                        "WAREHOUSE_NAME = 'default_warehouse'")
                .explainContains("SCAN SCHEMA");
    }

    @Test
    public void testTablesSystemTable() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("test_db").useDatabase("test_db");
        String createTblStmtStr = "CREATE TABLE test_db.`test_tbl1` (\n" +
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

        {
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_tbl2'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_NAME = 'test_tbl2'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_SCHEMA = 'test_db_xxx'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select * from information_schema.tables")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select * from information_schema.tables where TABLE_SCHEMA = 'test_db'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select * from information_schema.tables where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_tbl1'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.tables")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_SCHEMA = 'test_db'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_tbl1'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_tbl2'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_SCHEMA = 'test_db' or " +
                            "TABLE_NAME ='test_view1'")
                    .explainWithout("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.tables where ENGINE = 'xxx'")
                    .explainWithout("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.tables where TABLE_CATALOG= 'xxx'")
                    .explainWithout("     constant exprs: ");
        }

        TGetTablesInfoRequest params = new TGetTablesInfoRequest();
        TAuthInfo authInfo = new TAuthInfo();
        authInfo.setPattern("test_db");
        authInfo.setCurrent_user_ident(UserIdentityUtils.toThrift(connectContext.getCurrentUserIdentity()));
        params.setAuth_info(authInfo);
        {
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TGetTablesInfoResponse result = impl.getTablesInfo(params);
            Assertions.assertEquals(1, result.getTables_infos().size());
            TTableInfo tableStatus = result.getTables_infos().get(0);
            Assertions.assertEquals("test_tbl1", tableStatus.getTable_name());
        }
        {
            TGetTablesInfoResponse result = TablesSystemTable.query(params);
            Assertions.assertEquals(1, result.getTables_infos().size());
            TTableInfo tableStatus = result.getTables_infos().get(0);
            Assertions.assertEquals("test_tbl1", tableStatus.getTable_name());
        }
        {
            authInfo.setPattern("test_db2");
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TGetTablesInfoResponse result = impl.getTablesInfo(params);
            Assertions.assertEquals(0, result.getTables_infos().size(),
                    "Should return empty result for non-existing db");
        }
        {
            params.setTable_name("test_tabl2_xx");
            TGetTablesInfoResponse result = TablesSystemTable.query(params);
            Assertions.assertEquals(0, result.getTables_infos().size(),
                    "Should return empty result for non-existing db");
        }
    }

    @Test
    public void testTablesSystemView() throws Exception {
        starRocksAssert.withEnableMV().withDatabase("test_db").useDatabase("test_db");
        String createTblStmtStr = "CREATE TABLE test_db.`test_tbl1` (\n" +
                "  `k1` date  COMMENT \"\",\n" +
                "  `k2` datetime  COMMENT \"\",\n" +
                "  `k3` varchar(20)  COMMENT \"\"\n" +
                ") ENGINE=OLAP\n" +
                "PARTITION BY RANGE(k1)()\n" +
                "DISTRIBUTED BY HASH(k2) BUCKETS 3;";
        starRocksAssert.withTable(createTblStmtStr);
        starRocksAssert.withView("CREATE VIEW test_db.test_view1 AS SELECT k1, k2 FROM test_db.test_tbl1");

        {
            starRocksAssert.query("select count(1) from information_schema.views where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_tbl2'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select count(1) from information_schema.views where TABLE_NAME = 'test_tbl2'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select count(1) from information_schema.views where TABLE_SCHEMA = 'test_db_xxx'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select * from information_schema.views")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select * from information_schema.views where TABLE_SCHEMA = 'test_db'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select * from information_schema.views where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_view1'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select * from information_schema.views where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_tbl1'")
                    .explainContains("EMPTYSET");
            starRocksAssert.query("select count(1) from information_schema.views")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.views where TABLE_SCHEMA = 'test_db'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.views where TABLE_SCHEMA = 'test_db' " +
                            "and TABLE_NAME = 'test_view1'")
                    .explainContains("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.views where TABLE_SCHEMA = 'test_db' or " +
                            "TABLE_NAME ='test_view1'")
                    .explainWithout("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.views where DEFINER = 'xxx'")
                    .explainWithout("     constant exprs: ");
            starRocksAssert.query("select count(1) from information_schema.views where TABLE_CATALOG= 'xxx'")
                    .explainWithout("     constant exprs: ");
        }

        TGetTablesParams params = new TGetTablesParams();
        params.setCurrent_user_ident(UserIdentityUtils.toThrift(connectContext.getCurrentUserIdentity()));
        params.setDb("test_db");
        params.setType(TTableType.SCHEMA_TABLE);

        {
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TListTableStatusResult result = impl.listTableStatus(params);
            Assertions.assertEquals(2, result.getTables().size());
            Set<String> tableNames = result.getTables().stream()
                    .map(TTableStatus::getName)
                    .collect(Collectors.toUnmodifiableSet());
            Assertions.assertEquals(Set.of("test_tbl1", "test_view1"), tableNames);
        }
        {
            params.setDb("test_db");
            params.setType(TTableType.SCHEMA_TABLE);
            TListTableStatusResult result = ViewsSystemTable.query(params, connectContext);
            Assertions.assertEquals(2, result.getTables().size());
            Set<String> tableNames = result.getTables().stream()
                    .map(TTableStatus::getName)
                    .collect(Collectors.toUnmodifiableSet());
            Assertions.assertEquals(Set.of("test_tbl1", "test_view1"), tableNames);
        }
        {
            params.setDb("test_db");
            params.setType(TTableType.VIEW);
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TListTableStatusResult result = impl.listTableStatus(params);
            Assertions.assertEquals(1, result.getTables().size());
            TTableStatus tableStatus = result.getTables().get(0);
            Assertions.assertEquals("test_view1", tableStatus.getName());

        }
        {
            params.setDb("test_db");
            params.setType(TTableType.VIEW);
            TListTableStatusResult result = ViewsSystemTable.query(params, connectContext);
            Assertions.assertEquals(1, result.getTables().size());
            TTableStatus tableStatus = result.getTables().get(0);
            Assertions.assertEquals("test_view1", tableStatus.getName());
        }
        {
            params.setDb("test_db2");
            FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
            TListTableStatusResult result = impl.listTableStatus(params);
            Assertions.assertEquals(0, result.getTables().size(),
                    "Should return empty result for non-existing db");
        }
        {
            params.setDb("test_db1");
            params.setType(TTableType.VIEW);
            TListTableStatusResult result = ViewsSystemTable.query(params, connectContext);
            Assertions.assertEquals(0, result.getTables().size(),
                    "Should return empty result for non-existing db");
        }
    }
}
