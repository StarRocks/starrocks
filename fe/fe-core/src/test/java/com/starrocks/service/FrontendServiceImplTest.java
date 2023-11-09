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
import com.google.common.collect.Maps;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.DistributionInfo;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.Partition;
<<<<<<< HEAD
=======
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
>>>>>>> cb267d1332 ([BugFix] Fix bug list automatic partition lost meta info (#34590))
import com.starrocks.catalog.Table;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TGetLoadTxnStatusRequest;
import com.starrocks.thrift.TGetLoadTxnStatusResult;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTablesResult;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TResourceUsage;
import com.starrocks.thrift.TSetConfigRequest;
import com.starrocks.thrift.TSetConfigResponse;
import com.starrocks.thrift.TStatus;
import com.starrocks.thrift.TStatusCode;
import com.starrocks.thrift.TStreamLoadPutRequest;
import com.starrocks.thrift.TStreamLoadPutResult;
import com.starrocks.thrift.TTableInfo;
import com.starrocks.thrift.TTableStatus;
import com.starrocks.thrift.TTableType;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TUniqueId;
import com.starrocks.thrift.TUpdateResourceUsageRequest;
import com.starrocks.thrift.TUserIdentity;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.alter_scheduler_interval_millisecond = 100;
        Config.dynamic_partition_enable = true;
        Config.dynamic_partition_check_interval_seconds = 1;
        Config.enable_strict_storage_medium_check = false;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE site_access_empty (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE site_access_border (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE site_access_hour (\n" +
                        "    event_day DATETIME,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('hour', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE site_access_day (\n" +
                        "    event_day DATE,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day) (\n" +
                        "START (\"2020-06-01\") END (\"2022-06-05\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE site_access_month (\n" +
                        "    event_day DATE,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('month', event_day) (\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE site_access_slice (\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY time_slice(event_day, interval 5 day)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\"replication_num\" = \"1\");")
                .withTable("CREATE TABLE site_access_list (\n" +
                        "    event_day DATE not null,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY (event_day) (\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withView("create view v as select * from site_access_empty")
                .withView("create view v1 as select current_role()")
                .withView("create view v2 as select current_user()")
                .withView("create view v3 as select database()")
                .withView("create view v4 as select user()")
                .withView("create view v5 as select CONNECTION_ID()")
                .withView("create view v6 as select CATALOG()");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists site_access";
        String dropSQL2 = "drop table if exists site_access_2";
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
        Table table = db.getTable("site_access_day");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Partition p19900424 = table.getPartition("p19900424");
        Assert.assertNotNull(p19900424);

        partition = impl.createPartition(request);
        Assert.assertEquals(1, partition.partitions.size());
    }

    @Test
    public void testCreatePartitionApiSlice() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_slice");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Partition p19900424 = table.getPartition("p19900424");
        Assert.assertNotNull(p19900424);

        partition = impl.createPartition(request);
        Assert.assertEquals(1, partition.partitions.size());
    }

    @Test
    public void testCreatePartitionApiMultiValues() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_day");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24");
        partitionValues.add(values);
        List<String> values1 = Lists.newArrayList();
        partitionValues.add(values1);
        values1.add("1990-04-24");
        List<String> values2 = Lists.newArrayList();
        values2.add("1989-11-02");
        partitionValues.add(values2);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Partition p19891102 = table.getPartition("p19891102");
        Assert.assertNotNull(p19891102);

        partition = impl.createPartition(request);
        Assert.assertEquals(2, partition.partitions.size());
    }

    @Test
    public void testCreatePartitionApiMonth() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_month");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24");
        partitionValues.add(values);
        List<String> values1 = Lists.newArrayList();
        values1.add("1990-04-30");
        partitionValues.add(values1);
        List<String> values2 = Lists.newArrayList();
        values2.add("1990-04-01");
        partitionValues.add(values2);
        List<String> values3 = Lists.newArrayList();
        values3.add("1990-04-25");
        partitionValues.add(values3);
        List<String> values4 = Lists.newArrayList();
        values4.add("1989-11-02");
        partitionValues.add(values4);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Partition p199004 = table.getPartition("p199004");
        Assert.assertNotNull(p199004);

        partition = impl.createPartition(request);
        Assert.assertEquals(2, partition.partitions.size());
    }

    @Test
    public void testCreatePartitionApiBorder() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_border");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("NULL");
        partitionValues.add(values);
        List<String> values1 = Lists.newArrayList();
        values1.add("0000-01-01");
        partitionValues.add(values1);
        List<String> values2 = Lists.newArrayList();
        values2.add("9999-12-31");
        partitionValues.add(values2);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Partition p00000101 = table.getPartition("p00000101");
        Assert.assertNotNull(p00000101);
        Partition p99991231 = table.getPartition("p99991231");
        Assert.assertNotNull(p99991231);
        partition = impl.createPartition(request);
        Assert.assertEquals(2, partition.partitions.size());
    }

    @Test
    public void testAutomaticPartitionLimitExceed() throws TException {
        Config.max_automatic_partition_number = 1;
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_slice");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1991-04-24");
        partitionValues.add(values);
        List<String> values2 = Lists.newArrayList();
        values2.add("1991-04-25");
        partitionValues.add(values2);
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        Assert.assertTrue(partition.getStatus().getError_msgs().get(0).contains("max_automatic_partition_number"));
        Config.max_automatic_partition_number = 4096;
    }

    private TGetTablesParams buildListTableStatusParam() {
        TGetTablesParams request = new TGetTablesParams();
        request.setDb("test");
        TUserIdentity tUserIdentity = new TUserIdentity();
        tUserIdentity.setUsername("root");
        tUserIdentity.setHost("%");
        tUserIdentity.setIs_domain(false);
        request.setCurrent_user_ident(tUserIdentity);
        request.setType(TTableType.VIEW);

        return request;
    }

    @Test
<<<<<<< HEAD
=======
    public void testGetTableNames() throws TException {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesParams params = new TGetTablesParams();
        params.setCatalog_name("default_catalog");
        params.setDb("test");
        TUserIdentity tUserIdentity = new TUserIdentity();
        tUserIdentity.setUsername("root");
        tUserIdentity.setHost("%");
        tUserIdentity.setIs_domain(false);
        params.setCurrent_user_ident(tUserIdentity);

        TGetTablesResult result = impl.getTableNames(params);
        Assert.assertEquals(16, result.tables.size());
    }

    @Test
>>>>>>> cb267d1332 ([BugFix] Fix bug list automatic partition lost meta info (#34590))
    public void testListTableStatus() throws TException {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TListTableStatusResult result = impl.listTableStatus(buildListTableStatusParam());
        Assert.assertEquals(7, result.tables.size());
    }

    @Test
    public void testListViewStatusWithBaseTableDropped() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE site_access_empty_for_view (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('day', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id)\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");
        starRocksAssert.withView("create view test.view11 as select * from test.site_access_empty_for_view");
        // drop the base table referenced by test.view11
        starRocksAssert.dropTable("test.site_access_empty_for_view");
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TListTableStatusResult result = impl.listTableStatus(buildListTableStatusParam());
        System.out.println(result.tables.stream().map(TTableStatus::getName).collect(Collectors.toList()));
        Assert.assertEquals(8, result.tables.size());
        starRocksAssert.dropView("test.view11");
    }

    @Test
    public void testCreatePartitionApiHour() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_hour");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24 12:34:56");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        Assert.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Partition p1990042412 = table.getPartition("p1990042412");
        Assert.assertNotNull(p1990042412);

        partition = impl.createPartition(request);
        Assert.assertEquals(1, partition.partitions.size());
    }

    @Test
    public void testCreateEmptyPartition() {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_empty");
        Collection<Partition> partitions = table.getPartitions();
        Assert.assertEquals(1, partitions.size());
        String name = partitions.iterator().next().getName();
        Assert.assertEquals(ExpressionRangePartitionInfo.AUTOMATIC_SHADOW_PARTITION_NAME, name);
        Assert.assertTrue(name.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX));
    }

    @Test(expected = AnalysisException.class)
    public void testCreateCeilForbidAutomaticTable() throws Exception {
        starRocksAssert.withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE site_access_ceil (\n" +
                        "    event_day datetime,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY time_slice(event_day, interval 1 day, CEIL) \n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\"replication_num\" = \"1\");");
    }

    @Test(expected = AnalysisException.class)
    public void testCreateTimeSliceForbidAutomaticTable() throws Exception {
        starRocksAssert.withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE site_access_time_slice_hour_date (\n" +
                        "    event_day date,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY time_slice(event_day, interval 1 hour) \n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\"replication_num\" = \"1\");");
    }

    @Test(expected = AnalysisException.class)
    public void testCreateDateTruncForbidAutomaticTable() throws Exception {
        starRocksAssert.withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE site_access_date_trunc_hour_date (\n" +
                        "    event_day DATE,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('hour', event_day)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\"replication_num\" = \"1\");");
    }

    @Test(expected = AnalysisException.class)
    public void testUnsupportedAutomaticTableGranularityDoesNotMatch() throws Exception {
        starRocksAssert.withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE site_access_granularity_does_not_match(\n" +
                        "    event_day DATE NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ") \n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('month', event_day)(\n" +
                        "    START (\"2023-05-01\") END (\"2023-05-03\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");
    }

    @Test(expected = AnalysisException.class)
    public void testUnsupportedAutomaticTableGranularityDoesNotMatch2() throws Exception {
        starRocksAssert.withDatabase("test2").useDatabase("test2")
                .withTable("CREATE TABLE site_access_granularity_does_not_match2(\n" +
                        "    event_day DATE NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ") \n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "PARTITION BY date_trunc('month', event_day)(\n" +
                        "   START (\"2022-05-01\") END (\"2022-05-03\") EVERY (INTERVAL 1 day),\n" +
                        "    START (\"2023-05-01\") END (\"2023-05-03\") EVERY (INTERVAL 1 day)\n" +
                        ")\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"partition_live_number\" = \"3\",\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");
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
                        "COMMENT \"OLAP\"\n" +
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
                        "COMMENT \"OLAP\"\n" +
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
        String grantSql = "GRANT SELECT ON TABLE test_table.t1 TO USER `test1`@`%`;";
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
    public void testStreamLoadPutColumnMapException() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_hour");

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.setDb("test");
        request.setTbl("site_access_hour");
        request.setUser("root");
        request.setTxnId(1024);
        request.setColumnSeparator(",");
        request.setSkipHeader(1);
        request.setFileType(TFileType.FILE_STREAM);

        // wrong format of str_to_date()
        request.setColumns("col1,event_day=str_to_date(col1)");

        TStreamLoadPutResult result = impl.streamLoadPut(request);
        TStatus status = result.getStatus();
        request.setFileType(TFileType.FILE_STREAM);
        Assert.assertEquals(TStatusCode.ANALYSIS_ERROR, status.getStatus_code());
        List<String> errMsg = status.getError_msgs();
        Assert.assertEquals(1, errMsg.size());
        Assert.assertEquals(
                "Getting analyzing error from line 1, column 24 to line 1, column 40. Detail message: " +
                        "No matching function with signature: str_to_date(varchar).",
                errMsg.get(0));
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

    @Test
    public void testSetFrontendConfig() throws TException {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TSetConfigRequest request = new TSetConfigRequest();
        request.keys = Lists.newArrayList("mysql_server_version");
        request.values = Lists.newArrayList("5.1.1");

        TSetConfigResponse result = impl.setConfig(request);
        Assert.assertEquals("5.1.1", GlobalVariable.version);
    }
<<<<<<< HEAD
=======

    @Test
    public void testLoadTxnCommitRateLimitExceeded() throws UserException, TException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TLoadTxnCommitRequest request = new TLoadTxnCommitRequest();
        request.db = "test";
        request.tbl = "tbl_test";
        request.txnId = 1001L;
        request.setAuth_code(100);
        request.commitInfos = new ArrayList<>();
        long now = System.currentTimeMillis();
        doThrow(new CommitRateExceededException(1001, now + 100)).when(impl).loadTxnCommitImpl(any(), any());
        TLoadTxnCommitResult result = impl.loadTxnCommit(request);
        Assert.assertEquals(TStatusCode.SR_EAGAIN, result.status.status_code);
        Assert.assertTrue(result.retry_interval_ms >= (now + 100 - System.currentTimeMillis()));
    }

    @Test
    public void testLoadTxnCommitFailed() throws UserException, TException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TLoadTxnCommitRequest request = new TLoadTxnCommitRequest();
        request.db = "test";
        request.tbl = "tbl_test";
        request.txnId = 1001L;
        request.setAuth_code(100);
        request.commitInfos = new ArrayList<>();
        doThrow(new UserException("injected error")).when(impl).loadTxnCommitImpl(any(), any());
        TLoadTxnCommitResult result = impl.loadTxnCommit(request);
        Assert.assertEquals(TStatusCode.ANALYSIS_ERROR, result.status.status_code);
    }

    @Test
    public void testAddListPartitionConcurrency() throws UserException, TException {

        Database db = GlobalStateMgr.getCurrentState().getDb("test");
        Table table = db.getTable("site_access_list");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24");
        partitionValues.add(values);
        List<String> values2 = Lists.newArrayList();
        values2.add("1990-04-25");
        partitionValues.add(values2);
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);

        GlobalStateMgr currentState = GlobalStateMgr.getCurrentState();
        Database testDb = currentState.getDb("test");
        OlapTable olapTable = (OlapTable) testDb.getTable("site_access_list");
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
        List<PartitionDesc> partitionDescs = Lists.newArrayList();
        Partition p19910425 = olapTable.getPartition("p19900425");

        partitionDescs.add(new ListPartitionDesc(Lists.newArrayList("p19900425"),
                Lists.newArrayList(new SingleItemListPartitionDesc(true, "p19900425",
                        Lists.newArrayList("1990-04-25"), Maps.newHashMap()))));

        AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDescs.get(0),
                defaultDistributionInfo.toDistributionDesc(), Maps.newHashMap(), false);

        List<Partition> partitionList = Lists.newArrayList();
        partitionList.add(p19910425);

        currentState.getLocalMetastore().addListPartitionLog(testDb, olapTable, partitionDescs,
                addPartitionClause, partitionInfo, partitionList, Sets.newSet("p19900425"));

    }
>>>>>>> cb267d1332 ([BugFix] Fix bug list automatic partition lost meta info (#34590))
}
