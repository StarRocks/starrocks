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
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.load.batchwrite.BatchWriteMgr;
import com.starrocks.load.batchwrite.RequestLoadResult;
import com.starrocks.load.batchwrite.TableId;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.DDLStmtExecutor;
import com.starrocks.qe.GlobalVariable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.AddPartitionClause;
import com.starrocks.sql.ast.DropTableStmt;
import com.starrocks.sql.ast.ListPartitionDesc;
import com.starrocks.sql.ast.PartitionDesc;
import com.starrocks.sql.ast.SingleItemListPartitionDesc;
import com.starrocks.thrift.TAuthInfo;
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TGetDictQueryParamRequest;
import com.starrocks.thrift.TGetDictQueryParamResponse;
import com.starrocks.thrift.TGetLoadTxnStatusRequest;
import com.starrocks.thrift.TGetLoadTxnStatusResult;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTablesResult;
import com.starrocks.thrift.TImmutablePartitionRequest;
import com.starrocks.thrift.TImmutablePartitionResult;
import com.starrocks.thrift.TListMaterializedViewStatusResult;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TLoadTxnBeginRequest;
import com.starrocks.thrift.TLoadTxnBeginResult;
import com.starrocks.thrift.TLoadTxnCommitRequest;
import com.starrocks.thrift.TLoadTxnCommitResult;
import com.starrocks.thrift.TMergeCommitRequest;
import com.starrocks.thrift.TMergeCommitResult;
import com.starrocks.thrift.TPartitionMeta;
import com.starrocks.thrift.TPartitionMetaRequest;
import com.starrocks.thrift.TPartitionMetaResponse;
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
import com.starrocks.transaction.CommitRateExceededException;
import com.starrocks.transaction.GlobalTransactionMgr;
import com.starrocks.transaction.TransactionState;
import com.starrocks.transaction.TransactionState.TxnCoordinator;
import com.starrocks.transaction.TransactionState.TxnSourceType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.internal.util.collections.Sets;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_ASYNC;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_INTERVAL_MS;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_BATCH_WRITE_PARALLEL;
import static com.starrocks.load.streamload.StreamLoadHttpHeader.HTTP_ENABLE_BATCH_WRITE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.spy;

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
    public void testUpdateImmutablePartitionException() throws TException {
        new MockUp<FrontendServiceImpl>() {
            @Mock
            public synchronized TImmutablePartitionResult updateImmutablePartitionInternal(
                        TImmutablePartitionRequest request) {
                throw new RuntimeException("test");
            }
        };

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        TImmutablePartitionResult partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
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
        Config.stream_load_max_txn_num_per_be = 5;
        UtFrameUtils.createMinStarRocksCluster();
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                    .withTable("CREATE TABLE site_access_auto (\n" +
                                "    event_day DATETIME NOT NULL,\n" +
                                "    site_id INT DEFAULT '10',\n" +
                                "    city_code VARCHAR(100),\n" +
                                "    user_name VARCHAR(32) DEFAULT '',\n" +
                                "    pv BIGINT DEFAULT '0'\n" +
                                ")\n" +
                                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                                "DISTRIBUTED BY RANDOM\n" +
                                "PROPERTIES(\n" +
                                "    \"replication_num\" = \"1\"\n" +
                                ");")
                    .withTable("CREATE TABLE site_access_exception (\n" +
                                "    event_day DATETIME NOT NULL,\n" +
                                "    site_id INT DEFAULT '10',\n" +
                                "    city_code VARCHAR(100),\n" +
                                "    user_name VARCHAR(32) DEFAULT '',\n" +
                                "    pv BIGINT DEFAULT '0'\n" +
                                ")\n" +
                                "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                                "DISTRIBUTED BY RANDOM\n" +
                                "PROPERTIES(\n" +
                                "    \"replication_num\" = \"1\"\n" +
                                ");")
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
                    .withView("create view v6 as select CATALOG()")

                    .withMaterializedView("create materialized view mv refresh async as select * from site_access_empty");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists site_access";
        String dropSQL2 = "drop table if exists site_access_2";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
            DropTableStmt dropTableStmt2 = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL2, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt2);
        } catch (Exception ex) {

        }
    }

    @Test
    public void testImmutablePartitionException() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "site_access_exception");
        List<Long> partitionIds = Lists.newArrayList();
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        TImmutablePartitionResult partition = impl.updateImmutablePartition(request);
        Table t = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "v");

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);

        request.setDb_id(db.getId());
        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);

        request.setTable_id(t.getId());
        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);

        request.setTable_id(table.getId());
        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);

        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);

        partitionIds.add(1L);
        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);

        partitionIds = table.getPhysicalPartitions().stream()
                    .map(PhysicalPartition::getId).collect(Collectors.toList());
        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
    }

    @Test
    public void testImmutablePartitionApi() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "site_access_auto");
        List<Long> partitionIds = table.getPhysicalPartitions().stream()
                    .map(PhysicalPartition::getId).collect(Collectors.toList());
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_ids(partitionIds);
        TImmutablePartitionResult partition = impl.updateImmutablePartition(request);

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Assert.assertEquals(2, table.getPhysicalPartitions().size());

        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Assert.assertEquals(2, table.getPhysicalPartitions().size());

        partitionIds = table.getPhysicalPartitions().stream()
                    .map(PhysicalPartition::getId).collect(Collectors.toList());
        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Assert.assertEquals(3, table.getPhysicalPartitions().size());
    }

    @Test
    public void testCreatePartitionApi() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_day");
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
    public void testCreatePartitionWithSchemaChange() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_day");
        ((OlapTable) table).setState(OlapTable.OlapTableState.SCHEMA_CHANGE);

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

        Assert.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());
        ((OlapTable) table).setState(OlapTable.OlapTableState.NORMAL);
    }

    @Test
    public void testCreatePartitionWithRollup() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_day");
        ((OlapTable) table).setState(OlapTable.OlapTableState.ROLLUP);

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

        Assert.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());
        ((OlapTable) table).setState(OlapTable.OlapTableState.NORMAL);
    }

    @Test
    public void testCreatePartitionExceedLimit() throws TException {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_day");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-24");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);

        Config.thrift_server_max_worker_threads = 4;
        TCreatePartitionResult partition = impl.createPartition(request);
        Config.thrift_server_max_worker_threads = 4096;

        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.SERVICE_UNAVAILABLE);
    }

    @Test
    public void testLoadTxnBegin() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
        request.setLabel("test_label");
        request.setDb("test");
        request.setTbl("site_access_auto");
        request.setUser("root");
        request.setPasswd("");

        new MockUp<SessionVariable>() {
            @Mock
            public boolean isEnableLoadProfile() {
                return true;
            }
        };

        TLoadTxnBeginResult result = impl.loadTxnBegin(request);
        Assert.assertEquals(result.getStatus().getStatus_code(), TStatusCode.OK);
    }

    @Test
    public void testCreatePartitionApiSlice() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_slice");
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
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_day");
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
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_month");
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
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_border");
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
        Config.max_partition_number_per_table = 1;
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_slice");
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
        Assert.assertTrue(partition.getStatus().getError_msgs().get(0).contains("max_partition_number_per_table"));
        Config.max_partition_number_per_table = 100000;
    }

    @Test
    public void testAutomaticPartitionPerLoadLimitExceed() throws TException {
        TransactionState state = new TransactionState();
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return state;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_month");
        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1999-04-29");
        partitionValues.add(values);
        List<String> values2 = Lists.newArrayList();
        values2.add("1999-03-28");
        partitionValues.add(values2);
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);
        TCreatePartitionResult partition = impl.createPartition(request);
        Assert.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());

        Config.max_partitions_in_one_batch = 1;

        partition = impl.createPartition(request);
        Assert.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        Assert.assertTrue(partition.getStatus().getError_msgs().get(0).contains("max_partitions_in_one_batch"));

        Config.max_partitions_in_one_batch = 4096;
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
        Assert.assertEquals(17, result.tables.size());
    }

    @Test
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
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_hour");
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
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_empty");
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
    public void testGetSpecialColumnForSyncMv() throws Exception {
        starRocksAssert.withDatabase("test_table").useDatabase("test_table")
                    .withTable("CREATE TABLE `base1` (\n" +
                                "event_day DATE,\n" +
                                "department_id int(11) NOT NULL COMMENT \"\"\n" +
                                ") ENGINE=OLAP\n" +
                                "DUPLICATE KEY(event_day, department_id)\n" +
                                "DISTRIBUTED BY HASH(department_id) BUCKETS 1\n" +
                                "PROPERTIES (\n" +
                                "\"replication_num\" = \"1\",\n" +
                                "\"in_memory\" = \"false\",\n" +
                                "\"storage_format\" = \"DEFAULT\",\n" +
                                "\"enable_persistent_index\" = \"false\"\n" +
                                ");")
                    .withMaterializedView("create materialized view test_table.mv$test as select event_day from base1");

        ConnectContext ctx = starRocksAssert.getCtx();
        String createUserSql = "create user test4";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(createUserSql, ctx), ctx);
        String grantSql = "GRANT SELECT ON TABLE test_table.base1 TO USER `test4`@`%`;";
        DDLStmtExecutor.execute(UtFrameUtils.parseStmtWithNewParser(grantSql, ctx), ctx);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetTablesParams request = new TGetTablesParams();
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("test4");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        request.setCurrent_user_ident(userIdentity);
        request.setPattern("mv$test");
        request.setDb("test_table");
        request.setType(TTableType.MATERIALIZED_VIEW);
        TListMaterializedViewStatusResult response = impl.listMaterializedViewStatus(request);
        Assert.assertEquals(1, response.materialized_views.size());
    }

    @Test
    public void testGetLoadTxnStatus() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_day");
        UUID uuid = UUID.randomUUID();
        TUniqueId requestId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        long transactionId = GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().beginTransaction(db.getId(),
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
        Assert.assertNull(result1.getReason());
        request.setDb("test");
        TGetLoadTxnStatusResult result2 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.UNKNOWN, result2.getStatus());
        Assert.assertNull(result2.getReason());
        request.setTxnId(transactionId);
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);
        TGetLoadTxnStatusResult result3 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.UNKNOWN, result3.getStatus());
        Assert.assertNull(result3.getReason());
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        TGetLoadTxnStatusResult result4 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.PREPARE, result4.getStatus());
        Assert.assertEquals("", result4.getReason());
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                db.getId(), transactionId, "artificial failure");
        TGetLoadTxnStatusResult result5 = impl.getLoadTxnStatus(request);
        Assert.assertEquals(TTransactionStatus.ABORTED, result5.getStatus());
        Assert.assertEquals("artificial failure", result5.getReason());
    }

    @Test
    public void testStreamLoadPutColumnMapException() {
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
                    "Expr 'str_to_date(`col1`)' analyze error: No matching function with signature: str_to_date(varchar), " +
                                "derived column is 'event_day'",
                    errMsg.get(0));
    }

    @Test
    public void testSetFrontendConfig() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TSetConfigRequest request = new TSetConfigRequest();
        request.keys = Lists.newArrayList("mysql_server_version");
        request.values = Lists.newArrayList("5.1.1");

        TSetConfigResponse result = impl.setConfig(request);
        Assert.assertEquals("5.1.1", GlobalVariable.version);

        request.keys = Lists.newArrayList("adaptive_choose_instances_threshold");
        request.values = Lists.newArrayList("98");
        request.setUser_identity("root");
        request.setIs_persistent(true);
        impl.setConfig(request);

        PatternMatcher matcher = PatternMatcher.createMysqlPattern("adaptive_choose_instances_threshold", false);
        List<List<String>> configs = Config.getConfigInfo(matcher);
        Assert.assertEquals("98", configs.get(0).get(2));
        Assert.assertEquals(98, Config.adaptive_choose_instances_threshold);
    }

    @Test
    public void testLoadTxnCommitRateLimitExceeded() throws StarRocksException, TException, LockTimeoutException {
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
    public void testLoadTxnCommitTimeout() throws StarRocksException, TException, LockTimeoutException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TLoadTxnCommitRequest request = new TLoadTxnCommitRequest();
        request.db = "test";
        request.tbl = "tbl_test";
        request.txnId = 1001L;
        request.setAuth_code(100);
        request.commitInfos = new ArrayList<>();
        doThrow(new LockTimeoutException("get database write lock timeout")).when(impl).loadTxnCommitImpl(any(), any());
        TLoadTxnCommitResult result = impl.loadTxnCommit(request);
        Assert.assertEquals(TStatusCode.TIMEOUT, result.status.status_code);
    }

    @Test
    public void testLoadTxnCommitFailed() throws StarRocksException, TException, LockTimeoutException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TLoadTxnCommitRequest request = new TLoadTxnCommitRequest();
        request.db = "test";
        request.tbl = "tbl_test";
        request.txnId = 1001L;
        request.setAuth_code(100);
        request.commitInfos = new ArrayList<>();
        doThrow(new StarRocksException("injected error")).when(impl).loadTxnCommitImpl(any(), any());
        TLoadTxnCommitResult result = impl.loadTxnCommit(request);
        Assert.assertEquals(TStatusCode.ANALYSIS_ERROR, result.status.status_code);
    }

    @Test
    public void testStreamLoadPutTimeout() throws StarRocksException, TException, LockTimeoutException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.db = "test";
        request.tbl = "tbl_test";
        request.txnId = 1001L;
        request.setAuth_code(100);
        doThrow(new LockTimeoutException("get database read lock timeout")).when(impl).streamLoadPutImpl(any());
        TStreamLoadPutResult result = impl.streamLoadPut(request);
        Assert.assertEquals(TStatusCode.TIMEOUT, result.status.status_code);
    }

    @Test
    public void testRequestMergeCommit() throws Exception {
        // test success request
        testRequestMergeCommitBase(request -> {}, result -> {
            assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
            assertEquals("test_label", result.getLabel());
        });

        // test authentication failure
        testRequestMergeCommitBase(request -> request.setUser("fake_user"),
                result -> assertEquals(TStatusCode.NOT_AUTHORIZED, result.getStatus().getStatus_code()));

        // test database not exist
        testRequestMergeCommitBase(request -> request.setDb("mc_db_not_exist"),
                result -> {
                    assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
                    assertEquals(1, result.getStatus().getError_msgs().size());
                    assertEquals("unknown database [mc_db_not_exist]", result.getStatus().getError_msgs().get(0));
                });

        // test table not exist
        testRequestMergeCommitBase(request -> request.setTbl("mc_tbl_not_exist"),
                result -> {
                    assertEquals(TStatusCode.INTERNAL_ERROR, result.getStatus().getStatus_code());
                    assertEquals(1, result.getStatus().getError_msgs().size());
                    assertEquals("unknown table [test.mc_tbl_not_exist]", result.getStatus().getError_msgs().get(0));
                });
    }

    private void testRequestMergeCommitBase(
            Consumer<TMergeCommitRequest> setupRequest, Consumer<TMergeCommitResult> verifyResult) throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TMergeCommitRequest request = new TMergeCommitRequest();
        request.setDb("test");
        request.setTbl("site_access_hour");
        request.setUser("root");
        request.setPasswd("");
        request.setBackend_id(10001);
        request.setBackend_host("127.0.0.1");
        request.putToParams(HTTP_ENABLE_BATCH_WRITE, "true");
        request.putToParams(HTTP_BATCH_WRITE_ASYNC, "true");
        request.putToParams(HTTP_BATCH_WRITE_INTERVAL_MS, "1000");
        request.putToParams(HTTP_BATCH_WRITE_PARALLEL, "4");

        new MockUp<BatchWriteMgr>() {

            @Mock
            public RequestLoadResult requestLoad(
                    TableId tableId, StreamLoadKvParams params, long backendId, String backendHost) {
                return new RequestLoadResult(new TStatus(TStatusCode.OK), "test_label");
            }
        };
        setupRequest.accept(request);
        TMergeCommitResult result = impl.requestMergeCommit(request);
        verifyResult.accept(result);
    }

    @Test
    public void testMetaNotFound() throws StarRocksException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.db = "test";
        request.tbl = "foo";
        request.txnId = 1001L;
        request.setFileType(TFileType.FILE_STREAM);
        request.setLoadId(new TUniqueId(1, 2));

        Exception e = Assert.assertThrows(StarRocksException.class, () -> impl.streamLoadPutImpl(request));
        Assert.assertTrue(e.getMessage().contains("unknown table"));

        request.tbl = "v";
        e = Assert.assertThrows(StarRocksException.class, () -> impl.streamLoadPutImpl(request));
        Assert.assertTrue(e.getMessage().contains("load table type is not OlapTable"));

        request.tbl = "mv";
        e = Assert.assertThrows(StarRocksException.class, () -> impl.streamLoadPutImpl(request));
        Assert.assertTrue(e.getMessage().contains("is a materialized view"));
    }

    @Test
    public void testAddListPartitionConcurrency() throws StarRocksException, TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_list");
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
        Database testDb = currentState.getLocalMetastore().getDb("test");
        OlapTable olapTable = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(testDb.getFullName(), "site_access_list");
        PartitionInfo partitionInfo = olapTable.getPartitionInfo();
        DistributionInfo defaultDistributionInfo = olapTable.getDefaultDistributionInfo();
        List<PartitionDesc> partitionDescs = Lists.newArrayList();
        Partition p19910425 = olapTable.getPartition("p19900425");

        partitionDescs.add(new ListPartitionDesc(Lists.newArrayList("p19900425"),
                    Lists.newArrayList(new SingleItemListPartitionDesc(true, "p19900425",
                                Lists.newArrayList("1990-04-25"), Maps.newHashMap()))));

        AddPartitionClause addPartitionClause = new AddPartitionClause(partitionDescs.get(0),
                    defaultDistributionInfo.toDistributionDesc(table.getIdToColumn()), Maps.newHashMap(), false);

        List<Partition> partitionList = Lists.newArrayList();
        partitionList.add(p19910425);

        currentState.getLocalMetastore().addListPartitionLog(testDb, olapTable, partitionDescs,
                    addPartitionClause.isTempPartition(), partitionInfo, partitionList, Sets.newSet("p19900425"));

    }

    @Test
    public void testgetDictQueryParam() throws TException {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TGetDictQueryParamRequest request = new TGetDictQueryParamRequest();
        request.setDb_name("test");
        request.setTable_name("site_access_auto");

        TGetDictQueryParamResponse result = impl.getDictQueryParam(request);

        System.out.println(result);

        Assert.assertNotEquals(0, result.getLocation().getTabletsSize());
    }

    @Test
    public void testGetPartitionMeta() throws Exception {
        starRocksAssert.useDatabase("test")
                .withTable("CREATE TABLE site_access_fix_buckets (\n" +
                        "    event_day DATETIME NOT NULL,\n" +
                        "    site_id INT DEFAULT '10',\n" +
                        "    city_code VARCHAR(100),\n" +
                        "    user_name VARCHAR(32) DEFAULT '',\n" +
                        "    pv BIGINT DEFAULT '0'\n" +
                        ")\n" +
                        "DUPLICATE KEY(event_day, site_id, city_code, user_name)\n" +
                        "DISTRIBUTED BY HASH(event_day, site_id, city_code, user_name) BUCKETS 32\n" +
                        "PROPERTIES(\n" +
                        "    \"replication_num\" = \"1\"\n" +
                        ");");

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        { // nothing set to the request
            TPartitionMetaRequest request = new TPartitionMetaRequest();
            TPartitionMetaResponse response = impl.getPartitionMeta(request);
            TStatus status = response.getStatus();
            Assert.assertEquals(TStatusCode.INVALID_ARGUMENT, status.getStatus_code());
            Assert.assertEquals(1L, status.getError_msgs().size());
            Assert.assertEquals("Invalid parameter from getPartitionMeta request, tablet_ids is required",
                    status.getError_msgs().get(0));
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = db.getTable("site_access_fix_buckets");
        Assert.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        long bucketNum = 32;
        Assert.assertEquals(bucketNum, olapTable.getDefaultDistributionInfo().getBucketNum());

        List<Long> partitionIds = olapTable.getPhysicalPartitions().stream()
                .map(PhysicalPartition::getId).toList();
        long partitionId = partitionIds.get(0);
        List<Tablet> tablets = olapTable.getPhysicalPartition(partitionId).getBaseIndex().getTablets();
        Assert.assertEquals(bucketNum, tablets.size());

        long tabletId = tablets.get(0).getId();
        long tabletId2 = tablets.get(1).getId();

        { // has a single correct tablet_id
            TPartitionMetaRequest request = new TPartitionMetaRequest();
            request.setTablet_ids(List.of(tabletId));
            TPartitionMetaResponse response = impl.getPartitionMeta(request);
            TStatus status = response.getStatus();
            Assert.assertEquals(TStatusCode.OK, status.getStatus_code());
            List<TPartitionMeta> metaList = response.getPartition_metas();
            Map<Long, Integer> tabletIdMetaIndex = response.getTablet_id_partition_meta_index();
            Assert.assertEquals(1L, metaList.size());
            Assert.assertEquals(1L, tabletIdMetaIndex.size());
            Assert.assertTrue(tabletIdMetaIndex.containsKey(tabletId));
            TPartitionMeta meta = metaList.get(tabletIdMetaIndex.get(tabletId));
            PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(partitionId);
            Partition partition = olapTable.getPartition(physicalPartition.getParentId());
            Assert.assertEquals(physicalPartition.getName(), meta.getPartition_name());
            Assert.assertEquals(partitionId, meta.getPartition_id());
            Assert.assertEquals(partition.getState().name(), meta.getState());
            Assert.assertEquals(physicalPartition.getVisibleVersion(), meta.getVisible_version());
            Assert.assertEquals(physicalPartition.getNextVersion(), meta.getNext_version());
            Assert.assertEquals(olapTable.isTempPartition(partitionId), meta.isIs_temp());
        }
        { // has 2 correct tablet_ids points to the same partition, one non-exist tablet id
            TPartitionMetaRequest request = new TPartitionMetaRequest();
            long nonExistTabletId = 1356798018;
            request.setTablet_ids(List.of(tabletId, tabletId2, nonExistTabletId));
            TPartitionMetaResponse response = impl.getPartitionMeta(request);
            TStatus status = response.getStatus();
            Assert.assertEquals(TStatusCode.OK, status.getStatus_code());
            List<TPartitionMeta> metaList = response.getPartition_metas();
            Map<Long, Integer> tabletIdMetaIndex = response.getTablet_id_partition_meta_index();
            Assert.assertEquals(1L, metaList.size());
            Assert.assertEquals(2L, tabletIdMetaIndex.size());
            Assert.assertTrue(tabletIdMetaIndex.containsKey(tabletId));
            Assert.assertTrue(tabletIdMetaIndex.containsKey(tabletId2));
            Assert.assertFalse(tabletIdMetaIndex.containsKey(nonExistTabletId));

            // both pointed to the same partition meta
            Assert.assertEquals(0L, (long) tabletIdMetaIndex.get(tabletId));
            Assert.assertEquals(0L, (long) tabletIdMetaIndex.get(tabletId2));

            // verify the partitionMeta
            TPartitionMeta meta = metaList.get(0);
            PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(partitionId);
            Partition partition = olapTable.getPartition(physicalPartition.getParentId());
            Assert.assertEquals(physicalPartition.getName(), meta.getPartition_name());
            Assert.assertEquals(partitionId, meta.getPartition_id());
            Assert.assertEquals(partition.getState().name(), meta.getState());
            Assert.assertEquals(physicalPartition.getVisibleVersion(), meta.getVisible_version());
            Assert.assertEquals(physicalPartition.getNextVersion(), meta.getNext_version());
            Assert.assertEquals(olapTable.isTempPartition(partitionId), meta.isIs_temp());
        }
    }
}
