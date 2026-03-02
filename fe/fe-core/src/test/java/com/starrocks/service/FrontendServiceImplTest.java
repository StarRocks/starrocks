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
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionInfo;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Tablet;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.ConfigBase;
import com.starrocks.common.FeConstants;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.UUIDUtil;
import com.starrocks.common.util.concurrent.lock.LockTimeoutException;
import com.starrocks.ha.FrontendNodeType;
import com.starrocks.load.batchwrite.BatchWriteMgr;
import com.starrocks.load.batchwrite.RequestLoadResult;
import com.starrocks.load.batchwrite.TableId;
import com.starrocks.load.streamload.StreamLoadKvParams;
import com.starrocks.load.streamload.StreamLoadTask;
import com.starrocks.planner.StreamLoadPlanner;
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
import com.starrocks.thrift.TBatchGetTableSchemaRequest;
import com.starrocks.thrift.TBatchGetTableSchemaResponse;
import com.starrocks.thrift.TColumnDef;
import com.starrocks.thrift.TCreatePartitionRequest;
import com.starrocks.thrift.TCreatePartitionResult;
import com.starrocks.thrift.TDescribeTableParams;
import com.starrocks.thrift.TDescribeTableResult;
import com.starrocks.thrift.TExecPlanFragmentParams;
import com.starrocks.thrift.TFileType;
import com.starrocks.thrift.TGetDictQueryParamRequest;
import com.starrocks.thrift.TGetDictQueryParamResponse;
import com.starrocks.thrift.TGetLoadTxnStatusRequest;
import com.starrocks.thrift.TGetLoadTxnStatusResult;
import com.starrocks.thrift.TGetTableSchemaRequest;
import com.starrocks.thrift.TGetTableSchemaResponse;
import com.starrocks.thrift.TGetTablesInfoRequest;
import com.starrocks.thrift.TGetTablesInfoResponse;
import com.starrocks.thrift.TGetTablesParams;
import com.starrocks.thrift.TGetTablesResult;
import com.starrocks.thrift.TImmutablePartitionRequest;
import com.starrocks.thrift.TImmutablePartitionResult;
import com.starrocks.thrift.TListMaterializedViewStatusResult;
import com.starrocks.thrift.TListRecycleBinCatalogsInfo;
import com.starrocks.thrift.TListRecycleBinCatalogsParams;
import com.starrocks.thrift.TListRecycleBinCatalogsResult;
import com.starrocks.thrift.TListTableStatusResult;
import com.starrocks.thrift.TLoadInfo;
import com.starrocks.thrift.TLoadTxnBeginRequest;
import com.starrocks.thrift.TLoadTxnBeginResult;
import com.starrocks.thrift.TLoadTxnCommitRequest;
import com.starrocks.thrift.TLoadTxnCommitResult;
import com.starrocks.thrift.TLoadTxnRollbackRequest;
import com.starrocks.thrift.TLoadTxnRollbackResult;
import com.starrocks.thrift.TLoadType;
import com.starrocks.thrift.TManualLoadTxnCommitAttachment;
import com.starrocks.thrift.TMergeCommitRequest;
import com.starrocks.thrift.TMergeCommitResult;
import com.starrocks.thrift.TNetworkAddress;
import com.starrocks.thrift.TPartitionMeta;
import com.starrocks.thrift.TPartitionMetaRequest;
import com.starrocks.thrift.TPartitionMetaResponse;
import com.starrocks.thrift.TPlanFragmentExecParams;
import com.starrocks.thrift.TRefreshConnectionsRequest;
import com.starrocks.thrift.TRefreshConnectionsResponse;
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
import com.starrocks.thrift.TTabletCommitInfo;
import com.starrocks.thrift.TTransactionStatus;
import com.starrocks.thrift.TTxnCommitAttachment;
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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
import static com.starrocks.thrift.TFileType.FILE_STREAM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
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
        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
    }

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
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
                    .withTable("CREATE TABLE test_load (\n" +
                                "    id INT\n" +
                                ")\n" +
                                "DUPLICATE KEY(id)\n" +
                                "DISTRIBUTED BY HASH(id) BUCKETS 1\n" +
                                "PROPERTIES(\"replication_num\" = \"1\");")
                    .withView("create view v as select * from site_access_empty")
                    .withView("create view v1 as select current_role()")
                    .withView("create view v2 as select current_user()")
                    .withView("create view v3 as select database()")
                    .withView("create view v4 as select user()")
                    .withView("create view v5 as select CONNECTION_ID()")
                    .withView("create view v6 as select CATALOG()")

                    .withMaterializedView("create materialized view mv refresh async as select * from site_access_empty");
    }

    @AfterAll
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
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                    .getTable(db.getFullName(), "site_access_exception");
        List<Long> partitionIds = Lists.newArrayList();
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        TImmutablePartitionResult partition = impl.updateImmutablePartition(request);
        Table t = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "v");

        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());

        request.setDb_id(db.getId());
        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());

        request.setTable_id(t.getId());
        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());

        request.setTable_id(table.getId());
        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());

        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());

        partitionIds.add(1L);
        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());

        partitionIds = table.getPhysicalPartitions().stream()
                    .map(PhysicalPartition::getId).collect(Collectors.toList());
        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
    }

    @Test
    public void testImmutablePartitionTransactionNotExist() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return null;
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "site_access_exception");

        TImmutablePartitionRequest request = new TImmutablePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_ids(Lists.newArrayList());

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TImmutablePartitionResult partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());
        String errorMsg = partition.getStatus().getError_msgs().get(0);
        Assertions.assertTrue(errorMsg.contains("error: txn") && errorMsg.contains("does not exist"));
    }

    @Test
    public void testImmutablePartitionApi() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

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

        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Assertions.assertEquals(2, table.getPhysicalPartitions().size());

        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Assertions.assertEquals(2, table.getPhysicalPartitions().size());

        partitionIds = table.getPhysicalPartitions().stream()
                    .map(PhysicalPartition::getId).collect(Collectors.toList());
        request.setPartition_ids(partitionIds);
        partition = impl.updateImmutablePartition(request);
        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Assertions.assertEquals(3, table.getPhysicalPartitions().size());
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

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Partition p19900424 = table.getPartition("p19900424");
        Assertions.assertNotNull(p19900424);

        partition = impl.createPartition(request);
        Assertions.assertEquals(1, partition.partitions.size());
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

        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
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

        Assertions.assertEquals(TStatusCode.RUNTIME_ERROR, partition.getStatus().getStatus_code());
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

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.SERVICE_UNAVAILABLE);
    }

    @Test
    public void testCreatePartitionAlreadyFailed() throws TException {
        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                TransactionState transactionState = new TransactionState();
                transactionState.setIsCreatePartitionFailed(true);
                return transactionState;
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

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        Assertions.assertTrue(partition.getStatus().getError_msgs().get(0).contains("already"));
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
        Assertions.assertEquals(result.getStatus().getStatus_code(), TStatusCode.OK);
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

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Partition p19900424 = table.getPartition("p19900424");
        Assertions.assertNotNull(p19900424);

        partition = impl.createPartition(request);
        Assertions.assertEquals(1, partition.partitions.size());
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

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.OK);
        Partition p19891102 = table.getPartition("p19891102");
        Assertions.assertNotNull(p19891102);

        partition = impl.createPartition(request);
        Assertions.assertEquals(2, partition.partitions.size());
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

        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Partition p199004 = table.getPartition("p199004");
        Assertions.assertNotNull(p199004);

        partition = impl.createPartition(request);
        Assertions.assertEquals(2, partition.partitions.size());
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

        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Partition p00000101 = table.getPartition("p00000101");
        Assertions.assertNotNull(p00000101);
        Partition p99991231 = table.getPartition("p99991231");
        Assertions.assertNotNull(p99991231);
        partition = impl.createPartition(request);
        Assertions.assertEquals(2, partition.partitions.size());
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

        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        Assertions.assertTrue(partition.getStatus().getError_msgs().get(0).contains("max_partition_number_per_table"));
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
        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());

        Config.max_partitions_in_one_batch = 1;

        partition = impl.createPartition(request);
        Assertions.assertEquals(partition.getStatus().getStatus_code(), TStatusCode.RUNTIME_ERROR);
        Assertions.assertTrue(partition.getStatus().getError_msgs().get(0).contains("max_partitions_in_one_batch"));

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
        Assertions.assertEquals(18, result.tables.size());
    }

    @Test
    public void testListTableStatus() throws TException {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TListTableStatusResult result = impl.listTableStatus(buildListTableStatusParam());
        Assertions.assertEquals(7, result.tables.size());
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
        Assertions.assertEquals(8, result.tables.size());
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

        Assertions.assertEquals(TStatusCode.OK, partition.getStatus().getStatus_code());
        Partition p1990042412 = table.getPartition("p1990042412");
        Assertions.assertNotNull(p1990042412);

        partition = impl.createPartition(request);
        Assertions.assertEquals(1, partition.partitions.size());
    }

    @Test
    public void testCreateEmptyPartition() {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_empty");
        Collection<Partition> partitions = table.getPartitions();
        Assertions.assertEquals(1, partitions.size());
        String name = partitions.iterator().next().getName();
        Assertions.assertEquals(ExpressionRangePartitionInfo.AUTOMATIC_SHADOW_PARTITION_NAME, name);
        Assertions.assertTrue(name.startsWith(ExpressionRangePartitionInfo.SHADOW_PARTITION_PREFIX));
    }

    @Test
    public void testCreateDateTruncForbidAutomaticTable() {
        assertThrows(AnalysisException.class, () -> {
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
        });
    }

    @Test
    public void testUnsupportedAutomaticTableGranularityDoesNotMatch() {
        assertThrows(AnalysisException.class, () -> {
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
        });
    }

    @Test
    public void testUnsupportedAutomaticTableGranularityDoesNotMatch2() {
        assertThrows(AnalysisException.class, () -> {
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
        });
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
                                "\"enable_persistent_index\" = \"true\",\n" +
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
                                "\"enable_persistent_index\" = \"true\",\n" +
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
        Assertions.assertEquals(1, tablesInfos.size());
        Assertions.assertEquals("t1", tablesInfos.get(0).getTable_name());
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
                                "\"enable_persistent_index\" = \"true\",\n" +
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
        Assertions.assertEquals(2, testDefaultValue.size());
        Assertions.assertEquals("CURRENT_TIMESTAMP", testDefaultValue.get(0).getColumnDesc().getColumnDefault());
        Assertions.assertEquals("2", testDefaultValue.get(1).getColumnDesc().getColumnDefault());
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
                                "\"enable_persistent_index\" = \"true\"\n" +
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
        Assertions.assertEquals(1, response.tables.size());
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
                                "\"enable_persistent_index\" = \"true\"\n" +
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
        Assertions.assertEquals(1, response.materialized_views.size());
    }

    @Test
    public void testGetLoadTxnStatus() throws Exception {
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db.getFullName(), "site_access_day");
        TUniqueId requestId = UUIDUtil.genTUniqueId();
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
        Assertions.assertEquals(TTransactionStatus.UNKNOWN, result1.getStatus());
        Assertions.assertNull(result1.getReason());
        request.setDb("test");
        TGetLoadTxnStatusResult result2 = impl.getLoadTxnStatus(request);
        Assertions.assertEquals(TTransactionStatus.UNKNOWN, result2.getStatus());
        Assertions.assertNull(result2.getReason());
        request.setTxnId(transactionId);
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.FOLLOWER);
        TGetLoadTxnStatusResult result3 = impl.getLoadTxnStatus(request);
        Assertions.assertEquals(TTransactionStatus.UNKNOWN, result3.getStatus());
        Assertions.assertNull(result3.getReason());
        GlobalStateMgr.getCurrentState().setFrontendNodeType(FrontendNodeType.LEADER);
        TGetLoadTxnStatusResult result4 = impl.getLoadTxnStatus(request);
        Assertions.assertEquals(TTransactionStatus.PREPARE, result4.getStatus());
        Assertions.assertEquals("", result4.getReason());
        GlobalStateMgr.getCurrentState().getGlobalTransactionMgr().abortTransaction(
                db.getId(), transactionId, "artificial failure");
        TGetLoadTxnStatusResult result5 = impl.getLoadTxnStatus(request);
        Assertions.assertEquals(TTransactionStatus.ABORTED, result5.getStatus());
        Assertions.assertEquals("artificial failure", result5.getReason());
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
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, status.getStatus_code());
        List<String> errMsg = status.getError_msgs();
        Assertions.assertEquals(1, errMsg.size());
        Assertions.assertEquals(
                    "Expr 'str_to_date(`col1`)' analyze error: No matching function with signature: str_to_date(varchar), " +
                                "derived column is 'event_day'",
                    errMsg.get(0));
    }

    @Test
    public void testSetFrontendConfig() throws Exception {
        // Skip test if persistence is not available (container environments)
        Assumptions.assumeTrue(ConfigBase.isIsPersisted(),
                "Skipping persistence test - not available in container environment");

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TSetConfigRequest request = new TSetConfigRequest();
        request.keys = Lists.newArrayList("mysql_server_version");
        request.values = Lists.newArrayList("5.1.1");

        TSetConfigResponse result = impl.setConfig(request);
        Assertions.assertEquals("5.1.1", GlobalVariable.version);

        request.keys = Lists.newArrayList("adaptive_choose_instances_threshold");
        request.values = Lists.newArrayList("98");
        request.setUser_identity("root");
        request.setIs_persistent(true);
        impl.setConfig(request);

        PatternMatcher matcher = PatternMatcher.createMysqlPattern("adaptive_choose_instances_threshold", false);
        List<List<String>> configs = Config.getConfigInfo(matcher);
        Assertions.assertEquals("98", configs.get(0).get(2));
        Assertions.assertEquals(98, Config.adaptive_choose_instances_threshold);
    }

    @Test
    public void testLoadTxnPrepare() throws TException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TNetworkAddress address = new TNetworkAddress();
        address.setHostname("127.0.0.1");
        address.setPort(9030);
        doReturn(address).when(impl).getClientAddr();

        String db = "test";
        String table = "test_load";
        long txnId = testBeginAndPutTxn(impl, db, table);
        TLoadTxnCommitRequest request = buildCommitRequest(txnId, db, table);
        TLoadTxnCommitResult result = impl.loadTxnPrepare(request);
        assertEquals(TStatusCode.OK, result.getStatus().status_code);

        StreamLoadTask streamLoadTask =
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().getSyncSteamLoadTaskByTxnId(txnId);
        Assertions.assertNotNull(streamLoadTask);
        List<TLoadInfo> loadInfos = streamLoadTask.toThrift();
        Assertions.assertNotNull(loadInfos);
        Assertions.assertEquals(1, loadInfos.size());
        TLoadInfo loadInfo = loadInfos.get(0);
        Assertions.assertEquals("", loadInfo.getError_msg());
        Assertions.assertEquals(2, loadInfo.getNum_sink_rows());
        Assertions.assertEquals(1, loadInfo.getNum_filtered_rows());
    }

    @Test
    public void testLoadTxnCommit() throws TException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TNetworkAddress address = new TNetworkAddress();
        address.setHostname("127.0.0.1");
        address.setPort(9030);
        doReturn(address).when(impl).getClientAddr();

        String db = "test";
        String table = "test_load";
        long txnId = testBeginAndPutTxn(impl, db, table);
        TLoadTxnCommitRequest request = buildCommitRequest(txnId, db, table);
        TLoadTxnCommitResult result = impl.loadTxnCommit(request);
        if (result.getStatus().status_code == TStatusCode.OK) {
            StreamLoadTask streamLoadTask =
                    GlobalStateMgr.getCurrentState().getStreamLoadMgr().getSyncSteamLoadTaskByTxnId(txnId);
            Assertions.assertNotNull(streamLoadTask);
            List<TLoadInfo> loadInfos = streamLoadTask.toThrift();
            Assertions.assertNotNull(loadInfos);
            Assertions.assertEquals(1, loadInfos.size());
            TLoadInfo loadInfo = loadInfos.get(0);
            Assertions.assertEquals("", loadInfo.getError_msg());
            Assertions.assertEquals(2, loadInfo.getNum_sink_rows());
            Assertions.assertEquals(1, loadInfo.getNum_filtered_rows());
        }
    }

    @Test
    public void testLoadTxnRollback() throws TException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TNetworkAddress address = new TNetworkAddress();
        address.setHostname("127.0.0.1");
        address.setPort(9030);
        doReturn(address).when(impl).getClientAddr();

        String db = "test";
        String table = "test_load";
        long txnId = testBeginAndPutTxn(impl, db, table);
        TLoadTxnRollbackRequest request = new TLoadTxnRollbackRequest();
        request.setDb("test");
        request.setTbl("test_load");
        request.setTxnId(txnId);
        request.setReason("artificial failure");
        TManualLoadTxnCommitAttachment loadAttachment = new TManualLoadTxnCommitAttachment();
        loadAttachment.setLoadedRows(2);
        loadAttachment.setFilteredRows(1);
        TTxnCommitAttachment txnAttachment = new TTxnCommitAttachment();
        txnAttachment.setLoadType(TLoadType.MANUAL_LOAD);
        txnAttachment.setManualLoadTxnCommitAttachment(loadAttachment);
        request.setTxnCommitAttachment(txnAttachment);
        request.setAuth_code(100);
        TLoadTxnRollbackResult result = impl.loadTxnRollback(request);
        assertEquals(TStatusCode.OK, result.getStatus().status_code);

        StreamLoadTask streamLoadTask =
                GlobalStateMgr.getCurrentState().getStreamLoadMgr().getSyncSteamLoadTaskByTxnId(txnId);
        Assertions.assertNotNull(streamLoadTask);
        List<TLoadInfo> loadInfos = streamLoadTask.toThrift();
        Assertions.assertNotNull(loadInfos);
        Assertions.assertEquals(1, loadInfos.size());
        TLoadInfo loadInfo = loadInfos.get(0);
        Assertions.assertEquals("artificial failure", loadInfo.getError_msg());
        Assertions.assertEquals(2, loadInfo.getNum_sink_rows());
        Assertions.assertEquals(1, loadInfo.getNum_filtered_rows());
    }

    private long testBeginAndPutTxn(FrontendServiceImpl impl, String db, String table) throws TException {
        TLoadTxnBeginRequest beginRequest = new TLoadTxnBeginRequest();
        beginRequest.setLabel(UUID.randomUUID().toString());
        beginRequest.setDb(db);
        beginRequest.setTbl(table);
        beginRequest.setUser("root");
        beginRequest.setPasswd("");
        TLoadTxnBeginResult beginResult = impl.loadTxnBegin(beginRequest);
        assertEquals(TStatusCode.OK, beginResult.getStatus().getStatus_code());

        TUniqueId loadId = UUIDUtil.genTUniqueId();
        TStreamLoadPutRequest putRequest = new TStreamLoadPutRequest();
        putRequest.setDb(db);
        putRequest.setTbl(table);
        putRequest.setTxnId(beginResult.getTxnId());
        putRequest.setLoadId(loadId);
        putRequest.setFileType(FILE_STREAM);
        putRequest.setAuth_code(100);
        putRequest.setUser("user1");
        putRequest.setUser_ip("127.0.0.1");
        TStreamLoadPutResult putResult = impl.streamLoadPut(putRequest);
        assertEquals(TStatusCode.OK, putResult.getStatus().status_code);
        return beginResult.getTxnId();
    }

    private TLoadTxnCommitRequest buildCommitRequest(long txnId, String db, String table) {
        TLoadTxnCommitRequest request = new TLoadTxnCommitRequest();
        request.setDb("test");
        request.setTbl("test_load");
        request.setTxnId(txnId);
        List<TTabletCommitInfo> commitInfos = new ArrayList<>();
        OlapTable tbl = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore().getTable(db, table);
        tbl.getAllPhysicalPartitions().stream().map(PhysicalPartition::getLatestBaseIndex)
                .map(MaterializedIndex::getTablets).flatMap(List::stream).forEach(tablet -> {
                    TTabletCommitInfo commitInfo = new TTabletCommitInfo();
                    commitInfo.setTabletId(tablet.getId());
                    commitInfo.setBackendId(tablet.getBackendIds().iterator().next());
                    commitInfos.add(commitInfo);
                });
        request.setCommitInfos(commitInfos);
        TManualLoadTxnCommitAttachment loadAttachment = new TManualLoadTxnCommitAttachment();
        loadAttachment.setLoadedRows(2);
        loadAttachment.setFilteredRows(1);
        TTxnCommitAttachment txnAttachment = new TTxnCommitAttachment();
        txnAttachment.setLoadType(TLoadType.MANUAL_LOAD);
        txnAttachment.setManualLoadTxnCommitAttachment(loadAttachment);
        request.setTxnCommitAttachment(txnAttachment);
        request.setAuth_code(100);
        request.setThrift_rpc_timeout_ms(500);
        return request;
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
        Assertions.assertEquals(TStatusCode.SR_EAGAIN, result.status.status_code);
        Assertions.assertTrue(result.retry_interval_ms >= (now + 100 - System.currentTimeMillis()));
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
        Assertions.assertEquals(TStatusCode.TIMEOUT, result.status.status_code);
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
        Assertions.assertEquals(TStatusCode.ANALYSIS_ERROR, result.status.status_code);
    }

    @Test
    public void testStreamLoadPutDuplicateRequest() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TLoadTxnBeginRequest request = new TLoadTxnBeginRequest();
        request.setLabel("test_label1");
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
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());

        TUniqueId queryId = new TUniqueId(2, 3);
        new MockUp<StreamLoadPlanner>() {
            @Mock
            public TExecPlanFragmentParams plan(TUniqueId loadId) {
                return new TExecPlanFragmentParams().setParams(
                        new TPlanFragmentExecParams().setFragment_instance_id(queryId));
            }

            @Mock
            public TExecPlanFragmentParams getExecPlanFragmentParams() {
                return new TExecPlanFragmentParams().setParams(
                        new TPlanFragmentExecParams().setFragment_instance_id(queryId));
            }
        };

        new MockUp<FrontendServiceImpl>() {
            @Mock
            public TNetworkAddress getClientAddr() {
                return new TNetworkAddress("localhost", 8000);
            }
        };

        TStreamLoadPutRequest loadRequest = new TStreamLoadPutRequest();
        loadRequest.db = "test";
        loadRequest.tbl = "site_access_auto";
        loadRequest.txnId = result.getTxnId();
        loadRequest.loadId = queryId;
        loadRequest.setAuth_code(100);
        loadRequest.setUser("user1");
        loadRequest.setUser_ip("127.0.0.1");
        TStreamLoadPutResult loadResult1 = impl.streamLoadPut(loadRequest);
        TStreamLoadPutResult loadResult2 = impl.streamLoadPut(loadRequest);
    }

    @Test
    public void testStreamLoadPutTimeout() throws StarRocksException, TException, LockTimeoutException {
        FrontendServiceImpl impl = spy(new FrontendServiceImpl(exeEnv));
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.db = "test";
        request.tbl = "tbl_test";
        request.txnId = 1001L;
        request.setAuth_code(100);
        request.setUser("user1");
        request.setUser_ip("127.0.0.1");
        doThrow(new LockTimeoutException("get database read lock timeout")).when(impl).streamLoadPutImpl(any(), any());
        TStreamLoadPutResult result = impl.streamLoadPut(request);
        Assertions.assertEquals(TStatusCode.TIMEOUT, result.status.status_code);
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
                    TableId tableId, StreamLoadKvParams params, UserIdentity userIdentity, long backendId, String backendHost) {
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
        final ConnectContext context = new ConnectContext();
        TStreamLoadPutRequest request = new TStreamLoadPutRequest();
        request.db = "test";
        request.tbl = "foo";
        request.txnId = 1001L;
        request.setFileType(TFileType.FILE_STREAM);
        request.setLoadId(new TUniqueId(1, 2));

        Exception e = Assertions.assertThrows(StarRocksException.class, () -> impl.streamLoadPutImpl(context, request));
        Assertions.assertTrue(e.getMessage().contains("unknown table"));

        request.tbl = "v";
        e = Assertions.assertThrows(StarRocksException.class, () -> impl.streamLoadPutImpl(context, request));
        Assertions.assertTrue(e.getMessage().contains("load table type is not OlapTable"));

        request.tbl = "mv";
        e = Assertions.assertThrows(StarRocksException.class, () -> impl.streamLoadPutImpl(context, request));
        Assertions.assertTrue(e.getMessage().contains("is a materialized view"));
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

        Assertions.assertNotEquals(0, result.getLocation().getTabletsSize());
    }

    @Test
    public void testlistRecycleBinCatalogs() throws Exception {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

        starRocksAssert.withDatabase("test_rbc").useDatabase("test_rbc")
                    .withTable("CREATE TABLE tblRecycleBinCatalogs (\n" +
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
                                ");");

        ConnectContext ctx = starRocksAssert.getCtx();
        String dropSQL = "drop table if exists tblRecycleBinCatalogs";
        try {
            DropTableStmt dropTableStmt = (DropTableStmt) UtFrameUtils.parseStmtWithNewParser(dropSQL, ctx);
            GlobalStateMgr.getCurrentState().getLocalMetastore().dropTable(dropTableStmt);
        } catch (Exception ex) {

        }

        TListRecycleBinCatalogsParams request = new TListRecycleBinCatalogsParams();
        TUserIdentity userIdentity = new TUserIdentity();
        userIdentity.setUsername("root");
        userIdentity.setHost("%");
        userIdentity.setIs_domain(false);
        request.setUser_ident(userIdentity);

        TListRecycleBinCatalogsResult result = impl.listRecycleBinCatalogs(request);
        List<TListRecycleBinCatalogsInfo> tCatalogInfo = result.recyclebin_catalogs;
        boolean matched = false;
        for (int i = 0; i < tCatalogInfo.size(); ++i) {
            TListRecycleBinCatalogsInfo item = tCatalogInfo.get(i);
            if (item.getName().equals("tblRecycleBinCatalogs")) {
                matched = true;
            }
        }
        Assertions.assertEquals(true, matched);
    }

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
            Assertions.assertEquals(TStatusCode.INVALID_ARGUMENT, status.getStatus_code());
            Assertions.assertEquals(1L, status.getError_msgs().size());
            Assertions.assertEquals("Invalid parameter from getPartitionMeta request, tablet_ids is required",
                    status.getError_msgs().get(0));
        }
        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        Table table = db.getTable("site_access_fix_buckets");
        Assertions.assertTrue(table instanceof OlapTable);
        OlapTable olapTable = (OlapTable) table;
        long bucketNum = 32;
        Assertions.assertEquals(bucketNum, olapTable.getDefaultDistributionInfo().getBucketNum());

        List<Long> partitionIds = olapTable.getPhysicalPartitions().stream()
                .map(PhysicalPartition::getId).toList();
        long partitionId = partitionIds.get(0);
        List<Tablet> tablets = olapTable.getPhysicalPartition(partitionId).getLatestBaseIndex().getTablets();
        Assertions.assertEquals(bucketNum, tablets.size());

        long tabletId = tablets.get(0).getId();
        long tabletId2 = tablets.get(1).getId();

        { // has a single correct tablet_id
            TPartitionMetaRequest request = new TPartitionMetaRequest();
            request.setTablet_ids(List.of(tabletId));
            TPartitionMetaResponse response = impl.getPartitionMeta(request);
            TStatus status = response.getStatus();
            Assertions.assertEquals(TStatusCode.OK, status.getStatus_code());
            List<TPartitionMeta> metaList = response.getPartition_metas();
            Map<Long, Integer> tabletIdMetaIndex = response.getTablet_id_partition_meta_index();
            Assertions.assertEquals(1L, metaList.size());
            Assertions.assertEquals(1L, tabletIdMetaIndex.size());
            Assertions.assertTrue(tabletIdMetaIndex.containsKey(tabletId));
            TPartitionMeta meta = metaList.get(tabletIdMetaIndex.get(tabletId));
            PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(partitionId);
            Partition partition = olapTable.getPartition(physicalPartition.getParentId());
            Assertions.assertEquals(partition.getName(), meta.getPartition_name());
            Assertions.assertEquals(partitionId, meta.getPartition_id());
            Assertions.assertEquals(partition.getState().name(), meta.getState());
            Assertions.assertEquals(physicalPartition.getVisibleVersion(), meta.getVisible_version());
            Assertions.assertEquals(physicalPartition.getNextVersion(), meta.getNext_version());
            Assertions.assertEquals(olapTable.isTempPartition(partitionId), meta.isIs_temp());
        }
        { // has 2 correct tablet_ids points to the same partition, one non-exist tablet id
            TPartitionMetaRequest request = new TPartitionMetaRequest();
            long nonExistTabletId = 1356798018;
            request.setTablet_ids(List.of(tabletId, tabletId2, nonExistTabletId));
            TPartitionMetaResponse response = impl.getPartitionMeta(request);
            TStatus status = response.getStatus();
            Assertions.assertEquals(TStatusCode.OK, status.getStatus_code());
            List<TPartitionMeta> metaList = response.getPartition_metas();
            Map<Long, Integer> tabletIdMetaIndex = response.getTablet_id_partition_meta_index();
            Assertions.assertEquals(1L, metaList.size());
            Assertions.assertEquals(2L, tabletIdMetaIndex.size());
            Assertions.assertTrue(tabletIdMetaIndex.containsKey(tabletId));
            Assertions.assertTrue(tabletIdMetaIndex.containsKey(tabletId2));
            Assertions.assertFalse(tabletIdMetaIndex.containsKey(nonExistTabletId));

            // both pointed to the same partition meta
            Assertions.assertEquals(0L, (long) tabletIdMetaIndex.get(tabletId));
            Assertions.assertEquals(0L, (long) tabletIdMetaIndex.get(tabletId2));

            // verify the partitionMeta
            TPartitionMeta meta = metaList.get(0);
            PhysicalPartition physicalPartition = olapTable.getPhysicalPartition(partitionId);
            Partition partition = olapTable.getPartition(physicalPartition.getParentId());
            Assertions.assertEquals(partition.getName(), meta.getPartition_name());
            Assertions.assertEquals(partitionId, meta.getPartition_id());
            Assertions.assertEquals(partition.getState().name(), meta.getState());
            Assertions.assertEquals(physicalPartition.getVisibleVersion(), meta.getVisible_version());
            Assertions.assertEquals(physicalPartition.getNextVersion(), meta.getNext_version());
            Assertions.assertEquals(olapTable.isTempPartition(partitionId), meta.isIs_temp());
        }
    }

    @Test
    public void testRefreshConnectionsSuccess() throws TException {
        ExecuteEnv.setup();
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TRefreshConnectionsRequest request = new TRefreshConnectionsRequest();
        request.setForce(false);
        TRefreshConnectionsResponse response = impl.refreshConnections(request);
        Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatus_code());
    }

    @Test
    public void testRefreshConnectionsSuccessWithForce() throws TException {
        ExecuteEnv.setup();
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TRefreshConnectionsRequest request = new TRefreshConnectionsRequest();
        request.setForce(true);
        TRefreshConnectionsResponse response = impl.refreshConnections(request);
        Assertions.assertEquals(TStatusCode.OK, response.getStatus().getStatus_code());
    }

    @Test
    public void testRefreshConnectionsWithException() throws TException {
        new MockUp<com.starrocks.qe.VariableMgr>() {
            @Mock
            public void refreshConnectionsInternal(boolean force) {
                throw new RuntimeException("Test exception");
            }
        };
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TRefreshConnectionsRequest request = new TRefreshConnectionsRequest();
        request.setForce(false);
        TRefreshConnectionsResponse response = impl.refreshConnections(request);
        Assertions.assertEquals(TStatusCode.INTERNAL_ERROR, response.getStatus().getStatus_code());
        Assertions.assertNotNull(response.getStatus().getError_msgs());
        Assertions.assertFalse(response.getStatus().getError_msgs().isEmpty());
    }

    @Test
    public void testGetTableSchema() {
        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);

        // Test with empty request list
        TBatchGetTableSchemaRequest emptyRequest = new TBatchGetTableSchemaRequest();
        TBatchGetTableSchemaResponse emptyResponse = impl.getTableSchema(emptyRequest);
        Assertions.assertNotNull(emptyResponse);
        Assertions.assertFalse(emptyResponse.isSetResponses());
        Assertions.assertEquals(0, emptyResponse.getResponsesSize());

        // Test with single request
        try (org.mockito.MockedStatic<TableSchemaService> schemaService = mockStatic(TableSchemaService.class)) {
            TGetTableSchemaRequest request1 = new TGetTableSchemaRequest();
            TGetTableSchemaResponse response1 = new TGetTableSchemaResponse();
            TStatus status1 = new TStatus(TStatusCode.OK);
            response1.setStatus(status1);

            schemaService.when(() -> TableSchemaService.getTableSchema(any(TGetTableSchemaRequest.class)))
                    .thenReturn(response1);

            TBatchGetTableSchemaRequest batchRequest = new TBatchGetTableSchemaRequest();
            batchRequest.addToRequests(request1);
            TBatchGetTableSchemaResponse batchResponse = impl.getTableSchema(batchRequest);

            Assertions.assertNotNull(batchResponse);
            Assertions.assertTrue(batchResponse.isSetStatus());
            Assertions.assertEquals(TStatusCode.OK, batchResponse.getStatus().getStatus_code());
            Assertions.assertNotNull(batchResponse.getResponses());
            Assertions.assertEquals(1, batchResponse.getResponsesSize());
            Assertions.assertEquals(TStatusCode.OK, batchResponse.getResponses().get(0).getStatus().getStatus_code());

            schemaService.verify(() -> TableSchemaService.getTableSchema(any(TGetTableSchemaRequest.class)));
        }

        // Test with multiple requests
        try (org.mockito.MockedStatic<TableSchemaService> schemaService = mockStatic(TableSchemaService.class)) {
            TGetTableSchemaRequest request1 = new TGetTableSchemaRequest();
            TGetTableSchemaRequest request2 = new TGetTableSchemaRequest();
            TGetTableSchemaResponse response1 = new TGetTableSchemaResponse();
            TGetTableSchemaResponse response2 = new TGetTableSchemaResponse();
            TStatus status1 = new TStatus(TStatusCode.OK);
            TStatus status2 = new TStatus(TStatusCode.TABLE_NOT_EXIST);
            response1.setStatus(status1);
            response2.setStatus(status2);

            schemaService.when(() -> TableSchemaService.getTableSchema(same(request1)))
                    .thenReturn(response1);
            schemaService.when(() -> TableSchemaService.getTableSchema(same(request2)))
                    .thenReturn(response2);

            TBatchGetTableSchemaRequest batchRequest = new TBatchGetTableSchemaRequest();
            batchRequest.addToRequests(request1);
            batchRequest.addToRequests(request2);
            TBatchGetTableSchemaResponse batchResponse = impl.getTableSchema(batchRequest);

            Assertions.assertNotNull(batchResponse);
            Assertions.assertTrue(batchResponse.isSetStatus());
            Assertions.assertEquals(TStatusCode.OK, batchResponse.getStatus().getStatus_code());
            Assertions.assertNotNull(batchResponse.getResponses());
            Assertions.assertEquals(2, batchResponse.getResponsesSize());
            Assertions.assertEquals(TStatusCode.OK, batchResponse.getResponses().get(0).getStatus().getStatus_code());
            Assertions.assertEquals(TStatusCode.TABLE_NOT_EXIST,
                    batchResponse.getResponses().get(1).getStatus().getStatus_code());

            schemaService.verify(() -> TableSchemaService.getTableSchema(same(request1)));
            schemaService.verify(() -> TableSchemaService.getTableSchema(same(request2)));
        }
    }

    @Test
    public void testCreatePartitionSkipDroppedPartition() throws TException {
        // Test that when a partition is dropped between create and get (e.g. by TTL cleaner),
        // the partition should be skipped instead of causing NPE
        final String droppedPartitionName = "p19900426";

        new MockUp<GlobalTransactionMgr>() {
            @Mock
            public TransactionState getTransactionState(long dbId, long transactionId) {
                return new TransactionState();
            }
        };

        Database db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        OlapTable table = (OlapTable) GlobalStateMgr.getCurrentState().getLocalMetastore()
                .getTable(db.getFullName(), "site_access_day");

        // Mock getPartition to return null for a specific partition name, simulating TTL drop
        // This simulates the race condition where partition is created but then dropped by TTL cleaner
        // before buildCreatePartitionResponse can retrieve it
        new MockUp<OlapTable>() {
            @Mock
            public Partition getPartition(mockit.Invocation invocation, String partitionName, boolean isTemp) {
                // Return null for the specific partition we're testing, simulating it was dropped by TTL
                if (droppedPartitionName.equals(partitionName)) {
                    return null;
                }
                // Call the real method for other partitions
                return invocation.proceed(partitionName, isTemp);
            }
        };

        List<List<String>> partitionValues = Lists.newArrayList();
        List<String> values = Lists.newArrayList();
        values.add("1990-04-26");
        partitionValues.add(values);

        FrontendServiceImpl impl = new FrontendServiceImpl(exeEnv);
        TCreatePartitionRequest request = new TCreatePartitionRequest();
        request.setDb_id(db.getId());
        request.setTable_id(table.getId());
        request.setPartition_values(partitionValues);

        // Should not throw NPE, should return OK
        TCreatePartitionResult result = impl.createPartition(request);

        // The request should succeed
        Assertions.assertEquals(TStatusCode.OK, result.getStatus().getStatus_code());
        // partitions should be empty since the created partition was "dropped" by TTL
        Assertions.assertTrue(result.getPartitions() == null || result.getPartitions().isEmpty());
    }
}
