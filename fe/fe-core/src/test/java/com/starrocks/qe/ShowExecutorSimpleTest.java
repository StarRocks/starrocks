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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/qe/ShowExecutorTest.java

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.starrocks.qe;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.catalog.UserIdentity;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.ExceptionChecker;
import com.starrocks.common.FeConstants;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.proc.ComputeNodeProcDir;
import com.starrocks.datacache.DataCacheMetrics;
import com.starrocks.datacache.DataCacheMgr;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.server.NodeMgr;
import com.starrocks.server.RunMode;
import com.starrocks.server.WarehouseManager;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.LabelName;
import com.starrocks.sql.ast.QualifiedName;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.ShowAuthorStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowBasicStatsMetaStmt;
import com.starrocks.sql.ast.ShowCharsetStmt;
import com.starrocks.sql.ast.ShowComputeNodesStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowCreateExternalCatalogStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDataCacheRulesStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowEnginesStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.ast.ShowProcedureStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.sql.ast.expression.LimitElement;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.statistic.AnalyzeMgr;
import com.starrocks.statistic.ExternalBasicStatsMeta;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.system.Backend;
import com.starrocks.system.ComputeNode;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TDataCacheMetrics;
import com.starrocks.thrift.TDataCacheStatus;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.sparkproject.guava.collect.Maps;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;
import static com.starrocks.thrift.TStorageMedium.SSD;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Same as {@link ShowExecutorTest} but init most mockings only once to speed up the tests.
 */
public class ShowExecutorSimpleTest {
    private static ConnectContext ctx;
    private static GlobalStateMgr globalStateMgr;
    private static Database db;
    private static OlapTable table;
    @Mocked
    private MetadataMgr metadataMgr;

    @BeforeAll
    public static void beforeClass() {
        FeConstants.runningUnitTest = true;

        Column column1 = new Column("col1", Type.BIGINT);
        Column column2 = new Column("col2", Type.DOUBLE);
        column1.setIsKey(true);
        column2.setIsKey(true);
        Map<ColumnId, Column> idToColumn = Maps.newTreeMap(ColumnId.CASE_INSENSITIVE_ORDER);
        idToColumn.put(column1.getColumnId(), column1);
        idToColumn.put(column2.getColumnId(), column2);

        // mock index 1
        MaterializedIndex index1 = new MaterializedIndex();

        // mock partition
        PhysicalPartition physicalPartition = Deencapsulation.newInstance(PhysicalPartition.class);
        new Expectations(physicalPartition) {
            {
                physicalPartition.getBaseIndex();
                minTimes = 0;
                result = index1;
            }
        };

        // mock partition
        Partition partition = Deencapsulation.newInstance(Partition.class);
        new Expectations(partition) {
            {
                partition.getDefaultPhysicalPartition();
                minTimes = 0;
                result = physicalPartition;
            }
        };

        // mock table
        table = new OlapTable();
        table.setId(10001);
        new Expectations(table) {
            {
                table.getName();
                minTimes = 0;
                result = "testTbl";

                table.getType();
                minTimes = 0;
                result = TableType.OLAP;

                table.getBaseSchema();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2);

                table.getIdToColumn();
                minTimes = 0;
                result = idToColumn;

                table.getKeysType();
                minTimes = 0;
                result = KeysType.AGG_KEYS;

                table.getPartitionInfo();
                minTimes = 0;
                result = new SinglePartitionInfo();

                table.getDefaultDistributionInfo();
                minTimes = 0;
                result = new RandomDistributionInfo(10);

                table.getIndexIdByName(anyString);
                minTimes = 0;
                result = 0L;

                table.getStorageTypeByIndexId(0L);
                minTimes = 0;
                result = TStorageType.COLUMN;

                table.getPartition(anyLong);
                minTimes = 0;
                result = partition;

                table.getBfColumnNames();
                minTimes = 0;
                result = null;

                table.getIdToColumn();
                minTimes = 0;
                result = idToColumn;
            }
        };

        BaseTableInfo baseTableInfo = new BaseTableInfo(
                "default_catalog", "testDb", "testTbl", null);

        // mock materialized view
        MaterializedView mv = new MaterializedView();
        new Expectations(mv) {
            {
                mv.getName();
                minTimes = 0;
                result = "testMv";

                mv.getBaseTableInfos();
                minTimes = 0;
                result = baseTableInfo;

                mv.getBaseSchema();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2);

                mv.getOrderedOutputColumns();
                minTimes = 0;
                result = Lists.newArrayList(column1, column2);

                mv.getType();
                minTimes = 0;
                result = TableType.MATERIALIZED_VIEW;

                mv.getId();
                minTimes = 0;
                result = 1000L;

                mv.getIdToColumn();
                minTimes = 0;
                result = idToColumn;

                mv.getViewDefineSql();
                minTimes = 0;
                result = "select col1, col2 from table1";

                mv.getRowCount();
                minTimes = 0;
                result = 10L;

                mv.getComment();
                minTimes = 0;
                result = "TEST MATERIALIZED VIEW";

                mv.getDisplayComment();
                minTimes = 0;
                result = "TEST MATERIALIZED VIEW";

                mv.getPartitionInfo();
                minTimes = 0;
                result = new ExpressionRangePartitionInfo(
                        Collections.singletonList(
                                ColumnIdExpr.create(new SlotRef(
                                        new TableName("test", "testMv"), column1.getName()))),
                        Collections.singletonList(column1), PartitionType.RANGE);

                mv.getDefaultDistributionInfo();
                minTimes = 0;
                result = new HashDistributionInfo(10, Collections.singletonList(column1));

                mv.getRefreshScheme();
                minTimes = 0;
                result = new MaterializedView.MvRefreshScheme();

                mv.getDefaultReplicationNum();
                minTimes = 0;
                result = 1;

                mv.getStorageMedium();
                minTimes = 0;
                result = SSD.name();

                mv.getTableProperty();
                minTimes = 0;
                result = new TableProperty(
                        Collections.singletonMap(PROPERTIES_STORAGE_COOLDOWN_TIME, "100"));

                mv.getIdToColumn();
                minTimes = 0;
                result = idToColumn;
            }
        };

        // mock database
        db = new Database();
        new Expectations(db) {
            {
                db.getTable("testMv");
                minTimes = 0;
                result = mv;

                db.getTable("testTbl");
                minTimes = 0;
                result = table;

                db.getTable("emptyTable");
                minTimes = 0;
                result = table;

                db.getTables();
                minTimes = 0;
                result = Lists.newArrayList(table, mv);

                db.getMaterializedViews();
                minTimes = 0;
                result = Lists.newArrayList(mv);

                db.getFullName();
                minTimes = 0;
                result = "testDb";
            }
        };

        // mock globalStateMgr.
        new MockUp<LocalMetastore>() {
            @Mock
            public Database getDb(String dbName) {
                if (dbName.equalsIgnoreCase("emptyDb")) {
                    return null;
                }
                return db;
            }

            @Mock
            public Table getTable(String dbName, String tblName) {
                return db.getTable(tblName);
            }

            @Mock
            public List<Table> getTables(Long dbId) {
                return db.getTables();
            }
        };

        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setQualifiedUser("testUser");

        new Expectations(ctx) {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };


        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
    }

    @BeforeEach
    public void setUp() {
        LocalMetastore localMetastore = new LocalMetastore(globalStateMgr, null, null);
        new Expectations(globalStateMgr) {
            {
                /*
                globalStateMgr.getLocalMetastore().getDb("testDb");
                minTimes = 0;
                result = db;


                globalStateMgr.getLocalMetastore().getDb("emptyDb");
                minTimes = 0;
                result = null;

                 */

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                globalStateMgr.getLocalMetastore();
                minTimes = 0;
                result = localMetastore;

                globalStateMgr.getMetadataMgr();
                minTimes = 0;
                result = metadataMgr;

                metadataMgr.listDbNames((ConnectContext) any, "default_catalog");
                minTimes = 0;
                result = Lists.newArrayList("testDb");

                metadataMgr.getDb((ConnectContext) any, "default_catalog", "testDb");
                minTimes = 0;
                result = db;

                metadataMgr.getDb((ConnectContext) any, "default_catalog", "emptyDb");
                minTimes = 0;
                result = null;

                metadataMgr.getTable((ConnectContext) any, "default_catalog", "testDb", "testTbl");
                minTimes = 0;
                result = table;
            }
        };
    }

    @Test
    public void testShowDb() throws AnalysisException, DdlException {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        ShowDbStmt stmt = new ShowDbStmt(null);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("Database", resultSet.getMetaData().getColumn(0).getName());
        Assertions.assertEquals(resultSet.getResultRows().get(0).get(0), "testDb");
    }

    @Test
    public void testShowDbPattern() throws AnalysisException, DdlException {
        ShowDbStmt stmt = new ShowDbStmt("empty%");

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testShowDbPriv() throws AnalysisException, DdlException {
        ShowDbStmt stmt = new ShowDbStmt(null);

        ctx.setGlobalStateMgr(AccessTestUtil.fetchBlockCatalog());
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
    }

    @Test
    public void testShowTableFromUnknownDatabase() {
        ShowTableStmt stmt = new ShowTableStmt("emptyDb", false, null);

        Throwable exception = assertThrows(SemanticException.class, () -> ShowExecutor.execute(stmt, ctx));
        assertThat(exception.getMessage(), containsString("Unknown database 'emptyDb'"));
    }

    @Test
    public void testShowTablePattern() throws AnalysisException, DdlException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", false, "empty%");

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertFalse(resultSet.next());
    }

    @Disabled
    @Test
    public void testDescribe() throws DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        DescribeStmt stmt = (DescribeStmt) SqlParser.parse("desc testTbl",
                ctx.getSessionVariable().getSqlMode()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        ShowResultSet resultSet;
        try {
            resultSet = ShowExecutor.execute(stmt, ctx);
            Assertions.assertFalse(resultSet.next());
        } catch (SemanticException e) {
            e.printStackTrace();
            Assertions.fail();
        }
    }

    @Test
    public void testShowVariable2() throws AnalysisException, DdlException {
        ShowVariablesStmt stmt = new ShowVariablesStmt(SetType.VERBOSE, null);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals(4, resultSet.getMetaData().getColumnCount());
        Assertions.assertEquals("Variable_name", resultSet.getMetaData().getColumn(0).getName());
        Assertions.assertEquals("Value", resultSet.getMetaData().getColumn(1).getName());
        Assertions.assertEquals("Default_value", resultSet.getMetaData().getColumn(2).getName());
        Assertions.assertEquals("Is_changed", resultSet.getMetaData().getColumn(3).getName());

        Assertions.assertTrue(resultSet.getResultRows().size() > 0);
        Assertions.assertEquals(4, resultSet.getResultRows().get(0).size());

        ShowVariablesStmt stmt2 = new ShowVariablesStmt(SetType.VERBOSE, "query_%");
        ShowResultSet resultSet2 = ShowExecutor.execute(stmt2, ctx);
        Assertions.assertEquals(4, resultSet2.getMetaData().getColumnCount());
        Assertions.assertTrue(resultSet2.getResultRows().size() > 0);
        Assertions.assertEquals(4, resultSet2.getResultRows().get(0).size());
    }

    @Test
    public void testShowCreateNoDb() {
        assertThrows(SemanticException.class, () -> {
            ctx.setGlobalStateMgr(globalStateMgr);
            ctx.setQualifiedUser("testUser");

            ShowCreateDbStmt stmt = new ShowCreateDbStmt("emptyDb");

            ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

            Assertions.fail("No exception throws.");
        });
    }

    @Test
    public void testShowCreateTableEmptyDb() {
        assertThrows(SemanticException.class, () -> {
            ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("emptyDb", "testTable"),
                    ShowCreateTableStmt.CreateTableType.TABLE);

            ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

            Assertions.fail("No Exception throws.");
        });
    }

    @Test
    public void testShowBackendsSharedDataMode(@Mocked StarOSAgent starosAgent) {
        SystemInfoService clusterInfo = AccessTestUtil.fetchSystemInfoService();

        // mock backends
        Backend backend = new Backend(1L, "127.0.0.1", 12345);
        backend.setCpuCores(16);
        backend.setMemLimitBytes(100L);
        backend.updateResourceUsage(0, 1L, 30);
        backend.setAlive(false);
        clusterInfo.addBackend(backend);

        NodeMgr nodeMgr = new NodeMgr();
        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = clusterInfo;
            }
        };

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starosAgent;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;
            }
        };

        new MockUp<RunMode>() {
            @Mock
            RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        long tabletNum = 1024;
        long workerId = 1122;
        new Expectations() {
            {
                starosAgent.getWorkerTabletNum(anyString);
                minTimes = 1;
                result = tabletNum;

                starosAgent.getWorkerIdByNodeId(anyLong);
                minTimes = 1;
                result = workerId;
            }
        };

        ShowBackendsStmt stmt = new ShowBackendsStmt();

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertEquals(34, resultSet.getMetaData().getColumnCount());
        Assertions.assertEquals("BackendId", resultSet.getMetaData().getColumn(0).getName());
        Assertions.assertEquals("ResolvedIP", resultSet.getMetaData().getColumn(2).getName());
        Assertions.assertEquals("CpuCores", resultSet.getMetaData().getColumn(22).getName());
        Assertions.assertEquals("MemLimit", resultSet.getMetaData().getColumn(23).getName());
        Assertions.assertEquals("NumRunningQueries", resultSet.getMetaData().getColumn(24).getName());
        Assertions.assertEquals("MemUsedPct", resultSet.getMetaData().getColumn(25).getName());
        Assertions.assertEquals("CpuUsedPct", resultSet.getMetaData().getColumn(26).getName());
        Assertions.assertEquals("DataCacheMetrics", resultSet.getMetaData().getColumn(27).getName());
        Assertions.assertEquals("StatusCode", resultSet.getMetaData().getColumn(29).getName());
        Assertions.assertEquals("StarletPort", resultSet.getMetaData().getColumn(30).getName());
        Assertions.assertEquals("WorkerId", resultSet.getMetaData().getColumn(31).getName());

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("1", resultSet.getString(0));

        Assertions.assertEquals("16", resultSet.getString(23));
        Assertions.assertEquals("100.000B", resultSet.getString(24));
        Assertions.assertEquals("0", resultSet.getString(25));
        Assertions.assertEquals("N/A", resultSet.getString(28));
        Assertions.assertEquals("CONNECTING", resultSet.getString(30));
        Assertions.assertEquals(String.valueOf(workerId), resultSet.getString(32));
        Assertions.assertEquals(String.valueOf(tabletNum), resultSet.getString(12));
    }

    @Test
    public void testShowComputeNodesSharedData(@Mocked StarOSAgent starosAgent) throws AnalysisException, DdlException {
        SystemInfoService clusterInfo = AccessTestUtil.fetchSystemInfoService();

        ComputeNode node = new ComputeNode(1L, "127.0.0.1", 80);
        node.setCpuCores(16);
        node.setMemLimitBytes(100L);
        node.updateResourceUsage(10, 1L, 30);
        TDataCacheMetrics tDataCacheMetrics = new TDataCacheMetrics();
        tDataCacheMetrics.setStatus(TDataCacheStatus.NORMAL);
        tDataCacheMetrics.setDisk_quota_bytes(1024 * 1024 * 1024);
        tDataCacheMetrics.setMem_quota_bytes(1024 * 1024 * 1024);
        node.updateDataCacheMetrics(DataCacheMetrics.buildFromThrift(tDataCacheMetrics));
        node.setAlive(true);
        clusterInfo.addComputeNode(node);

        NodeMgr nodeMgr = new NodeMgr();
        new Expectations(nodeMgr) {
            {
                nodeMgr.getClusterInfo();
                minTimes = 0;
                result = clusterInfo;
            }
        };

        WarehouseManager warehouseManager = new WarehouseManager();
        warehouseManager.initDefaultWarehouse();
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getNodeMgr();
                minTimes = 0;
                result = nodeMgr;

                globalStateMgr.getStarOSAgent();
                minTimes = 0;
                result = starosAgent;

                globalStateMgr.getWarehouseMgr();
                minTimes = 0;
                result = warehouseManager;
            }
        };

        new MockUp<RunMode>() {
            @Mock
            RunMode getCurrentRunMode() {
                return RunMode.SHARED_DATA;
            }
        };

        long tabletNum = 1024;
        new Expectations() {
            {
                starosAgent.getWorkerTabletNum(anyString);
                minTimes = 0;
                result = tabletNum;
            }
        };

        ShowComputeNodesStmt stmt = new ShowComputeNodesStmt();

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertEquals(ComputeNodeProcDir.TITLE_NAMES_SHARED_DATA.size(),
                resultSet.getMetaData().getColumnCount());
        for (int i = 0; i < ComputeNodeProcDir.TITLE_NAMES_SHARED_DATA.size(); ++i) {
            Assertions.assertEquals(ComputeNodeProcDir.TITLE_NAMES_SHARED_DATA.get(i),
                    resultSet.getMetaData().getColumn(i).getName());
        }

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("16", resultSet.getString(13)); // CpuCores
        Assertions.assertEquals("100.000B", resultSet.getString(14)); // MemLimit
        Assertions.assertEquals("10", resultSet.getString(15));
        Assertions.assertEquals("1.00 %", resultSet.getString(16));
        Assertions.assertEquals("3.0 %", resultSet.getString(17));
        Assertions.assertEquals("Status: Normal, DiskUsage: 0B/1GB, MemUsage: 0B/1GB", resultSet.getString(18));
        Assertions.assertEquals("OK", resultSet.getString(20));
        Assertions.assertEquals(String.valueOf(tabletNum), resultSet.getString(24));
    }

    @Test
    public void testShowAuthors() throws AnalysisException, DdlException {
        ShowAuthorStmt stmt = new ShowAuthorStmt();

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertEquals(3, resultSet.getMetaData().getColumnCount());
        Assertions.assertEquals("Name", resultSet.getMetaData().getColumn(0).getName());
        Assertions.assertEquals("Location", resultSet.getMetaData().getColumn(1).getName());
        Assertions.assertEquals("Comment", resultSet.getMetaData().getColumn(2).getName());
    }

    @Test
    public void testShowEngine() throws AnalysisException, DdlException {
        ShowEnginesStmt stmt = new ShowEnginesStmt();

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("OLAP", resultSet.getString(0));
    }

    @Test
    public void testShowUser() throws AnalysisException, DdlException {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ShowUserStmt stmt = new ShowUserStmt(false);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("'root'@'%'", resultSet.getString(0));
    }

    @Test
    public void testShowCharset() throws DdlException, AnalysisException {
        // Dbeaver 23 Use
        ShowCharsetStmt stmt = new ShowCharsetStmt();

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertTrue(resultSet.next());
        List<List<String>> resultRows = resultSet.getResultRows();
        Assertions.assertTrue(resultRows.size() >= 1);
        Assertions.assertEquals(resultRows.get(0).get(0), "utf8");
    }

    @Test
    public void testShowEmpty() throws AnalysisException, DdlException {
        ShowProcedureStmt stmt = new ShowProcedureStmt();

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testShowMaterializedViewFromUnknownDatabase() {
        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("default_catalog", "emptyDb", (String) null);

        Throwable exception = assertThrows(SemanticException.class, () -> ShowExecutor.execute(stmt, ctx));
        assertThat(exception.getMessage(), containsString("Unknown database 'emptyDb'"));
    }

    @Test
    public void testShowRoutineLoadNonExisted() throws AnalysisException, DdlException {
        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(new LabelName("testDb", "non-existed-job-name"), false);

        // AnalysisException("There is no job named...") is expected.
        Assertions.assertThrows(SemanticException.class, () -> ShowExecutor.execute(stmt, ctx));
    }

    @Test
    public void testShowCreateExternalCatalogTable() throws DdlException, AnalysisException {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalogName, String dbName) {
                return new Database();
            }

            @Mock
            public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
                List<Column> fullSchema = new ArrayList<>();
                Column columnId = new Column("id", Type.INT, true);
                columnId.setComment("id");
                Column columnName = new Column("name", Type.VARCHAR);
                Column columnYear = new Column("year", Type.INT);
                Column columnDt = new Column("dt", Type.INT);
                fullSchema.add(columnId);
                fullSchema.add(columnName);
                fullSchema.add(columnYear);
                fullSchema.add(columnDt);
                List<String> partitions = Lists.newArrayList();
                partitions.add("year");
                partitions.add("dt");
                HiveTable.Builder tableBuilder = HiveTable.builder()
                        .setId(1)
                        .setTableName("test_table")
                        .setCatalogName("hive_catalog")
                        .setResourceName(toResourceName("hive_catalog", "hive"))
                        .setHiveDbName("hive_db")
                        .setHiveTableName("test_table")
                        .setPartitionColumnNames(partitions)
                        .setFullSchema(fullSchema)
                        .setTableLocation("hdfs://hadoop/hive/warehouse/test.db/test")
                        .setCreateTime(10000);
                return tableBuilder.build();
            }
        };

        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("hive_catalog", "hive_db", "test_table"),
                ShowCreateTableStmt.CreateTableType.TABLE);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("test_table", resultSet.getResultRows().get(0).get(0));
        Assertions.assertEquals("CREATE TABLE `test_table` (\n" +
                        "  `id` int(11) DEFAULT NULL COMMENT \"id\",\n" +
                        "  `name` varchar DEFAULT NULL,\n" +
                        "  `year` int(11) DEFAULT NULL,\n" +
                        "  `dt` int(11) DEFAULT NULL\n" +
                        ")\n" +
                        "PARTITION BY (year, dt)\n" +
                        "PROPERTIES (\"location\" = \"hdfs://hadoop/hive/warehouse/test.db/test\");",
                resultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testShowCreateHiveExternalTable() {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(ConnectContext context, String catalogName, String dbName) {
                return new Database();
            }

            @Mock
            public Table getTable(ConnectContext context, String catalogName, String dbName, String tblName) {
                List<Column> fullSchema = new ArrayList<>();
                Column columnId = new Column("id", Type.INT, true);
                columnId.setComment("id");
                Column columnName = new Column("name", Type.VARCHAR);
                Column columnYear = new Column("year", Type.INT);
                Column columnDt = new Column("dt", Type.INT);
                fullSchema.add(columnId);
                fullSchema.add(columnName);
                fullSchema.add(columnYear);
                fullSchema.add(columnDt);
                List<String> partitions = Lists.newArrayList();
                partitions.add("year");
                partitions.add("dt");
                HiveTable.Builder tableBuilder = HiveTable.builder()
                        .setId(1)
                        .setTableName("test_table")
                        .setCatalogName("hive_catalog")
                        .setResourceName(toResourceName("hive_catalog", "hive"))
                        .setHiveDbName("hive_db")
                        .setHiveTableName("test_table")
                        .setPartitionColumnNames(partitions)
                        .setFullSchema(fullSchema)
                        .setTableLocation("hdfs://hadoop/hive/warehouse/test.db/test")
                        .setCreateTime(10000)
                        .setHiveTableType(HiveTable.HiveTableType.EXTERNAL_TABLE);
                return tableBuilder.build();
            }
        };

        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("hive_catalog", "hive_db", "test_table"),
                ShowCreateTableStmt.CreateTableType.TABLE);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("test_table", resultSet.getResultRows().get(0).get(0));
        Assertions.assertEquals("CREATE EXTERNAL TABLE `test_table` (\n" +
                        "  `id` int(11) DEFAULT NULL COMMENT \"id\",\n" +
                        "  `name` varchar DEFAULT NULL,\n" +
                        "  `year` int(11) DEFAULT NULL,\n" +
                        "  `dt` int(11) DEFAULT NULL\n" +
                        ")\n" +
                        "PARTITION BY (year, dt)\n" +
                        "PROPERTIES (\"location\" = \"hdfs://hadoop/hive/warehouse/test.db/test\");",
                resultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testShowCreateExternalCatalog() throws AnalysisException, DdlException {
        new MockUp<CatalogMgr>() {
            @Mock
            public Catalog getCatalogByName(String name) {
                Map<String, String> properties = new HashMap<>();
                properties.put("hive.metastore.uris", "thrift://hadoop:9083");
                properties.put("type", "hive");
                Catalog catalog = new Catalog(1, "test_hive", properties, "hive_test");
                return catalog;
            }
        };
        ShowCreateExternalCatalogStmt stmt = new ShowCreateExternalCatalogStmt("test_hive");

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertEquals("test_hive", resultSet.getResultRows().get(0).get(0));
        Assertions.assertEquals("CREATE EXTERNAL CATALOG `test_hive`\n" +
                "comment \"hive_test\"\n" +
                "PROPERTIES (\"type\"  =  \"hive\",\n" +
                "\"hive.metastore.uris\"  =  \"thrift://hadoop:9083\"\n" +
                ")", resultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testShowCreateExternalCatalogNotExists() {
        new MockUp<CatalogMgr>() {
            @Mock
            public Catalog getCatalogByName(String name) {
                return null;
            }
        };

        ShowCreateExternalCatalogStmt stmt = new ShowCreateExternalCatalogStmt("catalog_not_exist");

        ExceptionChecker.expectThrowsWithMsg(SemanticException.class, "Unknown catalog 'catalog_not_exist'",
                () -> ShowExecutor.execute(stmt, ctx));
    }

    @Test
    public void testShowBasicStatsMeta() throws Exception {
        new MockUp<AnalyzeMgr>() {
            @Mock
            public Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> getExternalBasicStatsMetaMap() {
                Map<AnalyzeMgr.StatsMetaKey, ExternalBasicStatsMeta> map = new HashMap<>();
                map.put(new AnalyzeMgr.StatsMetaKey("hive0", "testDb", "testTable"),
                        new ExternalBasicStatsMeta("hive0", "testDb", "testTable", null,
                                StatsConstants.AnalyzeType.FULL, LocalDateTime.now(), Maps.newHashMap()));
                return map;
            }
        };
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ShowBasicStatsMetaStmt stmt = new ShowBasicStatsMetaStmt(null, List.of(), LimitElement.NO_LIMIT, NodePosition.ZERO);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals("hive0.testDb", resultSet.getResultRows().get(0).get(0));
        Assertions.assertEquals("testTable", resultSet.getResultRows().get(0).get(1));
        Assertions.assertEquals("ALL", resultSet.getResultRows().get(0).get(2));
        Assertions.assertEquals("FULL", resultSet.getResultRows().get(0).get(3));
    }

    @Test
    public void testShowCreateExternalCatalogWithMask() throws AnalysisException, DdlException {
        // More mask logic please write in CredentialUtilTest
        new MockUp<CatalogMgr>() {
            @Mock
            public Catalog getCatalogByName(String name) {
                Map<String, String> properties = new HashMap<>();
                properties.put("hive.metastore.uris", "thrift://hadoop:9083");
                properties.put("type", "hive");
                properties.put("aws.s3.access_key", "iam_user_access_key");
                properties.put("aws.s3.secret_key", "iam_user_secret_key");
                return new Catalog(1, "test_hive", properties, "hive_test");
            }
        };
        ShowCreateExternalCatalogStmt stmt = new ShowCreateExternalCatalogStmt("test_hive");

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertEquals("test_hive", resultSet.getResultRows().get(0).get(0));
        Assertions.assertEquals("CREATE EXTERNAL CATALOG `test_hive`\n" +
                "comment \"hive_test\"\n" +
                "PROPERTIES (\"aws.s3.access_key\"  =  \"ia******ey\",\n" +
                "\"aws.s3.secret_key\"  =  \"ia******ey\",\n" +
                "\"hive.metastore.uris\"  =  \"thrift://hadoop:9083\",\n" +
                "\"type\"  =  \"hive\"\n" +
                ")", resultSet.getResultRows().get(0).get(1));
    }

    @Test
    public void testShowDataCacheRules() throws DdlException, AnalysisException {
        DataCacheMgr dataCacheMgr = DataCacheMgr.getInstance();
        dataCacheMgr.createCacheRule(QualifiedName.of(ImmutableList.of("test1", "test1", "test1")), null, -1, null);

        Map<String, String> properties = new HashMap<>();
        properties.put("hello", "world");
        properties.put("ni", "hao");
        StringLiteral stringLiteral = new StringLiteral("hello");
        dataCacheMgr.createCacheRule(QualifiedName.of(ImmutableList.of("test2", "test2", "test2")),
                stringLiteral, -1, properties);

        ShowDataCacheRulesStmt stmt = new ShowDataCacheRulesStmt(NodePosition.ZERO);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        List<String> row1 = resultSet.getResultRows().get(0);
        List<String> row2 = resultSet.getResultRows().get(1);
        Assertions.assertEquals("[0, test1, test1, test1, -1, NULL, NULL]", row1.toString());
        Assertions.assertEquals("[1, test2, test2, test2, -1, 'hello', \"hello\"=\"world\", \"ni\"=\"hao\"]", row2.toString());
    }

    @Test
    public void testShouldMarkIdleCheck() {
        StmtExecutor stmtExecutor = new StmtExecutor(new ConnectContext(),
                SqlParser.parseSingleStatement("select @@query_timeout", SqlModeHelper.MODE_DEFAULT));

        Assertions.assertFalse(stmtExecutor.shouldMarkIdleCheck(
                SqlParser.parseSingleStatement("select @@query_timeout", SqlModeHelper.MODE_DEFAULT)));

        Assertions.assertFalse(stmtExecutor.shouldMarkIdleCheck(
                SqlParser.parseSingleStatement("SET NAMES utf8mb4", SqlModeHelper.MODE_DEFAULT)));

        Assertions.assertTrue(stmtExecutor.shouldMarkIdleCheck(
                SqlParser.parseSingleStatement("SET password = 'xxx'", SqlModeHelper.MODE_DEFAULT)));

        Assertions.assertTrue(stmtExecutor.shouldMarkIdleCheck(
                SqlParser.parseSingleStatement("select sleep(10)", SqlModeHelper.MODE_DEFAULT)));

        Assertions.assertFalse(stmtExecutor.shouldMarkIdleCheck(
                SqlParser.parseSingleStatement("show users", SqlModeHelper.MODE_DEFAULT)));

        Assertions.assertFalse(stmtExecutor.shouldMarkIdleCheck(
                SqlParser.parseSingleStatement("admin set frontend config('proc_profile_cpu_enable' = 'true')",
                        SqlModeHelper.MODE_DEFAULT)));
    }
}
