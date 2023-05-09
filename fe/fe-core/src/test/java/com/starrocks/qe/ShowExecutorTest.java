// This file is made available under Elastic License 2.0.
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

import com.google.common.collect.Lists;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.analysis.Analyzer;
import com.starrocks.analysis.LabelName;
import com.starrocks.analysis.SlotRef;
import com.starrocks.analysis.TableName;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Catalog;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.HiveTable;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfoTest;
import com.starrocks.catalog.MaterializedIndex;
import com.starrocks.catalog.MaterializedView;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Partition;
import com.starrocks.catalog.PartitionType;
import com.starrocks.catalog.RandomDistributionInfo;
import com.starrocks.catalog.SinglePartitionInfo;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Table.TableType;
import com.starrocks.catalog.TableProperty;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.Config;
import com.starrocks.common.DdlException;
import com.starrocks.common.PatternMatcher;
import com.starrocks.common.UserException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.lake.StarOSAgent;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.mysql.privilege.Auth;
import com.starrocks.server.CatalogMgr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.ast.DescribeStmt;
import com.starrocks.sql.ast.HelpStmt;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.ShowAuthorStmt;
import com.starrocks.sql.ast.ShowBackendsStmt;
import com.starrocks.sql.ast.ShowCharsetStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowCreateExternalCatalogStmt;
import com.starrocks.sql.ast.ShowCreateTableStmt;
import com.starrocks.sql.ast.ShowDbStmt;
import com.starrocks.sql.ast.ShowEnginesStmt;
import com.starrocks.sql.ast.ShowMaterializedViewStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.ShowProcedureStmt;
import com.starrocks.sql.ast.ShowRoutineLoadStmt;
import com.starrocks.sql.ast.ShowTableStmt;
import com.starrocks.sql.ast.ShowUserStmt;
import com.starrocks.sql.ast.ShowVariablesStmt;
import com.starrocks.system.Backend;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME;
import static com.starrocks.server.CatalogMgr.ResourceMappingCatalog.toResourceName;
import static com.starrocks.thrift.TStorageMedium.SSD;

public class ShowExecutorTest {

    private static final Logger LOG = LogManager.getLogger(ShowExecutorTest.class);

    private ConnectContext ctx;
    private GlobalStateMgr globalStateMgr;

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);

        Column column1 = new Column("col1", Type.BIGINT);
        Column column2 = new Column("col2", Type.DOUBLE);
        column1.setIsKey(true);
        column2.setIsKey(true);
        // mock index 1
        MaterializedIndex index1 = new MaterializedIndex();

        // mock index 2
        MaterializedIndex index2 = new MaterializedIndex();

        // mock partition
        Partition partition = Deencapsulation.newInstance(Partition.class);
        new Expectations(partition) {
            {
                partition.getBaseIndex();
                minTimes = 0;
                result = index1;
            }
        };

        // mock table
        OlapTable table = new OlapTable();
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

                table.getCopiedBfColumns();
                minTimes = 0;
                result = null;
            }
        };

        BaseTableInfo baseTableInfo = new BaseTableInfo(
                "default_catalog", "testDb", "testTbl");

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

                mv.getType();
                minTimes = 0;
                result = TableType.MATERIALIZED_VIEW;

                mv.getId();
                minTimes = 0;
                result = 1000L;

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
                                new SlotRef(
                                        new TableName("test", "testMv"), column1.getName())),
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
            }
        };

        // mock database
        Database db = new Database();
        new Expectations(db) {
            {
                db.readLock();
                minTimes = 0;

                db.readUnlock();
                minTimes = 0;

                db.getTable(anyString);
                minTimes = 0;
                result = table;

                db.getTables();
                minTimes = 0;
                result = Lists.newArrayList(table);

                db.getMaterializedViews();
                minTimes = 0;
                result = Lists.newArrayList(mv);
            }
        };

        // mock auth
        Auth auth = AccessTestUtil.fetchAdminAccess();

        // mock globalStateMgr.
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
        new Expectations(globalStateMgr) {
            {
                globalStateMgr.getDb("testDb");
                minTimes = 0;
                result = db;

                globalStateMgr.getDb("emptyDb");
                minTimes = 0;
                result = null;

                globalStateMgr.getAuth();
                minTimes = 0;
                result = auth;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getCurrentState();
                minTimes = 0;
                result = globalStateMgr;

                GlobalStateMgr.getDdlStmt((Table) any, (List) any, (List) any, (List) any, anyBoolean, anyBoolean);
                minTimes = 0;

                GlobalStateMgr.getDdlStmt((Table) any, (List) any, null, null, anyBoolean, anyBoolean);
                minTimes = 0;

                GlobalStateMgr.getCurrentState().getMetadataMgr().listDbNames("default_catalog");
                minTimes = 0;
                result = Lists.newArrayList("testDb");

                GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", "testDb");
                minTimes = 0;
                result = db;

                GlobalStateMgr.getCurrentState().getMetadataMgr().getDb("default_catalog", "emptyDb");
                minTimes = 0;
                result = null;

                GlobalStateMgr.getCurrentState().getMetadataMgr().getTable("default_catalog", "testDb",
                        "testTbl");
                minTimes = 0;
                result = table;
            }
        };

        // mock scheduler
        ConnectScheduler scheduler = new ConnectScheduler(10);
        new Expectations(scheduler) {
            {
                scheduler.listConnection("testUser");
                minTimes = 0;
                result = Lists.newArrayList(ctx.toThreadInfo());
            }
        };

        ctx.setConnectScheduler(scheduler);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchAdminCatalog());
        ctx.setQualifiedUser("testUser");

        new Expectations(ctx) {
            {
                ConnectContext.get();
                minTimes = 0;
                result = ctx;
            }
        };
    }

    @Test
    public void testShowDb() throws AnalysisException, DdlException {
        ShowDbStmt stmt = new ShowDbStmt(null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("Database", resultSet.getMetaData().getColumn(0).getName());
        Assert.assertEquals(resultSet.getResultRows().get(0).get(0), "testDb");
    }

    @Test
    public void testShowDbPattern() throws AnalysisException, DdlException {
        ShowDbStmt stmt = new ShowDbStmt("empty%");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowDbPriv() throws AnalysisException, DdlException {
        ShowDbStmt stmt = new ShowDbStmt(null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ctx.setGlobalStateMgr(AccessTestUtil.fetchBlockCatalog());
        ShowResultSet resultSet = executor.execute();
    }

    @Test
    public void testShowTable() throws AnalysisException, DdlException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", false, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowPartitions(@Mocked Analyzer analyzer) throws UserException {

        new MockUp<SystemInfoService>() {
            @Mock
            public List<Long> getAvailableBackendIds() {
                return Arrays.asList(10001L, 10002L, 10003L);
            }
        };
        // Prepare to Test
        ListPartitionInfoTest listPartitionInfoTest = new ListPartitionInfoTest();
        listPartitionInfoTest.setUp();
        OlapTable olapTable = listPartitionInfoTest.findTableForMultiListPartition();
        Database db = new Database();
        new Expectations(db) {
            {
                db.getTable(anyString);
                minTimes = 0;
                result = olapTable;

                db.getTable(0);
                minTimes = 0;
                result = olapTable;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getDb(0);
                minTimes = 0;
                result = db;
            }
        };

        // Ok to test
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTbl"),
                null, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        // Ready to Assert
        String partitionKeyTitle = resultSet.getMetaData().getColumn(6).getName();
        Assert.assertEquals(partitionKeyTitle, "PartitionKey");
        String valuesTitle = resultSet.getMetaData().getColumn(7).getName();
        Assert.assertEquals(valuesTitle, "List");

        String partitionKey1 = resultSet.getResultRows().get(0).get(6);
        Assert.assertEquals(partitionKey1, "dt, province");
        String partitionKey2 = resultSet.getResultRows().get(1).get(6);
        Assert.assertEquals(partitionKey2, "dt, province");

        String values1 = resultSet.getResultRows().get(0).get(7);
        Assert.assertEquals(values1, "(('2022-04-15', 'guangdong'), ('2022-04-15', 'tianjin'))");
        String values2 = resultSet.getResultRows().get(1).get(7);
        Assert.assertEquals(values2, "(('2022-04-16', 'shanghai'), ('2022-04-16', 'beijing'))");
    }

    @Test
    public void testShowTableFromUnknownDatabase() throws AnalysisException, DdlException {
        ShowTableStmt stmt = new ShowTableStmt("emptyDb", false, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        executor.execute();
    }

    @Test
    public void testShowTablePattern() throws AnalysisException, DdlException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", false, "empty%");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Ignore
    @Test
    public void testDescribe() throws DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        DescribeStmt stmt = (DescribeStmt) com.starrocks.sql.parser.SqlParser.parse("desc testTbl",
                ctx.getSessionVariable().getSqlMode()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet;
        try {
            resultSet = executor.execute();
            Assert.assertFalse(resultSet.next());
        } catch (AnalysisException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testShowVariable() throws AnalysisException, DdlException {
        // Mock variable
        VariableMgr variableMgr = new VariableMgr();
        List<List<String>> rows = Lists.newArrayList();
        rows.add(Lists.newArrayList("var1", "abc"));
        rows.add(Lists.newArrayList("var2", "abc"));
        new Expectations(variableMgr) {
            {
                VariableMgr.dump((SetType) any, (SessionVariable) any, (PatternMatcher) any);
                minTimes = 0;
                result = rows;

                VariableMgr.dump((SetType) any, (SessionVariable) any, null);
                minTimes = 0;
                result = rows;
            }
        };

        ShowVariablesStmt stmt = new ShowVariablesStmt(SetType.SESSION, "var%");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var1", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        stmt = new ShowVariablesStmt(SetType.SESSION, null);
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var1", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("var2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowTableVerbose() throws AnalysisException, DdlException {
        ShowTableStmt stmt = new ShowTableStmt("testDb", true, null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testTbl", resultSet.getString(0));
        Assert.assertEquals("BASE TABLE", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowCreateDb() throws AnalysisException, DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        ShowCreateDbStmt stmt = new ShowCreateDbStmt("testDb");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("testDb", resultSet.getString(0));
        Assert.assertEquals("CREATE DATABASE `testDb`", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test(expected = AnalysisException.class)
    public void testShowCreateNoDb() throws AnalysisException, DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        ShowCreateDbStmt stmt = new ShowCreateDbStmt("emptyDb");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.fail("No exception throws.");
    }

    @Test(expected = AnalysisException.class)
    public void testShowCreateTableEmptyDb() throws AnalysisException, DdlException {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("emptyDb", "testTable"),
                ShowCreateTableStmt.CreateTableType.TABLE);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.fail("No Exception throws.");
    }

    @Test
    public void testShowCreateTableEmptyTbl() throws AnalysisException, DdlException {
        ShowCreateTableStmt stmt = new ShowCreateTableStmt(new TableName("testDb", "emptyTable"),
                ShowCreateTableStmt.CreateTableType.TABLE);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Ignore
    @Test
    public void testShowColumn() throws AnalysisException, DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        ShowColumnStmt stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show columns from testTbl in testDb",
                ctx.getSessionVariable()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(2));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col2", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // verbose
        stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show full columns from testTbl in testDb",
                ctx.getSessionVariable()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col2", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertFalse(resultSet.next());

        // show full fields
        stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show full fields from testTbl in testDb",
                ctx.getSessionVariable()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col2", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertFalse(resultSet.next());

        // pattern
        stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show full columns from testTbl in testDb like \"%1\"",
                ctx.getSessionVariable().getSqlMode()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("col1", resultSet.getString(0));
        Assert.assertEquals("NO", resultSet.getString(3));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowColumnFromUnknownTable() throws AnalysisException, DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");
        ShowColumnStmt stmt = new ShowColumnStmt(new TableName("emptyDb", "testTable"), null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        executor.execute();

        // empty table
        stmt = new ShowColumnStmt(new TableName("testDb", "emptyTable"), null, null, true);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);
        executor = new ShowExecutor(ctx, stmt);

        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Unknown table 'testDb.emptyTable'");
        executor.execute();
    }

    @Test
    public void testShowBackends() throws AnalysisException, DdlException {
        SystemInfoService clusterInfo = AccessTestUtil.fetchSystemInfoService();
        StarOSAgent starosAgent = new StarOSAgent();

        // mock backends
        Backend backend = new Backend();
        new Expectations(clusterInfo) {
            {
                clusterInfo.getBackend(1L);
                minTimes = 0;
                result = backend;
            }
        };

        new MockUp<GlobalStateMgr>() {
            @Mock
            SystemInfoService getCurrentSystemInfo() {
                return clusterInfo;
            }

            @Mock
            StarOSAgent getStarOSAgent() {
                return starosAgent;
            }
        };

        new MockUp<SystemInfoService>() {
            @Mock
            List<Long> getBackendIds(boolean needAlive) {
                List<Long> backends = Lists.newArrayList();
                backends.add(1L);
                return backends;
            }
        };

        new MockUp<StarOSAgent>() {
            @Mock
            long getWorkerIdByBackendId(long backendId) {
                return 5;
            }
        };

        Config.integrate_starmgr = true;
        ShowBackendsStmt stmt = new ShowBackendsStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertEquals(28, resultSet.getMetaData().getColumnCount());
        Assert.assertEquals("BackendId", resultSet.getMetaData().getColumn(0).getName());
        Assert.assertEquals("NumRunningQueries", resultSet.getMetaData().getColumn(23).getName());
        Assert.assertEquals("MemUsedPct", resultSet.getMetaData().getColumn(24).getName());
        Assert.assertEquals("CpuUsedPct", resultSet.getMetaData().getColumn(25).getName());
        Assert.assertEquals("StarletPort", resultSet.getMetaData().getColumn(26).getName());
        Assert.assertEquals("WorkerId", resultSet.getMetaData().getColumn(27).getName());

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("1", resultSet.getString(0));
        Assert.assertEquals("0", resultSet.getString(23));
        Assert.assertEquals("5", resultSet.getString(27));

        Config.integrate_starmgr = false;
    }

    @Test
    public void testShowAuthors() throws AnalysisException, DdlException {
        ShowAuthorStmt stmt = new ShowAuthorStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertEquals(3, resultSet.getMetaData().getColumnCount());
        Assert.assertEquals("Name", resultSet.getMetaData().getColumn(0).getName());
        Assert.assertEquals("Location", resultSet.getMetaData().getColumn(1).getName());
        Assert.assertEquals("Comment", resultSet.getMetaData().getColumn(2).getName());
    }

    @Test
    public void testShowEngine() throws AnalysisException, DdlException {
        ShowEnginesStmt stmt = new ShowEnginesStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("OLAP", resultSet.getString(0));
    }

    @Test
    public void testShowUser() throws AnalysisException, DdlException {
        ctx.setQualifiedUser("root");
        ShowUserStmt stmt = new ShowUserStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("root", resultSet.getString(0));
    }

    @Test
    public void testShowCharset() throws DdlException, AnalysisException {
        // Dbeaver 23 Use
        ShowCharsetStmt stmt = new ShowCharsetStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertTrue(resultSet.next());
        List<List<String>> resultRows = resultSet.getResultRows();
        Assert.assertTrue(resultRows.size() >= 1);
        Assert.assertEquals(resultRows.get(0).get(0), "utf8");
    }

    @Test
    public void testShowEmpty() throws AnalysisException, DdlException {
        ShowProcedureStmt stmt = new ShowProcedureStmt();
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testHelp() throws AnalysisException, IOException, UserException {
        HelpModule module = new HelpModule();
        URL help = getClass().getClassLoader().getResource("test-help-resource-show-help.zip");
        module.setUpByZip(help.getPath());
        new Expectations(module) {
            {
                HelpModule.getInstance();
                minTimes = 0;
                result = module;
            }
        };

        // topic
        HelpStmt stmt = new HelpStmt("ADD");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("ADD", resultSet.getString(0));
        Assert.assertEquals("add function\n", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());

        // topic
        stmt = new HelpStmt("logical");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("OR", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // keywords
        stmt = new HelpStmt("MATH");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("ADD", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("MINUS", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // category
        stmt = new HelpStmt("functions");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("HELP", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("binary function", resultSet.getString(0));
        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("bit function", resultSet.getString(0));
        Assert.assertFalse(resultSet.next());

        // empty
        stmt = new HelpStmt("empty");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();

        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowMaterializedView() throws AnalysisException, DdlException {
        ShowMaterializedViewStmt stmt = new ShowMaterializedViewStmt("testDb", (String) null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        verifyShowMaterializedViewResult(resultSet);
    }

    @Test
    public void testShowMaterializedViewFromUnknownDatabase() throws DdlException, AnalysisException {
        ShowMaterializedViewStmt stmt = new ShowMaterializedViewStmt("emptyDb", (String) null);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        expectedEx.expect(AnalysisException.class);
        expectedEx.expectMessage("Unknown database 'emptyDb'");
        executor.execute();
    }

    @Test
    public void testShowMaterializedViewPattern() throws AnalysisException, DdlException {
        ShowMaterializedViewStmt stmt = new ShowMaterializedViewStmt("testDb", "bcd%");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertFalse(resultSet.next());

        stmt = new ShowMaterializedViewStmt("testDb", "%test%");
        executor = new ShowExecutor(ctx, stmt);
        resultSet = executor.execute();
        verifyShowMaterializedViewResult(resultSet);
    }

    private void verifyShowMaterializedViewResult(ShowResultSet resultSet) throws AnalysisException, DdlException {
        String expectedSqlText = "CREATE MATERIALIZED VIEW `testMv` (`col1`, `col2`)\n" +
                "COMMENT \"TEST MATERIALIZED VIEW\"\n" +
                "PARTITION BY (`col1`)\n" +
                "DISTRIBUTED BY HASH(`col1`) BUCKETS 10 \n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"replication_num\" = \"1\",\n" +
                "\"storage_medium\" = \"SSD\",\n" +
                "\"storage_cooldown_time\" = \"1970-01-01 08:00:00\"\n" +
                ")\n" +
                "AS select col1, col2 from table1;";

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("1000", resultSet.getString(0));
        Assert.assertEquals("testMv", resultSet.getString(1));
        Assert.assertEquals("testDb", resultSet.getString(2));
        Assert.assertEquals(expectedSqlText, resultSet.getString(3));
        Assert.assertEquals("10", resultSet.getString(4));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowRoutineLoadNonExisted() throws AnalysisException, DdlException {
        ShowRoutineLoadStmt stmt = new ShowRoutineLoadStmt(new LabelName("testDb", "non-existed-job-name"), false);
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        // AnalysisException("There is no job named...") is expected.
        Assert.assertThrows(AnalysisException.class, () -> executor.execute());
    }

    @Test
    public void testShowCreateExternalCatalogTable() throws DdlException, AnalysisException {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(String catalogName, String dbName) {
                return new Database();
            }
            @Mock
            public Table getTable(String catalogName, String dbName, String tblName) {
                List<Column> fullSchema = new ArrayList<>();
                Column columnId = new Column("id", Type.INT);
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
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();
        Assert.assertEquals("test_table", resultSet.getResultRows().get(0).get(0));
        Assert.assertEquals("CREATE TABLE `test_table` (\n" +
                "  `id` int(11) DEFAULT NULL,\n" +
                "  `name` varchar(1048576) DEFAULT NULL,\n" +
                "  `year` int(11) DEFAULT NULL,\n" +
                "  `dt` int(11) DEFAULT NULL\n" +
                ")\n" +
                "WITH (\n" +
                " partitioned_by = ARRAY [ year, dt ]\n" +
                ")\n" +
                "LOCATION 'hdfs://hadoop/hive/warehouse/test.db/test'", resultSet.getResultRows().get(0).get(1));
    }


    @Test
    public void testShowTablesFromExternalCatalog() throws AnalysisException, DdlException {
        new MockUp<MetadataMgr>() {
            @Mock
            public Database getDb(String catalogName, String dbName) {
                return new Database();
            }

            @Mock
            public List<String> listTableNames(String catalogName, String dbName) {
                List<String> tableNames = Lists.newArrayList();
                tableNames.add("hive_test");
                return tableNames;
            }
        };

        ShowTableStmt stmt = new ShowTableStmt("test", true, null, null, "hive_catalog");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertTrue(resultSet.next());
        Assert.assertEquals("hive_test", resultSet.getString(0));
        Assert.assertEquals("BASE TABLE", resultSet.getString(1));
        Assert.assertFalse(resultSet.next());
    }

    @Test
    public void testShowCreateExternalCatalogWithMask() throws AnalysisException, DdlException {
        new MockUp<CatalogMgr>() {
            @Mock
            public Catalog getCatalogByName(String name) {
                Map<String, String> properties = new HashMap<>();
                properties.put("hive.metastore.uris", "thrift://hadoop:9083");
                properties.put("type", "hive");
                properties.put("aws.s3.access_key", "iam_user_access_key");
                properties.put("aws.s3.secret_key", "iam_user_secret_key");
                Catalog catalog = new Catalog(1, "test_hive", properties, "hive_test");
                return catalog;
            }
        };
        ShowCreateExternalCatalogStmt stmt = new ShowCreateExternalCatalogStmt("test_hive");
        ShowExecutor executor = new ShowExecutor(ctx, stmt);
        ShowResultSet resultSet = executor.execute();

        Assert.assertEquals("test_hive", resultSet.getResultRows().get(0).get(0));
        Assert.assertEquals("CREATE EXTERNAL CATALOG `test_hive`\n" +
                "comment \"hive_test\"\n" +
                "PROPERTIES (\"aws.s3.access_key\"  =  \"ia******ey\",\n" +
                "\"aws.s3.secret_key\"  =  \"ia******ey\",\n" +
                "\"hive.metastore.uris\"  =  \"thrift://hadoop:9083\",\n" +
                "\"type\"  =  \"hive\"\n" +
                ")", resultSet.getResultRows().get(0).get(1));
    }
}
