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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AccessTestUtil;
import com.starrocks.authorization.PrivilegeBuiltinConstants;
import com.starrocks.catalog.BaseTableInfo;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.ExpressionRangePartitionInfo;
import com.starrocks.catalog.HashDistributionInfo;
import com.starrocks.catalog.KeysType;
import com.starrocks.catalog.ListPartitionInfoTest;
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
import com.starrocks.catalog.system.information.MaterializedViewsSystemTable;
import com.starrocks.common.AnalysisException;
import com.starrocks.common.DdlException;
import com.starrocks.common.FeConstants;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.common.proc.OptimizeProcDir;
import com.starrocks.mysql.MysqlCommand;
import com.starrocks.persist.ColumnIdExpr;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.server.LocalMetastore;
import com.starrocks.server.MetadataMgr;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.ShowAlterStmt;
import com.starrocks.sql.ast.ShowColumnStmt;
import com.starrocks.sql.ast.ShowCreateDbStmt;
import com.starrocks.sql.ast.ShowIndexStmt;
import com.starrocks.sql.ast.ShowMaterializedViewsStmt;
import com.starrocks.sql.ast.ShowPartitionsStmt;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.TableName;
import com.starrocks.sql.common.MetaUtils;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TStorageType;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sparkproject.guava.collect.Maps;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.starrocks.common.util.PropertyAnalyzer.PROPERTIES_STORAGE_COOLDOWN_TIME;
import static com.starrocks.thrift.TStorageMedium.SSD;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ShowExecutorTest {
    private ConnectContext ctx;
    private GlobalStateMgr globalStateMgr;

    @Mocked
    MetadataMgr metadataMgr;

    @BeforeAll
    public static void beforeClass() {
        FeConstants.runningUnitTest = true;
    }

    @BeforeEach
    public void setUp() throws Exception {
        ctx = new ConnectContext(null);
        ctx.setCommand(MysqlCommand.COM_SLEEP);

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
        OlapTable table = new OlapTable();
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

                mv.getOrderedOutputColumns(anyBoolean);
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
        Database db = new Database();
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
        globalStateMgr = Deencapsulation.newInstance(GlobalStateMgr.class);
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
    public void testShowPartitions() throws StarRocksException {

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

        /*
        new Expectations(db) {
            {
                db.getTable(anyString);
                minTimes = 0;
                result = olapTable;

                db.getTable(1000);
                minTimes = 1;
                result = olapTable;
            }
        };

         */

        new MockUp<MetaUtils>() {
            @Mock
            public Table getSessionAwareTable(ConnectContext ctx, Database db, TableName tableName) {
                return olapTable;
            }
        };

        new Expectations() {
            {
                globalStateMgr.getLocalMetastore().getDb(0);
                minTimes = 0;
                result = db;

                globalStateMgr.getLocalMetastore().getTable(anyLong, anyLong);
                minTimes = 0;
                result = olapTable;
            }
        };

        // Ok to test
        ShowPartitionsStmt stmt = new ShowPartitionsStmt(new TableName("testDb", "testTbl"),
                null, null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        // Ready to Assert
        String partitionKeyTitle = resultSet.getMetaData().getColumn(6).getName();
        Assertions.assertEquals(partitionKeyTitle, "PartitionKey");
        String valuesTitle = resultSet.getMetaData().getColumn(7).getName();
        Assertions.assertEquals(valuesTitle, "List");

        String partitionKey1 = resultSet.getResultRows().get(0).get(6);
        Assertions.assertEquals(partitionKey1, "dt, province");
        String partitionKey2 = resultSet.getResultRows().get(1).get(6);
        Assertions.assertEquals(partitionKey2, "dt, province");

        String values1 = resultSet.getResultRows().get(0).get(7);
        Assertions.assertEquals(values1, "[[\"2022-04-15\",\"guangdong\"],[\"2022-04-15\",\"tianjin\"]]");
        String values2 = resultSet.getResultRows().get(1).get(7);
        Assertions.assertEquals(values2, "[[\"2022-04-16\",\"shanghai\"],[\"2022-04-16\",\"beijing\"]]");
    }

    @Test
    public void testShowCreateDb() throws AnalysisException, DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        ShowCreateDbStmt stmt = new ShowCreateDbStmt("testDb");

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("testDb", resultSet.getString(0));
        Assertions.assertEquals("CREATE DATABASE `testDb`", resultSet.getString(1));
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testShowColumn() throws AnalysisException, DdlException {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");

        ShowColumnStmt stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show columns from testTbl in testDb",
                ctx.getSessionVariable()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col1", resultSet.getString(0));
        Assertions.assertEquals("NO", resultSet.getString(2));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col2", resultSet.getString(0));
        Assertions.assertFalse(resultSet.next());

        // verbose
        stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show full columns from testTbl in testDb",
                ctx.getSessionVariable()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col1", resultSet.getString(0));
        Assertions.assertEquals("NO", resultSet.getString(3));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col2", resultSet.getString(0));
        Assertions.assertEquals("NO", resultSet.getString(3));
        Assertions.assertFalse(resultSet.next());

        // show full fields
        stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show full fields from testTbl in testDb",
                ctx.getSessionVariable()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col1", resultSet.getString(0));
        Assertions.assertEquals("NO", resultSet.getString(3));
        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col2", resultSet.getString(0));
        Assertions.assertEquals("NO", resultSet.getString(3));
        Assertions.assertFalse(resultSet.next());

        // pattern
        stmt = (ShowColumnStmt) com.starrocks.sql.parser.SqlParser.parse("show full columns from testTbl in testDb like \"%1\"",
                ctx.getSessionVariable().getSqlMode()).get(0);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        resultSet = ShowExecutor.execute(stmt, ctx);

        Assertions.assertTrue(resultSet.next());
        Assertions.assertEquals("col1", resultSet.getString(0));
        Assertions.assertEquals("NO", resultSet.getString(3));
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testShowColumnFromUnknownTable() {
        ctx.setGlobalStateMgr(globalStateMgr);
        ctx.setQualifiedUser("testUser");
        ShowColumnStmt stmt = new ShowColumnStmt(new TableName("emptyDb", "testTable"), null, null, false);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt, ctx);

        Throwable exception = assertThrows(SemanticException.class, () -> ShowExecutor.execute(stmt, ctx));
        assertThat(exception.getMessage(), containsString("Unknown database 'emptyDb'"));

        // empty table
        ShowColumnStmt stmt2 = new ShowColumnStmt(new TableName("testDb", "emptyTable"), null, null, true);
        com.starrocks.sql.analyzer.Analyzer.analyze(stmt2, ctx);
        ShowExecutor.execute(stmt2, ctx);
    }

    @Test
    public void testShowMaterializedView() throws AnalysisException, DdlException {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("default_catalog", "testDb", (String) null);

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        verifyShowMaterializedViewResult(resultSet);
    }

    @Test
    public void testShowMaterializedViewPattern() throws AnalysisException, DdlException {
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.setCurrentRoleIds(Sets.newHashSet(PrivilegeBuiltinConstants.ROOT_ROLE_ID));

        ShowMaterializedViewsStmt stmt = new ShowMaterializedViewsStmt("default_catalog", "testDb", "bcd%");

        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertFalse(resultSet.next());

        stmt = new ShowMaterializedViewsStmt("default_catalog", "testDb", "%test%");

        resultSet = ShowExecutor.execute(stmt, ctx);
        verifyShowMaterializedViewResult(resultSet);
    }

    private void verifyShowMaterializedViewResult(ShowResultSet resultSet) throws AnalysisException, DdlException {
        String expectedSqlText = "CREATE MATERIALIZED VIEW `testMv` (`col1`, `col2`)\n" +
                "COMMENT \"TEST MATERIALIZED VIEW\"\n" +
                "PARTITION BY (`col1`)\n" +
                "DISTRIBUTED BY HASH(`col1`) BUCKETS 10 \n" +
                "REFRESH ASYNC\n" +
                "PROPERTIES (\n" +
                "\"storage_cooldown_time\" = \"1970-01-01 08:00:00\",\n" +
                "\"storage_medium\" = \"SSD\"\n" +
                ")\n" +
                "AS select col1, col2 from table1;";
        Assertions.assertTrue(resultSet.next());
        List<Column> mvSchemaTable = MaterializedViewsSystemTable.create().getFullSchema();
        Assertions.assertEquals("1000", resultSet.getString(0));
        Assertions.assertEquals("testDb", resultSet.getString(1));
        Assertions.assertEquals("testMv", resultSet.getString(2));
        Assertions.assertEquals("ASYNC", resultSet.getString(3));
        Assertions.assertEquals("true", resultSet.getString(4));
        Assertions.assertEquals("", resultSet.getString(5));
        Assertions.assertEquals("RANGE", resultSet.getString(6));
        Assertions.assertEquals("0", resultSet.getString(7));
        Assertions.assertEquals("", resultSet.getString(8));
        Assertions.assertEquals("\\N", resultSet.getString(9));
        Assertions.assertEquals("\\N", resultSet.getString(10));
        Assertions.assertEquals("0.000", resultSet.getString(11));
        Assertions.assertEquals("", resultSet.getString(12));
        Assertions.assertEquals("false", resultSet.getString(13));
        System.out.println(resultSet.getResultRows());
        for (int i = 14; i < 20; i++) {
            System.out.println(i);
            Assertions.assertEquals("", resultSet.getString(i));
        }
        Assertions.assertEquals("10", resultSet.getString(20));
        Assertions.assertEquals(expectedSqlText, resultSet.getString(21));
        Assertions.assertEquals("", resultSet.getString(22));
        Assertions.assertTrue(resultSet.getString(23).contains("UNKNOWN"));
        Assertions.assertEquals("", resultSet.getString(24));
        Assertions.assertEquals("\\N", resultSet.getString(25));
        Assertions.assertEquals("", resultSet.getString(26));
        Assertions.assertFalse(resultSet.next());
    }

    @Test
    public void testShowAlterTable() throws AnalysisException, DdlException {
        ShowAlterStmt stmt = new ShowAlterStmt(ShowAlterStmt.AlterType.OPTIMIZE, "testDb", null, null, null);
        stmt.setNode(new OptimizeProcDir(globalStateMgr.getSchemaChangeHandler(),
                globalStateMgr.getLocalMetastore().getDb("testDb")));

        ShowExecutor.execute(stmt, ctx);
    }

    @Test
    public void testShowKeysFromTable() {
        ShowIndexStmt stmt = new ShowIndexStmt("test_db",
                new TableName(null, "test_db", "test_table"));
        ShowResultSet resultSet = ShowExecutor.execute(stmt, ctx);
        Assertions.assertEquals(0, resultSet.getResultRows().size());
    }
}
