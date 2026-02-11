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

package com.starrocks.statistic.sample;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.common.FeConstants;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.statistic.StatisticsMetaManager;
import com.starrocks.statistic.StatsConstants;
import com.starrocks.type.AnyStructType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.DateType;
import com.starrocks.type.DecimalType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SampleInfoTest extends PlanTestBase {

    private static Database db;

    private static Table table;

    private static TabletSampleManager tabletSampleManager;


    @BeforeAll
    public static void beforeClass() throws Exception {
        PlanTestBase.beforeClass();
        StarRocksAssert starRocksAssert = new StarRocksAssert(connectContext);
        FeConstants.runningUnitTest = true;
        starRocksAssert.withTable("create table t_struct(c0 INT, " +
                "c1 date," +
                "c2 varchar(255)," +
                "c3 decimal(10, 2)," +
                "c4 struct<a int, b array<struct<a int, b int>>>," +
                "c5 struct<a int, b int>," +
                "c6 struct<a int, b int, c struct<a int, b int>, d array<int>>," +
                "carr array<int>, " +
                "carr2 array<int>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t_struct");
        tabletSampleManager = TabletSampleManager.init(Maps.newHashMap(), table);

        StatisticsMetaManager m = new StatisticsMetaManager();
        m.createStatisticsTablesForTest();
    }

    @Test
    public void generateComplexTypeColumnTask() {
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo, Map.of());

        String complexSql = sampleInfo.generateComplexTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnSampleManager.getComplexTypeStats());
        List<StatementBase> stmt = SqlParser.parse(complexSql, connectContext.getSessionVariable());
        Assertions.assertTrue(stmt.get(0) instanceof InsertStmt);
        InsertStmt insertStmt = (InsertStmt) stmt.get(0);
        Assertions.assertTrue(insertStmt.getQueryStatement().getQueryRelation() instanceof ValuesRelation);
        ValuesRelation valuesRelation = (ValuesRelation) insertStmt.getQueryStatement().getQueryRelation();
        Assertions.assertTrue(valuesRelation.getRows().size() == 5);
        Assertions.assertTrue(valuesRelation.getRows().get(0).size() == 12);
    }

    @Test
    public void generatePrimitiveTypeColumnTask() {
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo, Map.of());
        List<List<ColumnStats>> columnStatsBatch = columnSampleManager.splitPrimitiveTypeStats();
        String primitiveSql = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnStatsBatch.get(0), tabletSampleManager);
        List<StatementBase> stmt = SqlParser.parse(primitiveSql, connectContext.getSessionVariable());
        Assertions.assertTrue(stmt.get(0) instanceof InsertStmt);
        InsertStmt insertStmt = (InsertStmt) stmt.get(0);
        Assertions.assertTrue(insertStmt.getQueryStatement().getQueryRelation() instanceof UnionRelation);
        UnionRelation unionRelation = (UnionRelation) insertStmt.getQueryStatement().getQueryRelation();
        Assertions.assertEquals(2, unionRelation.getRelations().size());
        Assertions.assertTrue(unionRelation.getRelations().get(0) instanceof SelectRelation);
        SelectRelation selectRelation = (SelectRelation) unionRelation.getRelations().get(0);
        Assertions.assertTrue(selectRelation.getSelectList().getItems().size() == 12);
    }

    @Test
    public void generateSubFieldTypeColumnTask() {
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = Lists.newArrayList("c1", "c4.b", "c6.c.b");
        List<Type> columnTypes = Lists.newArrayList(DateType.DATE,
                new ArrayType(AnyStructType.ANY_STRUCT), IntegerType.INT);
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo, Map.of());
        List<List<ColumnStats>> columnStatsBatch = columnSampleManager.splitPrimitiveTypeStats();
        String sql = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnStatsBatch.get(0), tabletSampleManager);

        List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());
        Assertions.assertTrue(stmt.get(0) instanceof InsertStmt);
        InsertStmt insertStmt = (InsertStmt) stmt.get(0);
        Assertions.assertTrue(insertStmt.getQueryStatement().getQueryRelation() instanceof UnionRelation);
        UnionRelation unionRelation = (UnionRelation) insertStmt.getQueryStatement().getQueryRelation();
        Assertions.assertTrue(unionRelation.getRelations().size() == 2);
    }

    @Test
    public void testVirtualUnnestStatistics() throws Exception {
        // GIVEN
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = Lists.newArrayList("carr");
        List<Type> columnTypes = Lists.newArrayList(ArrayType.ARRAY_INT);
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo, Map.of(StatsConstants.UNNEST_VIRTUAL_STATISTICS, "true"));
        List<List<ColumnStats>> columnStatsBatch = columnSampleManager.splitPrimitiveTypeStats();

        // WHEN
        String sql = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnStatsBatch.get(0), tabletSampleManager);

        // THEN
        Assertions.assertTrue(sql.contains("unnest(`carr`)"));
        Assertions.assertNotNull(getFragmentPlan(sql));
    }

    @Test
    public void testMultipleVirtualUnnestStatistics() throws Exception {
        // GIVEN
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = Lists.newArrayList("carr", "carr2", "c3");
        List<Type> columnTypes = Lists.newArrayList(ArrayType.ARRAY_INT, ArrayType.ARRAY_INT, DecimalType.DECIMAL32);
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo, Map.of(StatsConstants.UNNEST_VIRTUAL_STATISTICS, "true"));

        List<List<ColumnStats>> columnStatsBatch;
        try {
            ConnectContext.get().getSessionVariable().setStatisticCollectParallelism(12);
            columnStatsBatch = columnSampleManager.splitPrimitiveTypeStats();
        } finally {
            ConnectContext.get().getSessionVariable().setStatisticCollectParallelism(1);

        }

        // WHEN
        String sql = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnStatsBatch.get(0), tabletSampleManager);

        // THEN
        // Unnest stats
        Assertions.assertTrue(sql.contains("unnest(`carr`)"));
        Assertions.assertTrue(sql.contains("unnest(`carr2`)"));
        // Normal stats
        Assertions.assertTrue(sql.contains("carr"));
        Assertions.assertTrue(sql.contains("carr2"));
        Assertions.assertTrue(sql.contains("c3"));

        Assertions.assertNotNull(getFragmentPlan(sql));
    }

    @Test
    public void testVirtualUnnestStatisticsWithTabletHints() throws Exception {
        // GIVEN
        final var partition = table.getPartitions().stream().findFirst().get().getDefaultPhysicalPartition();
        final var tablet = partition.getLatestBaseIndex().getTablets().get(0);

        // Add some more tablet stats to force tablet hints
        final var dummyTabletStats = new TabletStats(tablet.getId(), partition.getId(), 100_000_000);
        final var sampleInfo = new SampleInfo(
                0.5, 50_000_000, 50_000_000,
                List.of(dummyTabletStats), List.of(dummyTabletStats), List.of(dummyTabletStats), List.of(dummyTabletStats));
        List<String> columnNames = Lists.newArrayList("carr");
        List<Type> columnTypes = Lists.newArrayList(ArrayType.ARRAY_INT);
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo, Map.of(StatsConstants.UNNEST_VIRTUAL_STATISTICS, "true"));
        List<List<ColumnStats>> columnStatsBatch = columnSampleManager.splitPrimitiveTypeStats();

        // WHEN
        String sql = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnStatsBatch.get(0), tabletSampleManager);

        // THEN
        Assertions.assertTrue(sql.contains("unnest(`carr`)"));
        Assertions.assertNotNull(getFragmentPlan(sql));
    }

    @AfterAll
    public static void afterClass() {
        FeConstants.runningUnitTest = false;
    }
}