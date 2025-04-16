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
import com.starrocks.catalog.ArrayType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.Type;
import com.starrocks.common.FeConstants;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.sql.ast.InsertStmt;
import com.starrocks.sql.ast.SelectRelation;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.UnionRelation;
import com.starrocks.sql.ast.ValuesRelation;
import com.starrocks.sql.parser.SqlParser;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.utframe.StarRocksAssert;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

public class SampleInfoTest extends PlanTestBase {

    private static Database db;

    private static Table table;

    private static TabletSampleManager tabletSampleManager;


    @BeforeClass
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
                "c6 struct<a int, b int, c struct<a int, b int>, d array<int>>) " +
                "duplicate key(c0) distributed by hash(c0) buckets 1 " +
                "properties('replication_num'='1');");
        db = GlobalStateMgr.getCurrentState().getLocalMetastore().getDb("test");
        table = GlobalStateMgr.getCurrentState().getLocalMetastore().getTable("test", "t_struct");
        tabletSampleManager = TabletSampleManager.init(Maps.newHashMap(), table);
    }

    @Test
    public void generateComplexTypeColumnTask() {
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo);

        String complexSql = sampleInfo.generateComplexTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnSampleManager.getComplexTypeStats());
        List<StatementBase> stmt = SqlParser.parse(complexSql, connectContext.getSessionVariable());
        Assert.assertTrue(stmt.get(0) instanceof InsertStmt);
        InsertStmt insertStmt = (InsertStmt) stmt.get(0);
        Assert.assertTrue(insertStmt.getQueryStatement().getQueryRelation() instanceof ValuesRelation);
        ValuesRelation valuesRelation = (ValuesRelation) insertStmt.getQueryStatement().getQueryRelation();
        Assert.assertTrue(valuesRelation.getRows().size() == 3);
        Assert.assertTrue(valuesRelation.getRows().get(0).size() == 12);
    }

    @Test
    public void generatePrimitiveTypeColumnTask() {
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toList());
        List<Type> columnTypes = table.getColumns().stream().map(Column::getType).collect(Collectors.toList());
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo);
        List<List<ColumnStats>> columnStatsBatch = columnSampleManager.splitPrimitiveTypeStats();
        String primitiveSql = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnStatsBatch.get(0), tabletSampleManager);
        List<StatementBase> stmt = SqlParser.parse(primitiveSql, connectContext.getSessionVariable());
        Assert.assertTrue(stmt.get(0) instanceof InsertStmt);
        InsertStmt insertStmt = (InsertStmt) stmt.get(0);
        Assert.assertTrue(insertStmt.getQueryStatement().getQueryRelation() instanceof UnionRelation);
        UnionRelation unionRelation = (UnionRelation) insertStmt.getQueryStatement().getQueryRelation();
        Assert.assertEquals(2, unionRelation.getRelations().size());
        Assert.assertTrue(unionRelation.getRelations().get(0) instanceof SelectRelation);
        SelectRelation selectRelation = (SelectRelation) unionRelation.getRelations().get(0);
        Assert.assertTrue(selectRelation.getSelectList().getItems().size() == 12);
    }

    @Test
    public void generateSubFieldTypeColumnTask() {
        SampleInfo sampleInfo = tabletSampleManager.generateSampleInfo();
        List<String> columnNames = Lists.newArrayList("c1", "c4.b", "c6.c.b");
        List<Type> columnTypes = Lists.newArrayList(Type.DATE, new ArrayType(Type.ANY_STRUCT), Type.INT);
        ColumnSampleManager columnSampleManager = ColumnSampleManager.init(columnNames, columnTypes, table,
                sampleInfo);
        List<List<ColumnStats>> columnStatsBatch = columnSampleManager.splitPrimitiveTypeStats();
        String sql = sampleInfo.generatePrimitiveTypeColumnTask(table.getId(), db.getId(), table.getName(),
                db.getFullName(), columnStatsBatch.get(0), tabletSampleManager);

        List<StatementBase> stmt = SqlParser.parse(sql, connectContext.getSessionVariable());
        Assert.assertTrue(stmt.get(0) instanceof InsertStmt);
        InsertStmt insertStmt = (InsertStmt) stmt.get(0);
        Assert.assertTrue(insertStmt.getQueryStatement().getQueryRelation() instanceof UnionRelation);
        UnionRelation unionRelation = (UnionRelation) insertStmt.getQueryStatement().getQueryRelation();
        Assert.assertTrue(unionRelation.getRelations().size() == 2);
    }


    @AfterClass
    public static void afterClass() {
        FeConstants.runningUnitTest = false;
    }
}