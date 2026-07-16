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

package com.starrocks.sql.analyzer;

import com.google.common.collect.Sets;
import com.starrocks.catalog.InternalCatalog;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.Table;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.lake.LakeMaterializedView;
import com.starrocks.lake.LakeTable;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.RunMode;
import com.starrocks.sql.ast.QueryStatement;
import com.starrocks.sql.ast.StatementBase;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.operator.ColumnFilterConverter;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.type.ArrayType;
import com.starrocks.type.MapType;
import com.starrocks.type.NullType;
import com.starrocks.type.ScalarType;
import com.starrocks.type.StructField;
import com.starrocks.type.StructType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import com.starrocks.utframe.StarRocksAssert;
import com.starrocks.utframe.UtFrameUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class AnalyzerUtilsTest {

    private static ConnectContext connectContext;
    private static StarRocksAssert starRocksAssert;

    @BeforeAll
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        Config.dynamic_partition_enable = false;
        UtFrameUtils.createMinStarRocksCluster(RunMode.SHARED_DATA);
        // create connect context
        connectContext = UtFrameUtils.createDefaultCtx();
        starRocksAssert = new StarRocksAssert(connectContext);

        starRocksAssert.withDatabase("test").useDatabase("test")
                .withTable("CREATE TABLE `bill_detail` (\n" +
                        "  `bill_code` varchar(200) NOT NULL DEFAULT \"\" COMMENT \"\"\n" +
                        ") ENGINE=OLAP \n" +
                        "PRIMARY KEY(`bill_code`)\n" +
                        "COMMENT \"OLAP\"\n" +
                        "DISTRIBUTED BY HASH(`bill_code`) BUCKETS 10 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE `relation_src` (\n" +
                        "  `k1` int NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`k1`)\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withTable("CREATE TABLE `relation_target` (\n" +
                        "  `k1` int NOT NULL\n" +
                        ") ENGINE=OLAP \n" +
                        "DUPLICATE KEY(`k1`)\n" +
                        "DISTRIBUTED BY HASH(`k1`) BUCKETS 1 \n" +
                        "PROPERTIES (\n" +
                        "\"replication_num\" = \"1\"\n" +
                        ");")
                .withView("CREATE VIEW relation_view AS SELECT k1 FROM relation_src;")
                .withMaterializedView("CREATE MATERIALIZED VIEW mv1 REFRESH ASYNC AS " +
                        "SELECT * FROM bill_detail;");
    }

    @Test
    public void testGetFormatPartitionValue() {
        Assertions.assertEquals("_11", AnalyzerUtils.getFormatPartitionValue("-11"));
        Assertions.assertEquals("20200101", AnalyzerUtils.getFormatPartitionValue("2020-01-01"));
        Assertions.assertEquals("676d5dde", AnalyzerUtils.getFormatPartitionValue("杭州"));
    }

    @Test
    public void testRewritePredicate() throws Exception {
        String sql = "select cast(substr(bill_code, 3, 13) as bigint) from bill_detail;";
        ConnectContext ctx = starRocksAssert.getCtx();
        QueryStatement queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(sql, ctx);
        Expr expr = queryStatement.getQueryRelation().getOutputExpression().get(0);
        ColumnRefOperator columnRefOperator = new ColumnRefOperator(1, VarcharType.VARCHAR, "bill_code", false);
        ConstantOperator constantOperator = new ConstantOperator("JT2921712368984", VarcharType.VARCHAR);
        boolean success = ColumnFilterConverter.rewritePredicate(expr, columnRefOperator, constantOperator);
        Assertions.assertTrue(success);
        Expr shouldReplaceExpr = expr.getChild(0).getChild(0);
        Assertions.assertTrue(shouldReplaceExpr instanceof StringLiteral);
    }

    @Test
    public void testConvertCatalogStringToInferenceLength() {
        ScalarType catalogString = TypeFactory.createDefaultCatalogString();
        ScalarType convertedString = (ScalarType) AnalyzerUtils.transformTableColumnType(catalogString);
        Assertions.assertEquals(TypeFactory.getOlapVarcharInferenceLength(), convertedString.getLength());
    }

    @Test
    public void testTransformVarcharPreferStringFalseKeepsDeclaredLength() {
        ScalarType narrow = TypeFactory.createVarcharType(64);
        ScalarType result = (ScalarType) AnalyzerUtils.transformTableColumnType(narrow, true, false);
        Assertions.assertEquals(64, result.getLength());
    }

    @Test
    public void testTransformDeclaredStringLengthBoundariesIgnoreOlapMax() {
        int savedMaxVarcharLength = Config.max_varchar_length;
        int inferenceLength = TypeFactory.getOlapVarcharInferenceLength();
        try {
            Config.max_varchar_length = Integer.MAX_VALUE - 1;

            int[] declaredLengths = {1, inferenceLength - 1, inferenceLength,
                    inferenceLength + 1, Integer.MAX_VALUE - 1};
            for (int declaredLength : declaredLengths) {
                ScalarType varchar = TypeFactory.createVarcharType(declaredLength);
                ScalarType preserved = (ScalarType) AnalyzerUtils.transformTableColumnType(varchar, true, false);
                Assertions.assertEquals(Math.min(declaredLength, inferenceLength), preserved.getLength(),
                        "declared VARCHAR length " + declaredLength);

                ScalarType preferred = (ScalarType) AnalyzerUtils.transformTableColumnType(varchar, true, true);
                Assertions.assertEquals(inferenceLength, preferred.getLength(),
                        "preferred VARCHAR length " + declaredLength);
            }

            ScalarType wideChar = TypeFactory.createCharType(inferenceLength + 1);
            ScalarType convertedChar = (ScalarType) AnalyzerUtils.transformTableColumnType(wideChar, true, false);
            Assertions.assertTrue(convertedChar.isVarchar());
            Assertions.assertEquals(inferenceLength, convertedChar.getLength());
        } finally {
            Config.max_varchar_length = savedMaxVarcharLength;
        }
    }

    @Test
    public void testTransformVarcharPreferStringTrueUsesInferenceLength() {
        ScalarType narrow = TypeFactory.createVarcharType(64);
        ScalarType result = (ScalarType) AnalyzerUtils.transformTableColumnType(narrow, true, true);
        Assertions.assertEquals(TypeFactory.getOlapVarcharInferenceLength(), result.getLength());
    }

    @Test
    public void testTransformUnboundedVarcharWidensRegardlessOfPreferString() {
        ScalarType unbounded = TypeFactory.createVarcharType(-1);
        ScalarType keepLen = (ScalarType) AnalyzerUtils.transformTableColumnType(unbounded, true, false);
        Assertions.assertEquals(TypeFactory.getOlapVarcharInferenceLength(), keepLen.getLength());
        ScalarType preferStr = (ScalarType) AnalyzerUtils.transformTableColumnType(unbounded, true, true);
        Assertions.assertEquals(TypeFactory.getOlapVarcharInferenceLength(), preferStr.getLength());

        ScalarType wildcardChar = TypeFactory.createCharType(-1);
        ScalarType convertedChar = (ScalarType) AnalyzerUtils.transformTableColumnType(wildcardChar, true, false);
        Assertions.assertTrue(convertedChar.isVarchar());
        Assertions.assertEquals(TypeFactory.getOlapVarcharInferenceLength(), convertedChar.getLength());

        ScalarType convertedNull = (ScalarType) AnalyzerUtils.transformTableColumnType(NullType.NULL, true, false);
        Assertions.assertTrue(convertedNull.isVarchar());
        Assertions.assertEquals(TypeFactory.getOlapVarcharInferenceLength(), convertedNull.getLength());
    }

    @Test
    public void testTransformNestedStringTypesRecursively() {
        int inferenceLength = TypeFactory.getOlapVarcharInferenceLength();
        Type nested = new StructType(List.of(
                new StructField("array_field",
                        new ArrayType(TypeFactory.createVarcharType(inferenceLength + 1))),
                new StructField("map_field",
                        new MapType(TypeFactory.createVarcharType(8), TypeFactory.createVarcharType(-1)))), true);

        StructType transformed = (StructType) AnalyzerUtils.transformTableColumnType(nested, true, false);
        ArrayType array = (ArrayType) transformed.getField("array_field").getType();
        Assertions.assertEquals(inferenceLength, ((ScalarType) array.getItemType()).getLength());

        MapType map = (MapType) transformed.getField("map_field").getType();
        Assertions.assertEquals(8, ((ScalarType) map.getKeyType()).getLength());
        Assertions.assertEquals(inferenceLength, ((ScalarType) map.getValueType()).getLength());
    }

    @Test
    public void testTransformTwoArgOverloadFollowsConfigFlag() {
        ScalarType narrow = TypeFactory.createVarcharType(64);
        boolean saved = Config.transform_type_prefer_string_for_varchar;
        try {
            Config.transform_type_prefer_string_for_varchar = true;
            ScalarType widened = (ScalarType) AnalyzerUtils.transformTableColumnType(narrow, true);
            Assertions.assertEquals(TypeFactory.getOlapVarcharInferenceLength(), widened.getLength());

            Config.transform_type_prefer_string_for_varchar = false;
            ScalarType preserved = (ScalarType) AnalyzerUtils.transformTableColumnType(narrow, true);
            Assertions.assertEquals(64, preserved.getLength());
        } finally {
            Config.transform_type_prefer_string_for_varchar = saved;
        }
    }

    @Test
    public void testCopyOlapTable() throws Exception {
        {
            String sql = "select count(*) from bill_detail;";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            Set<OlapTable> tables = Sets.newHashSet();
            AnalyzerUtils.copyOlapTable(stmt, tables);
            Assertions.assertEquals(1, tables.size());
            Assertions.assertInstanceOf(LakeTable.class, tables.iterator().next());
            Assertions.assertSame(starRocksAssert.getTable("test", "bill_detail"), tables.iterator().next());
            Assertions.assertTrue(AnalyzerUtils.areTablesCopySafe(stmt));
        }

        {
            String sql = "select count(*) from mv1;";
            StatementBase stmt = UtFrameUtils.parseStmtWithNewParser(sql, starRocksAssert.getCtx());
            Set<OlapTable> tables = Sets.newHashSet();
            AnalyzerUtils.copyOlapTable(stmt, tables);
            Assertions.assertEquals(1, tables.size());
            Assertions.assertInstanceOf(LakeMaterializedView.class, tables.iterator().next());
            Assertions.assertSame(starRocksAssert.getTable("test", "mv1"), tables.iterator().next());
            Assertions.assertTrue(AnalyzerUtils.areTablesCopySafe(stmt));
        }
    }

    @Test
    public void testCollectAllTableAndViewRelationNames() throws Exception {
        StatementBase insertStmt = UtFrameUtils.parseStmtWithNewParser(
                "insert into relation_target " + "select src.k1 from relation_view rv " +
                        "join relation_src src on rv.k1 = src.k1 " + "join relation_view rv2 on rv2.k1 = src.k1",
                starRocksAssert.getCtx());
        List<String> relationNames = AnalyzerUtils.collectAllTableAndViewRelationNamesForAudit(insertStmt);
        Assertions.assertEquals(Arrays.asList(qualifiedRelationName("test", "relation_view"),
                qualifiedRelationName("test", "relation_src")), relationNames);

        StatementBase cteStmt = UtFrameUtils.parseStmtWithNewParser("with cte as (select k1 from relation_src) " +
                "select cte.k1 from cte join relation_view rv on cte.k1 = rv.k1", starRocksAssert.getCtx());
        Assertions.assertEquals(Arrays.asList(qualifiedRelationName("test", "relation_src"),
                        qualifiedRelationName("test", "relation_view")),
                AnalyzerUtils.collectAllTableAndViewRelationNamesForAudit(cteStmt));
    }

    @Test
    public void testCollectAllConnectorTableAndViewWithViewDefinitions() throws Exception {
        starRocksAssert.withView("CREATE VIEW relation_nested_view AS SELECT k1 FROM relation_view;");
        try {
            QueryStatement queryStatement = (QueryStatement) UtFrameUtils.parseStmtWithNewParser(
                    "SELECT k1 FROM relation_nested_view", starRocksAssert.getCtx());
            Set<String> tableNames = AnalyzerUtils.collectAllConnectorTableAndViewWithViewDefinition(queryStatement)
                    .values().stream()
                    .map(Table::getName)
                    .collect(Collectors.toSet());

            Assertions.assertEquals(Sets.newHashSet("relation_src", "relation_view", "relation_nested_view"),
                    tableNames);
        } finally {
            starRocksAssert.dropView("relation_nested_view");
        }
    }

    private String qualifiedRelationName(String dbName, String tableName) {
        return String.format("%s.%s.%s", InternalCatalog.DEFAULT_INTERNAL_CATALOG_NAME, dbName, tableName);
    }
}
