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

package com.starrocks.connector.jdbc;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class JDBCPushDownSQLBuilderTest {

    private JDBCTable newTable(String name, String jdbcUri, List<Column> columns) throws DdlException {
        Map<String, String> properties = new HashMap<>();
        properties.put(JDBCResource.URI, jdbcUri);
        properties.put(JDBCResource.USER, "user");
        properties.put(JDBCResource.PASSWORD, "password");
        properties.put(JDBCResource.DRIVER_URL, "driver_url");
        properties.put(JDBCResource.CHECK_SUM, "checksum");
        properties.put(JDBCResource.DRIVER_CLASS, "driver_class");
        return new JDBCTable(1, name, columns, properties);
    }

    private LogicalJDBCScanOperator newScan(JDBCTable table, long limit, ScalarOperator predicate,
                                            Map<ColumnRefOperator, Column> columns) {
        Map<Column, ColumnRefOperator> columnToRef = new LinkedHashMap<>();
        columns.forEach((ref, column) -> columnToRef.put(column, ref));
        return new LogicalJDBCScanOperator(table, columns, columnToRef, limit, predicate, null);
    }

    @Test
    public void testBuildJoinQueryWithPerTablePredicate() throws Exception {
        Column a0 = new Column("a", VarcharType.VARCHAR);
        Column c0 = new Column("c", IntegerType.INT);
        Column a1 = new Column("a", VarcharType.VARCHAR);
        Column b1 = new Column("b", VarcharType.VARCHAR);
        JDBCTable table0 = newTable("tbl0", "jdbc:mysql://localhost:3306/db", List.of(a0, c0));
        JDBCTable table1 = newTable("tbl1", "jdbc:mysql://localhost:3306/db", List.of(a1, b1));

        ColumnRefOperator t0a = new ColumnRefOperator(10, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t0c = new ColumnRefOperator(11, IntegerType.INT, "c", true);
        ColumnRefOperator t1a = new ColumnRefOperator(12, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1b = new ColumnRefOperator(13, VarcharType.VARCHAR, "b", true);
        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0a, a0);
        t0Columns.put(t0c, c0);
        Map<ColumnRefOperator, Column> t1Columns = new LinkedHashMap<>();
        t1Columns.put(t1a, a1);
        t1Columns.put(t1b, b1);

        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GT, t0c, ConstantOperator.createInt(5));
        LogicalJDBCScanOperator t0Scan = newScan(table0, Operator.DEFAULT_LIMIT, predicate, t0Columns);
        LogicalJDBCScanOperator t1Scan = newScan(table1, Operator.DEFAULT_LIMIT, null, t1Columns);

        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b),
                List.of(new BinaryPredicateOperator(BinaryType.GT, t0a, t1a)));
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c13 "
                + "FROM (SELECT `a`, `c` FROM `tbl0` WHERE (`c` > 5)) sr_t0, `tbl1` sr_t1 "
                + "WHERE (sr_t0.`a` > sr_t1.`a`)", sql);
    }

    @Test
    public void testBuildJoinQueryWithPerTableLimit() throws Exception {
        Column a0 = new Column("a", VarcharType.VARCHAR);
        Column c0 = new Column("c", IntegerType.INT);
        Column a1 = new Column("a", VarcharType.VARCHAR);
        Column b1 = new Column("b", VarcharType.VARCHAR);
        JDBCTable table0 = newTable("tbl0", "jdbc:mysql://localhost:3306/db", List.of(a0, c0));
        JDBCTable table1 = newTable("tbl1", "jdbc:mysql://localhost:3306/db", List.of(a1, b1));

        ColumnRefOperator t0a = new ColumnRefOperator(10, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t0c = new ColumnRefOperator(11, IntegerType.INT, "c", true);
        ColumnRefOperator t1a = new ColumnRefOperator(12, VarcharType.VARCHAR, "a", true);
        ColumnRefOperator t1b = new ColumnRefOperator(13, VarcharType.VARCHAR, "b", true);
        Map<ColumnRefOperator, Column> t0Columns = new LinkedHashMap<>();
        t0Columns.put(t0a, a0);
        t0Columns.put(t0c, c0);
        Map<ColumnRefOperator, Column> t1Columns = new LinkedHashMap<>();
        t1Columns.put(t1a, a1);
        t1Columns.put(t1b, b1);

        LogicalJDBCScanOperator t0Scan = newScan(table0, 7, null, t0Columns);
        LogicalJDBCScanOperator t1Scan = newScan(table1, Operator.DEFAULT_LIMIT, null, t1Columns);

        String sql = JDBCPushDownSQLBuilder.buildJoinQuery(List.of(t0Scan, t1Scan),
                List.of(t0a, t1b),
                List.of(new BinaryPredicateOperator(BinaryType.EQ, t0a, t1a)));
        Assertions.assertEquals("SELECT sr_t0.`a` AS sr_c10, sr_t1.`b` AS sr_c13 "
                + "FROM (SELECT `a`, `c` FROM `tbl0` LIMIT 7) sr_t0, `tbl1` sr_t1 "
                + "WHERE (sr_t0.`a` = sr_t1.`a`)", sql);
    }

    @Test
    public void testBuildScalarSelectQueryWithExpression() throws Exception {
        Column column = new Column("c", IntegerType.INT);
        JDBCTable table = newTable("tbl0", "jdbc:mysql://localhost:3306/db", List.of(column));
        ColumnRefOperator colRef = new ColumnRefOperator(10, IntegerType.INT, "c", true);
        Map<ColumnRefOperator, Column> scanColumns = new LinkedHashMap<>();
        scanColumns.put(colRef, column);
        LogicalJDBCScanOperator scan = newScan(table, Operator.DEFAULT_LIMIT, null, scanColumns);

        String sql = JDBCPushDownSQLBuilder.buildScalarSelectQuery(scan,
                List.of(new CallOperator(FunctionSet.ADD, IntegerType.INT, List.of(colRef, colRef))),
                List.of("s"), Collections.emptyList(), Collections.emptyList());
        Assertions.assertEquals("SELECT (`c` + `c`) AS `s` FROM `tbl0`", sql);
    }

    private CallOperator avgCall(ColumnRefOperator arg) {
        Function avgFn = ExprUtils.getBuiltinFunction(FunctionSet.AVG, new Type[] {arg.getType()},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNotNull(avgFn, "builtin avg function not found");
        return new CallOperator(FunctionSet.AVG, avgFn.getReturnType(), List.of(arg), avgFn);
    }

    @Test
    public void testAvgIntegerGetsFloatCastForUnknownDialect() throws Exception {
        // SQL Server's jdbc:sqlserver: URI resolves to the UNKNOWN dialect, whose AVG(<integer>)
        // truncates via integer division; the renderer must multiply the argument by 1.0 to force a
        // floating-point average that matches StarRocks' avg semantics.
        Column c = new Column("c", IntegerType.INT);
        JDBCTable table = newTable("tbl0", "jdbc:sqlserver://localhost:1433;DatabaseName=db", List.of(c));
        ColumnRefOperator colRef = new ColumnRefOperator(10, IntegerType.INT, "c", true);
        Map<ColumnRefOperator, Column> scanColumns = new LinkedHashMap<>();
        scanColumns.put(colRef, c);
        LogicalJDBCScanOperator scan = newScan(table, Operator.DEFAULT_LIMIT, null, scanColumns);

        String sql = JDBCPushDownSQLBuilder.buildScalarSelectQuery(scan,
                List.of(avgCall(colRef)), List.of("jdbc_agg_1"), Collections.emptyList(), Collections.emptyList());
        Assertions.assertTrue(sql.contains("* 1.0"), sql);
        Assertions.assertTrue(sql.toLowerCase().contains("avg("), sql);
    }

    @Test
    public void testAvgIntegerNoFloatCastForMySQL() throws Exception {
        // MySQL AVG(<integer>) is already fractional, so the argument is emitted unchanged.
        Column c = new Column("c", IntegerType.INT);
        JDBCTable table = newTable("tbl0", "jdbc:mysql://localhost:3306/db", List.of(c));
        ColumnRefOperator colRef = new ColumnRefOperator(10, IntegerType.INT, "c", true);
        Map<ColumnRefOperator, Column> scanColumns = new LinkedHashMap<>();
        scanColumns.put(colRef, c);
        LogicalJDBCScanOperator scan = newScan(table, Operator.DEFAULT_LIMIT, null, scanColumns);

        String sql = JDBCPushDownSQLBuilder.buildScalarSelectQuery(scan,
                List.of(avgCall(colRef)), List.of("jdbc_agg_1"), Collections.emptyList(), Collections.emptyList());
        Assertions.assertTrue(sql.contains("avg(`c`)"), sql);
        Assertions.assertFalse(sql.contains("* 1.0"), sql);
    }

    @Test
    public void testOracleTemporalColumnsUseJDBCTableHelper() throws Exception {
        Column column = new Column("ts_col", VarcharType.VARCHAR);
        JDBCTable table = newTable("orders", "jdbc:oracle:thin:@localhost:1521:orcl", List.of(column));
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("\"TS_COL\"", Types.TIMESTAMP);
        table.setOriginalJdbcColumnTypes(originalJdbcTypes);

        Assertions.assertEquals("ts_col", ScalarOperatorToJDBCSQLVisitor.normalizeColumnName("\"TS_COL\""));
        Assertions.assertEquals(ScalarOperatorToJDBCSQLVisitor.TemporalKind.TIMESTAMP,
                ScalarOperatorToJDBCSQLVisitor.temporalColumnsByNormalizedName(table).get("ts_col"));
    }

    @Test
    public void testOracleTemporalLiteralRenderingInScalarSelectQuery() throws Exception {
        Column column = new Column("ts_col", VarcharType.VARCHAR);
        JDBCTable table = newTable("orders", "jdbc:oracle:thin:@localhost:1521:orcl", List.of(column));
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("ts_col", Types.TIMESTAMP);
        table.setOriginalJdbcColumnTypes(originalJdbcTypes);

        ColumnRefOperator colRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "ts_col", true);
        Map<ColumnRefOperator, Column> scanColumns = new LinkedHashMap<>();
        scanColumns.put(colRef, column);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE,
                colRef, ConstantOperator.createVarchar("2026-03-12 09:30:15"));
        LogicalJDBCScanOperator scan = newScan(table, Operator.DEFAULT_LIMIT, predicate, scanColumns);

        String sql = JDBCPushDownSQLBuilder.buildScalarSelectQuery(scan,
                List.of(colRef), List.of("jdbc_agg_1"), List.of(colRef),
                List.of(new BinaryPredicateOperator(BinaryType.LE,
                        colRef, ConstantOperator.createVarchar("2026-03-13 09:30:15"))));
        Assertions.assertTrue(sql.contains("WHERE (ts_col >= TIMESTAMP '2026-03-12 09:30:15')"), sql);
        Assertions.assertTrue(sql.contains("GROUP BY ts_col"), sql);
        Assertions.assertTrue(sql.contains("HAVING (ts_col <= TIMESTAMP '2026-03-13 09:30:15')"), sql);
    }

    @Test
    public void testOracleTemporalLiteralKeywordFollowsLiteralShapeNotColumn() throws Exception {
        // The literal keyword is chosen from the literal's textual shape, not the column's declared
        // temporal kind. Following the column would emit DATE '2026-03-12 09:30:15' for a DATE column
        // (ORA-01861: a DATE literal must be exactly 'YYYY-MM-DD') or TIMESTAMP '2026-03-12' for a
        // TIMESTAMP column (also ORA-01861: a TIMESTAMP literal needs a time component). This mirrors
        // the scan path (JDBCScanNode.buildOracleTemporalLiteralExpr) so the same predicate renders
        // identically whether or not aggregate pushdown fires.

        // DATE column compared to a datetime string -> TIMESTAMP literal (not DATE).
        Column dateCol = new Column("d_col", VarcharType.VARCHAR);
        JDBCTable dateTable = newTable("orders", "jdbc:oracle:thin:@localhost:1521:orcl", List.of(dateCol));
        Map<String, Integer> dateTypes = new HashMap<>();
        dateTypes.put("d_col", Types.DATE);
        dateTable.setOriginalJdbcColumnTypes(dateTypes);
        ColumnRefOperator dateRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "d_col", true);
        Map<ColumnRefOperator, Column> dateColumns = new LinkedHashMap<>();
        dateColumns.put(dateRef, dateCol);
        LogicalJDBCScanOperator dateScan = newScan(dateTable, Operator.DEFAULT_LIMIT,
                new BinaryPredicateOperator(BinaryType.GE, dateRef,
                        ConstantOperator.createVarchar("2026-03-12 09:30:15")),
                dateColumns);
        String dateSql = JDBCPushDownSQLBuilder.buildScalarSelectQuery(dateScan,
                List.of(dateRef), List.of("jdbc_agg_1"), List.of(dateRef), Collections.emptyList());
        Assertions.assertTrue(dateSql.contains("WHERE (d_col >= TIMESTAMP '2026-03-12 09:30:15')"), dateSql);
        Assertions.assertFalse(dateSql.contains("DATE '2026-03-12 09:30:15'"), dateSql);

        // TIMESTAMP column compared to a date-only string -> DATE literal (not TIMESTAMP).
        Column tsCol = new Column("ts_col", VarcharType.VARCHAR);
        JDBCTable tsTable = newTable("orders", "jdbc:oracle:thin:@localhost:1521:orcl", List.of(tsCol));
        Map<String, Integer> tsTypes = new HashMap<>();
        tsTypes.put("ts_col", Types.TIMESTAMP);
        tsTable.setOriginalJdbcColumnTypes(tsTypes);
        ColumnRefOperator tsRef = new ColumnRefOperator(2, VarcharType.VARCHAR, "ts_col", true);
        Map<ColumnRefOperator, Column> tsColumns = new LinkedHashMap<>();
        tsColumns.put(tsRef, tsCol);
        LogicalJDBCScanOperator tsScan = newScan(tsTable, Operator.DEFAULT_LIMIT,
                new BinaryPredicateOperator(BinaryType.GE, tsRef,
                        ConstantOperator.createVarchar("2026-03-12")),
                tsColumns);
        String tsSql = JDBCPushDownSQLBuilder.buildScalarSelectQuery(tsScan,
                List.of(tsRef), List.of("jdbc_agg_1"), List.of(tsRef), Collections.emptyList());
        Assertions.assertTrue(tsSql.contains("WHERE (ts_col >= DATE '2026-03-12')"), tsSql);
        Assertions.assertFalse(tsSql.contains("TIMESTAMP '2026-03-12')"), tsSql);
    }

    @Test
    public void testClassifyOracleTemporalLiteralByShape() {
        // <= 10 chars matching YYYY-M-D is a DATE literal.
        Assertions.assertEquals(ScalarOperatorToJDBCSQLVisitor.OracleTemporalLiteralKind.DATE,
                ScalarOperatorToJDBCSQLVisitor.classifyOracleTemporalLiteral("2026-03-12"));
        Assertions.assertEquals(ScalarOperatorToJDBCSQLVisitor.OracleTemporalLiteralKind.DATE,
                ScalarOperatorToJDBCSQLVisitor.classifyOracleTemporalLiteral("2026-3-2"));
        // Anything longer carries a time component -> TIMESTAMP literal.
        Assertions.assertEquals(ScalarOperatorToJDBCSQLVisitor.OracleTemporalLiteralKind.TIMESTAMP,
                ScalarOperatorToJDBCSQLVisitor.classifyOracleTemporalLiteral("2026-03-12 09:30:15"));
        Assertions.assertEquals(ScalarOperatorToJDBCSQLVisitor.OracleTemporalLiteralKind.TIMESTAMP,
                ScalarOperatorToJDBCSQLVisitor.classifyOracleTemporalLiteral("2026-03-12 09:30:15.123456"));
        // A short string that is not a bare date needs no keyword.
        Assertions.assertEquals(ScalarOperatorToJDBCSQLVisitor.OracleTemporalLiteralKind.NONE,
                ScalarOperatorToJDBCSQLVisitor.classifyOracleTemporalLiteral("hello"));
        Assertions.assertEquals(ScalarOperatorToJDBCSQLVisitor.OracleTemporalLiteralKind.NONE,
                ScalarOperatorToJDBCSQLVisitor.classifyOracleTemporalLiteral(""));
    }

    @Test
    public void testBuildSelectQueryLimitDialect() {
        Assertions.assertEquals("SELECT `a` FROM `tbl` LIMIT 3",
                JDBCPushDownSQLBuilder.buildSelectQuery("jdbc:mysql://localhost:3306",
                        List.of("`a`"), "`tbl`", Collections.emptyList(), 3));
        Assertions.assertEquals("SELECT TOP(3) a FROM tbl",
                JDBCPushDownSQLBuilder.buildSelectQuery("jdbc:sqlserver://localhost:1433",
                        List.of("a"), "tbl", Collections.emptyList(), 3));
        Assertions.assertEquals("SELECT * FROM (SELECT a FROM tbl WHERE (a > 1)) WHERE ROWNUM <= 3",
                JDBCPushDownSQLBuilder.buildSelectQuery("jdbc:oracle:thin:@localhost:1521:orcl",
                        List.of("a"), "tbl", List.of("a > 1"), 3));
    }
}
