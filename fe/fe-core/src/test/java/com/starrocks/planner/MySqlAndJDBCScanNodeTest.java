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
package com.starrocks.planner;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.catalog.MysqlTable;
import com.starrocks.common.DdlException;
import com.starrocks.connector.jdbc.JDBCPushDownSQLBuilder;
import com.starrocks.qe.StmtExecutor;
import com.starrocks.sql.ast.expression.BinaryPredicate;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.sql.ast.expression.Expr;
import com.starrocks.sql.ast.expression.InPredicate;
import com.starrocks.sql.ast.expression.LargeStringLiteral;
import com.starrocks.sql.ast.expression.SlotRef;
import com.starrocks.sql.ast.expression.StringLiteral;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BetweenPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.parser.NodePosition;
import com.starrocks.thrift.TPlanNode;
import com.starrocks.type.DateType;
import com.starrocks.type.VarcharType;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MySqlAndJDBCScanNodeTest {

    private JDBCScanNode createOracleScanNode(List<Column> columns, List<SlotDescriptor> slots) throws DdlException {
        return createOracleScanNode(columns, slots, null);
    }

    private JDBCScanNode createOracleScanNode(List<Column> columns, List<SlotDescriptor> slots,
                                              Map<String, Integer> originalJdbcTypes) throws DdlException {
        JDBCTable oracleTable = createOracleTable(columns, originalJdbcTypes);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(oracleTable);
        for (SlotDescriptor slot : slots) {
            tupleDesc.addSlot(slot);
        }
        return new JDBCScanNode(new PlanNodeId(1), tupleDesc, oracleTable);
    }

    private JDBCTable createOracleTable(List<Column> columns, Map<String, Integer> originalJdbcTypes)
            throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "oracle");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:oracle:thin:@localhost:1521:orcl");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "oracle.jdbc.driver.OracleDriver");
        JDBCTable oracleTable = new JDBCTable(1, "orders", columns, properties);
        if (originalJdbcTypes != null) {
            oracleTable.setOriginalJdbcColumnTypes(originalJdbcTypes);
        }
        return oracleTable;
    }

    private static Map<ColumnRefOperator, Column> singleColumnRefMap(ColumnRefOperator ref, Column column) {
        Map<ColumnRefOperator, Column> colRefMap = new LinkedHashMap<>();
        colRefMap.put(ref, column);
        return colRefMap;
    }

    private static List<String> renderOracleScanFilters(JDBCTable table, ColumnRefOperator ref, Column column,
                                                        ScalarOperator predicate) {
        return JDBCPushDownSQLBuilder.renderScanFilters(table, singleColumnRefMap(ref, column),
                Lists.newArrayList(predicate));
    }

    private SlotDescriptor createSlotDescriptor(int slotId, Column column) {
        SlotDescriptor slot = new SlotDescriptor(new SlotId(slotId), column.getName(), column.getType(), true);
        slot.setColumn(column);
        slot.setIsMaterialized(true);
        return slot;
    }

    private LogicalJDBCScanOperator createJDBCScanOperator(JDBCTable table,
                                                           Map<ColumnRefOperator, Column> scanColumns,
                                                           long limit,
                                                           ScalarOperator predicate) {
        Map<Column, ColumnRefOperator> columnToRef = new LinkedHashMap<>();
        scanColumns.forEach((ref, column) -> columnToRef.put(column, ref));
        return new LogicalJDBCScanOperator(table, scanColumns, columnToRef, limit, predicate, null);
    }

    private List<Expr> createConjuncts() {
        Expr slotRef = new SlotRef("col", new SlotDescriptor(new SlotId(1), "col", VarcharType.VARCHAR, true));
        Expr expr0 = new InPredicate(slotRef,
                Lists.newArrayList(new LargeStringLiteral(Strings.repeat("ABCDE", 11), NodePosition.ZERO)), true);
        Expr expr1 = new BinaryPredicate(BinaryType.EQ, slotRef, StringLiteral.create("ABC"));
        Expr expr2 = new CompoundPredicate(CompoundPredicate.Operator.OR, expr0, expr1);
        return Lists.newArrayList(expr0, expr1, expr2);
    }

    @Test
    public void testFiltersInMySQLScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("host", "127.0.0.1");
        properties.put("port", "3036");
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("database", "test_db");
        properties.put("table", "test_table");
        MysqlTable mysqlTable = new MysqlTable(1, "mysql_table",
                Collections.singletonList(new Column("col", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        MysqlScanNode scanNode = new MysqlScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.getConjuncts().addAll(createConjuncts());
        scanNode.computeColumnsAndFilters();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("SELECT * FROM `test_table` " +
                "WHERE (col NOT IN ('ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE')) " +
                "AND (col = 'ABC') AND " +
                "((col NOT IN ('ABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDEABCDE')) OR " +
                "(col = 'ABC'))"), nodeString);

        TPlanNode thriftNode = new TPlanNode();
        scanNode.toThrift(thriftNode);
        Assertions.assertNotNull(thriftNode.getConnector_scan_node());
        Assertions.assertEquals(StmtExecutor.toCatalogType(mysqlTable.getType()),
                thriftNode.getConnector_scan_node().getCatalog_type());
    }

    @Test
    public void testFiltersInJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "driver_class");
        Column col = new Column("col", VarcharType.VARCHAR);
        JDBCTable mysqlTable = new JDBCTable(1, "jdbc_table", Collections.singletonList(col), properties);

        ColumnRefOperator colRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "col", true);
        String bigValue = Strings.repeat("ABCDE", 11);
        ScalarOperator notIn = new InPredicateOperator(true, colRef, ConstantOperator.createVarchar(bigValue));
        ScalarOperator eq = new BinaryPredicateOperator(BinaryType.EQ, colRef, ConstantOperator.createVarchar("ABC"));
        ScalarOperator or = new CompoundPredicateOperator(CompoundPredicateOperator.CompoundType.OR, notIn, eq);

        List<String> filters = JDBCPushDownSQLBuilder.renderScanFilters(mysqlTable,
                singleColumnRefMap(colRef, col), Lists.newArrayList(notIn, eq, or));

        // The full string literal renders inline -- no LargeStringLiteral / replaceLargeStringLiteral
        // round-trip is needed on the ScalarOperator path.
        Assertions.assertEquals(Lists.newArrayList(
                "(`col` NOT IN ('" + bigValue + "'))",
                "(`col` = 'ABC')",
                "((`col` NOT IN ('" + bigValue + "')) OR (`col` = 'ABC'))"), filters);
    }

    @Test
    public void testFiltersInPostgreSQLJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.postgres.Driver");
        JDBCTable pgTable = new JDBCTable(1, "order",
                Collections.singletonList(new Column("user", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: \"order\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"order\""), nodeString);
    }

    @Test
    public void testFiltersInPostgresShortURIJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgres://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.postgres.Driver");
        JDBCTable pgTable = new JDBCTable(1, "order",
                Collections.singletonList(new Column("user", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: \"order\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"order\""), nodeString);
    }

    @Test
    public void testFiltersInOracleJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "oracle");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:oracle:thin:@localhost:1521:orcl");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "oracle.jdbc.driver.OracleDriver");
        JDBCTable oracleTable = new JDBCTable(1, "select",
                Collections.singletonList(new Column("group", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(oracleTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, oracleTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: select"), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM select"), nodeString);
    }

    @Test
    public void testOracleJDBCScanNodeLimitUsesRowNum() throws DdlException {
        Column column = new Column("group", VarcharType.VARCHAR);
        JDBCScanNode scanNode = createOracleScanNode(Collections.singletonList(column),
                Collections.singletonList(createSlotDescriptor(1, column)));
        scanNode.setLimit(5);
        scanNode.createJDBCTableColumns();

        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains(
                "QUERY: SELECT * FROM (SELECT group FROM orders) WHERE ROWNUM <= 5"), nodeString);
    }

    @Test
    public void testOracleRewriteDateColumnAsDateLiteral() throws DdlException {
        Column dateColumn = new Column("date_col", DateType.DATE);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("date_col", Types.DATE);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(dateColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, dateColumn.getType(), "date_col", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createVarchar("2022-01-01"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, dateColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains("date_col = DATE '2022-01-01'"), filters.get(0));
    }

    @Test
    public void testOracleRewriteDatetimeColumnWithMicroseconds() throws DdlException {
        Column datetimeColumn = new Column("ts_col", DateType.DATETIME);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("ts_col", Types.TIMESTAMP);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(datetimeColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, datetimeColumn.getType(), "ts_col", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createVarchar("2026-03-12 09:30:15.123456"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, datetimeColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains("ts_col = TIMESTAMP '2026-03-12 09:30:15.123456'"),
                filters.get(0));
    }

    @Test
    public void testOracleDoesNotRewriteLiteralColumnPredicate() throws DdlException {
        Column datetimeColumn = new Column("ts_col", DateType.DATETIME);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(datetimeColumn), null);
        ColumnRefOperator ref = new ColumnRefOperator(1, datetimeColumn.getType(), "ts_col", true);
        // Literal on the left, column on the right: the renderer keys the temporal rewrite off the
        // column side (child 0), so a literal-first comparison is left untouched.
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ,
                ConstantOperator.createVarchar("2026-03-12 09:30:15"), ref);
        List<String> filters = renderOracleScanFilters(oracleTable, ref, datetimeColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains("'2026-03-12 09:30:15' = ts_col"), filters.get(0));
    }

    @Test
    public void testOracleRewriteBetweenPredicate() throws DdlException {
        Column datetimeColumn = new Column("ts_col", DateType.DATETIME);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("ts_col", Types.TIMESTAMP);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(datetimeColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, datetimeColumn.getType(), "ts_col", true);
        ScalarOperator predicate = new BetweenPredicateOperator(false, ref,
                ConstantOperator.createVarchar("2026-03-12 00:00:00"),
                ConstantOperator.createVarchar("2026-03-13 00:00:00"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, datetimeColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains(
                        "ts_col BETWEEN TIMESTAMP '2026-03-12 00:00:00' AND TIMESTAMP '2026-03-13 00:00:00'"),
                filters.get(0));
    }

    @Test
    public void testOracleRewriteInPredicate() throws DdlException {
        Column datetimeColumn = new Column("ts_col", DateType.DATETIME);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("ts_col", Types.TIMESTAMP);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(datetimeColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, datetimeColumn.getType(), "ts_col", true);
        ScalarOperator predicate = new InPredicateOperator(false, ref,
                ConstantOperator.createVarchar("2026-03-12 09:30:15"),
                ConstantOperator.createVarchar("2026-03-13 09:30:15"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, datetimeColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains(
                        "ts_col IN (TIMESTAMP '2026-03-12 09:30:15', TIMESTAMP '2026-03-13 09:30:15')"),
                filters.get(0));
    }

    @Test
    public void testOracleRewriteTimestampMappedToVarcharWithOriginalJdbcTypes() throws DdlException {
        // Oracle TIMESTAMP column mapped to VARCHAR (default behavior), but original JDBC type is available
        Column varcharColumn = new Column("ts_col", VarcharType.VARCHAR);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("ts_col", Types.TIMESTAMP);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(varcharColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, varcharColumn.getType(), "ts_col", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createVarchar("2026-03-12 09:30:15"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, varcharColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains("ts_col = TIMESTAMP '2026-03-12 09:30:15'"), filters.get(0));
    }

    @Test
    public void testOracleRewriteDateColumnWithOriginalJdbcTypes() throws DdlException {
        // Oracle DATE column with original JDBC type available
        Column dateColumn = new Column("date_col", DateType.DATE);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("date_col", Types.DATE);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(dateColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, dateColumn.getType(), "date_col", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createVarchar("2026-03-12"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, dateColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains("date_col = DATE '2026-03-12'"), filters.get(0));
    }

    @Test
    public void testOracleOriginalJdbcDateTypeOverridesSlotType() throws DdlException {
        // Slot is DATETIME but original JDBC type is DATE: should still use TO_DATE.
        Column datetimeColumn = new Column("date_col", DateType.DATETIME);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("date_col", Types.DATE);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(datetimeColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, datetimeColumn.getType(), "date_col", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createVarchar("2026-03-12"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, datetimeColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains("date_col = DATE '2026-03-12'"), filters.get(0));
    }

    @Test
    public void testOracleDoesNotRewriteTemporalVarcharColumnByName() throws DdlException {
        Column varcharColumn = new Column("tstz_col", VarcharType.VARCHAR);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(varcharColumn), null);
        ColumnRefOperator ref = new ColumnRefOperator(1, varcharColumn.getType(), "tstz_col", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, ref,
                ConstantOperator.createVarchar("2026-03-12 09:30:15.123456"));
        List<String> filters = renderOracleScanFilters(oracleTable, ref, varcharColumn, predicate);
        Assertions.assertEquals(1, filters.size());
        Assertions.assertTrue(filters.get(0).contains("tstz_col = '2026-03-12 09:30:15.123456'"), filters.get(0));
    }

    @Test
    public void testOracleRewriteFilterInThriftPayload() throws DdlException {
        Column datetimeColumn = new Column("ts_col", DateType.DATETIME);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("ts_col", Types.TIMESTAMP);
        JDBCTable oracleTable = createOracleTable(Collections.singletonList(datetimeColumn), originalJdbcTypes);
        ColumnRefOperator ref = new ColumnRefOperator(1, datetimeColumn.getType(), "ts_col", true);
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GE, ref,
                ConstantOperator.createVarchar("2026-03-12 09:30:15"));

        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(oracleTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, oracleTable);
        scanNode.setFilters(renderOracleScanFilters(oracleTable, ref, datetimeColumn, predicate));

        TPlanNode planNode = new TPlanNode();
        scanNode.toThrift(planNode);
        Assertions.assertTrue(planNode.isSetJdbc_scan_node());
        Assertions.assertEquals(1, planNode.getJdbc_scan_node().getFiltersSize());
        Assertions.assertTrue(planNode.getJdbc_scan_node().getFilters().get(0)
                .contains("ts_col >= TIMESTAMP '2026-03-12 09:30:15'"));
    }

    @Test
    public void testOracleAggregatePushDownQueryUsesSafeAliasAndTemporalRewrite() throws DdlException {
        Column datetimeColumn = new Column("ts_col", VarcharType.VARCHAR);
        Map<String, Integer> originalJdbcTypes = new HashMap<>();
        originalJdbcTypes.put("ts_col", Types.TIMESTAMP);

        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "oracle");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:oracle:thin:@localhost:1521:orcl");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "oracle.jdbc.driver.OracleDriver");
        JDBCTable oracleTable = new JDBCTable(1, "orders",
                Collections.singletonList(datetimeColumn), properties);
        oracleTable.setOriginalJdbcColumnTypes(originalJdbcTypes);

        ColumnRefOperator tsColRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "ts_col", true);
        Map<ColumnRefOperator, Column> scanColumns = new LinkedHashMap<>();
        scanColumns.put(tsColRef, datetimeColumn);

        ScalarOperator scanPredicate = new BinaryPredicateOperator(BinaryType.GE,
                tsColRef, ConstantOperator.createVarchar("2026-03-12 09:30:15"));
        LogicalJDBCScanOperator scan = createJDBCScanOperator(oracleTable, scanColumns,
                Operator.DEFAULT_LIMIT, scanPredicate);

        String pushDownQuery = JDBCPushDownSQLBuilder.buildScalarSelectQuery(scan,
                Lists.newArrayList(tsColRef),
                Lists.newArrayList("jdbc_agg_1"),
                Lists.newArrayList(tsColRef),
                Lists.newArrayList(new BinaryPredicateOperator(BinaryType.LE,
                        tsColRef, ConstantOperator.createVarchar("2026-03-13 09:30:15"))));

        Assertions.assertTrue(pushDownQuery.contains("ts_col AS jdbc_agg_1"), pushDownQuery);
        Assertions.assertFalse(pushDownQuery.contains("__jdbc_agg_"), pushDownQuery);
        Assertions.assertTrue(pushDownQuery.contains("WHERE (ts_col >= TIMESTAMP '2026-03-12 09:30:15')"),
                pushDownQuery);
        Assertions.assertTrue(pushDownQuery.contains("GROUP BY ts_col"), pushDownQuery);
        Assertions.assertTrue(pushDownQuery.contains("HAVING (ts_col <= TIMESTAMP '2026-03-13 09:30:15')"),
                pushDownQuery);
    }

    @Test
    public void testAggregatePushDownQueryWithTableLimitUsesDialect() throws DdlException {
        Column column = new Column("a", VarcharType.VARCHAR);
        ColumnRefOperator colRef = new ColumnRefOperator(1, VarcharType.VARCHAR, "a", true);
        Map<ColumnRefOperator, Column> scanColumns = new LinkedHashMap<>();
        scanColumns.put(colRef, column);
        ScalarOperator scanPredicate =
                new BinaryPredicateOperator(BinaryType.EQ, colRef, ConstantOperator.createVarchar("x"));

        Map<String, String> oracleProperties = Maps.newHashMap();
        oracleProperties.put("user", "oracle");
        oracleProperties.put("password", "123456");
        oracleProperties.put("jdbc_uri", "jdbc:oracle:thin:@localhost:1521:orcl");
        oracleProperties.put("driver_url", "driver_url");
        oracleProperties.put("checksum", "checksum");
        oracleProperties.put("driver_class", "oracle.jdbc.driver.OracleDriver");
        JDBCTable oracleTable = new JDBCTable(1, "orders", Collections.singletonList(column), oracleProperties);

        LogicalJDBCScanOperator oracleScan = createJDBCScanOperator(oracleTable, scanColumns, 4, scanPredicate);
        String oracleQuery = JDBCPushDownSQLBuilder.buildScalarSelectQuery(oracleScan,
                Lists.newArrayList(colRef), Lists.newArrayList("a"), Lists.newArrayList(colRef),
                Collections.emptyList());
        Assertions.assertTrue(oracleQuery.contains(
                "FROM (SELECT * FROM (SELECT a FROM orders WHERE (a = 'x')) WHERE ROWNUM <= 4) sr_limited"),
                oracleQuery);

        Map<String, String> sqlServerProperties = Maps.newHashMap();
        sqlServerProperties.put("user", "sa");
        sqlServerProperties.put("password", "123456");
        sqlServerProperties.put("jdbc_uri", "jdbc:sqlserver://localhost:1433;databaseName=testdb");
        sqlServerProperties.put("driver_url", "driver_url");
        sqlServerProperties.put("checksum", "checksum");
        sqlServerProperties.put("driver_class", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        JDBCTable sqlServerTable = new JDBCTable(1, "orders", Collections.singletonList(column), sqlServerProperties);

        LogicalJDBCScanOperator sqlServerScan = createJDBCScanOperator(sqlServerTable, scanColumns,
                4, scanPredicate);
        String sqlServerQuery = JDBCPushDownSQLBuilder.buildScalarSelectQuery(sqlServerScan,
                Lists.newArrayList(colRef), Lists.newArrayList("a"), Lists.newArrayList(colRef),
                Collections.emptyList());
        Assertions.assertTrue(sqlServerQuery.contains(
                "FROM (SELECT TOP(4) a FROM orders WHERE (a = 'x')) sr_limited"), sqlServerQuery);
    }

    @Test
    public void testFiltersInSqlServerJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "sa");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:sqlserver://localhost:1433;databaseName=testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        JDBCTable sqlServerTable = new JDBCTable(1, "table",
                Collections.singletonList(new Column("index", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(sqlServerTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, sqlServerTable);
        scanNode.setLimit(8);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: table"), nodeString);
        Assertions.assertTrue(nodeString.contains("QUERY: SELECT TOP(8) * FROM table"), nodeString);
    }

    @Test
    public void testBuildSelectQueryLimitDialect() {
        Assertions.assertEquals("SELECT `a` FROM `tbl` LIMIT 3",
                JDBCPushDownSQLBuilder.buildSelectQuery("jdbc:mysql://localhost:3306",
                        Collections.singletonList("`a`"), "`tbl`", Collections.emptyList(), 3));
        Assertions.assertEquals("SELECT TOP(3) a FROM tbl",
                JDBCPushDownSQLBuilder.buildSelectQuery("jdbc:sqlserver://localhost:1433",
                        Collections.singletonList("a"), "tbl", Collections.emptyList(), 3));
        Assertions.assertEquals("SELECT * FROM (SELECT a FROM tbl WHERE (a > 1)) WHERE ROWNUM <= 3",
                JDBCPushDownSQLBuilder.buildSelectQuery("jdbc:oracle:thin:@localhost:1521:orcl",
                        Collections.singletonList("a"), "tbl", Collections.singletonList("a > 1"), 3));
    }

    @Test
    public void testWrapWithIdentifierForMySQL() throws DdlException {
        // Test MySQL with backticks
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        JDBCTable mysqlTable = new JDBCTable(1, "test_table",
                Collections.singletonList(new Column("col1", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        // Should wrap table name with backticks
        Assertions.assertTrue(nodeString.contains("TABLE: `test_table`"), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM `test_table`"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForSchemaQualifiedTable() throws DdlException {
        // Test PostgreSQL with schema-qualified table name
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.postgresql.Driver");
        JDBCTable pgTable = new JDBCTable(1, "public.users",
                Collections.singletonList(new Column("id", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        // Should wrap each part with double quotes
        Assertions.assertTrue(nodeString.contains("TABLE: \"public\".\"users\""), nodeString);
        Assertions.assertTrue(nodeString.contains("FROM \"public\".\"users\""), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForAlreadyWrappedTable() throws DdlException {
        // Test that already wrapped identifiers are not double-wrapped
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        JDBCTable mysqlTable = new JDBCTable(1, "`test_table`",
                Collections.singletonList(new Column("col1", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        // Should not double-wrap
        Assertions.assertTrue(nodeString.contains("TABLE: `test_table`"), nodeString);
        Assertions.assertFalse(nodeString.contains("``test_table``"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForMariaDB() throws DdlException {
        // Test MariaDB with backticks
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mariadb://localhost:3306/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.mariadb.jdbc.Driver");
        JDBCTable mariadbTable = new JDBCTable(1, "test_table",
                Collections.singletonList(new Column("col1", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mariadbTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mariadbTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: `test_table`"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForClickHouse() throws DdlException {
        // Test ClickHouse with backticks
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "default");
        properties.put("password", "");
        properties.put("jdbc_uri", "jdbc:clickhouse://localhost:8123/default");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "ru.yandex.clickhouse.ClickHouseDriver");
        JDBCTable clickhouseTable = new JDBCTable(1, "events",
                Collections.singletonList(new Column("event_id", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(clickhouseTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, clickhouseTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: `events`"), nodeString);
    }

    @Test
    public void testCreateJDBCTableColumnsWithMultipleColumns() throws DdlException {
        // Test multiple columns are properly wrapped
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.postgresql.Driver");
        List<Column> columns = Lists.newArrayList(
                new Column("id", VarcharType.VARCHAR),
                new Column("name", VarcharType.VARCHAR),
                new Column("age", VarcharType.VARCHAR)
        );
        JDBCTable pgTable = new JDBCTable(1, "users", columns, properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        SlotDescriptor slot1 = new SlotDescriptor(new SlotId(1), "id", VarcharType.VARCHAR, true);
        slot1.setColumn(columns.get(0));
        slot1.setIsMaterialized(true);
        tupleDesc.addSlot(slot1);
        SlotDescriptor slot2 = new SlotDescriptor(new SlotId(2), "name", VarcharType.VARCHAR, true);
        slot2.setColumn(columns.get(1));
        slot2.setIsMaterialized(true);
        tupleDesc.addSlot(slot2);
        SlotDescriptor slot3 = new SlotDescriptor(new SlotId(3), "age", VarcharType.VARCHAR, true);
        slot3.setColumn(columns.get(2));
        slot3.setIsMaterialized(true);
        tupleDesc.addSlot(slot3);

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        // Should have all columns wrapped with double quotes
        Assertions.assertTrue(nodeString.contains("\"id\", \"name\", \"age\""), nodeString);
    }

    @Test
    public void testCreateJDBCTableColumnsWithAlreadyWrappedColumnName() throws DdlException {
        // Test column that already has identifier symbols
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        List<Column> columns = Lists.newArrayList(
                new Column("`select`", VarcharType.VARCHAR)
        );
        JDBCTable mysqlTable = new JDBCTable(1, "test_table", columns, properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        SlotDescriptor slot = new SlotDescriptor(new SlotId(1), "`select`", VarcharType.VARCHAR, true);
        slot.setColumn(columns.get(0));
        slot.setIsMaterialized(true);
        tupleDesc.addSlot(slot);

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        // Should not double-wrap the column name
        Assertions.assertTrue(nodeString.contains("SELECT `select`"), nodeString);
        Assertions.assertFalse(nodeString.contains("``select``"), nodeString);
    }

    @Test
    public void testCreateJDBCTableColumnsForCountStar() throws DdlException {
        // Test count(*) scenario where no columns are materialized
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        JDBCTable mysqlTable = new JDBCTable(1, "test_table",
                Collections.singletonList(new Column("col1", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(mysqlTable);
        // Don't add any materialized slots to simulate count(*)

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, mysqlTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        // Should use SELECT *
        Assertions.assertTrue(nodeString.contains("SELECT *"), nodeString);
    }

    @Test
    public void testWrapWithIdentifierForComplexSchemaPath() throws DdlException {
        // Test database.schema.table format
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "postgres");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:postgresql://localhost:5432/testdb");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "org.postgresql.Driver");
        JDBCTable pgTable = new JDBCTable(1, "mydb.public.users",
                Collections.singletonList(new Column("id", VarcharType.VARCHAR)), properties);
        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(pgTable);
        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, pgTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        // Should wrap each part separately
        Assertions.assertTrue(nodeString.contains("\"mydb\".\"public\".\"users\""), nodeString);
    }

    @Test
    public void testPassThroughQueryInJDBCScanNode() throws DdlException {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("user", "root");
        properties.put("password", "123456");
        properties.put("jdbc_uri", "jdbc:mysql://localhost:3306");
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "com.mysql.jdbc.Driver");
        List<Column> columns = Lists.newArrayList(
                new Column("id", VarcharType.VARCHAR),
                new Column("name", VarcharType.VARCHAR)
        );
        JDBCTable queryTable = new JDBCTable(1, "query_table", columns, properties);
        queryTable.setPassThroughQuery("select id, name from remote_table");

        TupleDescriptor tupleDesc = new TupleDescriptor(new TupleId(1));
        tupleDesc.setTable(queryTable);
        SlotDescriptor idSlot = new SlotDescriptor(new SlotId(1), "id", VarcharType.VARCHAR, true);
        idSlot.setColumn(columns.get(0));
        idSlot.setIsMaterialized(true);
        tupleDesc.addSlot(idSlot);
        SlotDescriptor nameSlot = new SlotDescriptor(new SlotId(2), "name", VarcharType.VARCHAR, true);
        nameSlot.setColumn(columns.get(1));
        nameSlot.setIsMaterialized(true);
        tupleDesc.addSlot(nameSlot);

        JDBCScanNode scanNode = new JDBCScanNode(new PlanNodeId(1), tupleDesc, queryTable);
        scanNode.createJDBCTableColumns();
        String nodeString = scanNode.getExplainString();
        Assertions.assertTrue(nodeString.contains("TABLE: (select id, name from remote_table) sr_inline"),
                nodeString);
        Assertions.assertTrue(nodeString.contains("SELECT `id`, `name` FROM " +
                        "(select id, name from remote_table) sr_inline"), nodeString);
        Assertions.assertFalse(nodeString.contains("FROM `(select id, name from remote_table) sr_inline`"),
                nodeString);
    }
}
