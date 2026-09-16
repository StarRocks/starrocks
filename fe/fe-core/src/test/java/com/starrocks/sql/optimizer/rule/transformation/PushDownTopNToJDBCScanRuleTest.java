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

package com.starrocks.sql.optimizer.rule.transformation;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.JDBCResource;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.SortPhase;
import com.starrocks.sql.optimizer.operator.TopNType;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.PrimitiveType;
import com.starrocks.type.Type;
import com.starrocks.type.TypeFactory;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PushDownTopNToJDBCScanRuleTest {
    private final PushDownTopNToJDBCScanRule rule = new PushDownTopNToJDBCScanRule();
    // transform() assumes check() admitted the input, so a rejection is asserted through check().
    private final SessionVariable variables = new SessionVariable();
    private final OptimizerContext context = mock(OptimizerContext.class);
    private final ColumnRefOperator id = new ColumnRefOperator(1, IntegerType.INT, "id", false);
    private final ColumnRefOperator key = new ColumnRefOperator(2, IntegerType.BIGINT, "sortKey", true);

    private LogicalJDBCScanOperator newScan(Map<ColumnRefOperator, Column> columns) throws Exception {
        return newScan(columns, "org.postgresql.Driver");
    }

    private LogicalJDBCScanOperator newScan(Map<ColumnRefOperator, Column> columns, String driverClass) throws Exception {
        Map<String, String> properties = Map.of(
                JDBCResource.URI, "jdbc:postgresql://localhost:5432/db",
                JDBCResource.USER, "user", JDBCResource.PASSWORD, "password",
                JDBCResource.DRIVER_URL, "driver_url", JDBCResource.CHECK_SUM, "checksum",
                JDBCResource.DRIVER_CLASS, driverClass,
                JDBCTable.JDBC_TABLENAME, "\"public\".\"probe\"");
        JDBCTable table = new JDBCTable(1, "probe", List.copyOf(columns.values()), properties);
        Map<Column, ColumnRefOperator> reverse = new LinkedHashMap<>();
        columns.forEach((ref, column) -> reverse.put(column, ref));
        return new LogicalJDBCScanOperator(table, columns, reverse, Operator.DEFAULT_LIMIT, null, null);
    }

    private LogicalJDBCScanOperator integerScan() throws Exception {
        return newScan(Map.of(id, new Column("id", IntegerType.INT, false)));
    }

    private OptExpression topN(LogicalJDBCScanOperator scan, Ordering... orderings) {
        return OptExpression.create(new LogicalTopNOperator(List.of(orderings), 10, 0), OptExpression.create(scan));
    }

    @BeforeEach
    public void bindSessionVariable() {
        when(context.getSessionVariable()).thenReturn(variables);
    }

    private OptExpression push(OptExpression input) {
        Assertions.assertTrue(rule.check(input, context));
        List<OptExpression> result = rule.transform(input, context);
        Assertions.assertEquals(1, result.size());
        return result.get(0);
    }

    private String sql(OptExpression output) {
        return ((JDBCTable) ((LogicalJDBCScanOperator) output.getOp()).getTable()).getCatalogTableName();
    }

    @Test
    public void testBaseMultiKeyAndUnprojectedOrderKey() throws Exception {
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        columns.put(id, new Column("id", IntegerType.INT, false));
        columns.put(key, new Column("\"sortKey\"", IntegerType.BIGINT, true));
        LogicalJDBCScanOperator scan = newScan(columns);
        JDBCTable catalogTable = (JDBCTable) scan.getTable();
        LogicalTopNOperator topN = LogicalTopNOperator.builder()
                .withOperator(new LogicalTopNOperator(List.of(
                        new Ordering(key, false, false), new Ordering(id, true, true)), 10, 0))
                .setProjection(new Projection(Map.of(id, id))).build();
        OptExpression input = OptExpression.create(topN, OptExpression.create(scan));
        OptExpression output = push(input);
        Assertions.assertEquals("SELECT \"id\", \"sortKey\" FROM \"public\".\"probe\" "
                + "ORDER BY \"sortKey\" DESC NULLS LAST, \"id\" ASC NULLS FIRST LIMIT 10", sql(output));
        // The TopN is dropped: the remote ORDER BY already sorted the rows, and the scan is
        // flagged so the BE does not reorder them on the way up.
        Assertions.assertInstanceOf(LogicalJDBCScanOperator.class, output.getOp());
        Assertions.assertEquals(0, output.arity());
        Assertions.assertEquals(10, output.getOp().getLimit());
        Assertions.assertNotEquals(scan, output.getOp());
        Assertions.assertTrue(((JDBCTable) ((LogicalJDBCScanOperator) output.getOp()).getTable())
                .isPreserveRemoteOrder());
        // The dropped TopN carried this projection; later operators still reference it.
        Assertions.assertSame(topN.getProjection(), output.getOp().getProjection());
        Assertions.assertFalse(catalogTable.isInlineTable());
        Assertions.assertEquals(1, rule.transform(input, context).size(), "input operators must remain untouched");
    }

    @Test
    public void testPredicateIsAppliedBeforeRemoteLimit() throws Exception {
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.GT, id, ConstantOperator.createInt(7));
        LogicalJDBCScanOperator scan = new LogicalJDBCScanOperator.Builder().withOperator(integerScan())
                .setPredicate(predicate).build();
        OptExpression output = push(topN(scan, new Ordering(id, false, true)));
        Assertions.assertEquals("SELECT \"id\" FROM \"public\".\"probe\" WHERE (\"id\" > 7) "
                + "ORDER BY \"id\" DESC NULLS FIRST LIMIT 10", sql(output));
        Assertions.assertNull(output.getOp().getPredicate());
        Assertions.assertSame(predicate, scan.getPredicate());
    }

    @Test
    public void testAggregateAliasAfterGroupByAndHaving() throws Exception {
        ColumnRefOperator total = new ColumnRefOperator(7, IntegerType.BIGINT, "sum", true);
        LogicalJDBCScanOperator scan = newScan(Map.of(total, new Column("jdbc_agg_7", IntegerType.BIGINT, true)));
        String aggregateSql = "SELECT sum(\"id\") AS \"jdbc_agg_7\" FROM \"public\".\"probe\" "
                + "GROUP BY \"tenant_id\" HAVING (count(*) > 2)";
        ((JDBCTable) scan.getTable()).setPushDownQuery(aggregateSql);
        OptExpression output = push(topN(scan, new Ordering(total, false, false)));
        Assertions.assertEquals("SELECT \"jdbc_agg_7\" FROM (" + aggregateSql + ") sr_inline "
                + "ORDER BY \"jdbc_agg_7\" DESC NULLS LAST LIMIT 10", sql(output));
    }

    @Test
    public void testRejectIncompatibleOrderingTypes() throws Exception {
        for (Type type : List.of(VarcharType.VARCHAR, FloatType.FLOAT, FloatType.DOUBLE, IntegerType.LARGEINT,
                DateType.DATE, DateType.DATETIME)) {
            ColumnRefOperator ref = new ColumnRefOperator(8, type, "key", true);
            LogicalJDBCScanOperator scan = newScan(Map.of(ref, new Column("key", type, true)));
            Assertions.assertFalse(rule.check(topN(scan, new Ordering(ref, true, false)), context));
        }
    }

    @Test
    public void testStringBooleanAndDecimalTypeNameGate() throws Exception {
        Type decimal = TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL64, 12, 2);
        Map<Type, String> accepted = new LinkedHashMap<>();
        accepted.put(TypeFactory.createVarcharType(100), "varchar");
        accepted.put(TypeFactory.createVarcharType(65533), "text");
        accepted.put(BooleanType.BOOLEAN, "bool");
        accepted.put(decimal, "numeric");
        for (Map.Entry<Type, String> entry : accepted.entrySet()) {
            ColumnRefOperator ref = new ColumnRefOperator(10, entry.getKey(), "sortKey", true);
            LogicalJDBCScanOperator scan = newScan(Map.of(ref, new Column("sortKey", entry.getKey(), true)));
            ((JDBCTable) scan.getTable()).setOriginalJdbcColumnTypeNames(Map.of("sortKey", entry.getValue()));
            String sql = sql(push(topN(scan, new Ordering(ref, false, false))));
            // Only a string key carries the collation that aligns PostgreSQL with StarRocks bytes.
            String expected = entry.getKey().getPrimitiveType() == PrimitiveType.VARCHAR
                    ? "ORDER BY \"sortKey\" COLLATE \"C\" DESC NULLS LAST LIMIT 10"
                    : "ORDER BY \"sortKey\" DESC NULLS LAST LIMIT 10";
            Assertions.assertTrue(sql.contains(expected), entry.getValue() + ": " + sql);
        }

        Map<Type, String> rejected = new LinkedHashMap<>();
        // An unconstrained numeric still maps to VARCHAR, and its string order is not its numeric order.
        rejected.put(TypeFactory.createVarcharType(65533), "numeric");
        // bpchar ignores trailing spaces when comparing; StarRocks CHAR does not.
        rejected.put(TypeFactory.createCharType(10), "bpchar");
        // Beyond DECIMAL256 the mapping clamps the precision or degrades to DOUBLE.
        rejected.put(TypeFactory.createDecimalV3Type(PrimitiveType.DECIMAL256, 50, 2), "numeric");
        rejected.put(BooleanType.BOOLEAN, "enum");
        for (Map.Entry<Type, String> entry : rejected.entrySet()) {
            ColumnRefOperator ref = new ColumnRefOperator(11, entry.getKey(), "sortKey", true);
            LogicalJDBCScanOperator scan = newScan(Map.of(ref, new Column("sortKey", entry.getKey(), true)));
            ((JDBCTable) scan.getTable()).setOriginalJdbcColumnTypeNames(Map.of("sortKey", entry.getValue()));
            Assertions.assertFalse(rule.check(topN(scan, new Ordering(ref, false, false)), context),
                    entry.getKey() + "/" + entry.getValue());
        }

        // Missing source-type metadata fails closed for the newly admitted types too.
        for (Type type : List.of(TypeFactory.createVarcharType(100), BooleanType.BOOLEAN, decimal)) {
            ColumnRefOperator ref = new ColumnRefOperator(12, type, "sortKey", true);
            LogicalJDBCScanOperator scan = newScan(Map.of(ref, new Column("sortKey", type, true)));
            Assertions.assertFalse(rule.check(topN(scan, new Ordering(ref, false, false)), context),
                    type.toString());
        }
    }

    @Test
    public void testTemporalTypeNameGate() throws Exception {
        for (String typeName : List.of("date", "timestamp", "timestamp without time zone")) {
            Type type = typeName.equals("date") ? DateType.DATE : DateType.DATETIME;
            ColumnRefOperator ref = new ColumnRefOperator(9, type, "createdAt", true);
            LogicalJDBCScanOperator scan = newScan(Map.of(ref, new Column("\"createdAt\"", type, true)));
            ((JDBCTable) scan.getTable()).setOriginalJdbcColumnTypeNames(Map.of("\"createdAt\"", typeName));
            Assertions.assertTrue(sql(push(topN(scan, new Ordering(ref, false, false))))
                    .contains("ORDER BY \"createdAt\" DESC NULLS LAST LIMIT 10"));
        }
        ColumnRefOperator ref = new ColumnRefOperator(9, DateType.DATETIME, "createdAt", true);
        for (String typeName : List.of("timestamptz", "timestamp with time zone", "")) {
            LogicalJDBCScanOperator scan = newScan(Map.of(ref, new Column("createdAt", DateType.DATETIME, true)));
            ((JDBCTable) scan.getTable()).setOriginalJdbcColumnTypeNames(Map.of("createdAt", typeName));
            Assertions.assertFalse(rule.check(topN(scan, new Ordering(ref, false, false)), context));
        }
        LogicalJDBCScanOperator scan = newScan(Map.of(ref, new Column("createdAt", DateType.DATETIME, true)));
        JDBCTable table = (JDBCTable) scan.getTable();
        table.setOriginalJdbcColumnTypeNames(Map.of("createdAt", "timestamp"));
        table.setPushDownQuery("SELECT max(timestamptz_column) AS \"createdAt\" FROM probe");
        Assertions.assertFalse(rule.check(topN(scan, new Ordering(ref, false, false)), context));
        LogicalJDBCScanOperator customDriver = newScan(
                Map.of(ref, new Column("createdAt", DateType.DATETIME, true)), "com.custom.Driver");
        ((JDBCTable) customDriver.getTable()).setOriginalJdbcColumnTypeNames(Map.of("createdAt", "timestamp"));
        Assertions.assertFalse(rule.check(topN(customDriver, new Ordering(ref, false, false)), context));
    }

    @Test
    public void testRejectLocalFilterAndProjection() throws Exception {
        ScalarOperator md5 = new CallOperator("md5", VarcharType.VARCHAR, List.of(id));
        ScalarOperator predicate = new BinaryPredicateOperator(BinaryType.EQ, md5, ConstantOperator.createVarchar("x"));
        LogicalJDBCScanOperator scan = integerScan();
        // A local filter between the TopN and the scan is excluded by the pattern rather than here;
        // JDBCTopNPushDownTest covers that shape end to end.
        LogicalJDBCScanOperator scanPredicate = new LogicalJDBCScanOperator.Builder().withOperator(scan)
                .setPredicate(predicate).build();
        Assertions.assertFalse(rule.check(topN(scanPredicate, new Ordering(id, true, false)), context));
        LogicalJDBCScanOperator projection = new LogicalJDBCScanOperator.Builder().withOperator(scan)
                .setProjection(new Projection(Map.of(id, ConstantOperator.createInt(1)))).build();
        Assertions.assertFalse(rule.check(topN(projection, new Ordering(id, true, false)), context));
    }

    @Test
    public void testRejectRankPartitionAndPartialTopN() throws Exception {
        LogicalJDBCScanOperator scan = integerScan();
        LogicalTopNOperator original = new LogicalTopNOperator(List.of(new Ordering(id, true, false)), 10, 0);
        for (LogicalTopNOperator topN : List.of(
                LogicalTopNOperator.builder().withOperator(original).setTopNType(TopNType.RANK).build(),
                LogicalTopNOperator.builder().withOperator(original).setPartitionByColumns(List.of(id)).build(),
                LogicalTopNOperator.builder().withOperator(original).setSortPhase(SortPhase.PARTIAL).build())) {
            Assertions.assertFalse(rule.check(OptExpression.create(topN, OptExpression.create(scan)), context));
        }
        // A sort with no limit transfers every row either way, so there is nothing to push.
        LogicalTopNOperator sort = new LogicalTopNOperator(List.of(new Ordering(id, true, false)));
        Assertions.assertFalse(rule.check(OptExpression.create(sort, OptExpression.create(scan)), context));
    }

    @Test
    public void testOffsetIsPushedAndScanLimitIsNot() throws Exception {
        LogicalJDBCScanOperator scan = integerScan();
        LogicalTopNOperator original = new LogicalTopNOperator(List.of(new Ordering(id, true, false)), 10, 0);
        // PostgreSQL applies OFFSET between ORDER BY and LIMIT, which is what the TopN asks for.
        OptExpression offsetInput = OptExpression.create(
                LogicalTopNOperator.builder().withOperator(original).setOffset(3).build(), OptExpression.create(scan));
        OptExpression offsetOutput = push(offsetInput);
        Assertions.assertEquals("SELECT \"id\" FROM \"public\".\"probe\" "
                + "ORDER BY \"id\" ASC NULLS LAST LIMIT 10 OFFSET 3", sql(offsetOutput));
        // The remote already skipped the offset, so the scan keeps only the rows the TopN would have.
        Assertions.assertEquals(10, offsetOutput.getOp().getLimit());

        // The remote ORDER BY would have to follow the scan's own LIMIT in the SQL text.
        LogicalJDBCScanOperator limited = new LogicalJDBCScanOperator.Builder().withOperator(scan).setLimit(15).build();
        Assertions.assertFalse(rule.check(topN(limited, new Ordering(id, true, false)), context));
    }

    @Test
    public void testSessionSwitch() throws Exception {
        OptExpression input = topN(integerScan(), new Ordering(id, true, false));
        Assertions.assertTrue(rule.check(input, context));
        variables.setEnableJdbcTopNPushDown(false);
        Assertions.assertFalse(rule.check(input, context));
        variables.setEnableJdbcTopNPushDown(true);
        Assertions.assertTrue(rule.check(input, context));
    }
}
