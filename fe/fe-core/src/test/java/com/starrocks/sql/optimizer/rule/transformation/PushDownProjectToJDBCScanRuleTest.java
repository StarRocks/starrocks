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
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.JDBCTable;
import com.starrocks.common.DdlException;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalJDBCScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.CharType;
import com.starrocks.type.DateType;
import com.starrocks.type.FloatType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.NullType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class PushDownProjectToJDBCScanRuleTest {
    private static final String MYSQL_URI = "jdbc:mysql://localhost:3306";
    private static final String ORACLE_URI = "jdbc:oracle:thin:@localhost:1521:orcl";
    private static final String POSTGRES_URI = "jdbc:postgresql://localhost:5432/db";
    private static final String CLICKHOUSE_URI = "jdbc:clickhouse://localhost:8123/db";

    @Test
    public void testProjectPushDownUsesUniqueOutputAliases() throws Exception {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, IntegerType.INT, "b", true);
        ColumnRefOperator out0 = new ColumnRefOperator(3, IntegerType.INT, "expr", true);
        ColumnRefOperator out1 = new ColumnRefOperator(4, IntegerType.INT, "expr", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(out0, add(a, 1));
        projectionMap.put(out1, add(b, 1));

        LogicalJDBCScanOperator scan = newJDBCScan(MYSQL_URI,
                columns(a, "a", b, "b"), new Projection(projectionMap));

        LogicalJDBCScanOperator resultScan = transformOne(scan);
        JDBCTable resultTable = (JDBCTable) resultScan.getTable();

        Assertions.assertTrue(resultTable.getCatalogTableName().contains(
                "SELECT (`a` + 1) AS `jdbc_proj_3`, (`b` + 1) AS `jdbc_proj_4` FROM `tbl0`"),
                resultTable.getCatalogTableName());
        Assertions.assertEquals(JDBCPushDownRuleUtils.JDBC_PROJECT_ALIAS_PREFIX + out0.getId(),
                resultScan.getColRefToColumnMetaMap().get(out0).getName());
        Assertions.assertEquals(JDBCPushDownRuleUtils.JDBC_PROJECT_ALIAS_PREFIX + out1.getId(),
                resultScan.getColRefToColumnMetaMap().get(out1).getName());
    }

    @Test
    public void testOraclePredicateProjectIsNotPushedDown() throws Exception {
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator out = new ColumnRefOperator(2, BooleanType.BOOLEAN, "p", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(out, new BinaryPredicateOperator(BinaryType.GT, a, ConstantOperator.createInt(1)));

        LogicalJDBCScanOperator scan = newJDBCScan(ORACLE_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testMysqlNarrowIntegerKeepsNarrowType() throws Exception {
        // A remotely-evaluated narrow integer (e.g. a % 3) comes back from MySQL/MariaDB as a wide
        // java.lang.Long, which the BE JDBC type checker maps into narrow integer slots via
        // type_checker_config.xml (materialized as BIGINT, then cast down to the slot type), so
        // the pushed column keeps its narrow type instead of being widened here.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator mod = new ColumnRefOperator(3, IntegerType.INT, "mod", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(mod, mod(a, 3));

        LogicalJDBCScanOperator resultScan = transformOne(newJDBCScan(MYSQL_URI,
                columns(a, "a"), new Projection(projectionMap)));

        Assertions.assertEquals(IntegerType.INT, resultScan.getColRefToColumnMetaMap().get(mod).getType());
    }

    @Test
    public void testBooleanComparisonNotPushedDown() throws Exception {
        // A boolean comparison in the SELECT list is never pushed down (any dialect): driver types for a
        // comparison rarely map cleanly into a BOOLEAN slot and it is a rare pattern, so it is evaluated
        // locally even on a dialect that would otherwise accept a pushed expression.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator gt = new ColumnRefOperator(2, BooleanType.BOOLEAN, "gt", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(gt, new BinaryPredicateOperator(BinaryType.GT, a, ConstantOperator.createInt(1)));

        LogicalJDBCScanOperator scan = newJDBCScan(MYSQL_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testClickHouseBooleanProjectIsNotPushedDown() throws Exception {
        // ClickHouse surfaces a UInt8 comparison as com.clickhouse.data.value.UnsignedByte, which the BE
        // JDBC type checker never maps into a BOOLEAN slot, so a boolean comparison SELECT item would be
        // rejected at scan time -- it must stay evaluated locally rather than being pushed down.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator gt = new ColumnRefOperator(2, BooleanType.BOOLEAN, "gt", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(gt, new BinaryPredicateOperator(BinaryType.GT, a, ConstantOperator.createInt(1)));

        LogicalJDBCScanOperator scan = newJDBCScan(CLICKHOUSE_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testOracleIntegerScalarProjectIsNotPushedDown() throws Exception {
        // Oracle evaluates a numeric literal as NUMBER, returned by the driver as java.math.BigDecimal,
        // which the BE JDBC type checker accepts only into DECIMAL/VARCHAR/DOUBLE slots -- never the integer
        // slot a constant/arithmetic SELECT item is declared with. So an integer-typed derived item must not
        // be pushed to Oracle; it is evaluated locally.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator lit = new ColumnRefOperator(2, IntegerType.INT, "lit", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(lit, ConstantOperator.createInt(1));

        LogicalJDBCScanOperator scan = newJDBCScan(ORACLE_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testImplicitCastOverScanColumnNotPushedDown() throws Exception {
        // An implicit cast wrapping a scan column is not pushed: the SQL renderer drops the cast, so the
        // remote returns the column's native type while the column is declared as the cast target -- a
        // mismatch the BE type checker may not be able to bridge -- so the whole projection stays local.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator castA = new ColumnRefOperator(3, IntegerType.BIGINT, "castA", true);
        ColumnRefOperator sum = new ColumnRefOperator(4, IntegerType.INT, "sum", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(a, a);
        projectionMap.put(castA, new CastOperator(IntegerType.BIGINT, a, true));
        projectionMap.put(sum, add(a, 1));

        LogicalJDBCScanOperator scan = newJDBCScan(MYSQL_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testMysqlTemporalConstantNotPushedDown() throws Exception {
        // A DATE/DATETIME constant renders as a bare quoted string on non-Oracle dialects while the
        // synthesized scan column keeps its temporal type, so the driver returns a java.lang.String the
        // BE JDBC type checker cannot map into the temporal slot -- the scan would fail. Keep it local.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator d = new ColumnRefOperator(2, DateType.DATE, "d", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(d, ConstantOperator.createDate(LocalDateTime.of(2024, 1, 1, 0, 0, 0)));

        LogicalJDBCScanOperator scan = newJDBCScan(MYSQL_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testNestedPredicateProjectIsNotPushedDown() throws Exception {
        // A comparison nested under another pushable expression (here CAST(a > 1 AS CHAR(10))) must not
        // be pushed down either: the renderer emits the bare scalar boolean inside the cast
        // (CAST((a > 1) AS CHAR(10))), which Oracle cannot evaluate -- it fails remotely with ORA-00907
        // (Oracle has no boolean scalar type). The CHAR cast itself is pushable, so only a recursive
        // predicate check -- not the top-level one -- rejects it, keeping the whole projection local.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator castGt = new ColumnRefOperator(2, new CharType(10), "castGt", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(castGt, new CastOperator(new CharType(10),
                new BinaryPredicateOperator(BinaryType.GT, a, ConstantOperator.createInt(1)), false));

        LogicalJDBCScanOperator scan = newJDBCScan(ORACLE_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testImplicitCastOverDerivedProjectIsNotPushedDown() throws Exception {
        // An implicit cast wrapping a DERIVED expression (here a + 1 widened to DOUBLE by set-operation
        // coercion) must not be pushed: passthroughScanColumn unwraps the implicit cast to the derived
        // a + 1 (not a scan column) and returns null, so the scan-column gate alone would not catch it.
        // The renderer drops the implicit cast, so the remote evaluates a + 1 and returns it as a wide
        // Long, while the synthesized column is declared DOUBLE -- a mismatch the BE JDBC type checker
        // rejects (Type mismatches ... type:DOUBLE ... java.lang.Long). Keep the whole projection local.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator castSum = new ColumnRefOperator(2, FloatType.DOUBLE, "castSum", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(castSum, new CastOperator(FloatType.DOUBLE, add(a, 1), true));

        LogicalJDBCScanOperator scan = newJDBCScan(MYSQL_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testMysqlBooleanConstantNotPushedDown() throws Exception {
        // A boolean SELECT item is declared BOOLEAN, but MySQL/MariaDB return a boolean result as a
        // numeric java.lang.Long the BE JDBC type checker does not map into a BOOLEAN slot, so the scan
        // fails. Only a boolean constant reaches this path (comparisons are rejected earlier); pushing a
        // constant gains nothing, so it is evaluated locally on non-PostgreSQL dialects.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, BooleanType.BOOLEAN, "b", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(b, ConstantOperator.createBoolean(true));

        LogicalJDBCScanOperator scan = newJDBCScan(MYSQL_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    @Test
    public void testPostgresBooleanConstantPushedDown() throws Exception {
        // PostgreSQL returns a native java.lang.Boolean the BOOLEAN slot accepts, so a boolean constant
        // is safe to push there -- the non-PostgreSQL boolean gate must not reject it.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator b = new ColumnRefOperator(2, BooleanType.BOOLEAN, "b", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(b, ConstantOperator.createBoolean(true));

        LogicalJDBCScanOperator scan = newJDBCScan(POSTGRES_URI,
                columns(a, "a"), new Projection(projectionMap));

        Assertions.assertNotNull(transformOne(scan));
    }

    @Test
    public void testNullConstantNotPushedDown() throws Exception {
        // A NULL/untyped constant synthesizes a NULL_TYPE scan column, but no JDBC result class maps
        // into a TYPE_NULL slot (MySQL returns it as java.lang.Object), so the scan fails at
        // initialization. Pushing a constant gains nothing, so it is evaluated locally on every dialect.
        ColumnRefOperator a = new ColumnRefOperator(1, IntegerType.INT, "a", true);
        ColumnRefOperator n = new ColumnRefOperator(2, NullType.NULL, "n", true);

        Map<ColumnRefOperator, ScalarOperator> projectionMap = new LinkedHashMap<>();
        projectionMap.put(n, ConstantOperator.createNull(NullType.NULL));

        LogicalJDBCScanOperator scan = newJDBCScan(MYSQL_URI,
                columns(a, "a"), new Projection(projectionMap));

        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        Assertions.assertTrue(rule.transform(OptExpression.create(scan), null).isEmpty());
    }

    private LogicalJDBCScanOperator transformOne(LogicalJDBCScanOperator scan) {
        PushDownProjectToJDBCScanRule rule = new PushDownProjectToJDBCScanRule();
        List<OptExpression> results = rule.transform(OptExpression.create(scan), null);
        Assertions.assertEquals(1, results.size());
        return results.get(0).getOp().cast();
    }

    private LogicalJDBCScanOperator newJDBCScan(String jdbcUri, Map<ColumnRefOperator, Column> scanColumns,
                                                Projection projection) throws DdlException {
        Map<String, String> properties = new LinkedHashMap<>();
        properties.put("user", "user");
        properties.put("password", "password");
        properties.put("jdbc_uri", jdbcUri);
        properties.put("driver_url", "driver_url");
        properties.put("checksum", "checksum");
        properties.put("driver_class", "driver_class");
        JDBCTable table = new JDBCTable(1, "tbl0", List.copyOf(scanColumns.values()), properties);

        Map<Column, ColumnRefOperator> columnToRef = new LinkedHashMap<>();
        scanColumns.forEach((ref, column) -> columnToRef.put(column, ref));
        return new LogicalJDBCScanOperator(table, scanColumns, columnToRef,
                Operator.DEFAULT_LIMIT, null, projection);
    }

    private Map<ColumnRefOperator, Column> columns(ColumnRefOperator ref, String name) {
        Map<ColumnRefOperator, Column> columns = new LinkedHashMap<>();
        columns.put(ref, new Column(name, ref.getType()));
        return columns;
    }

    private Map<ColumnRefOperator, Column> columns(ColumnRefOperator ref0, String name0,
                                                   ColumnRefOperator ref1, String name1) {
        Map<ColumnRefOperator, Column> columns = columns(ref0, name0);
        columns.put(ref1, new Column(name1, ref1.getType()));
        return columns;
    }

    private CallOperator add(ColumnRefOperator column, int value) {
        return new CallOperator(FunctionSet.ADD, IntegerType.INT,
                List.of(column, ConstantOperator.createInt(value)));
    }

    private CallOperator mod(ColumnRefOperator column, int value) {
        return new CallOperator(FunctionSet.MOD, IntegerType.INT,
                List.of(column, ConstantOperator.createInt(value)));
    }
}
