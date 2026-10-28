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

package com.starrocks.sql.plan;

import com.starrocks.catalog.Column;
import com.starrocks.catalog.DeltaLakeTable;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.connector.delta.ScalarOperationToDeltaLakeExpr;
import com.starrocks.connector.metastore.MetastoreTable;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptimizerFactory;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalDeltaLakeScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.OptExternalPartitionPruner;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.type.DateType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import io.delta.kernel.expressions.AlwaysTrue;
import io.delta.kernel.expressions.And;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimestampDeltaPredicateTest extends PlanTestBase {
    private CallOperator timestamp(ScalarOperator child) {
        Function fn = ExprUtils.getBuiltinFunction(FunctionSet.TIMESTAMP,
                new Type[] {DateType.DATETIME}, Function.CompareMode.IS_IDENTICAL);
        assertNotNull(fn);
        return new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(child), fn);
    }

    private LogicalDeltaLakeScanOperator scan(ColumnRefOperator time, ScalarOperator predicate) {
        Column column = new Column("dt", DateType.DATETIME);
        DeltaLakeTable table = new DeltaLakeTable(1, null, "db", "test_table",
                List.of(column), List.of(), null, null,
                new MetastoreTable("db", "test_table", "file:///tmp/timestamp-test", 0));
        return new LogicalDeltaLakeScanOperator(table,
                Map.of(time, column), Map.of(column, time), -1, predicate);
    }

    @ParameterizedTest
    @CsvSource({
            "Etc/UTC, 1674555201123456, 1677147201654321",
            "America/Los_Angeles, 1674584001123456, 1677176001654321"
    })
    public void exposesDeltaFilterAndMinMaxBounds(String timeZone, long lowerMicros, long upperMicros) {
        String previousTimeZone = connectContext.getSessionVariable().getTimeZone();
        try {
            connectContext.getSessionVariable().setTimeZone(timeZone);
            ColumnRefFactory refs = new ColumnRefFactory();
            ColumnRefOperator time = refs.create("dt", DateType.DATETIME, true);
            LocalDateTime lower = LocalDateTime.of(2023, 1, 24, 10, 13, 21, 123456000);
            LocalDateTime upper = LocalDateTime.of(2023, 2, 23, 10, 13, 21, 654321000);
            ScalarOperator wrapped = Utils.compoundAnd(
                    new BinaryPredicateOperator(BinaryType.GE, timestamp(timestamp(time)),
                            ConstantOperator.createDatetime(lower)),
                    new BinaryPredicateOperator(BinaryType.LT, timestamp(timestamp(time)),
                            ConstantOperator.createDatetime(upper)));
            var context = OptimizerFactory.initContext(connectContext, refs);

            // This negative control reproduces the current min/max eligibility problem.
            LogicalDeltaLakeScanOperator original = scan(time, wrapped);
            OptExternalPartitionPruner.prunePartitions(context, original);
            assertTrue(original.getScanOperatorPredicates().getMinMaxConjuncts().isEmpty());

            ScalarOperator rewritten = new ScalarOperatorRewriter().rewrite(
                    wrapped.clone(), ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
            LogicalDeltaLakeScanOperator optimized = scan(time, rewritten);
            OptExternalPartitionPruner.prunePartitions(context, optimized);
            List<ScalarOperator> bounds = optimized.getScanOperatorPredicates().getMinMaxConjuncts();
            assertEquals(2, bounds.size());
            assertEquals(Set.of(BinaryType.GE, BinaryType.LT),
                    Set.of(((BinaryPredicateOperator) bounds.get(0)).getBinaryType(),
                            ((BinaryPredicateOperator) bounds.get(1)).getBinaryType()));
            for (ScalarOperator bound : bounds) {
                assertEquals(time, bound.getChild(0));
            }
            assertEquals(Set.of(ConstantOperator.createDatetime(lower), ConstantOperator.createDatetime(upper)),
                    Set.of(bounds.get(0).getChild(1), bounds.get(1).getChild(1)));

            for (boolean ntz : List.of(false, true)) {
                StructType schema = new StructType().add("dt",
                        ntz ? TimestampNTZType.TIMESTAMP_NTZ : TimestampType.TIMESTAMP);
                var deltaContext = new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, Set.of());
                var converter = new ScalarOperationToDeltaLakeExpr();
                assertEquals(AlwaysTrue.ALWAYS_TRUE.toString(),
                        converter.convert(Utils.extractConjuncts(wrapped), deltaContext).toString());

                Literal lowerLiteral = ntz ? Literal.ofTimestampNtz(1674555201123456L)
                        : Literal.ofTimestamp(lowerMicros);
                Literal upperLiteral = ntz ? Literal.ofTimestampNtz(1677147201654321L)
                        : Literal.ofTimestamp(upperMicros);
                var deltaColumn = new io.delta.kernel.expressions.Column("dt");
                Predicate expected = new And(new Predicate(">=", deltaColumn, lowerLiteral),
                        new Predicate("<", deltaColumn, upperLiteral));
                Predicate actual = converter.convert(Utils.extractConjuncts(rewritten), deltaContext);
                assertEquals(expected.toString(), actual.toString());
            }
        } finally {
            connectContext.getSessionVariable().setTimeZone(previousTimeZone);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    public void doesNotPushStringComparisonForStringColumn(int timestampDepth) {
        ColumnRefFactory refs = new ColumnRefFactory();
        ColumnRefOperator ts = refs.create("ts", VarcharType.VARCHAR, true);
        ScalarOperator value = new CastOperator(DateType.DATETIME, ts, true);
        for (int i = 0; i < timestampDepth; i++) {
            value = timestamp(value);
        }
        ScalarOperator wrapped = new BinaryPredicateOperator(BinaryType.LT, value,
                ConstantOperator.createDatetime(LocalDateTime.of(2023, 2, 23, 10, 13, 21)));
        ScalarOperator rewritten = new ScalarOperatorRewriter().rewrite(
                wrapped.clone(), ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);

        StructType schema = new StructType().add("ts", StringType.STRING);
        var deltaContext = new ScalarOperationToDeltaLakeExpr.DeltaLakeContext(schema, Set.of());
        var converter = new ScalarOperationToDeltaLakeExpr();
        assertEquals(AlwaysTrue.ALWAYS_TRUE.toString(),
                converter.convert(Utils.extractConjuncts(wrapped), deltaContext).toString());

        // '20230125' parses before the bound, but sorts after it as a string.
        // A lexical Delta filter could therefore skip a file containing a matching row.
        assertTrue("20230125".compareTo("2023-02-23 10:13:21") > 0);
        assertEquals(AlwaysTrue.ALWAYS_TRUE.toString(),
                converter.convert(Utils.extractConjuncts(rewritten), deltaContext).toString(),
                "rewritten predicate: " + rewritten);
    }
}
