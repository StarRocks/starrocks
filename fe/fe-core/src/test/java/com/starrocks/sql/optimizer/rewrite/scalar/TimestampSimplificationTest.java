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

package com.starrocks.sql.optimizer.rewrite.scalar;

import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionName;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.ScalarFunction;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriter;
import com.starrocks.sql.plan.PlanTestBase;
import com.starrocks.thrift.TFunctionBinaryType;
import com.starrocks.type.DateType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TimestampSimplificationTest extends PlanTestBase {
    private Function timestampFunction() {
        Function fn = ExprUtils.getBuiltinFunction(FunctionSet.TIMESTAMP,
                new Type[] {DateType.DATETIME}, Function.CompareMode.IS_IDENTICAL);
        assertNotNull(fn);
        return fn;
    }

    private CallOperator timestamp(ScalarOperator child) {
        return new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME,
                List.of(child), timestampFunction());
    }

    @Test
    public void preservesTypedArgumentAndNullability() {
        for (boolean nullable : List.of(false, true)) {
            ColumnRefOperator child = new ColumnRefOperator(1, DateType.DATETIME, "dt", nullable);
            ScalarOperator result = new SimplifiedPredicateRule().apply(timestamp(child), null);
            assertSame(child, result);
            assertEquals(DateType.DATETIME, result.getType());
            assertEquals(nullable, result.isNullable());
        }
    }

    @Test
    public void preservesNullAndFractionalTimestamp() {
        ConstantOperator nullValue = ConstantOperator.createNull(DateType.DATETIME);
        ConstantOperator value = ConstantOperator.createDatetime(
                LocalDateTime.of(2023, 1, 25, 0, 30, 51, 868051000));
        for (ConstantOperator child : List.of(nullValue, value)) {
            ScalarOperator result = new SimplifiedPredicateRule().apply(timestamp(child), null);
            assertSame(child, result);
            assertEquals(child.isNull(), ((ConstantOperator) result).isNull());
        }
        assertEquals(868051000, value.getDatetime().getNano());
    }

    @Test
    public void preservesRequiredImplicitCasts() {
        for (Type source : List.of(VarcharType.VARCHAR, DateType.DATE)) {
            ColumnRefOperator child = new ColumnRefOperator(1, source, "input", true);
            CallOperator call = timestamp(child);
            ScalarOperator result = new ScalarOperatorRewriter().rewrite(call,
                    List.of(new ImplicitCastRule(), new SimplifiedPredicateRule()));
            assertSame(call, result);
            assertTrue(result.getChild(0) instanceof CastOperator);
            CastOperator cast = (CastOperator) result.getChild(0);
            assertEquals(DateType.DATETIME, cast.getType());
            assertSame(child, cast.getChild(0));
            assertTrue(cast.isImplicit());
        }
    }

    @Test
    public void preservesExplicitCastsIncludingInvalidStringInput() {
        for (ScalarOperator child : List.of(
                new ColumnRefOperator(1, VarcharType.VARCHAR, "text", true),
                new ColumnRefOperator(2, DateType.DATE, "date_col", true),
                ConstantOperator.createVarchar("not-a-date"))) {
            CastOperator cast = new CastOperator(DateType.DATETIME, child);
            CallOperator call = timestamp(cast);
            ScalarOperator result = new SimplifiedPredicateRule().apply(call, null);
            assertSame(call, result);
            assertSame(cast, result.getChild(0));
            assertSame(child, result.getChild(0).getChild(0));
        }
    }

    @Test
    public void removesNestedWrappersInBothDefaultPipelines() {
        ColumnRefOperator child = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        ScalarOperator result = new ScalarOperatorRewriter().rewrite(
                timestamp(timestamp(child)), ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        assertSame(child, result);
        result = new ScalarOperatorRewriter().rewrite(
                timestamp(timestamp(child)), ScalarOperatorRewriter.DEFAULT_REWRITE_SCAN_PREDICATE_RULES);
        assertSame(child, result);
    }

    @Test
    public void removesRedundantDatetimeCastBeforeSimplifying() {
        ColumnRefOperator child = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        ScalarOperator result = new ScalarOperatorRewriter().rewrite(
                timestamp(new CastOperator(DateType.DATETIME, child)), ScalarOperatorRewriter.DEFAULT_REWRITE_RULES);
        assertSame(child, result);
    }

    @Test
    public void recognizesMixedCaseCallName() {
        ColumnRefOperator child = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        CallOperator call = new CallOperator("TiMeStAmP", DateType.DATETIME,
                List.of(child), timestampFunction());
        assertSame(child, new SimplifiedPredicateRule().apply(call, null));
    }

    @Test
    public void leavesUnsupportedFunctionIdentitiesIntact() {
        ColumnRefOperator child = new ColumnRefOperator(1, DateType.DATETIME, "dt", true);
        Function external = timestampFunction().copy();
        external.setBinaryType(TFunctionBinaryType.HIVE);
        Function variadic = timestampFunction().copy();
        variadic.setHasVarArgs(true);
        Function otherSignature = new ScalarFunction(new FunctionName(FunctionSet.TIMESTAMP),
                new Type[] {DateType.DATE}, DateType.DATETIME, false);
        otherSignature.setBinaryType(TFunctionBinaryType.BUILTIN);
        Function otherReturnType = new ScalarFunction(new FunctionName(FunctionSet.TIMESTAMP),
                new Type[] {DateType.DATETIME}, DateType.DATE, false);
        otherReturnType.setBinaryType(TFunctionBinaryType.BUILTIN);
        Function otherName = new ScalarFunction(new FunctionName("different_function"),
                new Type[] {DateType.DATETIME}, DateType.DATETIME, false);
        otherName.setBinaryType(TFunctionBinaryType.BUILTIN);

        List<CallOperator> calls = List.of(
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(child)),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(child), external),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(child), variadic),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(child), otherSignature),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(child), otherReturnType),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(child), otherName),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATE, List.of(child), timestampFunction()),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME, List.of(), timestampFunction()),
                new CallOperator(FunctionSet.TIMESTAMP, DateType.DATETIME,
                        List.of(child, child), timestampFunction()),
                timestamp(new ColumnRefOperator(2, DateType.DATE, "untyped_date", true)));
        for (CallOperator call : calls) {
            assertSame(call, new SimplifiedPredicateRule().apply(call, null));
        }
    }

    @Test
    public void removesWrappersThroughParsedCteAliases() throws Exception {
        String sql = """
                WITH first_projection AS (
                    SELECT timestamp(id_datetime) AS dt FROM test_all_type
                ), second_projection AS (
                    SELECT timestamp(dt) AS dt FROM first_projection
                )
                SELECT count(*) FROM second_projection
                WHERE dt >= CAST('2023-01-24 10:13:21' AS DATETIME)
                  AND dt < CAST('2023-02-23 10:13:21' AS DATETIME)
                """;
        String plan = getFragmentPlan(sql);
        assertNotContains(plan.toLowerCase(Locale.ROOT), "timestamp(");
        assertContains(plan, "2023-01-24 10:13:21", "2023-02-23 10:13:21");
    }
}
