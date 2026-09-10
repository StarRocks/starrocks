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

import com.google.common.collect.ImmutableList;
import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.JsonType;
import com.starrocks.type.Type;
import com.starrocks.type.VarcharType;
import com.starrocks.type.VariantType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

public class SimplifiedScanColumnRuleTest {
    private final SimplifiedScanColumnRule rule = new SimplifiedScanColumnRule();
    private final ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();

    private ScalarOperator rewriteIsNull(ScalarOperator child, boolean isNotNull) {
        return rule.visitIsNullPredicate(new IsNullPredicateOperator(isNotNull, child), context);
    }

    private static ColumnRefOperator column(Type type, boolean nullable) {
        return new ColumnRefOperator(1, type, "c", nullable);
    }

    /**
     * A column declared NOT NULL can never hold SQL NULL, so IS NULL is constant false and
     * IS NOT NULL is constant true. This is the optimization the rule exists for, and it must
     * keep working for ordinary types.
     */
    @Test
    public void testFoldOnNotNullScalarColumn() {
        for (Type type : ImmutableList.of(IntegerType.INT, IntegerType.BIGINT, VarcharType.VARCHAR)) {
            assertEquals(ConstantOperator.createBoolean(false), rewriteIsNull(column(type, false), false),
                    "IS NULL on a NOT NULL " + type + " column should fold to false");
            assertEquals(ConstantOperator.createBoolean(true), rewriteIsNull(column(type, false), true),
                    "IS NOT NULL on a NOT NULL " + type + " column should fold to true");
        }
    }

    /**
     * A JSON column declared NOT NULL never holds SQL NULL either, but it can hold the JSON `null`
     * literal, for which IS NULL is documented to return true (release notes of 3.1/3.2/3.3,
     * "Behavior Changes"). Folding on the declared nullability answered `false` for those rows in
     * WHERE while the projection of the very same expression answered `true`. The predicate must
     * therefore survive the rule and be evaluated.
     */
    @Test
    public void testNoFoldOnNotNullJsonColumn() {
        ColumnRefOperator json = column(JsonType.JSON, false);

        IsNullPredicateOperator isNull = new IsNullPredicateOperator(false, json);
        assertSame(isNull, rule.visitIsNullPredicate(isNull, context));

        IsNullPredicateOperator isNotNull = new IsNullPredicateOperator(true, json);
        assertSame(isNotNull, rule.visitIsNullPredicate(isNotNull, context));
    }

    /**
     * The carve-out is deliberately limited to JSON. VARIANT has no such literal-null semantics on
     * the BE side -- VectorizedIsNullPredicate special-cases `column->is_json()` only -- so folding
     * a VARIANT column stays consistent with how the predicate would have been evaluated.
     */
    @Test
    public void testFoldStillAppliesToNotNullVariantColumn() {
        assertEquals(ConstantOperator.createBoolean(false),
                rewriteIsNull(column(VariantType.VARIANT, false), false));
        assertEquals(ConstantOperator.createBoolean(true),
                rewriteIsNull(column(VariantType.VARIANT, false), true));
    }

    /**
     * A nullable column tells us nothing, so nothing is folded -- JSON included.
     */
    @Test
    public void testNoFoldOnNullableColumn() {
        for (Type type : ImmutableList.of(IntegerType.INT, VarcharType.VARCHAR, JsonType.JSON)) {
            IsNullPredicateOperator isNull = new IsNullPredicateOperator(false, column(type, true));
            assertSame(isNull, rule.visitIsNullPredicate(isNull, context));

            IsNullPredicateOperator isNotNull = new IsNullPredicateOperator(true, column(type, true));
            assertSame(isNotNull, rule.visitIsNullPredicate(isNotNull, context));
        }
    }

    /**
     * The rule only ever looked at bare column references; an expression is left alone whatever
     * its nullability says.
     */
    @Test
    public void testNoFoldOnNonColumnRef() {
        ScalarOperator call = new CallOperator("abs", IntegerType.INT,
                ImmutableList.of(column(IntegerType.INT, false)));
        IsNullPredicateOperator isNull = new IsNullPredicateOperator(false, call);
        assertSame(isNull, rule.visitIsNullPredicate(isNull, context));
    }

    /**
     * The self-comparison folding in the same rule is untouched. `j = j` agrees with the BE for a
     * JSON column -- the JSON null literal compares equal to itself -- so it keeps folding, and so
     * do ordinary types.
     */
    @Test
    public void testSelfComparisonFoldingUnchanged() {
        for (Type type : ImmutableList.of(IntegerType.INT, JsonType.JSON)) {
            ColumnRefOperator col = column(type, false);
            assertEquals(ConstantOperator.createBoolean(true),
                    rule.visitBinaryPredicate(new BinaryPredicateOperator(BinaryType.EQ, col, col), context));
            assertEquals(ConstantOperator.createBoolean(false),
                    rule.visitBinaryPredicate(new BinaryPredicateOperator(BinaryType.NE, col, col), context));
        }
    }
}
