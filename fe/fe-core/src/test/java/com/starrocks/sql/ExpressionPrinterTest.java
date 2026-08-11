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

package com.starrocks.sql;

import com.starrocks.sql.ast.expression.MatchExpr;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.MatchExprOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.SimplifiedPredicateRule;
import com.starrocks.type.StringType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ExpressionPrinterTest {
    @Test
    public void testMatchOperatorIsPreserved() {
        ColumnRefOperator column = new ColumnRefOperator(1, StringType.STRING, "content", true);
        ConstantOperator query = ConstantOperator.createVarchar("foo bar");
        ExpressionPrinter<Void> printer = new ExpressionPrinter<>();
        for (MatchExpr.MatchOperator operator : MatchExpr.MatchOperator.values()) {
            MatchExprOperator predicate = new MatchExprOperator(operator, column, query);
            assertTrue(printer.print(predicate).contains(" " + operator.getName() + " "));
        }
    }

    @Test
    public void testMatchOperatorIdentityIncludesOperatorKind() {
        ColumnRefOperator column = new ColumnRefOperator(1, StringType.STRING, "content", true);
        ConstantOperator query = ConstantOperator.createVarchar("foo bar");
        MatchExprOperator match = new MatchExprOperator(MatchExpr.MatchOperator.MATCH, column, query);
        MatchExprOperator anotherMatch = new MatchExprOperator(MatchExpr.MatchOperator.MATCH, column, query);
        MatchExprOperator any = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ANY, column, query);
        MatchExprOperator anotherAny = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ANY, column, query);
        MatchExprOperator all = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ALL, column, query);
        MatchExprOperator anotherAll = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ALL, column, query);

        assertTrue(match.equalsSelf(anotherMatch));
        assertTrue(any.equalsSelf(anotherAny));
        assertTrue(all.equalsSelf(anotherAll));
        assertEquals(match.hashCodeSelf(), anotherMatch.hashCodeSelf());
        assertEquals(any.hashCodeSelf(), anotherAny.hashCodeSelf());
        assertEquals(all.hashCodeSelf(), anotherAll.hashCodeSelf());
        assertEquals(match, anotherMatch);
        assertEquals(any, anotherAny);
        assertEquals(all, anotherAll);
        assertEquals(match.hashCode(), anotherMatch.hashCode());
        assertEquals(any.hashCode(), anotherAny.hashCode());
        assertEquals(all.hashCode(), anotherAll.hashCode());

        assertFalse(match.equalsSelf(any));
        assertFalse(any.equalsSelf(all));
        assertFalse(match.equalsSelf(all));
        assertNotEquals(match, any);
        assertNotEquals(any, all);
        assertNotEquals(match, all);
    }

    @Test
    public void testDeMorganRewriteAcceptsMatchLeaves() {
        ColumnRefOperator column = new ColumnRefOperator(1, StringType.STRING, "content", true);
        MatchExprOperator any = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ANY,
                column, ConstantOperator.createVarchar("foo"));
        MatchExprOperator all = new MatchExprOperator(MatchExpr.MatchOperator.MATCH_ALL,
                column, ConstantOperator.createVarchar("bar"));
        CompoundPredicateOperator or = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.OR, any, all);
        CompoundPredicateOperator not = new CompoundPredicateOperator(
                CompoundPredicateOperator.CompoundType.NOT, or);

        ScalarOperator rewritten = new SimplifiedPredicateRule().apply(not, null);
        CompoundPredicateOperator and = (CompoundPredicateOperator) rewritten;
        assertEquals(CompoundPredicateOperator.CompoundType.AND, and.getCompoundType());
        assertTrue(((CompoundPredicateOperator) and.getChild(0)).isNot());
        assertTrue(((CompoundPredicateOperator) and.getChild(1)).isNot());
        assertEquals(MatchExpr.MatchOperator.MATCH_ANY,
                ((MatchExprOperator) and.getChild(0).getChild(0)).getMatchOperator());
        assertEquals(MatchExpr.MatchOperator.MATCH_ALL,
                ((MatchExprOperator) and.getChild(1).getChild(0)).getMatchOperator());
    }
}
