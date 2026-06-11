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

import com.starrocks.sql.ast.expression.BinaryType;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.ScalarOperatorRewriteContext;
import com.starrocks.type.IntegerType;
import com.starrocks.type.VarcharType;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DeriveGuardPredicateRuleTest {

    // Column helpers
    private static ColumnRefOperator colInt(int id, String name) {
        return new ColumnRefOperator(id, IntegerType.INT, name, true);
    }

    private static ColumnRefOperator colVarchar(int id, String name) {
        return new ColumnRefOperator(id, VarcharType.VARCHAR, name, true);
    }

    // Build a proper left-deep AND tree (matching optimizer output) via Utils.compoundAnd
    private static ScalarOperator and(ScalarOperator... children) {
        return Utils.compoundAnd(children);
    }

    // Build a proper left-deep OR tree via Utils.compoundOr
    private static ScalarOperator or(ScalarOperator... children) {
        return Utils.compoundOr(children);
    }

    private static ScalarOperator eq(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.EQ, left, right);
    }

    private static ScalarOperator ge(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.GE, left, right);
    }

    private static ScalarOperator le(ScalarOperator left, ScalarOperator right) {
        return new BinaryPredicateOperator(BinaryType.LE, left, right);
    }

    @Test
    public void testNarrowDateOR() {
        // (l_shipdate >= 1 AND l_shipdate <= 1 AND l_returnflag = 'R')
        // OR (l_shipdate >= 15 AND l_shipdate <= 15 AND l_returnflag = 'A')
        // => AND(guard_shipdate, original_OR)
        ColumnRefOperator shipdate = colInt(1, "l_shipdate");
        ColumnRefOperator returnflag = colVarchar(2, "l_returnflag");

        ScalarOperator orExpr = or(
                and(ge(shipdate, ConstantOperator.createInt(1)),
                    le(shipdate, ConstantOperator.createInt(1)),
                    eq(returnflag, ConstantOperator.createVarchar("R"))),
                and(ge(shipdate, ConstantOperator.createInt(15)),
                    le(shipdate, ConstantOperator.createInt(15)),
                    eq(returnflag, ConstantOperator.createVarchar("A")))
        );

        ScalarOperator derived = DeriveGuardPredicateRule.tryDeriveGuard(
                (CompoundPredicateOperator) orExpr);

        ScalarOperator expectedGuard = or(
                and(ge(shipdate, ConstantOperator.createInt(1)), le(shipdate, ConstantOperator.createInt(1))),
                and(ge(shipdate, ConstantOperator.createInt(15)), le(shipdate, ConstantOperator.createInt(15)))
        );
        ScalarOperator expected = and(expectedGuard, orExpr);
        assertEquals(expected, derived);
    }

    @Test
    public void testNoCommonColumn() {
        // (l_returnflag = 'R') OR (l_linestatus = 'O') => no guard
        ColumnRefOperator returnflag = colVarchar(1, "l_returnflag");
        ColumnRefOperator linestatus = colVarchar(2, "l_linestatus");

        ScalarOperator orExpr = or(
                eq(returnflag, ConstantOperator.createVarchar("R")),
                eq(linestatus, ConstantOperator.createVarchar("O"))
        );

        ScalarOperator result = DeriveGuardPredicateRule.tryDeriveGuard(
                (CompoundPredicateOperator) orExpr);
        assertEquals(orExpr, result);
    }

    @Test
    public void testSingleDisjunct() {
        // Single disjunct (not really an OR) — no rewriting
        ScalarOperator expr = eq(colInt(1, "a"), ConstantOperator.createInt(1));
        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();
        ScalarOperator result = new DeriveGuardPredicateRule().apply(expr, context);
        assertEquals(expr, result);
    }

    @Test
    public void testAllBranchesOnlyColumnPreds() {
        // (l_returnflag = 'R') OR (l_returnflag = 'A')
        // EQ-only predicates are not range predicates => no guard
        ColumnRefOperator returnflag = colVarchar(1, "l_returnflag");

        ScalarOperator orExpr = or(
                eq(returnflag, ConstantOperator.createVarchar("R")),
                eq(returnflag, ConstantOperator.createVarchar("A"))
        );

        ScalarOperator result = DeriveGuardPredicateRule.tryDeriveGuard(
                (CompoundPredicateOperator) orExpr);
        assertEquals(orExpr, result);
    }

    @Test
    public void testAndPredicate() {
        // AND predicate — not processed
        ColumnRefOperator a = colInt(1, "a");
        ColumnRefOperator b = colInt(2, "b");

        ScalarOperator andExpr = and(
                eq(a, ConstantOperator.createInt(1)),
                eq(b, ConstantOperator.createInt(2))
        );

        ScalarOperatorRewriteContext context = new ScalarOperatorRewriteContext();
        ScalarOperator result = new DeriveGuardPredicateRule().apply(andExpr, context);
        assertEquals(andExpr, result);
    }

    @Test
    public void testThreeBranches() {
        // Three OR branches, all have l_shipdate (GE+LE) + l_discount (EQ on INT).
        // Both columns get guards:
        //   guard_shipdate: OR over shipdate ranges
        //   guard_discount:  OR over discount EQ values (INT type, valid candidate)
        ColumnRefOperator shipdate = colInt(1, "l_shipdate");
        ColumnRefOperator discount = colInt(2, "l_discount");

        ScalarOperator orExpr = or(
                and(ge(shipdate, ConstantOperator.createInt(1)),
                    le(shipdate, ConstantOperator.createInt(10)),
                    eq(discount, ConstantOperator.createInt(5))),
                and(ge(shipdate, ConstantOperator.createInt(20)),
                    le(shipdate, ConstantOperator.createInt(30)),
                    eq(discount, ConstantOperator.createInt(6))),
                and(ge(shipdate, ConstantOperator.createInt(40)),
                    le(shipdate, ConstantOperator.createInt(50)),
                    eq(discount, ConstantOperator.createInt(7)))
        );

        ScalarOperator derived = DeriveGuardPredicateRule.tryDeriveGuard(
                (CompoundPredicateOperator) orExpr);

        ScalarOperator guardShipdate = or(
                and(ge(shipdate, ConstantOperator.createInt(1)), le(shipdate, ConstantOperator.createInt(10))),
                and(ge(shipdate, ConstantOperator.createInt(20)), le(shipdate, ConstantOperator.createInt(30))),
                and(ge(shipdate, ConstantOperator.createInt(40)), le(shipdate, ConstantOperator.createInt(50)))
        );
        ScalarOperator guardDiscount = or(
                eq(discount, ConstantOperator.createInt(5)),
                eq(discount, ConstantOperator.createInt(6)),
                eq(discount, ConstantOperator.createInt(7))
        );
        ScalarOperator expected = and(guardShipdate, guardDiscount, orExpr);
        assertEquals(expected, derived);
    }

    @Test
    public void testEQOnNumericColumn() {
        // EQ on INT columns should produce guards.
        // Both l_shipdate (INT) and l_discount (INT) are valid candidates.
        ColumnRefOperator shipdate = colInt(1, "l_shipdate");
        ColumnRefOperator discount = colInt(2, "l_discount");

        ScalarOperator orExpr = or(
                and(eq(shipdate, ConstantOperator.createInt(1)),
                    eq(discount, ConstantOperator.createInt(5))),
                and(eq(shipdate, ConstantOperator.createInt(15)),
                    eq(discount, ConstantOperator.createInt(6)))
        );

        ScalarOperator derived = DeriveGuardPredicateRule.tryDeriveGuard(
                (CompoundPredicateOperator) orExpr);

        // Guards for both columns
        ScalarOperator guardShipdate = or(
                eq(shipdate, ConstantOperator.createInt(1)),
                eq(shipdate, ConstantOperator.createInt(15))
        );
        ScalarOperator guardDiscount = or(
                eq(discount, ConstantOperator.createInt(5)),
                eq(discount, ConstantOperator.createInt(6))
        );
        ScalarOperator expected = and(guardShipdate, guardDiscount, orExpr);
        assertEquals(expected, derived);
    }

    @Test
    public void testEQOnVarcharColumn() {
        // EQ on varchar column should NOT produce guard (low cardinality risk)
        // (l_returnflag = 'R') OR (l_returnflag = 'A')
        ColumnRefOperator returnflag = colVarchar(1, "l_returnflag");

        ScalarOperator orExpr = or(
                eq(returnflag, ConstantOperator.createVarchar("R")),
                eq(returnflag, ConstantOperator.createVarchar("A"))
        );

        ScalarOperator derived = DeriveGuardPredicateRule.tryDeriveGuard(
                (CompoundPredicateOperator) orExpr);
        // No guard expected: EQ on VARCHAR is skipped
        assertEquals(orExpr, derived);
    }
}
