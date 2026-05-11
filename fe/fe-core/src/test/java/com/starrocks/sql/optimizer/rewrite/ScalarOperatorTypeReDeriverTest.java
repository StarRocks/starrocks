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

package com.starrocks.sql.optimizer.rewrite;

import com.google.common.collect.Lists;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.InPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.IsNullPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.BooleanType;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ScalarOperatorTypeReDeriverTest {

    @Test
    void columnRefPassesThrough() {
        ColumnRefOperator col = new ColumnRefOperator(1, IntegerType.BIGINT, "k3", true);
        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(col);
        Assertions.assertSame(col, out);
        Assertions.assertEquals(IntegerType.BIGINT, out.getType());
    }

    @Test
    void constantPassesThrough() {
        ConstantOperator c = ConstantOperator.createInt(42);
        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(c);
        Assertions.assertSame(c, out);
    }

    @Test
    void callRebindsFunctionWhenChildTypeWidens() {
        // add(SMALLINT k3, SMALLINT 1) — but left child is now BIGINT (mv_sum_k3 after substitution).
        // The original fn is ADD(SMALLINT, SMALLINT) -> SMALLINT.
        // After re-derive with BIGINT left child: expect ADD(BIGINT, BIGINT) -> BIGINT.
        ColumnRefOperator left = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        ConstantOperator right = ConstantOperator.createSmallInt((short) 1);
        Function origFn = ExprUtils.getBuiltinFunction(FunctionSet.ADD,
                new Type[] {IntegerType.SMALLINT, IntegerType.SMALLINT},
                Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(origFn, "ADD(SMALLINT,SMALLINT) builtin must exist");
        CallOperator call = new CallOperator(FunctionSet.ADD, IntegerType.SMALLINT,
                Lists.newArrayList(left, right), origFn);

        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(call);
        Assertions.assertTrue(out instanceof CallOperator, "result must be a CallOperator");
        CallOperator newCall = (CallOperator) out;
        // The rebound function's return type must equal the call's declared type
        // and must reflect the widened child (BIGINT, not SMALLINT).
        Assertions.assertEquals(IntegerType.BIGINT, newCall.getType(),
                "after re-deriving, ADD(BIGINT, SMALLINT) returns BIGINT");
        Assertions.assertEquals(IntegerType.BIGINT, newCall.getFunction().getReturnType());
        Assertions.assertEquals(IntegerType.BIGINT, newCall.getFunction().getArgs()[0]);
    }

    @Test
    void callThrowsWhenNoCompatibleFunction() {
        ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
        CallOperator call = new CallOperator("not_a_real_function_xyz_123", IntegerType.BIGINT,
                Lists.newArrayList(c), null);
        Assertions.assertThrows(TypeReDeriveException.class,
                () -> ScalarOperatorTypeReDeriver.reDerive(call));
    }

    @Test
    void callSumWidensSmallintToBigint() {
        // sum(SMALLINT k3) — declared. After substitution child becomes BIGINT (mv_sum_k3).
        // Expected: rebound sum(BIGINT) → BIGINT, with the function's argType updated
        // to BIGINT (not SMALLINT).
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        Function origSum = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
        Assertions.assertNotNull(origSum, "sum(SMALLINT) builtin must exist");

        // The original CallOperator may already declare BIGINT as its return type
        // because sum's intermediate type is BIGINT — that's how the bug appears in
        // the wild. We construct it as the bug would: claimed type BIGINT, but
        // function still claims SMALLINT input.
        CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), origSum);

        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(sum);
        Assertions.assertTrue(out instanceof CallOperator);
        CallOperator newSum = (CallOperator) out;
        Assertions.assertEquals(IntegerType.BIGINT, newSum.getFunction().getArgs()[0],
                "sum's argType must follow child after re-derive");
        Assertions.assertEquals(IntegerType.BIGINT, newSum.getType());
        Assertions.assertEquals(IntegerType.BIGINT, newSum.getFunction().getReturnType());
    }

    @Test
    void ifBranchesUnifyToCommonSuperType() {
        // if(k2 = 0, mv_sum_k3 (BIGINT), 0 (TINYINT))
        // Expected: if(BOOLEAN, BIGINT, BIGINT) returning BIGINT
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
        BinaryPredicateOperator cond = BinaryPredicateOperator.eq(
                new ColumnRefOperator(2, IntegerType.INT, "k2", true),
                ConstantOperator.createInt(0));
        Function origIf = ExprUtils.getBuiltinFunction(FunctionSet.IF,
                new Type[] {BooleanType.BOOLEAN, IntegerType.SMALLINT, IntegerType.TINYINT},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNotNull(origIf, "if(BOOL,SMALLINT,TINYINT) builtin must exist");
        CallOperator ifOp = new CallOperator(FunctionSet.IF, IntegerType.SMALLINT,
                Lists.newArrayList(cond, mv, zero), origIf);

        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(ifOp);
        Assertions.assertTrue(out instanceof CallOperator);
        CallOperator newIf = (CallOperator) out;
        Assertions.assertEquals(IntegerType.BIGINT, newIf.getType(),
                "if's return type follows unified branch type");
        Assertions.assertEquals(IntegerType.BIGINT, newIf.getChild(1).getType(),
                "then branch already BIGINT (mv ref)");
        Assertions.assertEquals(IntegerType.BIGINT, newIf.getChild(2).getType(),
                "else branch widened from TINYINT to BIGINT");
    }

    @Test
    void casewhenUnifiesValueClauses() {
        // case when k2 = 0 then mv_sum_k3 (BIGINT) else 0 (TINYINT) end
        // Expected unified type: BIGINT
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
        BinaryPredicateOperator cond = BinaryPredicateOperator.eq(
                new ColumnRefOperator(2, IntegerType.INT, "k2", true),
                ConstantOperator.createInt(0));
        CaseWhenOperator caseOp = new CaseWhenOperator(IntegerType.SMALLINT,
                null, zero, Lists.newArrayList(cond, mv));

        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(caseOp);
        Assertions.assertTrue(out instanceof CaseWhenOperator);
        CaseWhenOperator newCase = (CaseWhenOperator) out;
        Assertions.assertEquals(IntegerType.BIGINT, newCase.getType());
        Assertions.assertEquals(IntegerType.BIGINT, newCase.getThenClause(0).getType());
        Assertions.assertEquals(IntegerType.BIGINT, newCase.getElseClause().getType());
    }

    @Test
    void explicitCastTargetPreserved() {
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        CastOperator cast = new CastOperator(IntegerType.SMALLINT, mv, false /* explicit */);
        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(cast);
        Assertions.assertTrue(out instanceof CastOperator);
        Assertions.assertEquals(IntegerType.SMALLINT, out.getType(),
                "explicit cast target preserved despite child being BIGINT");
    }

    @Test
    void implicitCastDroppedWhenRedundant() {
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        CastOperator cast = new CastOperator(IntegerType.BIGINT, mv, true /* implicit */);
        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(cast);
        Assertions.assertSame(mv, out, "redundant implicit cast should be unwrapped");
    }

    @Test
    void binaryPredicateAlignsChildrenWithCommonSuperType() {
        // mv_sum_k3 (BIGINT) = 0 (TINYINT)
        // Expected: both children unified to BIGINT, predicate type stays BOOLEAN.
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
        BinaryPredicateOperator pred = BinaryPredicateOperator.eq(mv, zero);

        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(pred);
        Assertions.assertTrue(out instanceof BinaryPredicateOperator);
        BinaryPredicateOperator newPred = (BinaryPredicateOperator) out;
        Assertions.assertEquals(BooleanType.BOOLEAN, newPred.getType());
        Assertions.assertEquals(IntegerType.BIGINT, newPred.getChild(0).getType());
        Assertions.assertEquals(IntegerType.BIGINT, newPred.getChild(1).getType());
    }

    @Test
    void inPredicateAlignsChildrenWithCommonSuperType() {
        // mv_sum_k3 (BIGINT) IN (1 (TINYINT), 2 (INT))
        // Expected: all children unified to BIGINT, predicate type stays BOOLEAN.
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        ConstantOperator one = ConstantOperator.createTinyInt((byte) 1);
        ConstantOperator two = ConstantOperator.createInt(2);
        InPredicateOperator inPred = new InPredicateOperator(false, mv, one, two);

        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(inPred);
        Assertions.assertTrue(out instanceof InPredicateOperator);
        InPredicateOperator newIn = (InPredicateOperator) out;
        Assertions.assertEquals(BooleanType.BOOLEAN, newIn.getType());
        Assertions.assertEquals(IntegerType.BIGINT, newIn.getChild(0).getType());
        Assertions.assertEquals(IntegerType.BIGINT, newIn.getChild(1).getType());
        Assertions.assertEquals(IntegerType.BIGINT, newIn.getChild(2).getType());
    }

    @Test
    void coalesceBranchesUnifyToCommonSuperType() {
        // coalesce(mv_sum_k3 (BIGINT), 0 (TINYINT))
        // Expected: coalesce(BIGINT, BIGINT) returning BIGINT
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
        Function origCoalesce = ExprUtils.getBuiltinFunction(FunctionSet.COALESCE,
                new Type[] {IntegerType.SMALLINT, IntegerType.TINYINT},
                Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
        Assertions.assertNotNull(origCoalesce, "coalesce(SMALLINT,TINYINT) builtin must exist");
        CallOperator coalesce = new CallOperator(FunctionSet.COALESCE, IntegerType.SMALLINT,
                Lists.newArrayList((ScalarOperator) mv, zero), origCoalesce);

        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(coalesce);
        Assertions.assertTrue(out instanceof CallOperator);
        CallOperator newCoalesce = (CallOperator) out;
        Assertions.assertEquals(IntegerType.BIGINT, newCoalesce.getType());
        Assertions.assertEquals(IntegerType.BIGINT, newCoalesce.getChild(0).getType());
        Assertions.assertEquals(IntegerType.BIGINT, newCoalesce.getChild(1).getType());
    }

    @Test
    void unknownShapePassesThroughWhenNoChange() {
        // IsNullPredicate over a leaf — child unchanged → pass through.
        ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
        IsNullPredicateOperator inull = new IsNullPredicateOperator(false, c);
        ScalarOperator out = ScalarOperatorTypeReDeriver.reDerive(inull);
        Assertions.assertSame(inull, out);
    }

    @Test
    void unknownShapeThrowsWhenChildChanged() {
        // IsNullPredicate around a redundant implicit cast (which gets dropped),
        // so the child identity changes → default fallback must throw.
        ColumnRefOperator c = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
        CastOperator implicit = new CastOperator(IntegerType.BIGINT, c, true /* implicit, redundant */);
        IsNullPredicateOperator inull = new IsNullPredicateOperator(false, implicit);
        Assertions.assertThrows(TypeReDeriveException.class,
                () -> ScalarOperatorTypeReDeriver.reDerive(inull));
    }
}
