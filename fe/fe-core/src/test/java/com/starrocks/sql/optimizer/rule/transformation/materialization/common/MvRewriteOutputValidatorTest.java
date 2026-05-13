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

package com.starrocks.sql.optimizer.rule.transformation.materialization.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.common.FeConstants;
import com.starrocks.sql.ast.expression.ExprUtils;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CaseWhenOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.type.IntegerType;
import com.starrocks.type.Type;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

class MvRewriteOutputValidatorTest {

    @Test
    void validCallPasses() {
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {IntegerType.BIGINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator sum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), sumFn);
        Assertions.assertTrue(MvRewriteOutputValidator.isCoherent(sum));
    }

    @Test
    void callWithMismatchedArgTypeFails() {
        // CallOperator claims sum(SMALLINT) but its only child is BIGINT.
        // This is the exact bug the validator must catch.
        ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                new Type[] {IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
        CallOperator badSum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) k3), sumFn);
        Assertions.assertFalse(MvRewriteOutputValidator.isCoherent(badSum));
    }

    @Test
    void callWithNullFunctionPasses() {
        // Some legitimate async-MV-rewrite outputs contain a CallOperator with
        // a null Function reference (notably "cast" pseudo-calls). The validator
        // must tolerate these or it will reject perfectly valid candidates
        // (regression observed in testFilterProject0 prior to commit 18b1635897e).
        ColumnRefOperator child = new ColumnRefOperator(1, IntegerType.BIGINT, "x", true);
        CallOperator nullFnCall = new CallOperator("cast", IntegerType.BIGINT,
                Lists.newArrayList((ScalarOperator) child), null /* fn */);
        Assertions.assertTrue(MvRewriteOutputValidator.isCoherent(nullFnCall),
                "CallOperator with null Function must not be rejected");
    }

    @Test
    void casewhenBranchTypeMismatchFails() {
        // case-when's declared op type is SMALLINT, but its then branch is BIGINT
        // (e.g. left over from a leaf substitution that was not re-derived).
        ColumnRefOperator mv = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
        ConstantOperator zero = ConstantOperator.createTinyInt((byte) 0);
        BinaryPredicateOperator cond = BinaryPredicateOperator.eq(
                new ColumnRefOperator(2, IntegerType.INT, "k2", true),
                ConstantOperator.createInt(0));
        CaseWhenOperator bad = new CaseWhenOperator(IntegerType.SMALLINT,
                null, zero, Lists.newArrayList(cond, mv));
        Assertions.assertFalse(MvRewriteOutputValidator.isCoherent(bad));
    }

    @Test
    void strictModeThrowsOnIncoherentOutput() {
        boolean prev = FeConstants.strictMvRewriteValidator;
        try {
            FeConstants.strictMvRewriteValidator = true;

            // Construct a malformed CallOperator: sum claims SMALLINT arg, but
            // its only child is BIGINT. Same incoherence shape as
            // callWithMismatchedArgTypeFails, but wrapped in an OptExpression.
            ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
            ColumnRefOperator outRef = new ColumnRefOperator(2, IntegerType.BIGINT, "out", true);
            Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                    new Type[] {IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
            CallOperator badSum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                    Lists.newArrayList((ScalarOperator) k3), sumFn);

            Map<ColumnRefOperator, ScalarOperator> projMap = Maps.newHashMap();
            projMap.put(outRef, badSum);
            LogicalProjectOperator projOp = new LogicalProjectOperator(projMap);
            OptExpression expr = OptExpression.create(projOp);

            Assertions.assertThrows(IllegalStateException.class,
                    () -> MvRewriteOutputValidator.validate(expr, "test-mv-id"));
        } finally {
            FeConstants.strictMvRewriteValidator = prev;
        }
    }

    @Test
    void nonStrictModeReturnsFalseOnIncoherentOutput() {
        boolean prev = FeConstants.strictMvRewriteValidator;
        try {
            FeConstants.strictMvRewriteValidator = false;

            ColumnRefOperator k3 = new ColumnRefOperator(1, IntegerType.BIGINT, "mv_sum_k3", true);
            ColumnRefOperator outRef = new ColumnRefOperator(2, IntegerType.BIGINT, "out", true);
            Function sumFn = ExprUtils.getBuiltinFunction(FunctionSet.SUM,
                    new Type[] {IntegerType.SMALLINT}, Function.CompareMode.IS_IDENTICAL);
            CallOperator badSum = new CallOperator(FunctionSet.SUM, IntegerType.BIGINT,
                    Lists.newArrayList((ScalarOperator) k3), sumFn);

            Map<ColumnRefOperator, ScalarOperator> projMap = Maps.newHashMap();
            projMap.put(outRef, badSum);
            LogicalProjectOperator projOp = new LogicalProjectOperator(projMap);
            OptExpression expr = OptExpression.create(projOp);

            Assertions.assertFalse(MvRewriteOutputValidator.validate(expr, "test-mv-id"));
        } finally {
            FeConstants.strictMvRewriteValidator = prev;
        }
    }
}
