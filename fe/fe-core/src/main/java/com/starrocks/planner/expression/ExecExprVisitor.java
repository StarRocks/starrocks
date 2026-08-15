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

package com.starrocks.planner.expression;

/**
 * Visitor interface for the {@link ExecExpr} hierarchy.
 * Every visit method defaults to {@link #visitExecExpr} so that callers only
 * need to override the node types they care about.
 */
public interface ExecExprVisitor<R, C> {

    R visitExecExpr(ExecExpr expr, C context);

    default R visitExecSlotRef(ExecSlotRef expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecLiteral(ExecLiteral expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecFunctionCall(ExecFunctionCall expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecCast(ExecCast expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecBinaryPredicate(ExecBinaryPredicate expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecCompoundPredicate(ExecCompoundPredicate expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecInPredicate(ExecInPredicate expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecIsNullPredicate(ExecIsNullPredicate expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecLikePredicate(ExecLikePredicate expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecBetweenPredicate(ExecBetweenPredicate expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecCaseWhen(ExecCaseWhen expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecMatchExpr(ExecMatchExpr expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecArrayExpr(ExecArrayExpr expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecMapExpr(ExecMapExpr expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecCollectionElement(ExecCollectionElement expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecArraySlice(ExecArraySlice expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecSubfield(ExecSubfield expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecLambdaFunction(ExecLambdaFunction expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecDictMapping(ExecDictMapping expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecClone(ExecClone expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecDictQuery(ExecDictQuery expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecDictionaryGet(ExecDictionaryGet expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecPlaceHolder(ExecPlaceHolder expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecArithmetic(ExecArithmetic expr, C context) {
        return visitExecExpr(expr, context);
    }

    default R visitExecInformationFunction(ExecInformationFunction expr, C context) {
        return visitExecExpr(expr, context);
    }
}
