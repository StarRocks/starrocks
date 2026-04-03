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

import com.google.common.base.Preconditions;
import com.starrocks.sql.ast.AssertNumRowsElement;
import com.starrocks.sql.ast.JoinOperator;
import com.starrocks.sql.ast.KeysType;
import com.starrocks.sql.ast.SetType;
import com.starrocks.sql.ast.expression.AnalyticWindow;
import com.starrocks.sql.ast.expression.AnalyticWindowBoundary;
import com.starrocks.sql.ast.expression.CompoundPredicate;
import com.starrocks.thrift.TAnalyticWindow;
import com.starrocks.thrift.TAnalyticWindowBoundary;
import com.starrocks.thrift.TAnalyticWindowBoundaryType;
import com.starrocks.thrift.TAnalyticWindowType;
import com.starrocks.thrift.TAssertion;
import com.starrocks.thrift.TExprOpcode;
import com.starrocks.thrift.TJoinOp;
import com.starrocks.thrift.TKeysType;
import com.starrocks.thrift.TVarType;

/**
 * Pure enum-to-Thrift conversion methods that have no dependency on AST {@code Expr}.
 * <p>
 * Planner nodes use this class for enum conversions.
 */
public final class ThriftEnumConverter {

    private ThriftEnumConverter() {
    }

    public static TJoinOp joinOperatorToThrift(JoinOperator joinOperator) {
        Preconditions.checkNotNull(joinOperator, "Join operator should not be null");
        switch (joinOperator) {
            case INNER_JOIN:
                return TJoinOp.INNER_JOIN;
            case LEFT_OUTER_JOIN:
                return TJoinOp.LEFT_OUTER_JOIN;
            case LEFT_SEMI_JOIN:
                return TJoinOp.LEFT_SEMI_JOIN;
            case LEFT_ANTI_JOIN:
                return TJoinOp.LEFT_ANTI_JOIN;
            case RIGHT_SEMI_JOIN:
                return TJoinOp.RIGHT_SEMI_JOIN;
            case RIGHT_ANTI_JOIN:
                return TJoinOp.RIGHT_ANTI_JOIN;
            case RIGHT_OUTER_JOIN:
                return TJoinOp.RIGHT_OUTER_JOIN;
            case FULL_OUTER_JOIN:
                return TJoinOp.FULL_OUTER_JOIN;
            case CROSS_JOIN:
                return TJoinOp.CROSS_JOIN;
            case NULL_AWARE_LEFT_ANTI_JOIN:
                return TJoinOp.NULL_AWARE_LEFT_ANTI_JOIN;
            case ASOF_INNER_JOIN:
                return TJoinOp.ASOF_INNER_JOIN;
            case ASOF_LEFT_OUTER_JOIN:
                return TJoinOp.ASOF_LEFT_OUTER_JOIN;
            default:
                throw new IllegalStateException("Unsupported join operator: " + joinOperator);
        }
    }

    public static TAssertion assertionToThrift(AssertNumRowsElement.Assertion assertion) {
        Preconditions.checkNotNull(assertion, "Assertion should not be null");
        return switch (assertion) {
            case EQ -> TAssertion.EQ;
            case NE -> TAssertion.NE;
            case LT -> TAssertion.LT;
            case LE -> TAssertion.LE;
            case GT -> TAssertion.GT;
            case GE -> TAssertion.GE;
        };
    }

    public static TExprOpcode compoundPredicateOperatorToThrift(CompoundPredicate.Operator operator) {
        Preconditions.checkNotNull(operator, "Compound predicate operator should not be null");
        switch (operator) {
            case AND:
                return TExprOpcode.COMPOUND_AND;
            case OR:
                return TExprOpcode.COMPOUND_OR;
            case NOT:
                return TExprOpcode.COMPOUND_NOT;
            default:
                throw new IllegalStateException("Unsupported compound predicate operator: " + operator);
        }
    }

    public static TAnalyticWindow analyticWindowToThrift(AnalyticWindow window) {
        Preconditions.checkNotNull(window, "Analytic window should not be null when converting to thrift");
        TAnalyticWindow result = new TAnalyticWindow(analyticWindowTypeToThrift(window.getType()));
        AnalyticWindowBoundary leftBoundary = window.getLeftBoundary();
        if (leftBoundary.getBoundaryType() != AnalyticWindowBoundary.BoundaryType.UNBOUNDED_PRECEDING) {
            result.setWindow_start(analyticWindowBoundaryToThrift(leftBoundary, window.getType()));
        }
        AnalyticWindowBoundary rightBoundary = window.getRightBoundary();
        Preconditions.checkNotNull(rightBoundary, "Right boundary must be set before converting to thrift");
        if (rightBoundary.getBoundaryType() != AnalyticWindowBoundary.BoundaryType.UNBOUNDED_FOLLOWING) {
            result.setWindow_end(analyticWindowBoundaryToThrift(rightBoundary, window.getType()));
        }
        return result;
    }

    public static TKeysType keysTypeToThrift(KeysType keysType) {
        Preconditions.checkNotNull(keysType, "Keys type should not be null");
        return switch (keysType) {
            case PRIMARY_KEYS -> TKeysType.PRIMARY_KEYS;
            case DUP_KEYS -> TKeysType.DUP_KEYS;
            case UNIQUE_KEYS -> TKeysType.UNIQUE_KEYS;
            case AGG_KEYS -> TKeysType.AGG_KEYS;
        };
    }

    public static TVarType setTypeToThrift(SetType setType) {
        Preconditions.checkNotNull(setType, "Set type should not be null");
        if (setType == SetType.GLOBAL) {
            return TVarType.GLOBAL;
        }
        if (setType == SetType.VERBOSE) {
            return TVarType.VERBOSE;
        }
        return TVarType.SESSION;
    }

    public static SetType setTypeFromThrift(TVarType thriftType) {
        if (thriftType == TVarType.GLOBAL) {
            return SetType.GLOBAL;
        }
        if (thriftType == TVarType.VERBOSE) {
            return SetType.VERBOSE;
        }
        return SetType.SESSION;
    }

    private static TAnalyticWindowBoundary analyticWindowBoundaryToThrift(AnalyticWindowBoundary boundary,
                                                                          AnalyticWindow.Type windowType) {
        TAnalyticWindowBoundary result = new TAnalyticWindowBoundary(
                analyticWindowBoundaryTypeToThrift(boundary.getBoundaryType()));
        if (boundary.getBoundaryType().isOffset() && windowType == AnalyticWindow.Type.ROWS) {
            Preconditions.checkNotNull(boundary.getOffsetValue(), "Offset value is required for ROWS window");
            result.setRows_offset_value(boundary.getOffsetValue().longValue());
        }
        // TODO: range windows need range_offset_predicate
        return result;
    }

    private static TAnalyticWindowBoundaryType analyticWindowBoundaryTypeToThrift(
            AnalyticWindowBoundary.BoundaryType boundaryType) {
        Preconditions.checkState(!boundaryType.isAbsolutePos());
        switch (boundaryType) {
            case CURRENT_ROW:
                return TAnalyticWindowBoundaryType.CURRENT_ROW;
            case PRECEDING:
                return TAnalyticWindowBoundaryType.PRECEDING;
            case FOLLOWING:
                return TAnalyticWindowBoundaryType.FOLLOWING;
            default:
                throw new IllegalStateException("Unsupported boundary type: " + boundaryType);
        }
    }

    private static TAnalyticWindowType analyticWindowTypeToThrift(AnalyticWindow.Type type) {
        return type == AnalyticWindow.Type.ROWS ? TAnalyticWindowType.ROWS : TAnalyticWindowType.RANGE;
    }
}
