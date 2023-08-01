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

package com.starrocks.sql.analyzer;

import com.google.common.base.Preconditions;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.LargeIntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.catalog.AggregateFunction;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;

import java.math.BigDecimal;

import static com.starrocks.catalog.FunctionSet.STATISTIC_FUNCTIONS;

public class AnalyticAnalyzer {
    public static void verifyAnalyticExpression(AnalyticExpr analyticExpr) {
        for (Expr e : analyticExpr.getPartitionExprs()) {
            if (e.isConstant()) {
                throw new SemanticException("Expressions in the PARTITION BY clause must not be constant: "
                        + e.toSql() + " (in " + analyticExpr.toSql() + ")");
            }
            if (!e.getType().canPartitionBy()) {
                throw new SemanticException(e.getType().toSql() + " type can't as partition by column");
            }
        }

        for (OrderByElement e : analyticExpr.getOrderByElements()) {
            if (e.getExpr().isConstant()) {
                throw new SemanticException("Expressions in the ORDER BY clause must not be constant: "
                        + e.getExpr().toSql() + " (in " + analyticExpr.toSql() + ")");
            }
            if (!e.getExpr().getType().canOrderBy()) {
                throw new SemanticException(e.getExpr().getType().toString() + " type can't as order by column");
            }
        }

        FunctionCallExpr analyticFunction = analyticExpr.getFnCall();
        if (analyticFunction.getParams().isDistinct()) {
            throw new SemanticException("DISTINCT not allowed in analytic function: " + analyticFunction.toSql());
        }

        if (!isAnalyticFn(analyticFunction.getFn())) {
            throw new SemanticException("Function '%s' not supported with OVER clause.",
                    analyticExpr.getFnCall().toSql());
        }

        for (Expr e : analyticExpr.getFnCall().getChildren()) {
            if (e.getType().isBitmapType() &&
                    !analyticFunction.getFn().functionName().equals(FunctionSet.BITMAP_UNION_COUNT) &&
                    !analyticFunction.getFn().functionName().equals(FunctionSet.LEAD) &&
                    !analyticFunction.getFn().functionName().equals(FunctionSet.LAG)) {
                throw new SemanticException("bitmap type could only used for bitmap_union_count/lead/lag window function");
            } else if (e.getType().isHllType() &&
                    !analyticFunction.getFn().functionName().equals(AnalyticExpr.HLL_UNION_AGG) &&
                    !analyticFunction.getFn().functionName().equals(FunctionSet.LEAD) &&
                    !analyticFunction.getFn().functionName().equals(FunctionSet.LAG)) {
                throw new SemanticException("hll type could only used for hll_union_agg/lead/lag window function");
            } else if (e.getType().isPercentile()) {
                throw new SemanticException("window functions don't support percentile type");
            }
        }

        if (isOffsetFn(analyticFunction.getFn()) && analyticFunction.getChildren().size() > 1) {
            Expr offset = analyticFunction.getChild(1);
            if (!isPositiveConstantInteger(offset)) {
                throw new SemanticException(
                        "The offset parameter of LEAD/LAG must be a constant positive integer: " +
                                analyticFunction.toSql());
            }

            // TODO: remove this check when the backend can handle non-constants
            if (analyticFunction.getChildren().size() == 2) {
                // do nothing
            } else if (analyticFunction.getChildren().size() == 3) {
                Type firstType = analyticFunction.getChild(0).getType();

                if (analyticFunction.getChild(0) instanceof NullLiteral) {
                    firstType = analyticFunction.getFn().getArgs()[0];
                }

                try {
                    analyticFunction.uncheckedCastChild(firstType, 2);
                } catch (AnalysisException e) {
                    throw new SemanticException("The third parameter of LEAD/LAG can't convert to " + firstType);
                }

                // When the parameter is const and nullable in lead/lag, BE use create_const_null_column to store it.
                // but the nullable info in FE is a more relax than BE (such as the nullable info in upper('a') is true,
                // but the actually derived column in BE is not nullableColumn)
                // which make the input colum in chunk not match the _agg_input_column in BE. so add this check in FE.
                if (!analyticFunction.getChild(2).isLiteral() && analyticFunction.getChild(2).isNullable()) {
                    throw new SemanticException("The type of the third parameter of LEAD/LAG not match the type " + firstType);
                }
            } else {
                throw new SemanticException("The number of parameter in LEAD/LAG is uncorrected");
            }
        }

        if (isNtileFn(analyticFunction.getFn())) {
            Expr numBuckets = analyticFunction.getChild(0);
            if (!isPositiveConstantInteger(numBuckets)) {
                throw new SemanticException(
                        "The num_buckets parameter of NTILE must be a constant positive integer: " +
                                analyticFunction.toSql());
            }
        }

        if (isStatisticFn(analyticFunction.getFn()) && (!analyticExpr.getOrderByElements().isEmpty())) {
            throw new SemanticException("order by not allowed with '" + analyticFunction.toSql() + "'",
                    analyticExpr.getPos());
        }

        if (analyticExpr.getWindow() != null) {
<<<<<<< HEAD
            if ((isRankingFn(analyticFunction.getFn()) || isOffsetFn(analyticFunction.getFn()) ||
                    isHllAggFn(analyticFunction.getFn()))) {
                throw new SemanticException("Windowing clause not allowed with '" + analyticFunction.toSql() + "'");
=======
            if ((isRankingFn(analyticFunction.getFn()) || isCumeFn(analyticFunction.getFn()) ||
                    isOffsetFn(analyticFunction.getFn()) || isHllAggFn(analyticFunction.getFn())) ||
                    isStatisticFn(analyticFunction.getFn())) {
                throw new SemanticException("Windowing clause not allowed with '" + analyticFunction.toSql() + "'",
                        analyticExpr.getPos());
>>>>>>> e8c924949a ([Feature] Add some statistic function (#27845))
            }

            verifyWindowFrame(analyticExpr);
        }
    }

    private static boolean isPositiveConstantInteger(Expr expr) {
        if (!expr.isConstant()) {
            return false;
        }

        double value = 0;
        if (expr instanceof IntLiteral) {
            IntLiteral intl = (IntLiteral) expr;
            value = intl.getDoubleValue();
        } else if (expr instanceof LargeIntLiteral) {
            LargeIntLiteral intl = (LargeIntLiteral) expr;
            value = intl.getDoubleValue();
        }

        return value > 0;
    }

    private static void verifyWindowFrame(AnalyticExpr analyticExpr) {
        if (analyticExpr.getOrderByElements().isEmpty()) {
            throw new SemanticException("Windowing clause requires ORDER BY clause: " + analyticExpr.toSql());
        }

        AnalyticWindow windowFrame = analyticExpr.getWindow();
        AnalyticWindow.Boundary leftBoundary = windowFrame.getLeftBoundary();
        Preconditions.checkArgument(leftBoundary != null);
        if (windowFrame.getRightBoundary() == null) {
            if (leftBoundary.getType() == AnalyticWindow.BoundaryType.FOLLOWING) {
                throw new SemanticException(leftBoundary.getType().toString() + " requires a BETWEEN clause");
            } else {
                windowFrame
                        .setRightBoundary(new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null));
            }
        }
        AnalyticWindow.Boundary rightBoundary = windowFrame.getRightBoundary();

        if (leftBoundary.getType() == AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING) {
            throw new SemanticException(
                    leftBoundary.getType().toString() + " is only allowed for upper bound of BETWEEN");
        }
        if (rightBoundary.getType() == AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING) {
            throw new SemanticException(
                    rightBoundary.getType().toString() + " is only allowed for lower bound of BETWEEN");
        }

        if (windowFrame.getType() == AnalyticWindow.Type.RANGE) {
            if (leftBoundary.getType().isOffset()) {
                checkRangeOffsetBoundaryExpr(analyticExpr, leftBoundary);
            }
            if (rightBoundary.getType().isOffset()) {
                checkRangeOffsetBoundaryExpr(analyticExpr, rightBoundary);
            }

            // TODO: Remove when RANGE windows with offset boundaries are supported.
            if (leftBoundary.getType().isOffset() || (rightBoundary.getType().isOffset()) ||
                    (leftBoundary.getType() == AnalyticWindow.BoundaryType.CURRENT_ROW
                            && rightBoundary.getType() == AnalyticWindow.BoundaryType.CURRENT_ROW)) {
                throw new SemanticException("RANGE is only supported with both the lower and upper bounds UNBOUNDED or"
                        + " one UNBOUNDED and the other CURRENT ROW.");
            }
        }

        if (leftBoundary.getType().isOffset()) {
            checkOffsetExpr(windowFrame, leftBoundary);
        }

        if (rightBoundary.getType().isOffset()) {
            checkOffsetExpr(windowFrame, rightBoundary);
        }

        if (leftBoundary.getType() == AnalyticWindow.BoundaryType.FOLLOWING) {
            if (rightBoundary.getType() == AnalyticWindow.BoundaryType.FOLLOWING) {
                checkOffsetBoundaries(leftBoundary, rightBoundary);
            } else if (rightBoundary.getType() != AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING) {
                throw new SemanticException(
                        "A lower window bound of " + AnalyticWindow.BoundaryType.FOLLOWING.toString()
                                + " requires that the upper bound also be " +
                                AnalyticWindow.BoundaryType.FOLLOWING.toString());
            }
        }

        if (rightBoundary.getType() == AnalyticWindow.BoundaryType.PRECEDING) {
            if (leftBoundary.getType() == AnalyticWindow.BoundaryType.PRECEDING) {
                checkOffsetBoundaries(rightBoundary, leftBoundary);
            } else if (leftBoundary.getType() != AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING) {
                throw new SemanticException(
                        "An upper window bound of " + AnalyticWindow.BoundaryType.PRECEDING.toString()
                                + " requires that the lower bound also be " +
                                AnalyticWindow.BoundaryType.PRECEDING.toString());
            }
        }
    }

    /**
     * Checks that the value expr of an offset boundary of a RANGE window is compatible
     * with orderingExprs (and that there's only a single ordering expr).
     */
    private static void checkRangeOffsetBoundaryExpr(AnalyticExpr analyticExpr, AnalyticWindow.Boundary boundary) {
        if (analyticExpr.getOrderByElements().size() > 1) {
            throw new SemanticException("Only one ORDER BY expression allowed if used with "
                    + "a RANGE window with PRECEDING/FOLLOWING: " + analyticExpr.toSql());
        }

        if (!Type.isImplicitlyCastable(boundary.getExpr().getType(),
                analyticExpr.getOrderByElements().get(0).getExpr().getType(), false)) {
            throw new SemanticException("The value expression of a PRECEDING/FOLLOWING clause of a RANGE window "
                    + "must be implicitly convertable to the ORDER BY expression's type: "
                    + boundary.getExpr().toSql() + " cannot be implicitly converted to "
                    + analyticExpr.getOrderByElements().get(0).getExpr().toSql());
        }
    }

    /**
     * Semantic analysis for expr of a PRECEDING/FOLLOWING clause.
     */
    private static void checkOffsetExpr(AnalyticWindow windowFrame, AnalyticWindow.Boundary boundary) {
        Preconditions.checkState(boundary.getType().isOffset());
        Expr e = boundary.getExpr();
        Preconditions.checkNotNull(e);
        boolean isPos = true;
        Double val = null;

        if (e.isConstant() && e.getType().isNumericType()) {
            try {
                val = Expr.getConstFromExpr(e);
                if (val <= 0) {
                    isPos = false;
                }
            } catch (AnalysisException exc) {
<<<<<<< HEAD
                throw new SemanticException("Couldn't evaluate PRECEDING/FOLLOWING expression: " + exc.getMessage());
=======
                throw new SemanticException("Couldn't evaluate PRECEDING/FOLLOWING expression: " + exc.getMessage(),
                        e.getPos());
>>>>>>> e8c924949a ([Feature] Add some statistic function (#27845))
            }
        }

        if (windowFrame.getType() == AnalyticWindow.Type.ROWS) {
            if (!e.isConstant() || !e.getType().isFixedPointType() || !isPos) {
                throw new SemanticException("For ROWS window, the value of a PRECEDING/FOLLOWING offset must be a "
                        + "constant positive integer: " + boundary.toSql());
            }

            Preconditions.checkNotNull(val);
            boundary.setOffsetValue(new BigDecimal(val.longValue()));
        } else {
            if (!e.isConstant() || !e.getType().isNumericType() || !isPos) {
                throw new SemanticException("For RANGE window, the value of a PRECEDING/FOLLOWING offset must be a "
                        + "constant positive number: " + boundary.toSql());
            }

            boundary.setOffsetValue(BigDecimal.valueOf(val));
        }
    }

    /**
     * Check that b1 <= b2.
     */
    private static void checkOffsetBoundaries(AnalyticWindow.Boundary b1, AnalyticWindow.Boundary b2) {
        Preconditions.checkState(b1.getType().isOffset());
        Preconditions.checkState(b2.getType().isOffset());
        Expr e1 = b1.getExpr();
        Preconditions.checkState(e1 != null && e1.isConstant() && e1.getType().isNumericType());
        Expr e2 = b2.getExpr();
        Preconditions.checkState(e2 != null && e2.isConstant() && e2.getType().isNumericType());

        try {
            double left = Expr.getConstFromExpr(e1);
            double right = Expr.getConstFromExpr(e2);

            if (left > right) {
                throw new SemanticException("Offset boundaries are in the wrong order");
            }
        } catch (AnalysisException exc) {
            throw new SemanticException("Couldn't evaluate PRECEDING/FOLLOWING expression: " + exc.getMessage());
        }
    }

    private static boolean isAnalyticFn(Function fn) {
        return fn instanceof AggregateFunction && ((AggregateFunction) fn).isAnalyticFn();
    }

    private static boolean isOffsetFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(AnalyticExpr.LEAD) ||
                fn.functionName().equalsIgnoreCase(AnalyticExpr.LAG);
    }

    private static boolean isMinMax(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(AnalyticExpr.MIN) ||
                fn.functionName().equalsIgnoreCase(AnalyticExpr.MAX);
    }

    private static boolean isRankingFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(AnalyticExpr.RANK)
                || fn.functionName().equalsIgnoreCase(AnalyticExpr.DENSERANK)
                || fn.functionName().equalsIgnoreCase(AnalyticExpr.ROWNUMBER)
                || fn.functionName().equalsIgnoreCase(AnalyticExpr.NTILE);
    }

    private static boolean isNtileFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(AnalyticExpr.NTILE);
    }

    private static boolean isStatisticFn(Function fn) {
        return STATISTIC_FUNCTIONS.contains(fn.functionName().toLowerCase());
    }

    private static boolean isHllAggFn(Function fn) {
        if (!isAnalyticFn(fn)) {
            return false;
        }

        return fn.functionName().equalsIgnoreCase(AnalyticExpr.HLL_UNION_AGG);
    }
}
