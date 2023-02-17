// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.
package com.starrocks.sql.optimizer.transformer;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.starrocks.analysis.AnalyticExpr;
import com.starrocks.analysis.AnalyticWindow;
import com.starrocks.analysis.CastExpr;
import com.starrocks.analysis.DecimalLiteral;
import com.starrocks.analysis.Expr;
import com.starrocks.analysis.FunctionCallExpr;
import com.starrocks.analysis.FunctionName;
import com.starrocks.analysis.IntLiteral;
import com.starrocks.analysis.NullLiteral;
import com.starrocks.analysis.OrderByElement;
import com.starrocks.catalog.Type;
import com.starrocks.common.AnalysisException;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.logical.LogicalWindowOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WindowTransformer {
    /**
     * If necessary, rewrites the analytic function, window, and/or order-by elements into
     * a standard format for the purpose of simpler backend execution, as follows:
     * 1. row_number():
     * Set a window from UNBOUNDED PRECEDING to CURRENT_ROW.
     * 2. lead()/lag():
     * Explicitly set the default arguments to for BE simplicity.
     * Set a window for lead(): UNBOUNDED PRECEDING to OFFSET FOLLOWING.
     * Set a window for lag(): UNBOUNDED PRECEDING to OFFSET PRECEDING.
     * 3. UNBOUNDED FOLLOWING windows:
     * Reverse the ordering and window if the start bound is not UNBOUNDED PRECEDING.
     * Flip first_value() and last_value().
     * 4. first_value():
     * Set the upper boundary to CURRENT_ROW if the lower boundary is
     * UNBOUNDED_PRECEDING.
     * 5. Explicitly set the default window if no window was given but there
     * are order-by elements.
     * 6. FIRST_VALUE without UNBOUNDED PRECEDING gets rewritten to use a different window
     * and change the function to return the last value. We either set the fn to be
     * 'last_value' or 'first_value_rewrite', which simply wraps the 'last_value'
     * implementation but allows us to handle the first rows in a partition in a special
     * way in the backend. There are a few cases:
     * a) Start bound is X FOLLOWING or CURRENT ROW (X=0):
     * Use 'last_value' with a window where both bounds are X FOLLOWING (or
     * CURRENT ROW). Setting the start bound to X following is necessary because the
     * X rows at the end of a partition have no rows in their window. Note that X
     * FOLLOWING could be rewritten as lead(X) but that would not work for CURRENT
     * ROW.
     * b) Start bound is X PRECEDING and end bound is CURRENT ROW or FOLLOWING:
     * Use 'first_value_rewrite' and a window with an end bound X PRECEDING. An
     * extra parameter '-1' is added to indicate to the backend that NULLs should
     * not be added for the first X rows.
     * c) Start bound is X PRECEDING and end bound is Y PRECEDING:
     * Use 'first_value_rewrite' and a window with an end bound X PRECEDING. The
     * first Y rows in a partition have empty windows and should be NULL. An extra
     * parameter with the integer constant Y is added to indicate to the backend
     * that NULLs should be added for the first Y rows.
     */
    public static WindowOperator standardize(AnalyticExpr analyticExpr) {
        FunctionCallExpr callExpr = analyticExpr.getFnCall();
        AnalyticWindow windowFrame = analyticExpr.getWindow();
        List<OrderByElement> orderByElements = analyticExpr.getOrderByElements();

        // Set a window from UNBOUNDED PRECEDING to CURRENT_ROW for row_number().
        if (AnalyticExpr.isRowNumberFn(callExpr.getFn())) {
            Preconditions.checkState(windowFrame == null, "Unexpected window set for row_numer()");
            windowFrame = AnalyticWindow.DEFAULT_ROWS_WINDOW;
        } else if (AnalyticExpr.isNtileFn(callExpr.getFn())) {
            Preconditions.checkState(windowFrame == null, "Unexpected window set for NTILE()");
            windowFrame = AnalyticWindow.DEFAULT_ROWS_WINDOW;

            try {
                callExpr.uncheckedCastChild(Type.BIGINT, 0);
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        } else if (AnalyticExpr.isOffsetFn(callExpr.getFn())) {
            try {
                Preconditions.checkState(windowFrame == null);

                if (callExpr.getChildren().size() == 1) {
                    callExpr.addChild(new IntLiteral("1", Type.BIGINT));
                    callExpr.addChild(new NullLiteral());
                } else if (callExpr.getChildren().size() == 2) {
                    callExpr.addChild(new NullLiteral());
                } else {
                    Preconditions.checkState(callExpr.getChildren().size() == 3);
                }

                Type firstType = callExpr.getChild(0).getType();
                // In old planner, the NullLiteral will cast to function arg type.
                // But in new planner, the NullLiteral type is still null.
                if (callExpr.getChild(0) instanceof NullLiteral) {
                    firstType = callExpr.getFn().getArgs()[0];
                }
                try {
                    callExpr.uncheckedCastChild(firstType, 2);
                } catch (AnalysisException e) {
                    throw new SemanticException("Convert type error in offset fn(default value); old_type="
                            + callExpr.getChild(2).getType() + " new_type=" + callExpr.getChild(0).getType());
                }
                if (callExpr.getChild(2) instanceof CastExpr) {
                    throw new SemanticException(
                            "The third parameter of `" + callExpr.getFn().getFunctionName().getFunction() +
                                    "` can't not convert to " + callExpr.getChildren().get(0).getType());
                }

                AnalyticExpr.checkDefaultValue(callExpr);
                // check the value whether out of range
                callExpr.uncheckedCastChild(Type.BIGINT, 1);

                AnalyticWindow.BoundaryType rightBoundaryType = AnalyticWindow.BoundaryType.FOLLOWING;
                if (callExpr.getFnName().getFunction().equalsIgnoreCase(AnalyticExpr.LAG)) {
                    rightBoundaryType = AnalyticWindow.BoundaryType.PRECEDING;
                }

                Expr rightBoundary;
                if (callExpr.getChild(1) != null) {
                    rightBoundary = callExpr.getChild(1);
                } else {
                    rightBoundary = new DecimalLiteral(BigDecimal.valueOf(1));
                }
                BigDecimal offsetValue = BigDecimal.valueOf(Expr.getConstFromExpr(rightBoundary));

                windowFrame = new AnalyticWindow(AnalyticWindow.Type.ROWS,
                        new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING, null),
                        new AnalyticWindow.Boundary(rightBoundaryType, rightBoundary, offsetValue));
            } catch (AnalysisException e) {
                throw new SemanticException(e.getMessage());
            }
        }

        // Reverse the ordering and window for windows ending with UNBOUNDED FOLLOWING,
        // and not starting with UNBOUNDED PRECEDING.
        if (windowFrame != null
                && windowFrame.getRightBoundary().getType() == AnalyticWindow.BoundaryType.UNBOUNDED_FOLLOWING
                && windowFrame.getLeftBoundary().getType() != AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING) {
            orderByElements = OrderByElement.reverse(orderByElements);
            windowFrame = windowFrame.reverse();

            // Also flip first_value()/last_value(). For other analytic functions there is no
            // need to also change the function.
            FunctionName reversedFnName = null;

            if (callExpr.getFnName().getFunction().equalsIgnoreCase(AnalyticExpr.FIRSTVALUE)) {
                reversedFnName = new FunctionName(AnalyticExpr.LASTVALUE);
            } else if (callExpr.getFnName().getFunction().equalsIgnoreCase(AnalyticExpr.LASTVALUE)) {
                reversedFnName = new FunctionName(AnalyticExpr.FIRSTVALUE);
            }

            if (reversedFnName != null) {
                callExpr = new FunctionCallExpr(reversedFnName, callExpr.getParams());
                callExpr.setIsAnalyticFnCall(true);
            }
        }

        if (windowFrame != null
                && windowFrame.getLeftBoundary().getType() == AnalyticWindow.BoundaryType.UNBOUNDED_PRECEDING
                && windowFrame.getRightBoundary().getType() != AnalyticWindow.BoundaryType.PRECEDING
                && callExpr.getFnName().getFunction().equalsIgnoreCase(AnalyticExpr.FIRSTVALUE)) {
            windowFrame.setRightBoundary(new AnalyticWindow.Boundary(AnalyticWindow.BoundaryType.CURRENT_ROW, null));
        }

        // Set the default window.
        if (!orderByElements.isEmpty() && windowFrame == null) {
            windowFrame = AnalyticWindow.DEFAULT_WINDOW;
        }

        // Change first_value/last_value RANGE windows to ROWS
        if ((callExpr.getFnName().getFunction().equalsIgnoreCase(AnalyticExpr.FIRSTVALUE)
                || callExpr.getFnName().getFunction().equalsIgnoreCase(AnalyticExpr.LASTVALUE))
                && windowFrame != null
                && windowFrame.getType() == AnalyticWindow.Type.RANGE) {
            windowFrame = new AnalyticWindow(AnalyticWindow.Type.ROWS, windowFrame.getLeftBoundary(),
                    windowFrame.getRightBoundary());
        }

        // remove duplicate partition expr
        Set<Expr> duplicateCheck = Sets.newHashSet();
        List<Expr> partitions = Lists.newArrayList();
        for (Expr partitionExpression : analyticExpr.getPartitionExprs()) {
            if (!duplicateCheck.contains(partitionExpression)) {
                duplicateCheck.add(partitionExpression);
                partitions.add(partitionExpression);
            }
        }
        analyticExpr.getPartitionExprs().clear();
        analyticExpr.getPartitionExprs().addAll(partitions);

        // remove duplicate sort expr
        duplicateCheck.clear();
        List<OrderByElement> orderings = Lists.newArrayList();
        for (OrderByElement orderByElement : analyticExpr.getOrderByElements()) {
            if (!duplicateCheck.contains(orderByElement.getExpr())) {
                duplicateCheck.add(orderByElement.getExpr());
                orderings.add(orderByElement);
            }
        }

        analyticExpr.getOrderByElements().clear();
        analyticExpr.getOrderByElements().addAll(orderings);
        return new WindowOperator(analyticExpr, analyticExpr.getPartitionExprs(),
                orderByElements, windowFrame);
    }

    /**
     * Reorder window function and build SortGroup
     * SortGroup represent the window functions that can be calculated in one SortNode
     * to reduce the generation of SortNode
     */
    public static List<WindowTransformer.PartitionGroup> reorderWindowOperator(
            List<WindowOperator> windowOperators, ColumnRefFactory columnRefFactory, OptExprBuilder subOpt) {
        /*
         * Step 1.
         * Generate a LogicalAnalyticOperator for each group of
         * window function with the same window frame, partition and order by
         */
        List<LogicalWindowOperator> logicalWindowOperators = new ArrayList<>();
        for (WindowTransformer.WindowOperator windowOperator : windowOperators) {
            Map<ColumnRefOperator, CallOperator> analyticCall = new HashMap<>();

            for (AnalyticExpr analyticExpr : windowOperator.getWindowFunctions()) {
                // The conversion here cannot only convert functionCall,
                // because it may conflict with the function of the same name
                // in the aggregation and be converted into the expression generated on agg
                // eg. select sum(v1), sum(v1) over(order by v2) from foo
                ScalarOperator agg =
                        SqlToScalarOperatorTranslator.translate(analyticExpr, subOpt.getExpressionMapping(),
                                columnRefFactory);
                ColumnRefOperator columnRefOperator =
                        columnRefFactory.create(agg.toString(), agg.getType(), agg.isNullable());
                analyticCall.put(columnRefOperator, (CallOperator) agg);
                subOpt.getExpressionMapping().put(analyticExpr, columnRefOperator);
            }

            List<ScalarOperator> partitions = new ArrayList<>();
            for (Expr partitionExpression : windowOperator.getPartitionExprs()) {
                ScalarOperator operator = SqlToScalarOperatorTranslator
                        .translate(partitionExpression, subOpt.getExpressionMapping(), columnRefFactory);
                partitions.add(operator);
            }

            List<Ordering> orderings = new ArrayList<>();
            for (OrderByElement orderByElement : windowOperator.getOrderByElements()) {
                ColumnRefOperator col =
                        (ColumnRefOperator) SqlToScalarOperatorTranslator
                                .translate(orderByElement.getExpr(), subOpt.getExpressionMapping(), columnRefFactory);
                orderings.add(new Ordering(col, orderByElement.getIsAsc(),
                        OrderByElement.nullsFirst(orderByElement.getNullsFirstParam())));
            }

            // Each LogicalWindowOperator will belong to a SortGroup,
            // so we need to record sortProperty to ensure that only one SortNode is enforced
            List<Ordering> sortEnforceProperty = new ArrayList<>();
            partitions.forEach(p -> sortEnforceProperty.add(new Ordering((ColumnRefOperator) p, true, true)));
            for (Ordering ordering : orderings) {
                if (sortEnforceProperty.stream().noneMatch(sp -> sp.getColumnRef().equals(ordering.getColumnRef()))) {
                    sortEnforceProperty.add(ordering);
                }
            }

            logicalWindowOperators.add(new LogicalWindowOperator.Builder()
                    .setWindowCall(analyticCall)
                    .setPartitionExpressions(partitions)
                    .setOrderByElements(orderings)
                    .setAnalyticWindow(windowOperator.getWindow())
                    .setEnforceSortColumns(sortEnforceProperty.stream().distinct().collect(Collectors.toList()))
                    .build());
        }

        /*
         * Step 2.
         * SortGroup represent the window functions that can be calculated in one SortNode
         * to reduce the generation of SortNode
         */
        List<WindowTransformer.SortGroup> sortedGroups = new ArrayList<>();
        for (LogicalWindowOperator windowOperator : logicalWindowOperators) {
            boolean find = false;
            for (WindowTransformer.SortGroup windowInSorted : sortedGroups) {
                if (!isPrefixHyperPartitionSet(windowOperator.getPartitionExpressions(),
                        windowInSorted.getPartitionExprs())
                        && !isPrefixHyperPartitionSet(windowInSorted.getPartitionExprs(),
                        windowOperator.getPartitionExpressions())) {
                    continue;
                }

                if (isPrefixHyperSortSet(windowOperator.getEnforceSortColumns(),
                        windowInSorted.getEnforceSortColumns())) {
                    windowInSorted.setEnforceSortColumns(windowOperator.getEnforceSortColumns());
                } else if (!isPrefixHyperSortSet(windowInSorted.getEnforceSortColumns(),
                        windowOperator.getEnforceSortColumns())) {
                    continue;
                }
                if (isPrefixHyperPartitionSet(windowOperator.getPartitionExpressions(),
                        windowInSorted.getPartitionExprs())) {
                    windowInSorted.setPartitionExpressions(windowOperator.getPartitionExpressions());
                }
                windowInSorted.addWindowOperator(windowOperator);
                find = true;
                break;
            }

            if (!find) {
                WindowTransformer.SortGroup sortGroup = new WindowTransformer.SortGroup(
                        windowOperator.getEnforceSortColumns(), windowOperator.getPartitionExpressions());
                sortGroup.addWindowOperator(windowOperator);
                sortedGroups.add(sortGroup);
            }
        }

        /*
         * Step 3.
         * Put the nodes with more partition columns at the top of the query plan
         * to ensure that the Enforce operation can meet the conditions, and only one ExchangeNode will be generated
         */
        sortedGroups.forEach(sortGroup -> sortGroup.getWindowOperators()
                .sort(Comparator.comparingInt(w -> w.getPartitionExpressions().size())));

        /*
         * Step4.
         * The nodes with the same partition group are placed together to reduce the generation of Exchange nodes.
         */
        List<PartitionGroup> partitionGroups = new ArrayList<>();
        for (SortGroup sortGroup : sortedGroups) {
            boolean find = false;
            for (PartitionGroup partitionGroup : partitionGroups) {
                if (isPrefixHyperPartitionSet(sortGroup.partitionExpressions, partitionGroup.partitionExpressions)) {
                    partitionGroup.addSortGroup(sortGroup);
                    find = true;
                    break;
                } else if (isPrefixHyperPartitionSet(partitionGroup.partitionExpressions,
                        sortGroup.partitionExpressions)) {
                    partitionGroup.setPartitionExpressions(sortGroup.partitionExpressions);
                    partitionGroup.addSortGroup(sortGroup);
                    find = true;
                    break;
                }
            }
            if (!find) {
                PartitionGroup partitionGroup = new PartitionGroup();
                partitionGroup.setPartitionExpressions(sortGroup.partitionExpressions);
                partitionGroup.addSortGroup(sortGroup);
                partitionGroups.add(partitionGroup);
            }
        }
        partitionGroups.forEach(partitionGroup -> partitionGroup.sortGroups.sort(
                Comparator.comparingInt(s -> s.getPartitionExprs().size())));
        partitionGroups.sort(Comparator.comparingInt(p -> p.partitionExpressions.size() * -1));

        return partitionGroups;
    }

    private static boolean isPrefixHyperPartitionSet(List<ScalarOperator> hyperSet, List<ScalarOperator> subSet) {
        if (hyperSet.isEmpty() && subSet.isEmpty()) {
            return true;
        }

        return hyperSet.containsAll(subSet) && !subSet.isEmpty();
    }

    private static boolean isPrefixHyperSortSet(List<Ordering> hyperSet, List<Ordering> subSet) {
        List<List<Ordering>> partitionPrefix = IntStream.rangeClosed(0, hyperSet.size())
                .mapToObj(i -> hyperSet.subList(0, i)).collect(Collectors.toList());
        return partitionPrefix.contains(subSet);
    }

    /**
     * SortGroup represent the window functions that can be calculated in one SortNode
     * to reduce the generation of SortNode
     * eg. select sum(v1) over(partition by v2 order by v3), avg(v1) over(partition by v2 order by v3, v1) from t0;
     * sort(v3, v1) satisfy the demand of sort(v3), so two window operator will place in the same sortGroup
     */
    public static class SortGroup {
        private final List<LogicalWindowOperator> windowOperators = new ArrayList<>();
        private List<Ordering> enforceSortColumns;
        private List<ScalarOperator> partitionExpressions;

        public SortGroup(List<Ordering> enforceSortColumns, List<ScalarOperator> partitionExpressions) {
            this.enforceSortColumns = enforceSortColumns;
            this.partitionExpressions = partitionExpressions;
        }

        public List<ScalarOperator> getPartitionExprs() {
            return partitionExpressions;
        }

        public void setPartitionExpressions(List<ScalarOperator> partitionExpressions) {
            this.partitionExpressions = partitionExpressions;
        }

        public List<Ordering> getEnforceSortColumns() {
            if (enforceSortColumns == null) {
                return new ArrayList<>();
            } else {
                return enforceSortColumns;
            }
        }

        public void setEnforceSortColumns(List<Ordering> enforceSortColumns) {
            this.enforceSortColumns = enforceSortColumns;
        }

        public void addWindowOperator(LogicalWindowOperator windowOperator) {
            windowOperators.add(windowOperator);
        }

        public List<LogicalWindowOperator> getWindowOperators() {
            return windowOperators;
        }
    }

    public static class PartitionGroup {
        private final List<SortGroup> sortGroups;
        private List<ScalarOperator> partitionExpressions;

        public PartitionGroup() {
            sortGroups = new ArrayList<>();
        }

        public void addSortGroup(SortGroup sortGroup) {
            sortGroups.add(sortGroup);
        }

        public List<SortGroup> getSortGroups() {
            return sortGroups;
        }

        public void setPartitionExpressions(
                List<ScalarOperator> partitionExpressions) {
            this.partitionExpressions = partitionExpressions;
        }
    }

    public static class WindowOperator {
        private final List<AnalyticExpr> windowFunctions = Lists.newArrayList();
        private final List<Expr> partitionExprs;
        private List<OrderByElement> orderByElements;
        private final AnalyticWindow window;

        public WindowOperator(AnalyticExpr analyticExpr, List<Expr> partitionExprs,
                              List<OrderByElement> orderByElements, AnalyticWindow window) {
            this.windowFunctions.add(analyticExpr);
            this.partitionExprs = partitionExprs;
            this.orderByElements = orderByElements;
            this.window = window;
        }

        public void addFunction(AnalyticExpr analyticExpr) {
            if (!windowFunctions.contains(analyticExpr)) {
                windowFunctions.add(analyticExpr);
            }
        }

        public List<AnalyticExpr> getWindowFunctions() {
            return windowFunctions;
        }

        public List<Expr> getPartitionExprs() {
            return partitionExprs;
        }

        public List<OrderByElement> getOrderByElements() {
            return orderByElements;
        }

        public void setOrderByElements(List<OrderByElement> orderByElements) {
            this.orderByElements = orderByElements;
        }

        public AnalyticWindow getWindow() {
            return window;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WindowOperator that = (WindowOperator) o;
            return Objects.equals(partitionExprs, that.partitionExprs) &&
                    Objects.equals(orderByElements, that.orderByElements) && Objects.equals(window, that.window);
        }

        @Override
        public int hashCode() {
            return Objects.hash(partitionExprs, orderByElements, window);
        }
    }
}

