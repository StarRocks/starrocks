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

package com.starrocks.sql.optimizer.rule.transformation;

import com.google.common.collect.Lists;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalUnionOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rewrite.scalar.FilterSelectivityEvaluator;
import com.starrocks.sql.optimizer.rewrite.scalar.FilterSelectivityEvaluator.ColumnFilter;
import com.starrocks.sql.optimizer.rewrite.scalar.NegateFilterShuttle;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalcUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.stream.Collectors;

import static com.starrocks.sql.optimizer.rewrite.scalar.FilterSelectivityEvaluator.NON_SELECTIVITY;

// For sql like:
//      select * from tbl where col1 > 100 and (col2 = 1 or col3 =2 or col4 = 4), the (col2 = 1 or col3 =2 or col4 = 4)
// predicate can actually filter lots of rows, but our index doesn't support this predicate. We can transform it to:
//      select * from tbl where  col1 > 100 and col2 = 1
//      union all
//      select * from tbl where col1 > 100 and col3 = 2 and (col2 !=1 or col2 is null)
//      union all
//      select * from tbl where col1 > 100 and col4 = 4 and (col2 !=1 or col2 is null) and (col3 != 2 or col3 is null)
// Every scanNode has an equivalent predicate can use index to filter lots of rows.
public class SplitScanORToUnionRule extends TransformationRule {
    private static final Logger LOG = LogManager.getLogger(SplitScanORToUnionRule.class);

    private static final double HIGH_SELECTIVITY = 1E-4;

    private SplitScanORToUnionRule() {
        // TODO support the external table
        super(RuleType.TF_SPLIT_SCAN_OR, Pattern.create(OperatorType.LOGICAL_OLAP_SCAN));
    }

    public static SplitScanORToUnionRule getInstance() {
        return INSTANCE;
    }

    private static final SplitScanORToUnionRule INSTANCE = new SplitScanORToUnionRule();

    @Override
    public boolean check(final OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.getOp();
        return context.getSessionVariable().getScanOrToUnionLimit() > 1 && scan.getPredicate() != null
                && !scan.isFromSplitOR();
    }



    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        try {
            return transformImpl(input, context);
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("input: {}, msg: {}", input.explain(), e.getMessage());
            }
            return Lists.newArrayList();
        }
    }

    private List<OptExpression> transformImpl(OptExpression input, OptimizerContext context) {
        LogicalOlapScanOperator scan = (LogicalOlapScanOperator) input.getOp();
        long totalRowCount = StatisticsCalcUtils.getTableRowCount(scan.getTable(), scan);
        Statistics.Builder builder = StatisticsCalcUtils.estimateScanColumns(scan.getTable(),
                scan.getColRefToColumnMetaMap());
        Statistics statistics = builder.setOutputRowCount(totalRowCount).build();

        if (statistics.getComputeSize() <= context.getSessionVariable().getScanOrToUnionThreshold()) {
            return Lists.newArrayList();
        }

        FilterSelectivityEvaluator selectivityEvaluator = new FilterSelectivityEvaluator(scan.getPredicate(),
                statistics, false);
        List<ColumnFilter> columnFilters = selectivityEvaluator.evaluate();

        // already has a predicate can use index and late materialized to filter a large part of rows
        if (columnFilters.get(0).getSelectRatio() < HIGH_SELECTIVITY) {
            return Lists.newArrayList();
        }

        List<ColumnFilter> unknownSelectivityFilters = columnFilters.stream().filter(e -> e.isUnknownSelectRatio())
                .collect(Collectors.toList());
        List<ColumnFilter> remainingFilters = columnFilters.stream().filter(e -> !e.isUnknownSelectRatio())
                .collect(Collectors.toList());

        Pair<List<ColumnFilter>, List<ColumnFilter>> pair = chooseRewriteColumnFilter(unknownSelectivityFilters,
                statistics, columnFilters.get(0).getSelectRatio());
        if (pair.first == null) {
            return Lists.newArrayList();
        }

        if (CollectionUtils.isNotEmpty(pair.second)) {
            remainingFilters.addAll(pair.second);
        }

        List<ColumnFilter> decomposeFilters = pair.first;

        List<ScalarOperator> newScanPredicates = rebuildScanPredicate(decomposeFilters, remainingFilters);

        return Lists.newArrayList(OptExpression.create(buildUnionAllOperator(scan, newScanPredicates.size()),
                buildUnionAllInputs(scan, newScanPredicates)));
    }

    private Pair<List<ColumnFilter>, List<ColumnFilter>> chooseRewriteColumnFilter(List<ColumnFilter> columnFilters,
                                                                                   Statistics statistics,
                                                                                   double existSelectRatio) {
        List<List<ColumnFilter>> decomposeFilters = Lists.newArrayList();
        for (ColumnFilter columnFilter : columnFilters) {
            ScalarOperator scalarOperator = columnFilter.getFilter();
            FilterSelectivityEvaluator selectivityEvaluator = new FilterSelectivityEvaluator(scalarOperator,
                    statistics, true);
            List<ColumnFilter> filters = selectivityEvaluator.evaluate();
            decomposeFilters.add(filters);
        }

        int idx = -1;
        double min  = NON_SELECTIVITY;

        int childrenOfUnion = ConnectContext.get().getSessionVariable().getScanOrToUnionLimit();

        // choose the columnFilter with minSelectRatio to rewrite
        for (int i = 0; i < decomposeFilters.size(); i++) {
            List<ColumnFilter> filters = decomposeFilters.get(i);
            if (filters.size() > childrenOfUnion) {
                continue;
            }
            double maxSelectRatio = filters.get(filters.size() - 1).getSelectRatio();
            if (maxSelectRatio < min) {
                min = maxSelectRatio;
                idx = i;
            }
        }

        if (idx != -1) {
            List<ColumnFilter> selectedFilters = decomposeFilters.get(idx);
            double maxSelectRatio = selectedFilters.get(selectedFilters.size() - 1).getSelectRatio();
            if (canBenefitFromSplit(existSelectRatio, maxSelectRatio)) {
                columnFilters.remove(idx);
                return Pair.create(selectedFilters, columnFilters);
            }
        }
        return Pair.create(null, columnFilters);
    }

    private List<ScalarOperator> rebuildScanPredicate(List<ColumnFilter> decomposeFilters,
                                                      List<ColumnFilter> remainingFilters) {
        ScalarOperator remainingPredicate = Utils.compoundAnd(remainingFilters.stream().map(ColumnFilter::getFilter)
                .collect(Collectors.toList()));
        List<ScalarOperator> scanPredicates = Lists.newArrayList();
        NegateFilterShuttle shuttle = NegateFilterShuttle.getInstance();
        for (int i = 0; i < decomposeFilters.size(); i++) {
            List<ScalarOperator> elements = Lists.newArrayList();
            elements.add(decomposeFilters.get(i).getFilter());
            List<ColumnFilter> subList = decomposeFilters.subList(0, i);
            for (ColumnFilter columnFilter : subList) {
                elements.add(shuttle.negateFilter(columnFilter.getFilter()));
            }
            elements.add(remainingPredicate);
            scanPredicates.add(Utils.compoundAnd(elements));
        }
        return scanPredicates;
    }

    private LogicalUnionOperator buildUnionAllOperator(LogicalOlapScanOperator scanOperator, int childNum) {
        List<ColumnRefOperator> outputColumns = scanOperator.getOutputColumns();
        List<List<ColumnRefOperator>> childOutputColumns = Lists.newArrayList();
        for (int i = 0; i < childNum; i++) {
            childOutputColumns.add(outputColumns);
        }
        return new LogicalUnionOperator(outputColumns, childOutputColumns, true);
    }

    private List<OptExpression> buildUnionAllInputs(LogicalOlapScanOperator scanOperator,
                                                    List<ScalarOperator> scanPredicates) {
        List<OptExpression> inputs = Lists.newArrayList();
        for (ScalarOperator scanPredicate : scanPredicates) {
            LogicalOlapScanOperator.Builder builder = new LogicalOlapScanOperator.Builder();
            LogicalOlapScanOperator scan = builder.withOperator(scanOperator)
                    .setPredicate(scanPredicate).setFromSplitOR(true).build();
            inputs.add(OptExpression.create(scan));
        }
        return inputs;
    }

    private boolean canBenefitFromSplit(double existSelectRatio, double splitMaxSelectRatio) {
        SessionVariable sessionVariable = ConnectContext.get().getSessionVariable();
        int childrenNumOfUnion = sessionVariable.getScanOrToUnionLimit();
        existSelectRatio = Math.min(existSelectRatio, sessionVariable.getSelectRatioThreshold());
        return splitMaxSelectRatio < existSelectRatio / childrenNumOfUnion;
    }
}
