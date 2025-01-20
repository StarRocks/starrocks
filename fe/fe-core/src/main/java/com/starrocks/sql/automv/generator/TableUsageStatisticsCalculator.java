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

package com.starrocks.sql.automv.generator;

import com.google.common.base.Preconditions;
import com.starrocks.common.Pair;
import com.starrocks.qe.ConnectContext;
import com.starrocks.sql.automv.pieces.TablePiece;
import com.starrocks.sql.automv.pieces.TableUsage;
import com.starrocks.sql.automv.pn.Op;
import com.starrocks.sql.automv.pn.StrictOp;
import com.starrocks.sql.automv.qe.RboOptimizer;
import com.starrocks.sql.automv.util.TieredList;
import com.starrocks.sql.automv.util.Util;
import com.starrocks.sql.optimizer.ExpressionContext;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.statistics.PredicateStatisticsCalculator;
import com.starrocks.sql.optimizer.statistics.Statistics;
import com.starrocks.sql.optimizer.statistics.StatisticsCalculator;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class TableUsageStatisticsCalculator {
    public static void calculate(TableUsage tableUsage, ConnectContext ctx) {
        TieredList<Op> conjuncts = tableUsage.getConjunctFreq().entrySet()
                .stream()
                .flatMap(m -> m.getValue().keySet().stream().map(StrictOp::getOp))
                .collect(TieredList.<Op>toList());

        // Put all of the conjuncts into TablePiece to construct a LogicalPlan contains only one
        // LogicalScanOperator carrying conjuncts.
        TablePiece tablePiece = tableUsage.getTablePiece().setConjuncts(conjuncts).cast();

        QueryGenerateContext genCtx = QueryGenerateContext.of(false, true, false);
        String scanSql = QueryGenerator.generate(tablePiece, genCtx).getSubquery().getResult();
        RboOptimizer optimizer = RboOptimizer.getOptimizer(scanSql, ctx);
        OptExpression optExpression = optimizer.getPlan();
        List<OptExpression> scanOptExprList = Util.getOptExprStream(optExpression)
                .filter(optExpr -> optExpr.getOp() instanceof LogicalScanOperator)
                .collect(Collectors.toList());
        Preconditions.checkState(scanOptExprList.size() == 1);
        OptExpression scanOptExpr = scanOptExprList.get(0);

        // Evaluate each column statistics of LogicalScanOperator after its conjuncts removed
        Operator bareScanOp = OperatorBuilderFactory.build(scanOptExpr.getOp()).setPredicate(null).build();
        OptExpression bareScanOptExpr = OptExpression.builder().setOp(bareScanOp).build();
        ExpressionContext expressionContext = new ExpressionContext(bareScanOptExpr);
        StatisticsCalculator statCalculator =
                new StatisticsCalculator(expressionContext, optimizer.getOptimizerContext().getColumnRefFactory(),
                        optimizer.getOptimizerContext());

        statCalculator.estimatorStats();
        Statistics statistics = expressionContext.getStatistics();

        // Compute each predicate's selectivity
        Function<ScalarOperator, Double> computePredicateSelectivity = op -> {
            Statistics stat =
                    PredicateStatisticsCalculator.statisticsCalculate(op, Statistics.buildFrom(statistics).build());
            double outputRowCount = stat.getOutputRowCount();
            double inputRowCount = statistics.getOutputRowCount();
            if (Double.isFinite(outputRowCount) && Double.isFinite(inputRowCount)) {
                double selectivity = inputRowCount == 0 ? 0 : outputRowCount / inputRowCount;
                return Math.max(0.0, Math.min(1.0, selectivity));
            } else {
                return 1.0;
            }
        };

        Map<ColumnRefOperator, List<Double>> columnToSelectivityList =
                Optional.ofNullable(scanOptExpr.getOp().getPredicate())
                        .map(Utils::extractConjuncts)
                        .orElseGet(Collections::emptyList)
                        .stream()
                        .map(op -> Pair.create(op.getColumnRefs(), op))
                        .filter(p -> p.first.size() == 1)
                        .map(p -> Pair.create(p.first.get(0), computePredicateSelectivity.apply(p.second)))
                        .collect(Collectors.groupingBy(p -> p.first,
                                Collectors.mapping(p -> p.second, Collectors.toList())));

        // Summarize each column's selectivity, one column may have more than one predicates, so we
        // use the maximum predicate as the column's selectivity.
        LogicalScanOperator scanOp = bareScanOp.cast();
        Map<String, Double> columnToSelectivity = columnToSelectivityList.entrySet()
                .stream()
                .collect(Collectors.toMap(e -> scanOp.getColRefToColumnMetaMap().get(e.getKey()).getName(),
                        e -> e.getValue().stream().max(Double::compareTo).orElse(1.0)));

        Map<String, Double> columnToNdvRatio = statistics.getColumnStatistics().entrySet()
                .stream()
                .collect(Collectors.toMap(
                        e -> scanOp.getColRefToColumnMetaMap().get(e.getKey()).getName(),
                        e -> {
                            double ndv = e.getValue().getDistinctValuesCount();
                            double rowCount = statistics.getOutputRowCount();
                            if (Double.isFinite(ndv) && Double.isFinite(rowCount)) {
                                double ratio = rowCount == 0.0 ? 0.0 : ndv / rowCount;
                                return Math.max(Math.min(1.0, ratio), 0.0);
                            } else {
                                return 0.0;
                            }
                        }));

        // Finally, we get a selectivity map for TablePiece that maps columnId to selectivity.
        Map<Integer, Double> columnIdToSelectivity = tableUsage.getConjunctFreq().keySet()
                .stream().collect(Collectors.toMap(Function.identity(),
                        id -> columnToSelectivity.getOrDefault(tablePiece.getColumns().get(id).getColumnName(), 1.0)));

        Map<Integer, Double> columnIdToNdvRatio = tablePiece.getUsedColumns().getStream()
                .collect(Collectors.toMap(Function.identity(),
                        id -> columnToNdvRatio.getOrDefault(tablePiece.getColumns().get(id).getColumnName(), 1.0)));

        tableUsage.setColumnIdToSelectivity(columnIdToSelectivity);
        tableUsage.setColumnIdToNdvRatio(columnIdToNdvRatio);
        tableUsage.setRowCount(statistics.getOutputRowCount());
    }
}
