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
import com.starrocks.analysis.BinaryType;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.Type;
import com.starrocks.common.Config;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefFactory;
import com.starrocks.sql.optimizer.base.Ordering;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.BinaryPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.CompoundPredicateOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.analysis.BinaryType.GE;
import static com.starrocks.analysis.BinaryType.GT;
import static com.starrocks.analysis.BinaryType.LE;
import static com.starrocks.analysis.BinaryType.LT;
import static com.starrocks.catalog.FunctionSet.APPROX_COSINE_SIMILARITY;
import static com.starrocks.catalog.FunctionSet.APPROX_L2_DISTANCE;

public class RewriteToVectorPlanRule extends TransformationRule {

    public RewriteToVectorPlanRule() {
        super(RuleType.TF_VECTOR_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_OLAP_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOperator = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();

        scanOperator.setVectorSearchOptions(context.getVectorSearchOptions());
        VectorSearchOptions vectorSearchOptions = scanOperator.getVectorSearchOptions();

        if (!vectorSearchOptions.isEnableUseANN() || Config.enable_experimental_vector != true) {
            return false;
        }

        Map<ColumnRefOperator, ScalarOperator> columnRefMap = scanOperator.getProjection().getColumnRefMap();

        boolean isEnableUseANN = false;
        for (Map.Entry<ColumnRefOperator, ScalarOperator> entry : columnRefMap.entrySet()) {
            if (FunctionSet.VECTOR_COMPUTE_FUNCTIONS.contains(entry.getKey().getName())
                    && entry.getValue() instanceof CallOperator) {
                CallOperator callOperator = (CallOperator) entry.getValue();
                vectorSearchOptions.setQueryVector(collectVectorQuery(callOperator));
                isEnableUseANN = true;
                break;
            }
        }

        if (!isEnableUseANN) {
            vectorSearchOptions.setEnableUseANN(false);
            return false;
        }

        if (!topNOperator.getOrderByElements().isEmpty() &&
                FunctionSet.VECTOR_COMPUTE_FUNCTIONS.contains(
                        topNOperator.getOrderByElements().get(0).getColumnRef().getName())) {
            return topNOperator.getLimit() != Operator.DEFAULT_LIMIT &&
                    columnRefMap.entrySet().stream()
                    .filter(entry -> FunctionSet.VECTOR_COMPUTE_FUNCTIONS.contains(entry.getKey().getName()))
                    .anyMatch(entry -> entry.getValue() instanceof CallOperator);
        }
        return false;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOperator = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOperator = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();

        VectorSearchOptions options = scanOperator.getVectorSearchOptions();
        // set limit_K for ann searcher
        options.setVectorLimitK(topNOperator.getLimit());
        ScalarOperator predicate = scanOperator.getPredicate();
        Optional<ScalarOperator> newPredicate = Optional.empty();
        boolean isAscending = topNOperator.getOrderByElements().get(0).isAscending();
        if (predicate != null) {
            newPredicate = findAndSetVectorRange(predicate, isAscending, options);
            if (!options.isEnableUseANN()) {
                return Lists.newArrayList(input);
            }
        }
        options.setResultOrder(isAscending ? 0 : 1);
        String functionName = topNOperator.getOrderByElements().get(0).getColumnRef().getName();

        if (functionName.equalsIgnoreCase(APPROX_L2_DISTANCE) && !isAscending ||
                functionName.equalsIgnoreCase(APPROX_COSINE_SIMILARITY) && isAscending ||
                    !options.isEnableUseANN()) {
            options.setEnableUseANN(false);
            return Lists.newArrayList(input);
        }
        if (options.isUseIVFPQ()) {
            // Skip rewrite because IVFPQ is inaccurate and requires a brute force search after the ANN index search
            input.getInputs().get(0).getOp()
                    .setPredicate(newPredicate.isPresent() ? newPredicate.get() : null);
            return Lists.newArrayList(input);
        }

        Optional<OptExpression> result = buildVectorSortScanOperator(topNOperator,
                scanOperator, context, newPredicate, options);
        return result.isPresent() ? Lists.newArrayList(result.get()) : Lists.newArrayList(input);
    }

    public Optional<OptExpression> buildVectorSortScanOperator(LogicalTopNOperator topNOperator,
            LogicalOlapScanOperator scanOperator, OptimizerContext context,
            Optional<ScalarOperator> newPredicate, VectorSearchOptions vectorSearchOptions) {
        // bottom-up
        String distanceColumnName = scanOperator.getVectorSearchOptions().getVectorDistanceColumnName();
        Column distanceColumn = new Column(distanceColumnName, Type.FLOAT);
        scanOperator.getTable().addColumn(distanceColumn);

        ColumnRefFactory columnRefFactory = context.getColumnRefFactory();
        ColumnRefOperator distanceColumnRefOperator = columnRefFactory.create(distanceColumnName, Type.FLOAT, false);

        Map<ColumnRefOperator, Column> colRefToColumnMetaMap = new HashMap<>(scanOperator.getColRefToColumnMetaMap());
        colRefToColumnMetaMap.put(distanceColumnRefOperator, distanceColumn);

        Map<Column, ColumnRefOperator> columnMetaToColRefMap = new HashMap<>(scanOperator.getColumnMetaToColRefMap());
        columnMetaToColRefMap.put(distanceColumn, distanceColumnRefOperator);

        // new Scan operator
        LogicalOlapScanOperator newScanOperator = new LogicalOlapScanOperator(scanOperator.getTable(),
                colRefToColumnMetaMap, columnMetaToColRefMap, scanOperator.getDistributionSpec(),
                scanOperator.getLimit(), newPredicate.isPresent() ? newPredicate.get() : null,
                scanOperator.getSelectedIndexId(), scanOperator.getSelectedPartitionId(),
                scanOperator.getPartitionNames(), scanOperator.hasTableHints(),
                scanOperator.getSelectedTabletId(), scanOperator.getHintsTabletIds(),
                scanOperator.getHintsReplicaIds(), scanOperator.isUsePkIndex());

        newScanOperator.setVectorSearchOptions(vectorSearchOptions);
        Map<ColumnRefOperator, ScalarOperator> scanProjectMap = new HashMap<>();
        Map<ColumnRefOperator, ScalarOperator> topNProjectMap = new HashMap<>();
        // find original column and project it onto the topN
        Optional<ColumnRefOperator> originalColRef = scanOperator.getProjection().getColumnRefMap()
                .entrySet().stream().filter(entry -> FunctionSet.VECTOR_COMPUTE_FUNCTIONS
                        .contains(entry.getKey().getName())).map(entry -> entry.getKey())
                                .findFirst();
        if (originalColRef.isEmpty()) {
            return Optional.empty();
        }

        scanOperator.getProjection().getColumnRefMap().entrySet().stream()
                .forEach(entry -> {
                    if (FunctionSet.VECTOR_COMPUTE_FUNCTIONS.contains(entry.getKey().getName())
                            && entry.getValue() instanceof CallOperator) {
                        scanProjectMap.put(distanceColumnRefOperator, distanceColumnRefOperator);
                    } else {
                        scanProjectMap.put(entry.getKey(), entry.getValue());
                        topNProjectMap.put(entry.getKey(), entry.getValue());
                    }
                });
        newScanOperator.setProjection(new Projection(scanProjectMap));

        List<Ordering> orderByElements = topNOperator.getOrderByElements().stream().map(ordering ->
                FunctionSet.VECTOR_COMPUTE_FUNCTIONS.contains(ordering.getColumnRef().getName()) ?
                    new Ordering(distanceColumnRefOperator, ordering.isAscending(), ordering.isNullsFirst()) : ordering)
                .collect(Collectors.toList());

        boolean hasProjection = topNOperator.getProjection() != null;
        Map<ColumnRefOperator, ScalarOperator> newTopNProjectMap = new HashMap<>();
        if (hasProjection) {
            topNOperator.getProjection().getColumnRefMap().entrySet().stream()
                    .forEach(entry -> {
                        if (FunctionSet.VECTOR_COMPUTE_FUNCTIONS.contains(entry.getKey().getName())) {
                            newTopNProjectMap.put(originalColRef.get(), distanceColumnRefOperator);
                        } else {
                            newTopNProjectMap.put(entry.getKey(), entry.getValue());
                        }
                    });
        } else {
            topNProjectMap.put(originalColRef.get(), distanceColumnRefOperator);
        }

        // new TopN operator
        LogicalTopNOperator newTopNOperator = new LogicalTopNOperator(topNOperator.getLimit(),
                topNOperator.getPredicate(),
                hasProjection ? new Projection(newTopNProjectMap) : new Projection(topNProjectMap),
                topNOperator.getPartitionByColumns(), topNOperator.getPartitionLimit(), orderByElements,
                topNOperator.getOffset(), topNOperator.getSortPhase(), topNOperator.getTopNType(), topNOperator.isSplit());

        OptExpression topNExpression = OptExpression.create(newTopNOperator);
        topNExpression.getInputs().clear();
        topNExpression.getInputs().add(OptExpression.create(newScanOperator));

        return Optional.of(topNExpression);
    }

    public Optional<ScalarOperator> findAndSetVectorRange(ScalarOperator operator,
                boolean isAscending, VectorSearchOptions options) {
        if (!options.isEnableUseANN()) {
            return Optional.empty();
        }

        if (operator instanceof BinaryPredicateOperator && operator.getChild(1) instanceof ConstantOperator &&
                (isVectorCallOperator(operator.getChild(0)))) {
            BinaryType binaryType = ((BinaryPredicateOperator) operator).getBinaryType();
            if (((binaryType.equals(LE) || binaryType.equals(LT)) && !isAscending) ||
                    ((binaryType.equals(GE) || binaryType.equals(GT)) && isAscending)) {
                options.setEnableUseANN(false);
                return Optional.empty();
            }
            options.setVectorRange((double) (((ConstantOperator) operator.getChild(1)).getValue()));
            return Optional.empty();
        } else if (operator instanceof CompoundPredicateOperator) {
            List<ScalarOperator> newOperators = new ArrayList<>();
            for (ScalarOperator child : operator.getChildren()) {
                Optional<ScalarOperator> newChild = findAndSetVectorRange(child, isAscending, options);
                if (newChild.isPresent()) {
                    newOperators.add(newChild.get());
                }
            }
            if (newOperators.size() > 1) {
                return Optional.of(new CompoundPredicateOperator(((CompoundPredicateOperator) operator).getCompoundType(),
                        newOperators));
            } else if (newOperators.size() == 1) {
                return Optional.of(newOperators.get(0));
            } else {
                return Optional.empty();
            }
        } else {
            options.setEnableUseANN(false);
            return Optional.of(operator.clone());
        }
    }

    public boolean isVectorCallOperator(ScalarOperator scalarOperator) {
        if (scalarOperator instanceof CallOperator &&
                FunctionSet.VECTOR_COMPUTE_FUNCTIONS.contains(((CallOperator) scalarOperator).getFnName())) {
            return true;
        }
        if (scalarOperator.getChildren().size() == 0) {
            return false;
        }
        return isVectorCallOperator(scalarOperator.getChild(0));
    }

    public List<String> collectVectorQuery(CallOperator callOperator) {
        // suppose it's a standard vector query
        List<String> vectorQuery = new ArrayList<>();
        collectVector(callOperator, vectorQuery);
        return vectorQuery;
    }

    public void collectVector(ScalarOperator scalarOperator, List<String> vectorQuery) {
        if (scalarOperator instanceof ColumnRefOperator) {
            return;
        }

        if (scalarOperator instanceof ConstantOperator) {
            vectorQuery.add(String.valueOf(((ConstantOperator) scalarOperator).getValue()));
            return;
        }

        for (ScalarOperator child : scalarOperator.getChildren()) {
            collectVector(child, vectorQuery);
        }
    }
}
