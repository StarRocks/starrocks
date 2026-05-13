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

import com.google.common.base.Enums;
import com.google.common.base.Preconditions;
import com.starrocks.catalog.Column;
import com.starrocks.catalog.ColumnId;
import com.starrocks.catalog.Index;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.Config;
import com.starrocks.common.VectorIndexParams;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.ast.IndexDef;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalOlapScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.FloatType;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
        if (!Config.enable_experimental_vector) {
            return false;
        }

        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();

        if (scanOp.getProjection() == null) {
            return false;
        }

        if (topNOp.getLimit() <= 0 || topNOp.getOrderByElements().size() != 1) {
            return false;
        }

        return true;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalOlapScanOperator scanOp = (LogicalOlapScanOperator) input.getInputs().get(0).getOp();
        VectorSearchOptions opts = scanOp.getVectorSearchOptions();

        Optional<OlapVectorFuncInfo> optionalInfo = extractOrderByVectorFuncInfo(topNOp, scanOp);
        if (optionalInfo.isEmpty()) {
            return List.of();
        }
        OlapVectorFuncInfo info = optionalInfo.get();

        int dim =
                Integer.parseInt(info.index.getProperties().get(VectorIndexParams.CommonIndexParamKey.DIM.name().toLowerCase()));
        if (info.vectorQuery.size() != dim) {
            throw new SemanticException(
                    String.format("The vector query size (%s) is not equal to the vector index dimension (%d)",
                            info.vectorQuery, dim));
        }

        ScalarOperator predicate = scanOp.getPredicate();
        if (predicate != null) {
            Optional<Double> value = VectorSearchRuleUtils.extractVectorRange(predicate, info);
            if (value.isEmpty()) {
                return List.of();
            }
            predicate = null;
            opts.setPredicateRange(value.get());
        }

        opts.setEnableUseANN(true);
        String indexType = info.index.getProperties().get(VectorIndexParams.CommonIndexParamKey.INDEX_TYPE.name().toLowerCase());
        opts.setUseIVFPQ(VectorIndexParams.VectorIndexType.IVFPQ.name().equalsIgnoreCase(indexType));
        opts.setLimitK(topNOp.getLimit());
        opts.setResultOrder(info.isAscending);
        opts.setDistanceColumnName("__vector_" + info.outColumnRef.getName());
        opts.setQueryVector(info.vectorQuery);

        if (opts.isUseIVFPQ()) {
            LogicalOlapScanOperator newScanOp = LogicalOlapScanOperator.builder()
                    .withOperator(scanOp)
                    .setPredicate(predicate)
                    .build();
            return List.of(OptExpression.create(topNOp, OptExpression.create(newScanOp)));
        }

        return List.of(rewriteOptByDistanceColumn(topNOp, scanOp, context, predicate, info, opts));
    }

    private OptExpression rewriteOptByDistanceColumn(LogicalTopNOperator topNOp,
                                                     LogicalOlapScanOperator scanOp,
                                                     OptimizerContext context,
                                                     ScalarOperator newPredicate,
                                                     OlapVectorFuncInfo info,
                                                     VectorSearchOptions opts) {
        String distanceColumnName = scanOp.getVectorSearchOptions().getDistanceColumnName();
        Column distanceColumn = new Column(distanceColumnName, FloatType.FLOAT);
        scanOp.getTable().addColumn(distanceColumn);

        ColumnRefOperator distanceColRef = context.getColumnRefFactory().create(distanceColumnName, FloatType.FLOAT, false);
        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = new HashMap<>(scanOp.getColRefToColumnMetaMap());
        newColRefToColumnMetaMap.put(distanceColRef, distanceColumn);

        Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = new HashMap<>(scanOp.getColumnMetaToColRefMap());
        newColumnMetaToColRefMap.put(distanceColumn, distanceColRef);

        opts.setDistanceSlotId(distanceColRef.getId());

        Map<ColumnRefOperator, ScalarOperator> newScanProjectMap = scanOp.getProjection().getColumnRefMap().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> VectorSearchRuleUtils.rewriteScalarOperatorByDistanceColumn(
                                entry.getValue(), info, distanceColRef)
                ));

        LogicalOlapScanOperator newScanOp = LogicalOlapScanOperator.builder()
                .withOperator(scanOp)
                .setProjection(new Projection(newScanProjectMap))
                .setPredicate(newPredicate)
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setColumnMetaToColRefMap(newColumnMetaToColRefMap)
                .build();

        return OptExpression.create(topNOp, OptExpression.create(newScanOp));
    }

    private Optional<OlapVectorFuncInfo> extractOrderByVectorFuncInfo(LogicalTopNOperator topNOp,
                                                                      LogicalOlapScanOperator scanOp) {
        OlapTable table = (OlapTable) scanOp.getTable();
        Index index = table.getIndexes().stream()
                .filter(i -> i.getIndexType() == IndexDef.IndexType.VECTOR)
                .findFirst()
                .orElse(null);
        if (index == null) {
            return Optional.empty();
        }

        ColumnRefOperator outColRef = topNOp.getOrderByElements().get(0).getColumnRef();
        final boolean isAscending = topNOp.getOrderByElements().get(0).isAscending();

        String rawMetricType = index.getProperties().get(VectorIndexParams.CommonIndexParamKey.METRIC_TYPE.name().toLowerCase());
        VectorIndexParams.MetricsType metricType =
                Enums.getIfPresent(VectorIndexParams.MetricsType.class, StringUtils.upperCase(rawMetricType)).orNull();
        Preconditions.checkNotNull(metricType, "Invalid metric type [" + rawMetricType + "] for vector index");

        ScalarOperator inOperator = scanOp.getProjection().getColumnRefMap().get(outColRef);
        if (!(inOperator instanceof CallOperator)) {
            return Optional.empty();
        }
        CallOperator inCallOperator = (CallOperator) inOperator;

        boolean matchedFunc;
        switch (metricType) {
            case L2_DISTANCE:
                matchedFunc = inCallOperator.getFnName().equalsIgnoreCase(APPROX_L2_DISTANCE) && isAscending;
                break;
            case COSINE_SIMILARITY:
                matchedFunc = inCallOperator.getFnName().equalsIgnoreCase(APPROX_COSINE_SIMILARITY) && !isAscending;
                break;
            default:
                matchedFunc = false;
        }
        if (!matchedFunc) {
            return Optional.empty();
        }

        ScalarOperator lhs = inCallOperator.getChild(0);
        ScalarOperator rhs = inCallOperator.getChild(1);
        ColumnRefOperator colRefArgument;
        if (VectorSearchRuleUtils.isConstantArrayFloat(lhs) && rhs.isColumnRef()) {
            colRefArgument = (ColumnRefOperator) rhs;
        } else if (VectorSearchRuleUtils.isConstantArrayFloat(rhs) && lhs.isColumnRef()) {
            colRefArgument = (ColumnRefOperator) lhs;
        } else {
            return Optional.empty();
        }

        Column column = scanOp.getColRefToColumnMetaMap().get(colRefArgument);
        if (column == null) {
            return Optional.empty();
        }

        ColumnId indexColumnId = index.getColumns().get(0);
        if (!column.getColumnId().equals(indexColumnId)) {
            return Optional.empty();
        }

        List<String> vectorQuery = new ArrayList<>();
        VectorSearchRuleUtils.extractValuesFromConstantArray(inCallOperator, vectorQuery);

        return Optional.of(
                new OlapVectorFuncInfo(index, colRefArgument, outColRef, inCallOperator, metricType, vectorQuery, isAscending));
    }

    private static class OlapVectorFuncInfo extends VectorSearchRuleUtils.VectorFuncInfo {
        private final Index index;

        public OlapVectorFuncInfo(Index index, ColumnRefOperator inColumnRef, ColumnRefOperator outColumnRef,
                                  CallOperator vectorFuncCallOperator,
                                  VectorIndexParams.MetricsType metricType, List<String> vectorQuery, boolean isAscending) {
            super(inColumnRef, outColumnRef, vectorFuncCallOperator, metricType, vectorQuery, isAscending);
            this.index = index;
        }
    }
}
