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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.PaimonTable;
import com.starrocks.common.Config;
import com.starrocks.common.VectorIndexParams;
import com.starrocks.connector.paimon.PaimonVectorSearchOptions;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalPaimonScanOperator;
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

/**
 * Rewrite rule: TopN(ORDER BY approx_xxx_distance) + PaimonScan  =>  PaimonScan with single-round vector search.
 *
 * Vector index metadata is read from Paimon table properties:
 *   - vector.column      : the vector column name
 *   - vector.metric_type : L2_DISTANCE / COSINE_SIMILARITY
 *   - vector.dim         : dimension of vectors
 */
public class RewriteToPaimonVectorPlanRule extends TransformationRule {

    private static final String PROP_VECTOR_COLUMN = "vector.column";
    private static final String PROP_VECTOR_METRIC_TYPE = "vector.metric_type";
    private static final String PROP_VECTOR_DIM = "vector.dim";

    public RewriteToPaimonVectorPlanRule() {
        super(RuleType.TF_PAIMON_VECTOR_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_PAIMON_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!Config.enable_experimental_vector) {
            return false;
        }

        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalPaimonScanOperator scanOp = (LogicalPaimonScanOperator) input.getInputs().get(0).getOp();

        if (scanOp.getProjection() == null) {
            return false;
        }

        if (topNOp.getLimit() <= 0 || topNOp.getOrderByElements().size() != 1) {
            return false;
        }

        PaimonTable paimonTable = (PaimonTable) scanOp.getTable();
        Map<String, String> props = paimonTable.getProperties();
        return props.containsKey(PROP_VECTOR_COLUMN) && props.containsKey(PROP_VECTOR_METRIC_TYPE);
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalPaimonScanOperator scanOp = (LogicalPaimonScanOperator) input.getInputs().get(0).getOp();

        Optional<VectorSearchRuleUtils.VectorFuncInfo> optionalInfo = extractOrderByVectorFuncInfo(topNOp, scanOp);
        if (optionalInfo.isEmpty()) {
            return List.of();
        }
        VectorSearchRuleUtils.VectorFuncInfo info = optionalInfo.get();

        PaimonTable paimonTable = (PaimonTable) scanOp.getTable();
        Map<String, String> props = paimonTable.getProperties();

        String dimStr = props.get(PROP_VECTOR_DIM);
        if (dimStr != null) {
            int dim = Integer.parseInt(dimStr);
            if (info.vectorQuery.size() != dim) {
                throw new SemanticException(
                        String.format("The vector query size (%d) is not equal to the vector index dimension (%d)",
                                info.vectorQuery.size(), dim));
            }
        }

        ScalarOperator predicate = scanOp.getPredicate();
        if (predicate != null) {
            Optional<Double> rangeVal = VectorSearchRuleUtils.extractVectorRange(predicate, info);
            if (rangeVal.isEmpty()) {
                return List.of();
            }
            predicate = null;
        }

        PaimonVectorSearchOptions vecOpts = new PaimonVectorSearchOptions();
        vecOpts.setScoreFunctionName(info.vectorFuncCallOperator.getFnName());
        vecOpts.setVectorColumnName(info.inColumnRef.getName());

        List<Double> queryVectorDoubles = info.vectorQuery.stream()
                .map(Double::parseDouble)
                .collect(Collectors.toList());
        vecOpts.setQueryVector(queryVectorDoubles);
        vecOpts.setLimitGlobal(topNOp.getLimit());
        vecOpts.setLimitPerShard(topNOp.getLimit());
        vecOpts.setResultOrder(info.isAscending);

        return List.of(rewriteOptByDistanceColumn(topNOp, scanOp, context, predicate, info, vecOpts));
    }

    private OptExpression rewriteOptByDistanceColumn(LogicalTopNOperator topNOp,
                                                     LogicalPaimonScanOperator scanOp,
                                                     OptimizerContext context,
                                                     ScalarOperator newPredicate,
                                                     VectorSearchRuleUtils.VectorFuncInfo info,
                                                     PaimonVectorSearchOptions vecOpts) {
        String distanceColumnName = "__vector_" + info.outColumnRef.getName();
        Column distanceColumn = new Column(distanceColumnName, FloatType.FLOAT);
        scanOp.getTable().addColumn(distanceColumn);

        ColumnRefOperator distanceColRef =
                context.getColumnRefFactory().create(distanceColumnName, FloatType.FLOAT, false);

        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = new HashMap<>(scanOp.getColRefToColumnMetaMap());
        newColRefToColumnMetaMap.put(distanceColRef, distanceColumn);

        Map<ColumnRefOperator, ScalarOperator> newScanProjectMap =
                scanOp.getProjection().getColumnRefMap().entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> VectorSearchRuleUtils.rewriteScalarOperatorByDistanceColumn(
                                        entry.getValue(), info, distanceColRef)
                        ));

        LogicalPaimonScanOperator newScanOp = new LogicalPaimonScanOperator.Builder()
                .withOperator(scanOp)
                .setProjection(new Projection(newScanProjectMap))
                .setPredicate(newPredicate)
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setPaimonVectorSearchOptions(vecOpts)
                .build();

        return OptExpression.create(topNOp, OptExpression.create(newScanOp));
    }

    private Optional<VectorSearchRuleUtils.VectorFuncInfo> extractOrderByVectorFuncInfo(
            LogicalTopNOperator topNOp, LogicalPaimonScanOperator scanOp) {
        PaimonTable paimonTable = (PaimonTable) scanOp.getTable();
        Map<String, String> props = paimonTable.getProperties();

        String vectorColumn = props.get(PROP_VECTOR_COLUMN);
        String rawMetricType = props.get(PROP_VECTOR_METRIC_TYPE);
        VectorIndexParams.MetricsType metricType =
                Enums.getIfPresent(VectorIndexParams.MetricsType.class, StringUtils.upperCase(rawMetricType)).orNull();
        if (metricType == null) {
            return Optional.empty();
        }

        ColumnRefOperator outColRef = topNOp.getOrderByElements().get(0).getColumnRef();
        final boolean isAscending = topNOp.getOrderByElements().get(0).isAscending();

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
        if (column == null || !column.getName().equalsIgnoreCase(vectorColumn)) {
            return Optional.empty();
        }

        List<String> vectorQuery = new ArrayList<>();
        VectorSearchRuleUtils.extractValuesFromConstantArray(inCallOperator, vectorQuery);

        return Optional.of(new VectorSearchRuleUtils.VectorFuncInfo(
                colRefArgument, outColRef, inCallOperator, metricType, vectorQuery, isAscending));
    }
}
