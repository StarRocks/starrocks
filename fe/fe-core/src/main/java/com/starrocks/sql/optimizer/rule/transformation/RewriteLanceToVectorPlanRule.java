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

import com.starrocks.catalog.Column;
import com.starrocks.common.Config;
import com.starrocks.common.VectorSearchOptions;
import com.starrocks.qe.SessionVariable;
import com.starrocks.sql.analyzer.SemanticException;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.base.ColumnRefSet;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.Projection;
import com.starrocks.sql.optimizer.operator.logical.LogicalLanceScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalTopNOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ArrayOperator;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ConstantOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.type.ArrayType;
import com.starrocks.type.FloatType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.starrocks.catalog.FunctionSet.APPROX_COSINE_DISTANCE;
import static com.starrocks.catalog.FunctionSet.APPROX_INNER_PRODUCT;
import static com.starrocks.catalog.FunctionSet.APPROX_L2_DISTANCE;

public class RewriteLanceToVectorPlanRule extends TransformationRule {
    public static final String LANCE_VECTOR_COLUMN_PARAM = "lance.vector_column";
    public static final String LANCE_VECTOR_METRIC_PARAM = "lance.metric_type";
    private static final String LANCE_VECTOR_METRIC_L2 = "l2";
    private static final String LANCE_VECTOR_METRIC_COSINE = "cosine";
    private static final String LANCE_VECTOR_METRIC_DOT = "dot";

    public RewriteLanceToVectorPlanRule() {
        super(RuleType.TF_LANCE_VECTOR_REWRITE_RULE,
                Pattern.create(OperatorType.LOGICAL_TOPN)
                        .addChildren(Pattern.create(OperatorType.LOGICAL_LANCE_SCAN)));
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!Config.enable_experimental_vector) {
            return false;
        }

        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalLanceScanOperator scanOp = (LogicalLanceScanOperator) input.getInputs().get(0).getOp();
        return scanOp.getProjection() != null && topNOp.getLimit() > 0 && topNOp.getOrderByElements().size() == 1;
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalTopNOperator topNOp = (LogicalTopNOperator) input.getOp();
        LogicalLanceScanOperator scanOp = (LogicalLanceScanOperator) input.getInputs().get(0).getOp();

        Optional<VectorFuncInfo> optionalInfo = extractOrderByVectorFuncInfo(topNOp, scanOp);
        if (optionalInfo.isEmpty()) {
            return List.of();
        }
        if (scanOp.getPredicate() != null) {
            throw new SemanticException("Lance vector search does not support predicates in v1");
        }

        VectorFuncInfo info = optionalInfo.get();
        VectorSearchOptions opts = scanOp.getVectorSearchOptions().copy();
        opts.setEnableUseANN(true);
        opts.setRefineDistance(false);
        opts.setLimitK(resolveK(topNOp, context));
        opts.setResultOrder(info.isAscending);
        opts.setDistanceColumnName("__vector_" + info.outColumnRef.getName());
        opts.setQueryVector(info.vectorQuery);
        Map<String, String> queryParams = new HashMap<>(context.getSessionVariable().getAnnParams());
        queryParams.put(LANCE_VECTOR_COLUMN_PARAM, info.inColumnRef.getName());
        queryParams.put(LANCE_VECTOR_METRIC_PARAM, info.metricType);
        opts.setQueryParams(queryParams);

        return List.of(rewriteOptByDistanceColumn(topNOp, scanOp, context, info, opts));
    }

    private static long resolveK(LogicalTopNOperator topNOp, OptimizerContext context) {
        SessionVariable sessionVariable = context.getSessionVariable();
        int sessionVar = sessionVariable.getTopIndexLocalRows();
        if (sessionVar > 0) {
            return sessionVar;
        }
        long requested = topNOp.getLimit() + Math.max(topNOp.getOffset(), 0);
        long candidate = requested * sessionVariable.getTopIndexLocalRowsMultiplier();
        return Math.min(candidate, Integer.MAX_VALUE);
    }

    private OptExpression rewriteOptByDistanceColumn(LogicalTopNOperator topNOp,
                                                     LogicalLanceScanOperator scanOp,
                                                     OptimizerContext context,
                                                     VectorFuncInfo info,
                                                     VectorSearchOptions opts) {
        String distanceColumnName = opts.getDistanceColumnName();
        Column distanceColumn = new Column(distanceColumnName, FloatType.FLOAT, true);
        scanOp.getTable().addColumn(distanceColumn);

        ColumnRefOperator distanceColRef =
                context.getColumnRefFactory().create(distanceColumnName, FloatType.FLOAT, true);
        Map<ColumnRefOperator, Column> newColRefToColumnMetaMap = new HashMap<>(scanOp.getColRefToColumnMetaMap());
        newColRefToColumnMetaMap.put(distanceColRef, distanceColumn);

        Map<Column, ColumnRefOperator> newColumnMetaToColRefMap = new HashMap<>(scanOp.getColumnMetaToColRefMap());
        newColumnMetaToColRefMap.put(distanceColumn, distanceColRef);

        opts.setDistanceSlotId(distanceColRef.getId());

        Map<ColumnRefOperator, ScalarOperator> newScanProjectMap = scanOp.getProjection().getColumnRefMap().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> rewriteScalarOperatorByDistanceColumn(entry.getValue(), info, distanceColRef)
                ));

        ColumnRefOperator vectorColRef = info.inColumnRef;
        ColumnRefSet usedColumns = new ColumnRefSet();
        for (ScalarOperator value : newScanProjectMap.values()) {
            usedColumns.union(value.getUsedColumns());
        }
        if (!usedColumns.contains(vectorColRef.getId())) {
            Column vectorColMeta = newColRefToColumnMetaMap.remove(vectorColRef);
            if (vectorColMeta != null) {
                newColumnMetaToColRefMap.remove(vectorColMeta);
            }
        }

        LogicalLanceScanOperator newScanOp = new LogicalLanceScanOperator.Builder()
                .withOperator(scanOp)
                .setProjection(new Projection(newScanProjectMap))
                .setPredicate(null)
                .setColRefToColumnMetaMap(newColRefToColumnMetaMap)
                .setColumnMetaToColRefMap(newColumnMetaToColRefMap)
                .build();
        newScanOp.setVectorSearchOptions(opts);

        return OptExpression.create(topNOp, OptExpression.create(newScanOp));
    }

    private ScalarOperator rewriteScalarOperatorByDistanceColumn(ScalarOperator scalarOperator, VectorFuncInfo info,
                                                                 ColumnRefOperator distanceColRef) {
        if (scalarOperator.equals(info.vectorFuncCallOperator)) {
            return distanceColRef;
        }

        for (int i = 0; i < scalarOperator.getChildren().size(); i++) {
            ScalarOperator child = scalarOperator.getChild(i);
            scalarOperator.setChild(i, rewriteScalarOperatorByDistanceColumn(child, info, distanceColRef));
        }

        return scalarOperator;
    }

    private Optional<VectorFuncInfo> extractOrderByVectorFuncInfo(LogicalTopNOperator topNOp,
                                                                  LogicalLanceScanOperator scanOp) {
        ColumnRefOperator outColRef = topNOp.getOrderByElements().get(0).getColumnRef();
        boolean isAscending = topNOp.getOrderByElements().get(0).isAscending();

        ScalarOperator inOperator = scanOp.getProjection().getColumnRefMap().get(outColRef);
        if (!(inOperator instanceof CallOperator)) {
            return Optional.empty();
        }
        CallOperator inCallOperator = (CallOperator) inOperator;
        String fnName = inCallOperator.getFnName();
        boolean isL2Distance = fnName.equalsIgnoreCase(APPROX_L2_DISTANCE);
        boolean isCosineDistance = fnName.equalsIgnoreCase(APPROX_COSINE_DISTANCE);
        boolean isInnerProduct = fnName.equalsIgnoreCase(APPROX_INNER_PRODUCT);
        if (!(((isL2Distance || isCosineDistance) && isAscending) || (isInnerProduct && !isAscending))) {
            return Optional.empty();
        }
        String metricType;
        if (isL2Distance) {
            metricType = LANCE_VECTOR_METRIC_L2;
        } else if (isCosineDistance) {
            metricType = LANCE_VECTOR_METRIC_COSINE;
        } else {
            metricType = LANCE_VECTOR_METRIC_DOT;
        }

        ScalarOperator lhs = inCallOperator.getChild(0);
        ScalarOperator rhs = inCallOperator.getChild(1);
        ColumnRefOperator colRefArgument;
        ScalarOperator queryArgument;
        if (isConstantArrayFloat(lhs) && rhs.isColumnRef()) {
            colRefArgument = (ColumnRefOperator) rhs;
            queryArgument = lhs;
        } else if (isConstantArrayFloat(rhs) && lhs.isColumnRef()) {
            colRefArgument = (ColumnRefOperator) lhs;
            queryArgument = rhs;
        } else {
            return Optional.empty();
        }

        Column column = scanOp.getColRefToColumnMetaMap().get(colRefArgument);
        if (column == null || !column.getType().isArrayType()) {
            return Optional.empty();
        }
        ArrayType arrayType = (ArrayType) column.getType();
        if (!arrayType.getItemType().isFloatingPointType()) {
            return Optional.empty();
        }

        List<String> vectorQuery = new ArrayList<>();
        extractValuesFromConstantArray(queryArgument, vectorQuery);
        return Optional.of(new VectorFuncInfo(colRefArgument, outColRef, inCallOperator, vectorQuery, isAscending,
                metricType));
    }

    private static boolean isConstantArrayFloat(ScalarOperator scalarOperator) {
        if (!scalarOperator.isConstant()) {
            return false;
        }

        if (scalarOperator instanceof CastOperator) {
            if (!scalarOperator.getType().isArrayType()) {
                return false;
            }
            ArrayType arrayType = (ArrayType) scalarOperator.getType();
            if (!arrayType.getItemType().isFloatingPointType()) {
                return false;
            }
            if (isCastStringToArrayFloat(scalarOperator)) {
                return true;
            }
            return scalarOperator.getChildren().stream().allMatch(RewriteLanceToVectorPlanRule::isConstantArrayFloat);
        } else if (scalarOperator instanceof ArrayOperator) {
            if (!scalarOperator.getType().isArrayType()) {
                return false;
            }
            ArrayType innerArrayType = (ArrayType) scalarOperator.getType();
            return innerArrayType.getItemType().isNumericType();
        } else {
            return false;
        }
    }

    private static boolean isCastStringToArrayFloat(ScalarOperator op) {
        if (!(op instanceof CastOperator) || !op.getType().isArrayType()) {
            return false;
        }
        ArrayType arrayType = (ArrayType) op.getType();
        if (!arrayType.getItemType().isFloatingPointType() || op.getChildren().size() != 1) {
            return false;
        }
        ScalarOperator child = op.getChild(0);
        return child instanceof ConstantOperator && child.getType() != null && child.getType().isStringType();
    }

    private static void extractValuesFromConstantArray(ScalarOperator scalarOperator, List<String> vectorQuery) {
        if (isCastStringToArrayFloat(scalarOperator)) {
            ConstantOperator stringConst = (ConstantOperator) scalarOperator.getChild(0);
            parseStringAsFloatList(String.valueOf(stringConst.getValue()), vectorQuery);
            return;
        }

        if (scalarOperator instanceof ConstantOperator) {
            vectorQuery.add(String.valueOf(((ConstantOperator) scalarOperator).getValue()));
            return;
        }

        for (ScalarOperator child : scalarOperator.getChildren()) {
            extractValuesFromConstantArray(child, vectorQuery);
        }
    }

    private static void parseStringAsFloatList(String literal, List<String> out) {
        if (literal == null) {
            throw new SemanticException("Vector array literal cannot be null");
        }
        String trimmed = literal.trim();
        int open = trimmed.indexOf('[');
        int close = trimmed.lastIndexOf(']');
        if (open < 0 || close <= open || close != trimmed.length() - 1) {
            throw new SemanticException("Vector array literal must be enclosed in [..]: " + literal);
        }
        String body = trimmed.substring(open + 1, close);
        for (String token : body.split(",", -1)) {
            String value = token.trim();
            if (value.isEmpty()) {
                throw new SemanticException("Empty element in vector array literal: " + literal);
            }
            try {
                double parsed = Double.parseDouble(value);
                if (!Double.isFinite(parsed)) {
                    throw new SemanticException("Non-finite float in vector array literal: '" + value + "'");
                }
            } catch (NumberFormatException e) {
                throw new SemanticException("Invalid float in vector array literal: '" + value + "'");
            }
            out.add(value);
        }
    }

    private static class VectorFuncInfo {
        private final ColumnRefOperator inColumnRef;
        private final ColumnRefOperator outColumnRef;
        private final CallOperator vectorFuncCallOperator;
        private final List<String> vectorQuery;
        private final boolean isAscending;
        private final String metricType;

        private VectorFuncInfo(ColumnRefOperator inColumnRef, ColumnRefOperator outColumnRef,
                               CallOperator vectorFuncCallOperator, List<String> vectorQuery, boolean isAscending,
                               String metricType) {
            this.inColumnRef = inColumnRef;
            this.outColumnRef = outColumnRef;
            this.vectorFuncCallOperator = vectorFuncCallOperator;
            this.vectorQuery = vectorQuery;
            this.isAscending = isAscending;
            this.metricType = metricType;
        }
    }
}
