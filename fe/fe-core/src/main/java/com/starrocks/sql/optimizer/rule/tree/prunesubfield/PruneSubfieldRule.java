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

package com.starrocks.sql.optimizer.rule.tree.prunesubfield;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.Expr;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.Function;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.catalog.PrimitiveType;
import com.starrocks.catalog.Type;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.CallOperator;
import com.starrocks.sql.optimizer.operator.scalar.CastOperator;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperatorVisitor;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;
import java.util.Map;

public class PruneSubfieldRule extends TransformationRule {
    public static final List<String> SUPPORT_JSON_FUNCTIONS = ImmutableList.<String>builder()
            // arguments: Json, path
            .add(FunctionSet.GET_JSON_BOOL)
            .add(FunctionSet.GET_JSON_INT)
            .add(FunctionSet.GET_JSON_DOUBLE)
            .add(FunctionSet.GET_JSON_STRING)
            .add(FunctionSet.GET_JSON_OBJECT)
            .add(FunctionSet.JSON_QUERY)
            .add(FunctionSet.JSON_EXISTS)
            .add(FunctionSet.JSON_LENGTH)
            .build();

    public static final List<String> PRUNE_FUNCTIONS = ImmutableList.<String>builder()
            .add(FunctionSet.MAP_KEYS, FunctionSet.MAP_SIZE)
            .add(FunctionSet.ARRAY_LENGTH)
            .add(FunctionSet.CARDINALITY)
            .addAll(SUPPORT_JSON_FUNCTIONS)
            .build();

    public static final List<String> PUSHDOWN_FUNCTIONS = ImmutableList.<String>builder()
            .addAll(PRUNE_FUNCTIONS)
            .add(FunctionSet.ARRAY_CONTAINS, FunctionSet.ARRAY_CONTAINS_ALL)
            .add(FunctionSet.ARRAY_MAX, FunctionSet.ARRAY_MIN, FunctionSet.ARRAY_SUM, FunctionSet.ARRAY_AVG)
            .add(FunctionSet.ARRAY_POSITION)
            .add(FunctionSet.ARRAY_JOIN)
            .build();

    public PruneSubfieldRule() {
        super(RuleType.TF_PRUNE_SUBFIELD, Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // normalize json cast expression
        LogicalProjectOperator project = input.getOp().cast();
        LogicalScanOperator scan = input.getInputs().get(0).getOp().cast();
        ScalarOperator predicate = scan.getPredicate();

        if (context.getSessionVariable().isCboPruneSubfield()) {
            NormalizeCastJsonExpr normalizer = new NormalizeCastJsonExpr();
            Map<ColumnRefOperator, ScalarOperator> projectMap = Maps.newHashMap();
            project.getColumnRefMap().forEach((k, v) -> projectMap.put(k, v.accept(normalizer, null)));
            project = new LogicalProjectOperator(projectMap);

            if (predicate != null) {
                predicate = predicate.accept(normalizer, null);
            }
        }

        // project expression
        SubfieldExpressionCollector collector =
                new SubfieldExpressionCollector(context.getSessionVariable().isCboPruneJsonSubfield());
        for (ScalarOperator value : project.getColumnRefMap().values()) {
            value.accept(collector, null);
        }

        // scan predicate
        if (predicate != null) {
            predicate.accept(collector, null);
        }

        // normalize access path
        SubfieldAccessPathNormalizer normalizer = new SubfieldAccessPathNormalizer();
        normalizer.collect(collector.getComplexExpressions());

        List<ColumnAccessPath> accessPaths = Lists.newArrayList();
        for (ColumnRefOperator ref : scan.getColRefToColumnMetaMap().keySet()) {
            if (!normalizer.hasPath(ref)) {
                continue;
            }
            String columnName = scan.getColRefToColumnMetaMap().get(ref).getName();
            ColumnAccessPath p = normalizer.normalizePath(ref, columnName);

            if (p.hasChildPath()) {
                accessPaths.add(p);
            }
        }

        if (accessPaths.isEmpty()) {
            return Lists.newArrayList(input);
        }

        LogicalScanOperator.Builder builder = OperatorBuilderFactory.build(scan);
        Operator newScan = builder.withOperator(scan)
                .setColumnAccessPaths(accessPaths)
                .setPredicate(predicate)
                .build();
        return Lists.newArrayList(OptExpression.create(project, OptExpression.create(newScan)));
    }

    // normalize cast(json as other_type) to cast(get_json_xxx(json, path) as other_type), to use
    // flat json path optimization json query
    // e.g. cast(k1['a'] as int) => get_json_int(k1, "a")
    //      cast(k1['a'] as string) => get_json_string(k1, "a")
    //      cast(k1['a'] as decimal) => cast(get_json_double(k1, "a") as decimal)
    private static class NormalizeCastJsonExpr extends ScalarOperatorVisitor<ScalarOperator, ScalarOperator> {
        private static final Map<PrimitiveType, Function> SUPPORT_GET_TYPE;

        private static final Map<PrimitiveType, Function> SUPPORT_CAST_TYPE;

        static {
            Function jsonInt = Expr.getBuiltinFunction(FunctionSet.GET_JSON_INT,
                    new Type[] {Type.JSON, Type.VARCHAR}, Function.CompareMode.IS_IDENTICAL);
            Function jsonDouble = Expr.getBuiltinFunction(FunctionSet.GET_JSON_DOUBLE,
                    new Type[] {Type.JSON, Type.VARCHAR}, Function.CompareMode.IS_IDENTICAL);
            Function jsonString = Expr.getBuiltinFunction(FunctionSet.GET_JSON_STRING,
                    new Type[] {Type.JSON, Type.VARCHAR}, Function.CompareMode.IS_IDENTICAL);

            SUPPORT_GET_TYPE = ImmutableMap.<PrimitiveType, Function>builder()
                    .put(PrimitiveType.BIGINT, jsonInt)
                    .put(PrimitiveType.DOUBLE, jsonDouble)
                    .put(PrimitiveType.VARCHAR, jsonString)
                    .put(PrimitiveType.CHAR, jsonString).build();

            SUPPORT_CAST_TYPE = ImmutableMap.<PrimitiveType, Function>builder()
                    .put(PrimitiveType.NULL_TYPE, jsonInt)
                    .put(PrimitiveType.TINYINT, jsonInt)
                    .put(PrimitiveType.SMALLINT, jsonInt)
                    .put(PrimitiveType.INT, jsonInt)
                    .put(PrimitiveType.FLOAT, jsonDouble)
                    .put(PrimitiveType.DECIMAL32, jsonDouble)
                    .put(PrimitiveType.DECIMAL64, jsonDouble)
                    .put(PrimitiveType.DECIMAL128, jsonDouble)
                    .build();
        }

        @Override
        public ScalarOperator visit(ScalarOperator scalarOperator, ScalarOperator context) {
            for (int i = 0; i < scalarOperator.getChildren().size(); i++) {
                scalarOperator.setChild(i, scalarOperator.getChild(i).accept(this, context));
            }
            return scalarOperator;
        }

        @Override
        public ScalarOperator visitCastOperator(CastOperator cast, ScalarOperator context) {
            visit(cast.getChild(0), context);
            if (cast.getChild(0).getType().isJsonType() && (cast.getChild(0) instanceof CallOperator)) {
                CallOperator childCall = cast.getChild(0).cast();
                if (!FunctionSet.JSON_QUERY.equalsIgnoreCase(childCall.getFnName()) ||
                        childCall.getChildren().size() != 2) {
                    return cast;
                }
                if (SUPPORT_GET_TYPE.containsKey(cast.getType().getPrimitiveType())) {
                    Function func = SUPPORT_GET_TYPE.get(cast.getType().getPrimitiveType());
                    return new CallOperator(func.functionName(), cast.getType(), childCall.getChildren(), func);
                } else if (SUPPORT_CAST_TYPE.containsKey(cast.getType().getPrimitiveType())) {
                    Function func = SUPPORT_CAST_TYPE.get(cast.getType().getPrimitiveType());
                    return new CastOperator(cast.getType(), new CallOperator(func.functionName(), func.getReturnType(),
                            childCall.getChildren(), func));
                }
            }
            return cast;
        }
    }
}
