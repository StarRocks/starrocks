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
import com.google.common.collect.Lists;
import com.starrocks.catalog.ColumnAccessPath;
import com.starrocks.catalog.FunctionSet;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.operator.Operator;
import com.starrocks.sql.optimizer.operator.OperatorBuilderFactory;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalProjectOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalScanOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import com.starrocks.sql.optimizer.rule.transformation.TransformationRule;

import java.util.List;

public class PruneSubfieldRule extends TransformationRule {
    public static final List<String> SUPPORT_JSON_FUNCTIONS = ImmutableList.<String>builder()
            // arguments: Json, path
            .add(FunctionSet.GET_JSON_INT)
            .add(FunctionSet.GET_JSON_DOUBLE)
            .add(FunctionSet.GET_JSON_STRING)
            .add(FunctionSet.GET_JSON_OBJECT)
            .add(FunctionSet.JSON_QUERY)
            .add(FunctionSet.JSON_EXISTS)
            .add(FunctionSet.JSON_LENGTH)
            .build();

    public static final List<String> SUPPORT_FUNCTIONS = ImmutableList.<String>builder()
            .add(FunctionSet.MAP_KEYS, FunctionSet.MAP_VALUES, FunctionSet.MAP_SIZE)
            .add(FunctionSet.ARRAY_LENGTH)
            .add(FunctionSet.CARDINALITY)
            .addAll(SUPPORT_JSON_FUNCTIONS)
            .build();

    public PruneSubfieldRule() {
        super(RuleType.TF_PRUNE_SUBFIELD, Pattern.create(OperatorType.LOGICAL_PROJECT, OperatorType.PATTERN_SCAN));
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        // project expression
        LogicalProjectOperator project = input.getOp().cast();
        SubfieldExpressionCollector collector =
                new SubfieldExpressionCollector(context.getSessionVariable().isCboPruneJsonSubfield());
        for (ScalarOperator value : project.getColumnRefMap().values()) {
            value.accept(collector, null);
        }

        // scan predicate
        LogicalScanOperator scan = input.getInputs().get(0).getOp().cast();
        if (scan.getPredicate() != null) {
            scan.getPredicate().accept(collector, null);
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
        Operator newScan = builder.withOperator(scan).setColumnAccessPaths(accessPaths).build();
        return Lists.newArrayList(OptExpression.create(project, OptExpression.create(newScan)));
    }
}
