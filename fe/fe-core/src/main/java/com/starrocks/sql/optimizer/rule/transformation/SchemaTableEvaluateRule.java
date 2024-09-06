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
import com.starrocks.catalog.Column;
import com.starrocks.catalog.Table;
import com.starrocks.catalog.system.SystemTable;
import com.starrocks.sql.optimizer.OptExpression;
import com.starrocks.sql.optimizer.OptimizerContext;
import com.starrocks.sql.optimizer.Utils;
import com.starrocks.sql.optimizer.operator.OperatorType;
import com.starrocks.sql.optimizer.operator.logical.LogicalSchemaScanOperator;
import com.starrocks.sql.optimizer.operator.logical.LogicalValuesOperator;
import com.starrocks.sql.optimizer.operator.pattern.Pattern;
import com.starrocks.sql.optimizer.operator.scalar.ColumnRefOperator;
import com.starrocks.sql.optimizer.operator.scalar.ScalarOperator;
import com.starrocks.sql.optimizer.rule.RuleType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Evaluate the schema-table query in optimizer
 * A regular schema-scan would send query plan to BE and retrieve metadata from FE with RPC, which can be pretty slow
 * in particular cases.
 * So in this rule we try to evaluate some simple queries in optimizer directly, to eliminate the overhead of RPC
 */
public class SchemaTableEvaluateRule extends TransformationRule {

    private static final Pattern PATTERN = Pattern.create(OperatorType.LOGICAL_SCHEMA_SCAN);
    private static final SchemaTableEvaluateRule INSTANCE = new SchemaTableEvaluateRule();

    protected SchemaTableEvaluateRule() {
        super(RuleType.TF_SCHEMA_TABLE_EVALUATE_RULE, PATTERN);
    }

    public static SchemaTableEvaluateRule getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean check(OptExpression input, OptimizerContext context) {
        if (!context.getSessionVariable().isEnableEvaluateSchemaScanRule()) {
            return false;
        }
        LogicalSchemaScanOperator operator = input.getOp().cast();
        if (!checkConjuncts(operator.getPredicate())) {
            return false;
        }
        if (!checkTable(operator.getTable())) {
            return false;
        }
        return true;
    }

    // Only support the pattern: c1=v1 AND c2=v2
    private static boolean checkConjuncts(ScalarOperator predicate) {
        List<ScalarOperator> conjuncts = Utils.extractConjuncts(predicate);
        return CollectionUtils.isNotEmpty(conjuncts) &&
                conjuncts.stream().allMatch(ScalarOperator::isColumnEqualConstant);
    }

    private static boolean checkTable(Table table) {
        if (!(table instanceof SystemTable)) {
            return false;
        }
        SystemTable systemTable = ((SystemTable) table);
        return systemTable.supportFeEvaluation();
    }

    @Override
    public List<OptExpression> transform(OptExpression input, OptimizerContext context) {
        LogicalSchemaScanOperator operator = input.getOp().cast();
        SystemTable systemTable = ((SystemTable) operator.getTable());

        // Evaluate result
        List<List<ScalarOperator>> values;
        try {
            values = systemTable.evaluate(operator.getPredicate());
        } catch (NotImplementedException e) {
            return Lists.newArrayList();
        }

        // Compute the column index
        List<Column> columns = systemTable.getColumns();
        Map<Column, Integer> columnIndex = IntStream.range(0, columns.size())
                .boxed()
                .collect(Collectors.toMap(columns::get, Function.identity()));

        // Compute the output index
        List<ColumnRefOperator> output = operator.getOutputColumns();
        List<Integer> outputIndex = output.stream()
                .map(ref -> columnIndex.get(operator.getColRefToColumnMetaMap().get(ref)))
                .collect(Collectors.toList());

        // Reorder the values according to output index
        List<List<ScalarOperator>> reorderedValues = Lists.newArrayList();
        for (List<ScalarOperator> row : values) {
            List<ScalarOperator> result = outputIndex.stream().map(row::get).collect(Collectors.toList());
            reorderedValues.add(result);
        }

        LogicalValuesOperator valuesOperator = new LogicalValuesOperator.Builder()
                .setRows(reorderedValues)
                .setColumnRefSet(operator.getOutputColumns())
                .build();
        return Lists.newArrayList(OptExpression.create(valuesOperator));
    }
}
